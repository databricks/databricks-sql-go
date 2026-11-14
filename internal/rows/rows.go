package rows

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlclient "github.com/databricks/databricks-sql-go/internal/client"
	context2 "github.com/databricks/databricks-sql-go/internal/compat/context"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerr_int "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/arrowbased"
	"github.com/databricks/databricks-sql-go/internal/rows/columnbased"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

// rows implements the following interfaces from database.sql.driver
// Rows
// RowsColumnTypeScanType
// RowsColumnTypeDatabaseTypeName
// RowsColumnTypeNullable
// RowsColumnTypeLength
type rows struct {
	// The RowScanner is responsible for handling the different
	// formats in which the query results can be returned
	rowscanner.RowScanner
	rowscanner.ResultPageIterator

	// Handle for the associated database operation.
	opHandle *cli_service.TOperationHandle

	client cli_service.TCLIService

	location *time.Location

	// Metadata for result set
	resultSetMetadata                 *cli_service.TGetResultSetMetadataResp
	schema                            *cli_service.TTableSchema
	pendingArrowPage                  *cli_service.TFetchResultsResp
	databaseTypeNameOverrides         map[int]string
	metadataMu                        sync.Mutex
	databaseTypeNameOverridesMetadata *cli_service.TGetResultSetMetadataResp

	config *config.Config

	// connId and correlationId are used for creating a context
	// when accessing the server and when logging
	connId        string
	correlationId string

	// Row number within the overall result set
	nextRowNumber int64

	logger_ *dbsqllog.DBSQLLogger

	// ctx is the context used for all server-side result RPCs (FetchResults,
	// GetResultSetMetadata, CloseOperation) and CloudFetch downloads. It is
	// detached from the caller's QueryContext cancellation so that a deadline
	// gating statement submission does not truncate result streaming, while
	// preserving context values used for auth/logging. It remains abortable via
	// Close() through resultsCancel.
	ctx context.Context
	// resultsCancel aborts in-flight result RPCs/downloads when Close() is
	// called, so the detached ctx never leaves an operation uncancellable.
	resultsCancel context.CancelFunc

	// Telemetry tracking
	// telemetryUpdate is called after each chunk is fetched with:
	//   chunkCount: total chunks fetched so far (including direct results)
	//   bytesDownloaded: cumulative bytes
	//   chunkIndex: 0-based index of the chunk just fetched
	//   chunkLatencyMs: fetch latency for this chunk (0 for direct results or CloudFetch pages)
	//   totalChunksPresent: server-reported total, 0 if unknown
	telemetryUpdate func(chunkCount int, bytesDownloaded int64, chunkIndex int, chunkLatencyMs int64, totalChunksPresent int32)
	// cloudFetchCallback is invoked per S3 file download for CloudFetch result sets.
	// It receives the individual file download duration so that telemetry can track
	// initial/slowest/sum download times matching JDBC's per-chunk HTTP GET timing.
	cloudFetchCallback func(downloadMs int64)
	closeCallback      func(latencyMs int64, chunkCount int, iterErr error, closeErr error)
	chunkCount         int
	bytesDownloaded    int64
	iterationErr       error // first error from Next()/fetchResultPage, passed to closeCallback

	resultFormatCallback func(format string)
}

var _ driver.Rows = (*rows)(nil)
var _ driver.RowsColumnTypeScanType = (*rows)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
var _ driver.RowsColumnTypeNullable = (*rows)(nil)
var _ driver.RowsColumnTypeLength = (*rows)(nil)
var _ dbsqlrows.Rows = (*rows)(nil)

// TelemetryCallbacks bundles the optional telemetry hooks passed into NewRows.
// Pass nil when telemetry is not active; individual fields may also be nil.
type TelemetryCallbacks struct {
	// OnChunkFetched is called after each result page fetch with chunk-level stats.
	OnChunkFetched func(chunkCount int, bytesDownloaded int64, chunkIndex int, chunkLatencyMs int64, totalChunksPresent int32)
	// OnClose is called from rows.Close() after all rows have been consumed.
	// iterErr is the first error from Next()/fetchResultPage (nil if iteration succeeded).
	// closeErr is the error from the CloseOperation RPC (nil if close succeeded).
	OnClose func(latencyMs int64, chunkCount int, iterErr error, closeErr error)
	// OnCloudFetchFile is called per S3 file download for CloudFetch result sets.
	OnCloudFetchFile func(downloadMs int64)
	// OnResultFormat is called from rows.Close(), before OnClose, with the
	// telemetry execution_result of the result-set metadata the rows already
	// hold. It is not called when that format is unknown.
	OnResultFormat func(format string)
}

// ExecutionResultFormat maps the result format declared by Thrift result-set
// metadata to the telemetry execution_result value, or "" when none is declared.
func ExecutionResultFormat(md *cli_service.TGetResultSetMetadataResp) string {
	if md == nil || !md.IsSetResultFormat() {
		return ""
	}
	switch md.GetResultFormat() {
	case cli_service.TSparkRowSetType_ARROW_BASED_SET:
		return "INLINE_ARROW"
	case cli_service.TSparkRowSetType_URL_BASED_SET:
		return "EXTERNAL_LINKS"
	case cli_service.TSparkRowSetType_COLUMN_BASED_SET:
		return "COLUMNAR_INLINE"
	default:
		return ""
	}
}

// ResultFormat returns the execution_result of the metadata the rows hold
// (from direct results or already fetched), or "". When the rows hold none, it
// falls back to the metadata carried by fetched result pages, which covers
// results consumed through GetArrowBatches/GetArrowIPCStreams. It never issues
// an RPC, and is valid on the rows NewRows returns alongside an error.
func (r *rows) ResultFormat() string {
	if r == nil {
		return ""
	}
	if format := ExecutionResultFormat(r.metadataSnapshot()); format != "" {
		return format
	}
	if r.ResultPageIterator != nil {
		return ExecutionResultFormat(r.ResultPageIterator.ResultSetMetadata())
	}
	return ""
}

func NewRows(
	ctx context.Context,
	opHandle *cli_service.TOperationHandle,
	client cli_service.TCLIService,
	config *config.Config,
	directResults *cli_service.TSparkDirectResults,
	callbacks *TelemetryCallbacks,
) (driver.Rows, dbsqlerr.DBError) {

	connId := driverctx.ConnIdFromContext(ctx)
	correlationId := driverctx.CorrelationIdFromContext(ctx)

	var logger *dbsqllog.DBSQLLogger
	if opHandle != nil {
		logger = dbsqllog.WithContext(connId, correlationId, dbsqlclient.SprintGuid(opHandle.OperationId.GUID))
		ctx = driverctx.NewContextWithQueryId(ctx, dbsqlclient.SprintGuid(opHandle.OperationId.GUID))
	} else {
		logger = dbsqllog.WithContext(connId, correlationId, "")
	}

	if client == nil {
		logger.Error().Msg(errRowsNoClient)
		return nil, dbsqlerr_int.NewDriverError(ctx, errRowsNoClient, nil)
	}

	var pageSize int64 = 10000
	location := time.UTC
	if config != nil {
		pageSize = int64(config.MaxRows)

		if config.Location != nil {
			location = config.Location
		}
	}

	logger.Debug().Msgf("databricks: creating Rows, pageSize: %d, location: %v", pageSize, location)

	// QueryContext may use a short deadline to gate statement submission and
	// status polling (see ES-1934053 / #295 / #371). Result handles can outlive
	// that phase, especially for paginated CloudFetch streams, so detach
	// server-side result RPCs from the caller's cancellation while preserving
	// context values used for auth/logging. The detached context is still wired
	// to a cancel func invoked from Close(), so the result handle remains
	// abortable (no uncancellable in-flight FetchResults or CloudFetch download).
	resultsCtx, resultsCancel := context.WithCancel(context2.WithoutCancel(ctx))

	r := &rows{
		client:          client,
		opHandle:        opHandle,
		connId:          connId,
		correlationId:   correlationId,
		location:        location,
		config:          config,
		logger_:         logger,
		ctx:             resultsCtx,
		resultsCancel:   resultsCancel,
		chunkCount:      0,
		bytesDownloaded: 0,
	}
	if callbacks != nil {
		r.telemetryUpdate = callbacks.OnChunkFetched
		r.cloudFetchCallback = callbacks.OnCloudFetchFile
		r.resultFormatCallback = callbacks.OnResultFormat
		r.closeCallback = callbacks.OnClose
	}

	// if we already have results for the query do some additional initialization
	if directResults != nil {
		logger.Debug().Msgf("databricks: creating Rows with direct results")
		// set the result set metadata
		if directResults.ResultSetMetadata != nil {
			r.resultSetMetadata = directResults.ResultSetMetadata
			r.schema = directResults.ResultSetMetadata.Schema
		}

		// initialize the row scanner
		err := r.makeRowScanner(directResults.ResultSet)
		if err != nil {
			return r, err
		}

		r.chunkCount++
		if directResults.ResultSet != nil && directResults.ResultSet.Results != nil && directResults.ResultSet.Results.ArrowBatches != nil {
			for _, batch := range directResults.ResultSet.Results.ArrowBatches {
				r.bytesDownloaded += int64(len(batch.Batch))
			}
		}

		if r.telemetryUpdate != nil {
			// Determine totalChunksPresent for direct results.
			// If the server already closed the operation, all data is here (totalPresent=1).
			// For CloudFetch direct results, use the number of result links.
			var totalPresent int32
			if directResults.CloseOperation != nil {
				totalPresent = int32(r.chunkCount) //nolint:gosec
			} else if directResults.ResultSet != nil && directResults.ResultSet.Results != nil &&
				directResults.ResultSet.Results.ResultLinks != nil {
				totalPresent = int32(len(directResults.ResultSet.Results.ResultLinks)) //nolint:gosec
			}
			// chunkIndex=0, chunkLatencyMs=0: direct results have no separate fetch latency.
			r.telemetryUpdate(r.chunkCount, r.bytesDownloaded, 0, 0, totalPresent)
		}
	}

	var d rowscanner.Delimiter
	if r.RowScanner != nil {
		d = rowscanner.NewDelimiter(r.RowScanner.Start(), r.RowScanner.Count())
	} else {
		d = rowscanner.NewDelimiter(0, 0)
	}

	// CloudFetch may deliver all links without closing the server operation.
	closedOnServer := directResults != nil && directResults.CloseOperation != nil
	var hasMoreRows *bool
	if directResults != nil && directResults.ResultSet != nil {
		hasMoreRows = directResults.ResultSet.HasMoreRows
	}
	r.ResultPageIterator = rowscanner.NewResultPageIterator(
		resultsCtx,
		d,
		pageSize,
		opHandle,
		hasMoreRows,
		closedOnServer,
		client,
		r.logger(),
	)

	return r, nil
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	err := isValidRows(r)
	if err != nil {
		return []string{}
	}

	schema, err := r.getResultSetSchema()
	if err != nil {
		return []string{}
	}

	tColumns := schema.GetColumns()
	colNames := make([]string, len(tColumns))

	for i := range tColumns {
		colNames[i] = tColumns[i].ColumnName
	}

	return colNames
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	if r == nil {
		return nil
	}

	// Release the detached results context after the close RPC runs, aborting
	// any in-flight FetchResults/CloudFetch downloads still referencing it.
	if r.resultsCancel != nil {
		defer r.resultsCancel()
	}

	if r.RowScanner != nil {
		// make sure the row scanner frees up any resources
		r.RowScanner.Close()
	}

	if r.ResultPageIterator != nil {
		r.logger().Debug().Msgf("databricks: closing Rows operation")
		closeStart := time.Now()
		err := r.ResultPageIterator.Close()
		if r.resultFormatCallback != nil {
			if format := r.ResultFormat(); format != "" {
				r.resultFormatCallback(format)
			}
		}
		if r.closeCallback != nil {
			r.closeCallback(time.Since(closeStart).Milliseconds(), r.chunkCount, r.iterationErr, err)
		}
		if err != nil {
			r.logger().Err(err).Msg(errRowsCloseFailed)
			return dbsqlerr_int.NewRequestError(r.ctx, errRowsCloseFailed, err)
		}
	}

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the number of columns.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	err := isValidRows(r)
	if err != nil {
		r.trackIterationErr(err)
		return err
	}

	// if the next row is not in the current result page
	// fetch the containing page
	var b bool
	var e error
	if b, e = r.isNextRowInPage(); !b && e == nil {
		err := r.fetchResultPage()
		if err != nil {
			r.trackIterationErr(err)
			return err
		}
	}

	if e != nil {
		r.trackIterationErr(e)
		return e
	}

	// Put values into the destination slice
	err = r.ScanRow(dest, r.nextRowNumber)
	if err != nil {
		r.trackIterationErr(err)
		return err
	}

	r.nextRowNumber++

	return nil
}

// ColumnTypeScanType returns column's native type
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	err := isValidRows(r)
	if err != nil {
		return nil
	}

	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return nil
	}

	scanType := getScanType(column)
	return scanType
}

// ColumnTypeDatabaseTypeName returns column's database type name
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	err := isValidRows(r)
	if err != nil {
		return ""
	}

	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return ""
	}

	if name, ok := r.cacheDatabaseTypeNameOverrides()[index]; ok {
		return name
	}

	dbtype := rowscanner.GetDBTypeName(column)

	return dbtype
}

// Thrift shares TIMESTAMP_TYPE between TIMESTAMP and TIMESTAMP_NTZ. Cache the
// distinction from server metadata so it survives result-page scanner changes.
// The override is only applied when the result set metadata indicates Arrow
// decoding: columnar (TColumn) results keep legacy session-location timestamp
// parsing, so they keep the legacy TIMESTAMP type name too.
//
// The decision is made from result set metadata, not the encoding of each page. A result set
// with an ArrowSchema, Arrow batches enabled, and ResultFormat unset is treated
// as Arrow. If a server sent such metadata but then returned TColumn pages, the
// column would still report TIMESTAMP_NTZ while its values went through the
// legacy column scanner.
//
// ColumnTypes may run concurrently. Publish a new map if a fetched page supplies
// an Arrow schema that was absent from the original metadata.
func (r *rows) cacheDatabaseTypeNameOverrides() map[int]string {
	r.metadataMu.Lock()
	defer r.metadataMu.Unlock()
	if r.databaseTypeNameOverrides == nil || r.databaseTypeNameOverridesMetadata != r.resultSetMetadata {
		r.databaseTypeNameOverrides = r.buildDatabaseTypeNameOverrides()
		r.databaseTypeNameOverridesMetadata = r.resultSetMetadata
	}
	return r.databaseTypeNameOverrides
}

func (r *rows) buildDatabaseTypeNameOverrides() map[int]string {
	overrides := make(map[int]string)
	if r.resultSetMetadata == nil || len(r.resultSetMetadata.ArrowSchema) == 0 {
		return overrides
	}
	if r.config != nil && !r.config.UseArrowBatches {
		return overrides
	}
	if r.resultSetMetadata.IsSetResultFormat() {
		switch r.resultSetMetadata.GetResultFormat() {
		case cli_service.TSparkRowSetType_ARROW_BASED_SET, cli_service.TSparkRowSetType_URL_BASED_SET:
		default:
			return overrides
		}
	}

	reader, err := ipc.NewReader(bytes.NewReader(r.resultSetMetadata.ArrowSchema))
	if err != nil {
		// Leave the cache empty until new metadata arrives. Columns fall back to the legacy
		// TIMESTAMP name; the Arrow row scanner surfaces the parse error itself.
		r.logger().Debug().Err(err).Msg("databricks: failed to parse arrow schema for column type name overrides; falling back to thrift type names")
		return overrides
	}
	defer reader.Release()
	for i, field := range reader.Schema().Fields() {
		// Same check the Arrow value scanner uses (metadata first, then
		// TimeZone), so the type name and the value handling stay in sync.
		if arrowscan.IsTimestampNTZField(field) {
			overrides[i] = "TIMESTAMP_NTZ"
		}
	}
	return overrides
}

// ColumnTypeNullable returns a flag indicating whether the column is nullable
// and an ok value of true if the status of the column is known. Otherwise
// a value of false is returned for ok.
func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, false
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	columnInfo, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		return 0, false
	}
	return columnTypeLengthForID(rowscanner.GetDBTypeID(columnInfo))
}

// columnTypeLengthForID is the variable-length classification behind
// ColumnTypeLength, factored out of the method (which needs a live *rows and a
// server round-trip to reach it) so the exact rule can be reused verbatim — in
// particular by the pure-Go coltype parity guard (TestColumnTypeInfoMatchesThriftMapping),
// which asserts the kernel path's arrowscan mapping against this same classifier
// rather than a hand-copied replica that could drift with it.
//
// Variable-length types (string/varchar/binary and the nested array/map/struct)
// report an unbounded length (math.MaxInt64); every fixed-width type reports
// (0, false).
func columnTypeLengthForID(id cli_service.TTypeId) (length int64, ok bool) {
	switch id {
	case cli_service.TTypeId_STRING_TYPE,
		cli_service.TTypeId_VARCHAR_TYPE,
		cli_service.TTypeId_BINARY_TYPE,
		cli_service.TTypeId_ARRAY_TYPE,
		cli_service.TTypeId_MAP_TYPE,
		cli_service.TTypeId_STRUCT_TYPE:
		return math.MaxInt64, true
	default:
		return 0, false
	}
}

var (
	scanTypeNull     = reflect.TypeOf(nil)
	scanTypeBoolean  = reflect.TypeOf(true)
	scanTypeFloat32  = reflect.TypeOf(float32(0))
	scanTypeFloat64  = reflect.TypeOf(float64(0))
	scanTypeInt8     = reflect.TypeOf(int8(0))
	scanTypeInt16    = reflect.TypeOf(int16(0))
	scanTypeInt32    = reflect.TypeOf(int32(0))
	scanTypeInt64    = reflect.TypeOf(int64(0))
	scanTypeString   = reflect.TypeOf("")
	scanTypeDateTime = reflect.TypeOf(time.Time{})
	scanTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown  = reflect.TypeOf(new(any))
)

func getScanType(column *cli_service.TColumnDesc) reflect.Type {

	// Currently all types are returned from the thrift server using
	// the primitive entry
	entry := column.TypeDesc.Types[0].PrimitiveEntry

	switch entry.Type {
	case cli_service.TTypeId_BOOLEAN_TYPE:
		return scanTypeBoolean
	case cli_service.TTypeId_TINYINT_TYPE:
		return scanTypeInt8
	case cli_service.TTypeId_SMALLINT_TYPE:
		return scanTypeInt16
	case cli_service.TTypeId_INT_TYPE:
		return scanTypeInt32
	case cli_service.TTypeId_BIGINT_TYPE:
		return scanTypeInt64
	case cli_service.TTypeId_FLOAT_TYPE:
		return scanTypeFloat32
	case cli_service.TTypeId_DOUBLE_TYPE:
		return scanTypeFloat64
	case cli_service.TTypeId_NULL_TYPE:
		return scanTypeNull
	case cli_service.TTypeId_STRING_TYPE:
		return scanTypeString
	case cli_service.TTypeId_CHAR_TYPE:
		return scanTypeString
	case cli_service.TTypeId_VARCHAR_TYPE:
		return scanTypeString
	case cli_service.TTypeId_DATE_TYPE, cli_service.TTypeId_TIMESTAMP_TYPE:
		return scanTypeDateTime
	case cli_service.TTypeId_DECIMAL_TYPE, cli_service.TTypeId_BINARY_TYPE, cli_service.TTypeId_ARRAY_TYPE,
		cli_service.TTypeId_STRUCT_TYPE, cli_service.TTypeId_MAP_TYPE, cli_service.TTypeId_UNION_TYPE:
		return scanTypeRawBytes
	case cli_service.TTypeId_USER_DEFINED_TYPE:
		return scanTypeUnknown
	case cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE, cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE:
		return scanTypeString
	default:
		return scanTypeUnknown
	}
}

// isValidRows checks that the row instance is not nil
// and that it has a client
func isValidRows(r *rows) dbsqlerr.DBError {
	var err dbsqlerr.DBError
	if r == nil {
		err = dbsqlerr_int.NewDriverError(context.Background(), errRowsNilRows, nil)
	} else if r.client == nil {
		err = dbsqlerr_int.NewDriverError(r.ctx, errRowsNoClient, nil)
		r.logger().Err(err).Msg(errRowsNoClient)
	}

	return err
}

func (r *rows) getColumnMetadataByIndex(index int) (*cli_service.TColumnDesc, dbsqlerr.DBError) {
	err := isValidRows(r)
	if err != nil {
		return nil, err
	}

	schema, err := r.getResultSetSchema()
	if err != nil {
		return nil, err
	}

	columns := schema.GetColumns()
	if index < 0 || index >= len(columns) {
		err = dbsqlerr_int.NewDriverError(r.ctx, errRowsInvalidColumnIndex(index), nil)
		r.logger().Err(err).Msg(err.Error())
		return nil, err
	}

	return columns[index], nil
}

// isNextRowInPage returns a boolean flag indicating whether
// the next result set row is in the current result set page
func (r *rows) isNextRowInPage() (bool, dbsqlerr.DBError) {
	if r == nil || r.RowScanner == nil {
		return false, nil
	}

	return r.RowScanner.Contains(r.nextRowNumber), nil
}

// getResultMetadata does a one time fetch of the result set schema
func (r *rows) getResultSetSchema() (*cli_service.TTableSchema, dbsqlerr.DBError) {
	r.metadataMu.Lock()
	schema := r.schema
	r.metadataMu.Unlock()
	if schema == nil {
		err := isValidRows(r)
		if err != nil {
			return nil, err
		}

		req := cli_service.TGetResultSetMetadataReq{
			OperationHandle: r.opHandle,
		}

		resp, err2 := r.client.GetResultSetMetadata(r.ctx, &req)
		if err2 != nil {
			r.logger().Err(err2).Msg(err2.Error())
			// A fetch aborted via the results context (e.g. Close) isn't a
			// result-set failure; leave it untagged. Mirrors the CloudFetch path.
			if r.ctx != nil && r.ctx.Err() != nil {
				return nil, dbsqlerr_int.NewRequestError(r.ctx, errRowsMetadataFetchFailed, err2)
			}
			return nil, dbsqlerr_int.NewRequestError(r.ctx, errRowsMetadataFetchFailed, err2).WithCategory(dbsqlerr_int.CategoryResultSet)
		}

		r.metadataMu.Lock()
		if r.schema == nil {
			r.resultSetMetadata, r.schema = resp, resp.Schema
		}
		schema = r.schema
		r.metadataMu.Unlock()

	}

	return schema, nil
}

// fetchResultPage will fetch the result page containing the next row, if necessary
func (r *rows) fetchResultPage() error {
	err := isValidRows(r)
	if err != nil {
		return err
	}

	if r.RowScanner != nil && r.RowScanner.Contains(r.nextRowNumber) {
		return nil
	}

	if r.RowScanner != nil && r.nextRowNumber < r.RowScanner.Start() {
		return dbsqlerr_int.NewDriverError(r.ctx, errRowsOnlyForward, nil)
	}

	// Close/release the existing row scanner before loading the next result page to
	// help keep memory usage down.
	if r.RowScanner != nil {
		r.RowScanner.Close()
		r.RowScanner = nil
	}

	// Record 0-based chunk index before fetching (direct results occupied index 0 if present).
	chunkIndex := r.chunkCount
	fetchStart := time.Now()
	fetchResult := r.pendingArrowPage
	var err1 error
	if fetchResult == nil {
		fetchResult, err1 = r.ResultPageIterator.Next()
	}
	chunkLatencyMs := time.Since(fetchStart).Milliseconds()
	if err1 != nil {
		return err1
	}

	r.chunkCount++
	if fetchResult != nil && fetchResult.Results != nil {
		if fetchResult.Results.ArrowBatches != nil {
			for _, batch := range fetchResult.Results.ArrowBatches {
				r.bytesDownloaded += int64(len(batch.Batch))
			}
		}
	}

	// For CloudFetch, the FetchResults RPC only returns presigned S3 URLs — the actual data
	// transfer happens later via S3 HTTP GETs timed by cloudFetchCallback. Report 0 latency
	// here so the Thrift round-trip is not misreported as chunk download time.
	var totalPresent int32
	isCloudFetch := false
	if fetchResult != nil && fetchResult.Results != nil && fetchResult.Results.ResultLinks != nil {
		totalPresent = int32(len(fetchResult.Results.ResultLinks)) //nolint:gosec
		isCloudFetch = true
	}

	effectiveLatencyMs := chunkLatencyMs
	if isCloudFetch {
		effectiveLatencyMs = 0
	}

	if r.telemetryUpdate != nil {
		r.telemetryUpdate(r.chunkCount, r.bytesDownloaded, chunkIndex, effectiveLatencyMs, totalPresent)
	}

	err1 = r.makeRowScanner(fetchResult)
	if err1 != nil {
		return err1
	}
	r.pendingArrowPage = nil

	// We should be iterating over the rows so the next row number should be in the
	// next result page
	if !r.RowScanner.Contains(r.nextRowNumber) {
		return dbsqlerr_int.NewDriverError(r.ctx, errInvalidRowNumberState, nil)
	}

	return nil
}

// makeRowScanner creates the embedded RowScanner instance based on the format
// of the returned query results
func (r *rows) makeRowScanner(fetchResults *cli_service.TFetchResultsResp) dbsqlerr.DBError {

	if fetchResults != nil {
		// Trust fetched metadata; the server may already have closed the operation.
		r.useServerArrowSchema(fetchResults.ResultSetMetadata)
	}
	schema, err1 := r.getResultSetSchema()
	if err1 != nil {
		return err1
	}

	if fetchResults == nil {
		return nil
	}

	var rs rowscanner.RowScanner
	var err dbsqlerr.DBError
	if fetchResults.Results != nil {

		if fetchResults.Results.Columns != nil {
			rs, err = columnbased.NewColumnRowScanner(schema, fetchResults.Results, r.config, r.logger(), r.ctx)
		} else if fetchResults.Results.ArrowBatches != nil {
			rs, err = arrowbased.NewArrowRowScanner(r.metadataSnapshot(), fetchResults.Results, r.config, r.logger(), r.ctx, nil)
		} else if fetchResults.Results.ResultLinks != nil {
			rs, err = arrowbased.NewArrowRowScanner(r.metadataSnapshot(), fetchResults.Results, r.config, r.logger(), r.ctx, r.cloudFetchCallback)
		} else {
			r.logger().Error().Msg(errRowsUnknowRowType)
			err = dbsqlerr_int.NewDriverError(r.ctx, errRowsUnknowRowType, nil)
		}

	} else {
		r.logger().Error().Msg(errRowsUnknowRowType)
		err = dbsqlerr_int.NewDriverError(r.ctx, errRowsUnknowRowType, nil)
	}

	if r.RowScanner != nil {
		r.RowScanner.Close()
	}

	r.RowScanner = rs

	return err
}

// trackIterationErr records the first non-EOF error from Next()/fetchResultPage
// so that closeCallback can report it as the statement's error.
func (r *rows) trackIterationErr(err error) {
	if r != nil && r.iterationErr == nil && err != nil && err != io.EOF {
		r.iterationErr = err
	}
}

func (r *rows) logger() *dbsqllog.DBSQLLogger {
	if r.logger_ == nil {
		if r.opHandle != nil {
			r.logger_ = dbsqllog.WithContext(r.connId, r.correlationId, dbsqlclient.SprintGuid(r.opHandle.OperationId.GUID))
		} else {
			r.logger_ = dbsqllog.WithContext(r.connId, r.correlationId, "")
		}
	}
	return r.logger_
}

func (r *rows) GetArrowBatches(ctx context.Context) (dbsqlrows.ArrowBatchIterator, error) {
	// Result fetching must outlive the caller's QueryContext deadline: both the
	// inter-page FetchResults RPCs (via r.ResultPageIterator) AND the CloudFetch
	// S3 downloads created from the iterator context. Build the iterator from the
	// detached results context (abortable via Close) rather than the caller ctx,
	// so passing a deadline-bound ctx here cannot truncate the stream. Driver
	// values for logging/auth are already carried by r.ctx; re-apply the ids
	// defensively. See ES-1934053 / #371.
	iterCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(r.ctx, r.connId), r.correlationId)

	// If a row scanner exists we use it to create the iterator, that way the iterator includes
	// data returned as direct results
	if r.RowScanner != nil {
		return r.RowScanner.GetArrowBatches(iterCtx, *r.config, r.ResultPageIterator)
	}

	return arrowbased.DeferArrowBatches(func() (dbsqlrows.ArrowBatchIterator, error) {
		schemaBytes, err := r.getArrowSchemaBytes(iterCtx)
		if err != nil {
			return nil, err
		}
		if r.RowScanner != nil {
			return r.RowScanner.GetArrowBatches(iterCtx, *r.config, r.ResultPageIterator)
		}
		if len(schemaBytes) == 0 {
			var schemaErr error
			schemaBytes, schemaErr = arrowbased.GetArrowSchemaBytes(iterCtx, r.metadataSnapshot(), r.config.ArrowConfig)
			return arrowbased.NewArrowRecordIterator(iterCtx, r.ResultPageIterator, nil, schemaBytes, *r.config, schemaErr), nil
		}
		return arrowbased.NewArrowRecordIterator(iterCtx, r.ResultPageIterator, nil, schemaBytes, *r.config), nil
	}), nil
}

func (r *rows) GetArrowIPCStreams(ctx context.Context) (dbsqlrows.ArrowIPCStreamIterator, error) {
	// See GetArrowBatches: result fetching is detached from the caller ctx so a
	// submit-gating deadline cannot truncate streaming; it stays abortable via Close.
	iterCtx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(r.ctx, r.connId), r.correlationId)

	// If a row scanner exists we use it to create the iterator, that way the iterator includes
	// data returned as direct results
	if r.RowScanner != nil {
		return r.RowScanner.GetArrowIPCStreams(iterCtx, *r.config, r.ResultPageIterator)
	}

	return arrowbased.DeferArrowIPCStreams(func() (dbsqlrows.ArrowIPCStreamIterator, error) {
		schemaBytes, err := r.getArrowSchemaBytes(iterCtx)
		if err != nil {
			return nil, err
		}
		if r.RowScanner != nil {
			return r.RowScanner.GetArrowIPCStreams(iterCtx, *r.config, r.ResultPageIterator)
		}
		if len(schemaBytes) == 0 {
			var schemaErr error
			schemaBytes, schemaErr = arrowbased.GetArrowSchemaBytes(iterCtx, r.metadataSnapshot(), r.config.ArrowConfig)
			return arrowbased.NewArrowIPCStreamIterator(iterCtx, r.ResultPageIterator, nil, schemaBytes, *r.config, schemaErr), nil
		}
		return arrowbased.NewArrowIPCStreamIterator(iterCtx, r.ResultPageIterator, nil, schemaBytes, *r.config), nil
	}), nil
}

func (r *rows) getArrowSchemaBytes(ctx context.Context) ([]byte, error) {
	metadata := r.metadataSnapshot()
	if metadata == nil || len(metadata.ArrowSchema) == 0 {
		if r.pendingArrowPage != nil || r.ResultPageIterator.HasNext() {
			r.useServerArrowSchema(r.ResultPageIterator.ResultSetMetadata())
			metadata = r.metadataSnapshot()
			if metadata != nil && len(metadata.ArrowSchema) > 0 {
				return metadata.ArrowSchema, nil
			}
			page := r.pendingArrowPage
			var err error
			if page == nil {
				page, err = r.ResultPageIterator.Next()
			}
			if err != nil {
				return nil, err
			}
			r.pendingArrowPage = page
			r.useServerArrowSchema(page.ResultSetMetadata)
			if err := r.makeRowScanner(page); err != nil {
				return nil, err
			}
			r.pendingArrowPage = nil
			return nil, nil
		}
		if err := r.ResultPageIterator.Err(); err != nil {
			return nil, err
		}
		r.useServerArrowSchema(r.ResultPageIterator.ResultSetMetadata())
	}
	if _, err := r.getResultSetSchema(); err != nil {
		return nil, err
	}
	metadata = r.metadataSnapshot()
	if len(metadata.ArrowSchema) == 0 {
		return nil, nil
	}
	return metadata.ArrowSchema, nil
}

func (r *rows) metadataSnapshot() *cli_service.TGetResultSetMetadataResp {
	r.metadataMu.Lock()
	defer r.metadataMu.Unlock()
	return r.resultSetMetadata
}

func (r *rows) useServerArrowSchema(metadata *cli_service.TGetResultSetMetadataResp) {
	r.metadataMu.Lock()
	defer r.metadataMu.Unlock()
	if metadata == nil || (r.resultSetMetadata != nil && len(r.resultSetMetadata.ArrowSchema) > 0) {
		return
	}
	if r.resultSetMetadata == nil {
		r.resultSetMetadata = metadata
		r.schema = metadata.Schema
		return
	}
	if len(metadata.ArrowSchema) == 0 {
		return
	}
	merged := *r.resultSetMetadata
	merged.ArrowSchema = metadata.ArrowSchema
	r.resultSetMetadata = &merged
}
