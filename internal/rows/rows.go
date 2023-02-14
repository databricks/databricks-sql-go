package rows

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"math"
	"reflect"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	// "github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	dbsqlclient "github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/arrowbased"
	"github.com/databricks/databricks-sql-go/internal/rows/columnbased"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
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

	// Handle for the associated database operation.
	opHandle client.Handle

	client client.DatabricksClient

	// Maximum number of rows to return from a single fetch operation
	maxPageSize int64

	location *time.Location

	// Metadata for result set
	// resultSetMetadata *cli_service.TGetResultSetMetadataResp
	schema            *client.ResultSchema

	hasMoreRows bool

	config *config.Config

	// connId and correlationId are used for creating a context
	// when accessing the server and when logging
	connId        string
	correlationId string

	// Index in the current page of rows
	nextRowIndex int64

	// Row number within the overall result set
	nextRowNumber int64

	// starting row number of the current results page
	pageStartingRowNum int64

	// If the server returns an entire result set
	// in the direct results it may have already
	// closed the operation.
	closedOnServer bool

	logger_ *dbsqllog.DBSQLLogger
}

var _ driver.Rows = (*rows)(nil)
var _ driver.RowsColumnTypeScanType = (*rows)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
var _ driver.RowsColumnTypeNullable = (*rows)(nil)
var _ driver.RowsColumnTypeLength = (*rows)(nil)

var errRowsFetchPriorToStart = "databricks: unable to fetch row page prior to start of results"
var errRowsUnandledFetchDirection = "databricks: unhandled fetch direction %s"
var errRowsNoClient = "databricks: instance of Rows missing client"
var errRowsNilRows = "databricks: nil Rows instance"
var errRowsUnknowRowType = "databricks: unknown rows representation"

func NewRows(
	connId string,
	correlationId string,
	opHandle client.Handle,
	client client.DatabricksClient,
	config *config.Config,
	stmtResp *client.ExecuteStatementResp,
) (driver.Rows, error) {

	var logger *dbsqllog.DBSQLLogger
	if opHandle != nil {
		logger = dbsqllog.WithContext(connId, correlationId, opHandle.Id())
	} else {
		logger = dbsqllog.WithContext(connId, correlationId, "")
	}

	if client == nil {
		logger.Error().Msg(errRowsNoClient)
		return nil, errors.New(errRowsNoClient)
	}

	var pageSize int64 = 10000
	var location *time.Location = time.UTC
	if config != nil {
		pageSize = int64(config.MaxRows)

		if config.Location != nil {
			location = config.Location
		}
	}

	logger.Debug().Msgf("databricks: creating Rows, pageSize: %d, location: %v", pageSize, location)

	r := &rows{
		client:        client,
		opHandle:      opHandle,
		connId:        connId,
		correlationId: correlationId,
		maxPageSize:   pageSize,
		location:      location,
		config:        config,
		hasMoreRows:   true,
		logger_:       logger,
	}

	// if we already have results for the query do some additional initialization
	if stmtResp != nil {
		logger.Debug().Msgf("databricks: creating Rows with direct results")
		// set the result set metadata
		if stmtResp.Schema != nil {
			// r.resultSetMetadata = stmtResp.ResultSetMetadata
			r.schema = stmtResp.Schema
		}

		// If the entire query result set fits in direct results the server closes
		// the operations.
		if stmtResp.IsClosed {
			logger.Debug().Msgf("databricks: creating Rows with server operation closed")
			r.closedOnServer = true
		}

		// initialize the row scanner
		err := r.makeRowScanner(stmtResp.Result)
		if err != nil {
			return r, err
		}
	}

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

	colNames := make([]string, len(schema.Columns))

	for i := range colNames {
		colNames[i] = schema.Columns[i].Name
	}

	return colNames
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	if r == nil {
		return nil
	}

	if r.RowScanner != nil {
		// make sure the row scanner frees up any resources
		r.RowScanner.Close()
	}

	if !r.closedOnServer {
		r.logger().Debug().Msgf("databricks: closing Rows operation")

		// if the operation hasn't already been closed on the server we
		// need to do that now
		r.closedOnServer = true

		req := client.CloseExecutionReq{
			ExecutionHandle: r.opHandle,
		}
		ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), r.connId), r.correlationId)

		_, err1 := r.client.CloseExecution(ctx, &req)
		if err1 != nil {
			r.logger().Err(err1).Msg("databricks: Rows instance failed to close operation")
			return err1
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
		return err
	}

	// if the next row is not in the current result page
	// fetch the containing page
	var b bool
	var e error
	if b, e = r.isNextRowInPage(); !b && e == nil {
		err := r.fetchResultPage()
		if err != nil {
			return err
		}
	}

	if e != nil {
		return e
	}

	// Put values into the destination slice
	err = r.ScanRow(dest, r.nextRowIndex)
	if err != nil {
		return err
	}

	r.nextRowIndex++
	r.nextRowNumber++

	return nil
}

// ColumnTypeScanType returns column's native type
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	err := isValidRows(r)
	if err != nil {
		// TODO: is there a better way to handle this
		return nil
	}

	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		// TODO: is there a better way to handle this
		return nil
	}

	scanType := getScanType(column)
	return scanType
}

// ColumnTypeDatabaseTypeName returns column's database type name
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	err := isValidRows(r)
	if err != nil {
		// TODO: is there a better way to handle this
		return ""
	}

	column, err := r.getColumnMetadataByIndex(index)
	if err != nil {
		// TODO: is there a better way to handle this
		return ""
	}

	
	return column.TypeName
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

	switch columnInfo.TypeName {
	case "STRING_TYPE",
		"TTypeId_VARCHAR_TYPE",
		"TTypeId_BINARY_TYPE",
		"TTypeId_ARRAY_TYPE",
		"TTypeId_MAP_TYPE",
		"TTypeId_STRUCT_TYPE":
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

func getScanType(column *client.ColumnInfo) reflect.Type {
	switch column.Type {
	case client.BOOLEAN_TYPE:
		return scanTypeBoolean
	case client.TINYINT_TYPE:
		return scanTypeInt8
	case client.SMALLINT_TYPE:
		return scanTypeInt16
	case client.INT_TYPE:
		return scanTypeInt32
	case client.BIGINT_TYPE:
		return scanTypeInt64
	case client.FLOAT_TYPE:
		return scanTypeFloat32
	case client.DOUBLE_TYPE:
		return scanTypeFloat64
	case client.NULL_TYPE:
		return scanTypeNull
	case client.STRING_TYPE:
		return scanTypeString
	case client.CHAR_TYPE:
		return scanTypeString
	case client.VARCHAR_TYPE:
		return scanTypeString
	case client.TIMESTAMP_TYPE, client.DATE_TYPE:
		return scanTypeDateTime
	case client.DECIMAL_TYPE, client.BINARY_TYPE, client.ARRAY_TYPE,
		client.STRUCT_TYPE, client.MAP_TYPE, client.UNION_TYPE:
		return scanTypeRawBytes
	case client.USER_DEFINED_TYPE:
		return scanTypeUnknown
	case client.INTERVAL_DAY_TIME_TYPE, client.INTERVAL_YEAR_MONTH_TYPE:
		return scanTypeString
	default:
		return scanTypeUnknown
	}
}

// isValidRows checks that the row instance is not nil
// and that it has a client
func isValidRows(r *rows) error {
	if r == nil {
		return errors.New(errRowsNilRows)
	}

	if r.client == nil {
		r.logger().Error().Msg(errRowsNoClient)
		return errors.New(errRowsNoClient)
	}

	return nil
}

func (r *rows) getColumnMetadataByIndex(index int) (*client.ColumnInfo, error) {
	err := isValidRows(r)
	if err != nil {
		return nil, err
	}

	schema, err := r.getResultSetSchema()
	if err != nil {
		return nil, err
	}

	columns := schema.Columns
	if index < 0 || index >= len(columns) {
		return nil, errors.Errorf("invalid column index: %d", index)
	}

	return columns[index], nil
}

// isNextRowInPage returns a boolean flag indicating whether
// the next result set row is in the current result set page
func (r *rows) isNextRowInPage() (bool, error) {
	if r == nil || r.RowScanner == nil {
		return false, nil
	}

	nRowsInPage := r.NRows()
	if nRowsInPage == 0 {
		return false, nil
	}

	startRowOffset := r.pageStartingRowNum
	return r.nextRowNumber >= startRowOffset && r.nextRowNumber < (startRowOffset+nRowsInPage), nil
}

// getResultMetadata does a one time fetch of the result set schema
func (r *rows) getResultSetSchema() (*client.ResultSchema, error) {
	if r.schema == nil {
		err := isValidRows(r)
		if err != nil {
			return nil, err
		}

		req := client.GetResultsMetadataReq{
			ExecutionHandle: r.opHandle,
		}
		ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), r.connId), r.correlationId)

		resp, err := r.client.GetResultsMetadata(ctx, &req)
		if err != nil {
			r.logger().Err(err).Msg("databricks: Rows instance failed to retrieve result set metadata")
			return nil, err
		}

		// r.resultSetMetadata = resp
		r.schema = resp.Schema

	}

	return r.schema, nil
}

// fetchResultPage will fetch the result page containing the next row, if necessary
func (r *rows) fetchResultPage() error {
	err := isValidRows(r)
	if err != nil {
		return err
	}

	r.logger().Debug().Msgf("databricks: fetching result page for row %d", r.nextRowNumber)

	var b bool
	var e error
	for b, e = r.isNextRowInPage(); !b && e == nil; b, e = r.isNextRowInPage() {

		// determine the direction of page fetching. Currently we only handle
		// TFetchOrientation_FETCH_PRIOR and TFetchOrientation_FETCH_NEXT
		var direction cli_service.TFetchOrientation = r.getPageFetchDirection()
		if direction == cli_service.TFetchOrientation_FETCH_PRIOR {
			// can't fetch rows previous to the start
			if r.pageStartingRowNum == 0 {
				return errors.New(errRowsFetchPriorToStart)
			}
		} else if direction == cli_service.TFetchOrientation_FETCH_NEXT {
			// can't fetch past the end of the query results
			if !r.hasMoreRows {
				return io.EOF
			}
		} else {
			r.logger().Error().Msgf(errRowsUnandledFetchDirection, direction.String())
			return errors.Errorf(errRowsUnandledFetchDirection, direction.String())
		}

		r.logger().Debug().Msgf("fetching next batch of up to %d rows, %s", r.maxPageSize, direction.String())

		req := dbsqlclient.FetchResultsReq{
			ExecutionHandle: r.opHandle,
			MaxRows:         r.maxPageSize,
			Orientation:     direction,
		}
		ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), r.connId), r.correlationId)

		fetchResult, err := r.client.FetchResults(ctx, &req)
		if err != nil {
			r.logger().Err(err).Msg("databricks: Rows instance failed to retrieve results")
			return err
		}

		err = r.makeRowScanner(fetchResult.Result)
		if err != nil {
			return err
		}

		r.logger().Debug().Msgf("databricks: new result page startRow: %d, nRows: %v, hasMoreRows: %v", fetchResult.Result.StartRowOffset, r.NRows(), fetchResult.Result.HasMoreRows)
	}

	if e != nil {
		return e
	}

	// don't assume the next row is the first row in the page
	r.nextRowIndex = r.nextRowNumber - r.pageStartingRowNum

	return nil
}

// getPageFetchDirection returns the cli_service.TFetchOrientation
// necessary to fetch a result page containing the next row number.
// Note: if the next row number is in the current page TFetchOrientation_FETCH_NEXT
// is returned. Use rows.nextRowInPage to determine if a fetch is necessary
func (r *rows) getPageFetchDirection() cli_service.TFetchOrientation {
	if r == nil {
		return cli_service.TFetchOrientation_FETCH_NEXT
	}

	if r.nextRowNumber < r.pageStartingRowNum {
		return cli_service.TFetchOrientation_FETCH_PRIOR
	}

	return cli_service.TFetchOrientation_FETCH_NEXT
}

// makeRowScanner creates the embedded RowScanner instance based on the format
// of the returned query results
func (r *rows) makeRowScanner(fetchResults *client.ResultData) error {

	schema, err1 := r.getResultSetSchema()
	if err1 != nil {
		return err1
	}

	if fetchResults == nil {
		return nil
	}

	var rs rowscanner.RowScanner
	var err error

	if fetchResults.Columns != nil {
		rs, err = columnbased.NewColumnRowScanner(schema, fetchResults, r.config, r.logger())
	} else if fetchResults.ArrowBatches != nil {
		rs, err = arrowbased.NewArrowRowScanner(r.schema, fetchResults, r.config, r.logger())
	} else {
		r.logger().Error().Msg(errRowsUnknowRowType)
		err = errors.New(errRowsUnknowRowType)
	}

	r.pageStartingRowNum = fetchResults.StartRowOffset

	r.RowScanner = rs
	r.hasMoreRows = fetchResults.HasMoreRows

	return err
}

func (r *rows) logger() *dbsqllog.DBSQLLogger {
	if r.logger_ == nil {
		if r.opHandle != nil {
			r.logger_ = dbsqllog.WithContext(r.connId, r.correlationId, r.opHandle.Id())
		} else {
			r.logger_ = dbsqllog.WithContext(r.connId, r.correlationId, "")
		}
	}
	return r.logger_
}
