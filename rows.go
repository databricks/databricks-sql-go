package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

type rows struct {
	client        client.DatabricksClient
	connId        string
	correlationId string
	opHandle      client.Handle
	pageSize      int64
	location      *time.Location
	results       *client.ResultData
	Schema        *client.ResultSchema
	nextRowIndex  int64
	nextRowNumber int64
	closed        bool
}

var _ driver.Rows = (*rows)(nil)
var _ driver.RowsColumnTypeScanType = (*rows)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
var _ driver.RowsColumnTypeNullable = (*rows)(nil)
var _ driver.RowsColumnTypeLength = (*rows)(nil)

var errRowsFetchPriorToStart = "databricks: unable to fetch row page prior to start of results"
var errRowsNoSchemaAvailable = "databricks: no schema in result set metadata response"
var errRowsNoClient = "databricks: instance of Rows missing client"
var errRowsNilRows = "databricks: nil Rows instance"
var errRowsParseValue = "databricks: unable to parse %s value '%s' from column %s"

// NewRows generates a new rows object given the rows' fields.
// NewRows will also parse directResults if it is available for some rows' fields.
func NewRows(connID string, corrId string, client client.DatabricksClient, opHandle client.Handle, pageSize int64, location *time.Location, resp *client.ExecuteStatementResp) driver.Rows {
	r := &rows{
		connId:        connID,
		correlationId: corrId,
		client:        client,
		opHandle:      opHandle,
		pageSize:      pageSize,
		location:      location,
	}

	if resp.Result != nil {
		r.results = resp.Result
		r.Schema = resp.Schema
		r.closed = resp.IsClosed
	}

	return r
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

	resultMetadata, err := r.getResultSchema()
	if err != nil {
		return []string{}
	}

	if resultMetadata == nil {
		return []string{}
	}

	tColumns := resultMetadata.Columns
	colNames := make([]string, len(tColumns))

	for i := range tColumns {
		colNames[i] = tColumns[i].Name
	}

	return colNames
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	if !r.closed {
		err := isValidRows(r)
		if err != nil {
			return err
		}

		req := client.CloseExecutionReq{
			ExecutionHandle: r.opHandle,
		}
		ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), r.connId), r.correlationId)

		_, err1 := r.client.CloseExecution(ctx, &req)
		if err1 != nil {
			return err1
		}
	}

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
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
	if !r.isNextRowInPage() {
		err := r.fetchResultPage()
		if err != nil {
			return err
		}
	}

	// need the column info to retrieve/convert values
	metadata, err := r.getResultSchema()
	if err != nil {
		return err
	}

	// populate the destination slice
	for i := range dest {
		val, err := value(r.results.Columns[i], metadata.Columns[i], r.nextRowIndex, r.location)

		if err != nil {
			return err
		}

		dest[i] = val
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

	return strings.TrimSuffix(column.TypeName, "_TYPE")
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
	case cli_service.TTypeId_STRING_TYPE.String(),
		cli_service.TTypeId_VARCHAR_TYPE.String(),
		cli_service.TTypeId_BINARY_TYPE.String(),
		cli_service.TTypeId_ARRAY_TYPE.String(),
		cli_service.TTypeId_MAP_TYPE.String(),
		cli_service.TTypeId_STRUCT_TYPE.String():
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

	switch column.TypeName {
	case cli_service.TTypeId_BOOLEAN_TYPE.String():
		return scanTypeBoolean
	case cli_service.TTypeId_TINYINT_TYPE.String():
		return scanTypeInt8
	case cli_service.TTypeId_SMALLINT_TYPE.String():
		return scanTypeInt16
	case cli_service.TTypeId_INT_TYPE.String():
		return scanTypeInt32
	case cli_service.TTypeId_BIGINT_TYPE.String():
		return scanTypeInt64
	case cli_service.TTypeId_FLOAT_TYPE.String():
		return scanTypeFloat32
	case cli_service.TTypeId_DOUBLE_TYPE.String():
		return scanTypeFloat64
	case cli_service.TTypeId_NULL_TYPE.String():
		return scanTypeNull
	case cli_service.TTypeId_STRING_TYPE.String():
		return scanTypeString
	case cli_service.TTypeId_CHAR_TYPE.String():
		return scanTypeString
	case cli_service.TTypeId_VARCHAR_TYPE.String():
		return scanTypeString
	case cli_service.TTypeId_DATE_TYPE.String(), cli_service.TTypeId_TIMESTAMP_TYPE.String():
		return scanTypeDateTime
	case cli_service.TTypeId_DECIMAL_TYPE.String(), cli_service.TTypeId_BINARY_TYPE.String(), cli_service.TTypeId_ARRAY_TYPE.String(),
		cli_service.TTypeId_STRUCT_TYPE.String(), cli_service.TTypeId_MAP_TYPE.String(), cli_service.TTypeId_UNION_TYPE.String():
		return scanTypeRawBytes
	case cli_service.TTypeId_USER_DEFINED_TYPE.String():
		return scanTypeUnknown
	case cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE.String(), cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE.String():
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
		return errors.New(errRowsNoClient)
	}

	return nil
}

func (r *rows) getColumnMetadataByIndex(index int) (*client.ColumnInfo, error) {
	err := isValidRows(r)
	if err != nil {
		return nil, err
	}

	resultSchema, err := r.getResultSchema()
	if err != nil {
		return nil, err
	}

	if resultSchema == nil {
		return nil, errors.New(errRowsNoSchemaAvailable)
	}

	columns := resultSchema.Columns
	if index < 0 || index >= len(columns) {
		return nil, errors.Errorf("invalid column index: %d", index)
	}

	return columns[index], nil
}

// isNextRowInPage returns a boolean flag indicating whether
// the next result set row is in the current result set page
func (r *rows) isNextRowInPage() bool {
	if r == nil || r.results == nil {
		return false
	}

	nRowsInPage := getNRows(r.results)
	if nRowsInPage == 0 {
		return false
	}

	startRowOffset := r.getPageStartRowNum()
	return r.nextRowNumber >= startRowOffset && r.nextRowNumber < (startRowOffset+nRowsInPage)
}

func (r *rows) getResultSchema() (*client.ResultSchema, error) {
	if r.Schema == nil {
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
			return nil, err
		}

		r.Schema = resp.Schema

	}

	return r.Schema, nil
}

func (r *rows) fetchResultPage() error {
	err := isValidRows(r)
	if err != nil {
		return err
	}
	var log *logger.DBSQLLogger
	if r.opHandle != nil {
		log = logger.WithContext(r.connId, r.correlationId, r.opHandle.Id())
	} else {
		log = logger.WithContext(r.connId, r.correlationId, "")
	}

	for !r.isNextRowInPage() {

		// determine the direction of page fetching. Currently we only handle
		// TFetchOrientation_FETCH_PRIOR and TFetchOrientation_FETCH_NEXT
		var direction cli_service.TFetchOrientation = r.getPageFetchDirection()
		if direction == cli_service.TFetchOrientation_FETCH_PRIOR {
			if r.getPageStartRowNum() == 0 {
				return errors.New(errRowsFetchPriorToStart)
			}
		} else if direction == cli_service.TFetchOrientation_FETCH_NEXT {
			if r.results != nil && !r.results.HasMoreRows {
				return io.EOF
			}
		} else {
			return errors.Errorf("unhandled fetch result orientation: %s", direction)
		}

		req := client.FetchResultsReq{
			ExecutionHandle: r.opHandle,
			MaxRows:         r.pageSize,
			Orientation:     direction,
		}
		ctx := driverctx.NewContextWithCorrelationId(driverctx.NewContextWithConnId(context.Background(), r.connId), r.correlationId)
		log.Debug().Msgf("fetching next batch of %d rows", r.pageSize)
		fetchResult, err := r.client.FetchResults(ctx, &req)
		if err != nil {
			return err
		}

		r.results = fetchResult.Result
	}

	// don't assume the next row is the first row in the page
	r.nextRowIndex = r.nextRowNumber - r.getPageStartRowNum()

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

	if r.nextRowNumber < r.getPageStartRowNum() {
		return cli_service.TFetchOrientation_FETCH_PRIOR
	}

	return cli_service.TFetchOrientation_FETCH_NEXT
}

// getPageStartRowNum returns an int64 value which is the
// starting row number of the current result page, -1 is returned
// if there is no result page
func (r *rows) getPageStartRowNum() int64 {
	if r == nil || r.results == nil {
		return 0
	}

	return r.results.StartRowOffset
}

var dateTimeFormats map[string]string = map[string]string{
	"TIMESTAMP_TYPE": "2006-01-02 15:04:05.999999999",
	"DATE_TYPE":      "2006-01-02",
}

func value(tColumn *cli_service.TColumn, tColumnDesc *client.ColumnInfo, rowNum int64, location *time.Location) (val any, err error) {
	if location == nil {
		location = time.UTC
	}

	if tVal := tColumn.GetStringVal(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
		val, err = handleDateTime(val, tColumnDesc.TypeName, tColumnDesc.Name, location)
	} else if tVal := tColumn.GetByteVal(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetI16Val(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetI32Val(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetI64Val(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetBoolVal(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetDoubleVal(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		if tColumnDesc.TypeName == "FLOAT_TYPE" {
			// database types FLOAT and DOUBLE are both returned as a float64
			// convert to a float32 is valid because the FLOAT type would have
			// only been four bytes on the server
			val = float32(tVal.Values[rowNum])
		} else {
			val = tVal.Values[rowNum]
		}

	} else if tVal := tColumn.GetBinaryVal(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	}

	return val, err
}

// handleDateTime will convert the passed val to a time.Time value if necessary
func handleDateTime(val any, dbType, columnName string, location *time.Location) (any, error) {
	// if there is a date/time format corresponding to the column type we need to
	// convert to time.Time
	if format, ok := dateTimeFormats[dbType]; ok {
		t, err := parseInLocation(format, val.(string), location)
		if err != nil {
			err = wrapErrf(err, errRowsParseValue, dbType, val, columnName)
		}
		return t, err
	}

	return val, nil
}

func isNull(nulls []byte, position int64) bool {
	index := position / 8
	if int64(len(nulls)) > index {
		b := nulls[index]
		return (b & (1 << (uint)(position%8))) != 0
	}
	return false
}

func getNRows(rs *client.ResultData) int64 {
	if rs == nil {
		return 0
	}
	for _, col := range rs.Columns {
		if col.BoolVal != nil {
			return int64(len(col.BoolVal.Values))
		}
		if col.ByteVal != nil {
			return int64(len(col.ByteVal.Values))
		}
		if col.I16Val != nil {
			return int64(len(col.I16Val.Values))
		}
		if col.I32Val != nil {
			return int64(len(col.I32Val.Values))
		}
		if col.I64Val != nil {
			return int64(len(col.I64Val.Values))
		}
		if col.StringVal != nil {
			return int64(len(col.StringVal.Values))
		}
		if col.DoubleVal != nil {
			return int64(len(col.DoubleVal.Values))
		}
		if col.BinaryVal != nil {
			return int64(len(col.BinaryVal.Values))
		}
	}
	return 0
}

// parseInLocation parses a date/time string in the given format and using the provided
// location.
// This is, essentially, a wrapper around time.ParseInLocation to handle negative year
// values
func parseInLocation(format, dateTimeString string, loc *time.Location) (time.Time, error) {
	// we want to handle dates with negative year values and currently we only
	// support formats that start with the year so we can just strip a leading minus
	// sign
	var isNegative bool
	dateTimeString, isNegative = stripLeadingNegative(dateTimeString)

	date, err := time.ParseInLocation(format, dateTimeString, loc)
	if err != nil {
		return time.Time{}, err
	}

	if isNegative {
		date = date.AddDate(-2*date.Year(), 0, 0)
	}

	return date, nil
}

// stripLeadingNegative will remove a leading ascii or unicode minus
// if present. The possibly shortened string is returned and a flag indicating if
// the string was altered
func stripLeadingNegative(dateTimeString string) (string, bool) {
	if dateStartsWithNegative(dateTimeString) {
		// strip leading rune from dateTimeString
		// using range because it is supposed to be faster than utf8.DecodeRuneInString
		for i := range dateTimeString {
			if i > 0 {
				return dateTimeString[i:], true
			}
		}
	}

	return dateTimeString, false
}

// ISO 8601 allows for both the ascii and unicode characters for minus
const (
	// unicode minus sign
	uMinus string = "\u2212"
	// ascii hyphen/minus
	aMinus string = "\x2D"
)

// dateStartsWithNegative returns true if the string starts with
// a minus sign
func dateStartsWithNegative(val string) bool {
	return strings.HasPrefix(val, aMinus) || strings.HasPrefix(val, uMinus)
}
