package dbsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

type rows struct {
	client               cli_service.TCLIService
	opHandle             *cli_service.TOperationHandle
	pageSize             int64
	fetchResults         *cli_service.TFetchResultsResp
	fetchResultsMetadata *cli_service.TGetResultSetMetadataResp
	nextRowIndex         int64
	nextRowNumber        int64
}

var _ driver.Rows = (*rows)(nil)

var errFetchPriorToStart error = errors.New("unable to fetch row page prior to start of results")

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	if r == nil || r.client == nil {
		return []string{}
	}

	resultMetadata, err := r.getResultMetadata()
	if err != nil {
		return []string{}
	}

	if !resultMetadata.IsSetSchema() {
		return []string{}
	}

	tColumns := resultMetadata.Schema.GetColumns()
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

	req := cli_service.TCloseOperationReq{
		OperationHandle: r.opHandle,
	}

	resp, err := r.client.CloseOperation(context.Background(), &req)
	if err != nil {
		return err
	}
	if err := checkStatus(resp.GetStatus()); err != nil {
		return err
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
	if r == nil {
		return nil
	}

	// if the next row is not in the current result page
	// fetch the containing page
	if !r.nextRowInPage() {
		err := r.fetchResultPage()
		if err != nil {
			return err
		}
	}

	// need the column info to retrieve/convert values
	metadata, err := r.getResultMetadata()
	if err != nil {
		return err
	}

	// populate the destinatino slice
	for i := range dest {
		val, err := value(r.fetchResults.Results.Columns[i], metadata.Schema.Columns[i], r.nextRowIndex)

		if err != nil {
			return err
		}

		dest[i] = val
	}

	r.nextRowIndex++
	r.nextRowNumber++

	return nil
}

// nextRowInPage returns a boolean flag indicating whether
// the next result set row is in the current result set page
func (r *rows) nextRowInPage() bool {
	if r == nil || r.fetchResults == nil {
		return false
	}

	nRowsInPage := length(r.fetchResults.GetResults())
	if nRowsInPage == 0 {
		return false
	}

	startRowOffset := r.getPageStartRowNum()
	return r.nextRowNumber >= startRowOffset && r.nextRowNumber < (startRowOffset+nRowsInPage)
}

func (r *rows) getResultMetadata() (*cli_service.TGetResultSetMetadataResp, error) {
	if r.fetchResultsMetadata == nil {

		req := cli_service.TGetResultSetMetadataReq{
			OperationHandle: r.opHandle,
		}

		resp, err := r.client.GetResultSetMetadata(context.Background(), &req)
		if err != nil {
			return nil, err
		}

		if err := checkStatus(resp.GetStatus()); err != nil {
			return nil, err
		}

		r.fetchResultsMetadata = resp

	}

	return r.fetchResultsMetadata, nil
}

func (r *rows) fetchResultPage() error {
	if r == nil {
		return nil
	}

	for !r.nextRowInPage() {

		// determine the direction of page fetching.  Currently we only handle
		// TFetchOrientation_FETCH_PRIOR and TFetchOrientation_FETCH_NEXT
		var direction cli_service.TFetchOrientation = r.getPageFetchDirection()
		if direction == cli_service.TFetchOrientation_FETCH_PRIOR {
			if r.getPageStartRowNum() == 0 {
				return errFetchPriorToStart
			}
		} else if direction == cli_service.TFetchOrientation_FETCH_NEXT {
			if r.fetchResults != nil && !r.fetchResults.GetHasMoreRows() {
				return io.EOF
			}
		} else {
			return fmt.Errorf("unhandled fetch result orientation: %s", direction)
		}

		req := cli_service.TFetchResultsReq{
			OperationHandle: r.opHandle,
			MaxRows:         r.pageSize,
			Orientation:     direction,
		}

		fetchResult, err := r.client.FetchResults(context.Background(), &req)
		if err != nil {
			return err
		}

		r.fetchResults = fetchResult
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
	if r == nil || r.fetchResults == nil || r.fetchResults.GetResults() == nil {
		return 0
	}

	return r.fetchResults.GetResults().GetStartRowOffset()
}

// type tableSchema struct {
// 	columns []*colDesc
// }

// type colDesc struct {
// 	name string

// 	databaseTypeName string
// 	scanType         reflect.Type

// 	// columnTypeNullable  bool
// 	// columnTypeLength    int64
// 	// columnTypePrecision int64
// 	// columnTypeScale     int64
// }

func checkStatus(status *cli_service.TStatus) error {
	if status.StatusCode == cli_service.TStatusCode_ERROR_STATUS {
		return errors.New(status.GetErrorMessage())
	}

	if status.StatusCode == cli_service.TStatusCode_INVALID_HANDLE_STATUS {
		return errors.New("thrift: invalid handle")
	}

	return nil
}

// var (
// 	dataTypeNull     = reflect.TypeOf(nil)
// 	dataTypeBoolean  = reflect.TypeOf(true)
// 	dataTypeFloat32  = reflect.TypeOf(float32(0))
// 	dataTypeFloat64  = reflect.TypeOf(float64(0))
// 	dataTypeInt8     = reflect.TypeOf(int8(0))
// 	dataTypeInt16    = reflect.TypeOf(int16(0))
// 	dataTypeInt32    = reflect.TypeOf(int32(0))
// 	dataTypeInt64    = reflect.TypeOf(int64(0))
// 	dataTypeString   = reflect.TypeOf("")
// 	dataTypeDateTime = reflect.TypeOf(time.Time{})
// 	dataTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
// 	dataTypeUnknown  = reflect.TypeOf(new(interface{}))
// )

// func typeOf(entry *cli_service.TPrimitiveTypeEntry) reflect.Type {
// 	switch entry.Type {
// 	case cli_service.TTypeId_BOOLEAN_TYPE:
// 		return dataTypeBoolean
// 	case cli_service.TTypeId_TINYINT_TYPE:
// 		return dataTypeInt8
// 	case cli_service.TTypeId_SMALLINT_TYPE:
// 		return dataTypeInt16
// 	case cli_service.TTypeId_INT_TYPE:
// 		return dataTypeInt32
// 	case cli_service.TTypeId_BIGINT_TYPE:
// 		return dataTypeInt64
// 	case cli_service.TTypeId_FLOAT_TYPE:
// 		return dataTypeFloat32
// 	case cli_service.TTypeId_DOUBLE_TYPE:
// 		return dataTypeFloat64
// 	case cli_service.TTypeId_NULL_TYPE:
// 		return dataTypeNull
// 	case cli_service.TTypeId_STRING_TYPE:
// 		return dataTypeString
// 	case cli_service.TTypeId_CHAR_TYPE:
// 		return dataTypeString
// 	case cli_service.TTypeId_VARCHAR_TYPE:
// 		return dataTypeString
// 	case cli_service.TTypeId_DATE_TYPE, cli_service.TTypeId_TIMESTAMP_TYPE:
// 		return dataTypeDateTime
// 	case cli_service.TTypeId_DECIMAL_TYPE, cli_service.TTypeId_BINARY_TYPE, cli_service.TTypeId_ARRAY_TYPE,
// 		cli_service.TTypeId_STRUCT_TYPE, cli_service.TTypeId_MAP_TYPE, cli_service.TTypeId_UNION_TYPE:
// 		return dataTypeRawBytes
// 	case cli_service.TTypeId_USER_DEFINED_TYPE:
// 		return dataTypeUnknown
// 	default:
// 		return dataTypeUnknown
// 	}
// }

const (
	// TimestampFormat is JDBC compliant timestamp format
	TimestampFormat = "2006-01-02 15:04:05.999999999"
)

func value(tColumn *cli_service.TColumn, tColumnDesc *cli_service.TColumnDesc, rowNum int64) (val interface{}, err error) {

	entry := tColumnDesc.TypeDesc.Types[0].PrimitiveEntry
	dbtype := strings.TrimSuffix(entry.Type.String(), "_TYPE")
	if tVal := tColumn.GetStringVal(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
		if dbtype == "TIMESTAMP" || dbtype == "DATETIME" {
			t, err := time.Parse(TimestampFormat, val.(string))
			if err == nil {
				val = t
			}
		}
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
		val = tVal.Values[rowNum]
	}

	return val, err
}

func isNull(nulls []byte, position int64) bool {
	index := position / 8
	if int64(len(nulls)) > index {
		b := nulls[index]
		return (b & (1 << (uint)(position%8))) != 0
	}
	return false
}

func length(rs *cli_service.TRowSet) int64 {
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
	}
	return 0
}
