package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/cli_service"
)

type rows struct {
	client       *cli_service.TCLIServiceClient
	opHandle     *cli_service.TOperationHandle
	tableSchema  *tableSchema
	pageSize     int64
	fetchResults *cli_service.TFetchResultsResp
	rowIndex     int
}

func (r *rows) Columns() []string {
	schema, err := r.getTableSchema()
	if err != nil {
		return []string{}
	}

	names := make([]string, len(schema.columns))
	for i := range schema.columns {
		names[i] = schema.columns[i].name
	}

	return names
}

func (r *rows) Close() error {
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

func (r *rows) Next(dest []driver.Value) error {
	if r == nil {
		return nil
	}

	err := r.fetch()
	if err != nil {
		return err
	}

	schema, err := r.getTableSchema()
	if err != nil {
		return err
	}

	for i := range dest {
		val, err := value(r.fetchResults.Results.Columns[i], schema.columns[i], r.rowIndex)

		if err != nil {
			return err
		}

		dest[i] = val
	}

	r.rowIndex++

	return nil
}

func (r *rows) getTableSchema() (*tableSchema, error) {
	if r.tableSchema == nil {

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

		schema := tableSchema{}

		if resp.IsSetSchema() {
			tColumns := resp.Schema.GetColumns()
			schema.columns = make([]*colDesc, len(tColumns))
			for i, desc := range resp.Schema.Columns {
				entry := desc.TypeDesc.Types[0].PrimitiveEntry
				dbtype := strings.TrimSuffix(entry.Type.String(), "_TYPE")
				schema.columns[i] = &colDesc{
					name:             desc.ColumnName,
					databaseTypeName: dbtype,
					scanType:         typeOf(entry),
				}
			}

		}

		r.tableSchema = &schema
	}

	return r.tableSchema, nil
}

func (r *rows) fetch() error {
	if r.fetchResults != nil {
		l := length(r.fetchResults.Results)
		if l == 0 && !*r.fetchResults.HasMoreRows {
			return io.EOF
		} else if r.rowIndex < l {
			return nil
		}
	}

	r.rowIndex = 0
	r.fetchResults = nil

	req := cli_service.TFetchResultsReq{
		OperationHandle: r.opHandle,
		MaxRows:         r.pageSize,
	}

	fetchResult, err := r.client.FetchResults(context.Background(), &req)
	if err != nil {
		return err
	}

	if err := checkStatus(fetchResult.GetStatus()); err != nil {
		return err
	}

	if length(fetchResult.GetResults()) == 0 {
		return io.EOF
	}

	r.fetchResults = fetchResult

	return nil
}

type tableSchema struct {
	columns []*colDesc
}

type colDesc struct {
	name string

	databaseTypeName string
	scanType         reflect.Type

	// columnTypeNullable  bool
	// columnTypeLength    int64
	// columnTypePrecision int64
	// columnTypeScale     int64
}

func checkStatus(status *cli_service.TStatus) error {
	if status.StatusCode == cli_service.TStatusCode_ERROR_STATUS {
		return errors.New(status.GetErrorMessage())
	}

	if status.StatusCode == cli_service.TStatusCode_INVALID_HANDLE_STATUS {
		return errors.New("thrift: invalid handle")
	}

	return nil
}

var (
	dataTypeNull     = reflect.TypeOf(nil)
	dataTypeBoolean  = reflect.TypeOf(true)
	dataTypeFloat32  = reflect.TypeOf(float32(0))
	dataTypeFloat64  = reflect.TypeOf(float64(0))
	dataTypeInt8     = reflect.TypeOf(int8(0))
	dataTypeInt16    = reflect.TypeOf(int16(0))
	dataTypeInt32    = reflect.TypeOf(int32(0))
	dataTypeInt64    = reflect.TypeOf(int64(0))
	dataTypeString   = reflect.TypeOf("")
	dataTypeDateTime = reflect.TypeOf(time.Time{})
	dataTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
	dataTypeUnknown  = reflect.TypeOf(new(interface{}))
)

func typeOf(entry *cli_service.TPrimitiveTypeEntry) reflect.Type {
	switch entry.Type {
	case cli_service.TTypeId_BOOLEAN_TYPE:
		return dataTypeBoolean
	case cli_service.TTypeId_TINYINT_TYPE:
		return dataTypeInt8
	case cli_service.TTypeId_SMALLINT_TYPE:
		return dataTypeInt16
	case cli_service.TTypeId_INT_TYPE:
		return dataTypeInt32
	case cli_service.TTypeId_BIGINT_TYPE:
		return dataTypeInt64
	case cli_service.TTypeId_FLOAT_TYPE:
		return dataTypeFloat32
	case cli_service.TTypeId_DOUBLE_TYPE:
		return dataTypeFloat64
	case cli_service.TTypeId_NULL_TYPE:
		return dataTypeNull
	case cli_service.TTypeId_STRING_TYPE:
		return dataTypeString
	case cli_service.TTypeId_CHAR_TYPE:
		return dataTypeString
	case cli_service.TTypeId_VARCHAR_TYPE:
		return dataTypeString
	case cli_service.TTypeId_DATE_TYPE, cli_service.TTypeId_TIMESTAMP_TYPE:
		return dataTypeDateTime
	case cli_service.TTypeId_DECIMAL_TYPE, cli_service.TTypeId_BINARY_TYPE, cli_service.TTypeId_ARRAY_TYPE,
		cli_service.TTypeId_STRUCT_TYPE, cli_service.TTypeId_MAP_TYPE, cli_service.TTypeId_UNION_TYPE:
		return dataTypeRawBytes
	case cli_service.TTypeId_USER_DEFINED_TYPE:
		return dataTypeUnknown
	default:
		return dataTypeUnknown
	}
}

const (
	// TimestampFormat is JDBC compliant timestamp format
	TimestampFormat = "2006-01-02 15:04:05.999999999"
)

func value(tColumn *cli_service.TColumn, cd *colDesc, rowNum int) (val interface{}, err error) {

	if tVal := tColumn.GetStringVal(); tVal != nil && !isNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
		if cd.databaseTypeName == "TIMESTAMP" || cd.databaseTypeName == "DATETIME" {
			t, err := time.Parse(TimestampFormat, tColumn.StringVal.Values[rowNum])
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

func isNull(nulls []byte, position int) bool {
	index := position / 8
	if len(nulls) > index {
		b := nulls[index]
		return (b & (1 << (uint)(position%8))) != 0
	}
	return false
}

func length(rs *cli_service.TRowSet) int {
	if rs == nil {
		return 0
	}
	for _, col := range rs.Columns {
		if col.BoolVal != nil {
			return len(col.BoolVal.Values)
		}
		if col.ByteVal != nil {
			return len(col.ByteVal.Values)
		}
		if col.I16Val != nil {
			return len(col.I16Val.Values)
		}
		if col.I32Val != nil {
			return len(col.I32Val.Values)
		}
		if col.I32Val != nil {
			return len(col.I32Val.Values)
		}
		if col.I64Val != nil {
			return len(col.I64Val.Values)
		}
		if col.StringVal != nil {
			return len(col.StringVal.Values)
		}
		if col.DoubleVal != nil {
			return len(col.DoubleVal.Values)
		}
	}
	return 0
}
