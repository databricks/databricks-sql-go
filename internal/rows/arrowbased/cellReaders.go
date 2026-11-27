package arrowbased

import (
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
)

type columnReader func(int) (any, error)

var errNativeInterval = errors.New("unsupported native interval")

func (rv *rowValues) bindReaders(columns []colInfo, cfg config.ArrowConfig, arrays []arrow.Array) {
	if len(rv.readers) != len(rv.columnValueHolders) {
		rv.readers = make([]columnReader, len(rv.columnValueHolders))
	}
	for column, holder := range rv.columnValueHolders {
		if holder == nil {
			continue
		}
		read := scalarColumnReader(holder)
		var values arrow.Array
		if column < len(arrays) {
			values = arrays[column]
		}
		if cfg.UseArrowNativeDecimal && columns[column].dbType == cli_service.TTypeId_DECIMAL_TYPE {
			if decimal, ok := holder.(*decimal128Container); ok {
				read = func(row int) (any, error) { return decimal.ValueString(row), nil }
			}
		}
		if cfg.UseArrowNativeIntervalTypes && isIntervalType(columns[column].dbType) {
			read = func(int) (any, error) { return nil, errNativeInterval }
		}
		if values != nil && values.NullN() == 0 {
			rv.readers[column] = read
			continue
		}
		if values != nil && len(values.NullBitmapBytes()) > 0 {
			bitmap, offset := values.NullBitmapBytes(), values.Data().Offset()
			rv.readers[column] = func(row int) (any, error) {
				if bitutil.BitIsNotSet(bitmap, offset+row) {
					return nil, nil
				}
				return read(row)
			}
		} else {
			rv.readers[column] = func(row int) (any, error) {
				if holder.IsNull(row) {
					return nil, nil
				}
				return read(row)
			}
		}
	}
}

func scalarColumnReader(holder columnValues) columnReader {
	switch column := holder.(type) {
	case *columnValuesTyped[*array.Boolean, bool]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.Int8, int8]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.Int16, int16]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.Int32, int32]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.Int64, int64]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.Float32, float32]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.Float64, float64]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.String, string]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	case *columnValuesTyped[*array.Binary, []byte]:
		values := column.holder
		return func(row int) (any, error) { return values.Value(row), nil }
	default:
		return holder.Value
	}
}
