package arrowscan

import (
	"database/sql/driver"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
)

// CellScanner reads a cell while its caller-owned Arrow array is alive.
type CellScanner func(row int) (driver.Value, error)

// BindCellScanner resolves a column's scalar reader and null bitmap once per
// batch. Like ScanCellCachedWithOptions, loc is authoritative for timestamps.
func BindCellScanner(col arrow.Array, loc *time.Location, keys *StructKeyCache, options ScanOptions) CellScanner {
	if options.GeospatialBinary {
		return func(row int) (driver.Value, error) {
			return scanCell(col, row, loc, keys, options)
		}
	}
	var read CellScanner
	switch column := col.(type) {
	case *array.Boolean:
		var values []byte
		if buffer := column.Data().Buffers()[1]; buffer != nil {
			values = buffer.Bytes()
		}
		offset := column.Data().Offset()
		read = func(row int) (driver.Value, error) { return bitutil.BitIsSet(values, offset+row), nil }
	case *array.Int8:
		read = bindValues(column.Values())
	case *array.Int16:
		read = bindValues(column.Values())
	case *array.Int32:
		read = bindValues(column.Values())
	case *array.Int64:
		read = bindValues(column.Values())
	case *array.Float32:
		read = bindValues(column.Values())
	case *array.Float64:
		read = bindValues(column.Values())
	case *array.Date32:
		values := column.Values()
		read = func(row int) (driver.Value, error) { return inLocation(values[row].ToTime(), loc), nil }
	case *array.Date64:
		values := column.Values()
		read = func(row int) (driver.Value, error) { return inLocation(values[row].ToTime(), loc), nil }
	case *array.Timestamp:
		values := column.Values()
		unit := column.DataType().(*arrow.TimestampType).Unit
		read = func(row int) (driver.Value, error) {
			return inLocation(timestampToTime(int64(values[row]), unit), loc), nil
		}
	case *array.String:
		read = func(row int) (driver.Value, error) { return strings.Clone(column.Value(row)), nil }
	case *array.Binary:
		read = func(row int) (driver.Value, error) { return column.Value(row), nil }
	default:
		return func(row int) (driver.Value, error) {
			return scanCell(col, row, loc, keys, options)
		}
	}
	if col.NullN() == 0 || len(col.NullBitmapBytes()) == 0 {
		return read
	}
	bitmap, offset := col.NullBitmapBytes(), col.Data().Offset()
	return func(row int) (driver.Value, error) {
		if !bitutil.BitIsSet(bitmap, offset+row) {
			return nil, nil
		}
		return read(row)
	}
}

func bindValues[Value any](values []Value) CellScanner {
	return func(row int) (driver.Value, error) { return values[row], nil }
}
