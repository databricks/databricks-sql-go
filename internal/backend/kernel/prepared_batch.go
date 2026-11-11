package kernel

import (
	"database/sql/driver"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

type columnAccessor func(row int) (driver.Value, error)

// prepareBatch resolves per-column scan state once per batch. It is pure Go so
// buffer ownership can be tested without the kernel. Accessors may read Arrow
// buffers only while the record is retained.
func prepareBatch(rec arrow.Record, location *time.Location, ntz, geoBinary []bool, decimalAsFloat bool, keys *arrowscan.StructKeyCache) []columnAccessor {
	columns := make([]columnAccessor, rec.NumCols())
	for i, col := range rec.Columns() {
		loc := location
		if i < len(ntz) && ntz[i] {
			loc = time.UTC
		}
		columns[i] = prepareColumn(col, loc, keys, arrowscan.ScanOptions{
			DecimalAsFloat:   decimalAsFloat,
			GeospatialBinary: i < len(geoBinary) && geoBinary[i],
		})
	}
	return columns
}

func prepareColumn(col arrow.Array, loc *time.Location, keys *arrowscan.StructKeyCache, options arrowscan.ScanOptions) columnAccessor {
	if !options.GeospatialBinary && col.Len() > 0 {
		switch c := col.(type) {
		case *array.String:
			return ownedStrings(c, c.ValueBytes(), c.ValueOffsets())
		case *array.LargeString:
			return ownedStrings(c, c.ValueBytes(), c.ValueOffsets())
		}
	}
	return func(row int) (driver.Value, error) {
		return arrowscan.ScanCellCachedWithOptions(col, row, loc, keys, options)
	}
}

// ownedStrings copies a string column's value bytes into Go memory once per
// batch: the imported buffers are freed with the batch, and database/sql hands
// string values to callers without copying. A retained value pins its batch
// column's copy.
func ownedStrings[O int32 | int64](col arrow.Array, data []byte, offsets []O) columnAccessor {
	values := string(data)
	base := offsets[0]
	return func(row int) (driver.Value, error) {
		if col.IsNull(row) {
			return nil, nil
		}
		return values[offsets[row]-base : offsets[row+1]-base], nil
	}
}
