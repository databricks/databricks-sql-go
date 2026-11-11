package kernel

import (
	"database/sql/driver"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

type columnAccessor func(row int) (driver.Value, error)

// prepareBatch is pure Go so buffer ownership can be tested without the kernel.
// Accessors may read Arrow buffers only while the record is retained.
func prepareBatch(rec arrow.Record, location *time.Location, ntz, geoBinary []bool, decimalAsFloat bool, keys *arrowscan.StructKeyCache) []columnAccessor {
	columns := make([]columnAccessor, rec.NumCols())
	for i, col := range rec.Columns() {
		loc := location
		if ntz[i] {
			loc = time.UTC
		}
		columns[i] = prepareColumn(col, loc, keys, arrowscan.ScanOptions{
			DecimalAsFloat:   decimalAsFloat,
			GeospatialBinary: geoBinary[i],
		})
	}
	return columns
}

func prepareColumn(col arrow.Array, loc *time.Location, keys *arrowscan.StructKeyCache, options arrowscan.ScanOptions) columnAccessor {
	// One Go-owned copy per string column. Retaining a substring pins that
	// batch column's copy, independently of the imported Arrow record's lifetime.
	if !options.GeospatialBinary {
		switch c := col.(type) {
		case *array.String:
			values := string(c.ValueBytes())
			base := c.ValueOffset(0)
			return func(row int) (driver.Value, error) {
				if c.IsNull(row) {
					return nil, nil
				}
				return values[c.ValueOffset(row)-base : c.ValueOffset(row+1)-base], nil
			}
		case *array.LargeString:
			values := string(c.ValueBytes())
			base := c.ValueOffset(0)
			return func(row int) (driver.Value, error) {
				if c.IsNull(row) {
					return nil, nil
				}
				return values[c.ValueOffset(row)-base : c.ValueOffset(row+1)-base], nil
			}
		}
	}
	return func(row int) (driver.Value, error) {
		return arrowscan.ScanCellCachedWithOptions(col, row, loc, keys, options)
	}
}
