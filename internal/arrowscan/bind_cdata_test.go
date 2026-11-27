//go:build cgo

package arrowscan

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

func TestBoundCellCDataParity(t *testing.T) {
	fixtures := []struct {
		name     string
		dataType arrow.DataType
		values   []string
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, []string{"true", "false"}},
		{"int8", arrow.PrimitiveTypes.Int8, []string{"-128", "127"}},
		{"int16", arrow.PrimitiveTypes.Int16, []string{"-32768", "32767"}},
		{"int32", arrow.PrimitiveTypes.Int32, []string{"-12345", "67890"}},
		{"int64", arrow.PrimitiveTypes.Int64, []string{"-9223372036854775808", "9223372036854775807"}},
		{"float32", arrow.PrimitiveTypes.Float32, []string{"0.1", "-0.0"}},
		{"float64", arrow.PrimitiveTypes.Float64, []string{"0.1", "-0.0"}},
		{"date32", arrow.FixedWidthTypes.Date32, []string{`"0001-01-01"`, `"9999-12-31"`}},
		{"date64", arrow.FixedWidthTypes.Date64, []string{`"0001-01-01"`, `"9999-12-31"`}},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, []string{"-123456789", "123456789"}},
		{"string", arrow.BinaryTypes.String, []string{`""`, `"héllo\u0000世界"`}},
		{"binary", arrow.BinaryTypes.Binary, []string{`""`, `"AAEC/w=="`}},
		{"decimal", &arrow.Decimal128Type{Precision: 10, Scale: 2}, []string{`"-123.45"`, `"678.90"`}},
		{"list", arrow.ListOf(arrow.PrimitiveTypes.Int32), []string{"[1,null,3]", "[]"}},
	}
	for _, mode := range []string{"nullable", "no-bitmap"} {
		t.Run(mode, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(mallocator.NewMallocator())
			defer pool.AssertSize(t, 0)
			fields := make([]arrow.Field, len(fixtures))
			columns := make([]arrow.Array, len(fixtures))
			for index, fixture := range fixtures {
				values := make([]string, 40)
				for row := range values {
					values[row] = fixture.values[row%len(fixture.values)]
					if mode == "nullable" && row%5 == 0 {
						values[row] = "null"
					}
				}
				column, _, err := array.FromJSON(pool, fixture.dataType, strings.NewReader("["+strings.Join(values, ",")+"]"))
				if err != nil {
					t.Fatal(err)
				}
				if mode == "no-bitmap" {
					data := column.Data()
					buffers := append([]*memory.Buffer(nil), data.Buffers()...)
					buffers[0] = nil
					withoutBitmap := array.NewData(data.DataType(), data.Len(), buffers, data.Children(), 0, data.Offset())
					column.Release()
					column = array.MakeFromData(withoutBitmap)
					withoutBitmap.Release()
				}
				defer column.Release()
				fields[index] = arrow.Field{Name: fixture.name, Type: fixture.dataType, Nullable: true}
				columns[index] = column
			}
			record := array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, 40)
			defer record.Release()
			sliced := record.NewSlice(7, 31)
			defer sliced.Release()
			var exportedArray cdata.CArrowArray
			var exportedSchema cdata.CArrowSchema
			cdata.ExportArrowRecordBatch(sliced, &exportedArray, &exportedSchema)
			defer cdata.ReleaseCArrowArray(&exportedArray)
			defer cdata.ReleaseCArrowSchema(&exportedSchema)
			imported, err := cdata.ImportCRecordBatch(&exportedArray, &exportedSchema)
			if err != nil {
				t.Fatal(err)
			}
			defer imported.Release()
			if imported.NumRows() != sliced.NumRows() || imported.NumCols() != sliced.NumCols() {
				t.Fatal("C Data round-trip changed the record shape")
			}
			for _, column := range imported.Columns() {
				if column.Data().Offset() != 7 {
					t.Fatalf("C Data round-trip lost the slice offset: %d", column.Data().Offset())
				}
				if mode == "no-bitmap" && len(column.NullBitmapBytes()) != 0 {
					t.Fatal("C Data round-trip introduced a validity bitmap")
				}
				checkBoundParity(t, column)
			}
		})
	}
}
