//go:build cgo && databricks_kernel

package kernel

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/v2/internal/arrowscan"
)

func benchmarkRecord(b *testing.B, types []arrow.DataType, count int, nulls bool) arrow.RecordBatch {
	b.Helper()
	fields := make([]arrow.Field, len(types))
	columns := make([]arrow.Array, len(types))
	for column, dataType := range types {
		fields[column] = arrow.Field{Name: fmt.Sprint(column), Type: dataType, Nullable: true}
		values := make([]string, count)
		for row := range values {
			value := "12345"
			switch dataType.ID() {
			case arrow.BOOL:
				value = "true"
			case arrow.FLOAT32, arrow.FLOAT64:
				value = "123.456"
			case arrow.STRING:
				value = `"catalog-sales-value"`
			case arrow.DECIMAL128:
				value = `"12345.67"`
			}
			if nulls && row%20 == 0 {
				value = "null"
			}
			values[row] = value
		}
		columnArray, _, err := array.FromJSON(memory.DefaultAllocator, dataType, strings.NewReader("["+strings.Join(values, ",")+"]"))
		if err != nil {
			b.Fatal(err)
		}
		columns[column] = columnArray
		defer columnArray.Release()
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, int64(count))
}

func BenchmarkKernelRows(b *testing.B) {
	decimalType := &arrow.Decimal128Type{Precision: 18, Scale: 2}
	catalog := make([]arrow.DataType, 0, 34)
	for column := 0; column < 18; column++ {
		catalog = append(catalog, arrow.PrimitiveTypes.Int32)
	}
	catalog = append(catalog, arrow.PrimitiveTypes.Int64)
	withoutDecimals := append([]arrow.DataType(nil), catalog...)
	for column := 0; column < 15; column++ {
		catalog = append(catalog, decimalType)
	}
	scenarios := []struct {
		name  string
		types []arrow.DataType
	}{
		{"catalog", catalog},
		{"catalog-no-decimals", withoutDecimals},
		{"boolean", []arrow.DataType{arrow.FixedWidthTypes.Boolean}},
		{"int32", []arrow.DataType{arrow.PrimitiveTypes.Int32}},
		{"float32", []arrow.DataType{arrow.PrimitiveTypes.Float32}},
		{"float64", []arrow.DataType{arrow.PrimitiveTypes.Float64}},
		{"string", []arrow.DataType{arrow.BinaryTypes.String}},
	}
	for _, scenario := range scenarios {
		for _, nulls := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/nulls=%t", scenario.name, nulls), func(b *testing.B) {
				record := benchmarkRecord(b, scenario.types, 4096, nulls)
				defer record.Release()
				destination := make([]driver.Value, len(scenario.types))
				columns := make([]string, len(scenario.types))
				for column := range columns {
					columns[column] = record.ColumnName(column)
				}
				ntz := make([]bool, len(scenario.types))
				geoBinary := make([]bool, len(scenario.types))
				operation := &kernelOp{location: time.UTC}
				keys := arrowscan.NewStructKeyCache()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					rows := kernelRows{cur: record, op: operation, cols: columns, ntz: ntz, geoBinary: geoBinary, keyCache: keys}
					for row := 0; row < 4096; row++ {
						if err := rows.next(destination); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
