//go:build cgo && databricks_kernel

package kernel

import (
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestDecimalMetadataKernelSchema(t *testing.T) {
	for _, rowCount := range []int{0, 1} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("rows=%d/text=%t", rowCount, text), func(t *testing.T) {
				pairs := [][2]int32{{10, 2}, {38, 0}, {38, 38}}
				fields := make([]arrow.Field, len(pairs))
				for index, pair := range pairs {
					fields[index] = arrow.Field{Type: &arrow.Decimal128Type{Precision: pair[0], Scale: pair[1]}}
					if text {
						fields[index].Type = arrow.BinaryTypes.String
						fields[index].Metadata = arrow.MetadataFrom(map[string]string{
							"databricks.type_name": "DECIMAL", "databricks.type_precision": fmt.Sprint(pair[0]), "databricks.type_scale": fmt.Sprint(pair[1]),
						})
					}
				}
				schema := arrow.NewSchema(fields, nil)
				resultRows := &kernelRows{schema: schema}
				if rowCount > 0 {
					builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
					defer builder.Release()
					for _, field := range builder.Fields() {
						field.AppendNull()
					}
					resultRows.cur = builder.NewRecordBatch()
					defer resultRows.cur.Release()
				}
				decimalRows, supported := any(resultRows).(driver.RowsColumnTypePrecisionScale)
				require.True(t, supported, "kernel rows must expose declared DECIMAL qualifiers")
				for index, pair := range pairs {
					precision, scale, ok := decimalRows.ColumnTypePrecisionScale(index)
					require.True(t, ok)
					require.Equal(t, int64(pair[0]), precision)
					require.Equal(t, int64(pair[1]), scale)
				}
			})
		}
	}
}

func TestDecimalMetadataKernelUnavailable(t *testing.T) {
	for name, field := range map[string]arrow.Field{
		"integer": {Type: arrow.PrimitiveTypes.Int64},
		"string":  {Type: arrow.BinaryTypes.String},
		"float":   {Type: arrow.PrimitiveTypes.Float64},
		"nested":  {Type: arrow.ListOf(&arrow.Decimal128Type{Precision: 10, Scale: 2})},
		"missing_scale": {Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "DECIMAL", "databricks.type_precision": "10",
		})},
		"malformed_precision": {Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "DECIMAL", "databricks.type_precision": "bad", "databricks.type_scale": "2",
		})},
		"malformed_scale": {Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "DECIMAL", "databricks.type_precision": "10", "databricks.type_scale": "bad",
		})},
		"missing_precision": {Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "DECIMAL", "databricks.type_scale": "0",
		})},
		"non_decimal_qualifiers": {Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "STRING", "databricks.type_precision": "10", "databricks.type_scale": "2",
		})},
	} {
		t.Run(name, func(t *testing.T) {
			resultRows := &kernelRows{schema: arrow.NewSchema([]arrow.Field{field}, nil)}
			decimalRows, supported := any(resultRows).(driver.RowsColumnTypePrecisionScale)
			require.True(t, supported)
			for _, index := range []int{-1, 0, 1} {
				precision, scale, ok := decimalRows.ColumnTypePrecisionScale(index)
				require.Equal(t, int64(0), precision)
				require.Equal(t, int64(0), scale)
				require.False(t, ok)
			}
		})
	}
}

func TestDecimalMetadataKernelManifestPriority(t *testing.T) {
	for _, precisionText := range []string{"38", "invalid"} {
		t.Run(precisionText, func(t *testing.T) {
			field := arrow.Field{
				Type: &arrow.Decimal128Type{Precision: 10, Scale: 2},
				Metadata: arrow.MetadataFrom(map[string]string{
					"databricks.type_name": "DECIMAL", "databricks.type_precision": precisionText, "databricks.type_scale": "0",
				}),
			}
			resultRows := &kernelRows{schema: arrow.NewSchema([]arrow.Field{field}, nil)}
			decimalRows, supported := any(resultRows).(driver.RowsColumnTypePrecisionScale)
			require.True(t, supported)
			precision, scale, ok := decimalRows.ColumnTypePrecisionScale(0)
			require.True(t, ok)
			if precisionText == "invalid" {
				require.Equal(t, int64(10), precision)
			} else {
				require.Equal(t, int64(38), precision)
			}
			require.Equal(t, int64(0), scale)
		})
	}
}
