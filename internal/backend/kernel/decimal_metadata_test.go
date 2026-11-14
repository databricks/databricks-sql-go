//go:build cgo && databricks_kernel

package kernel

import (
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	"github.com/stretchr/testify/require"
)

func TestDecimalMetadataKernelSchema(t *testing.T) {
	for _, metadata := range []bool{false, true} {
		t.Run(fmt.Sprintf("metadata=%t", metadata), func(t *testing.T) {
			pairs := [][2]int32{{10, 2}, {38, 0}, {38, 38}}
			fields := make([]arrow.Field, len(pairs))
			columnTypes := make([]arrowscan.ColumnTypeInfo, len(pairs))
			for index, pair := range pairs {
				fields[index] = arrow.Field{Type: &arrow.Decimal128Type{Precision: pair[0], Scale: pair[1]}}
				if metadata {
					fields[index].Metadata = arrow.MetadataFrom(map[string]string{
						"databricks.type_name": "DECIMAL", "databricks.type_precision": fmt.Sprint(pair[0]), "databricks.type_scale": fmt.Sprint(pair[1]),
					})
				}
				columnTypes[index] = arrowscan.ColumnTypeInfoForField(fields[index])
			}
			schema := arrow.NewSchema(fields, nil)
			resultRows := &kernelRows{schema: schema, colTypes: columnTypes}
			decimalRows, supported := any(resultRows).(driver.RowsColumnTypePrecisionScale)
			require.True(t, supported, "kernel rows must expose declared DECIMAL qualifiers")
			for index, pair := range pairs {
				precision, scale, ok := decimalRows.ColumnTypePrecisionScale(index)
				require.True(t, ok)
				require.Equal(t, int64(pair[0]), precision)
				require.Equal(t, int64(pair[1]), scale)
				require.Equal(t, "DECIMAL", resultRows.ColumnTypeDatabaseTypeName(index))
				_, hasLength := resultRows.ColumnTypeLength(index)
				require.False(t, hasLength)
			}
		})
	}
}

func TestDecimalMetadataKernelUnavailable(t *testing.T) {
	for name, field := range map[string]arrow.Field{
		"integer":    {Type: arrow.PrimitiveTypes.Int64},
		"string":     {Type: arrow.BinaryTypes.String},
		"float":      {Type: arrow.PrimitiveTypes.Float64},
		"nested":     {Type: arrow.ListOf(&arrow.Decimal128Type{Precision: 10, Scale: 2})},
		"decimal32":  {Type: &arrow.Decimal32Type{Precision: 7, Scale: 2}},
		"decimal64":  {Type: &arrow.Decimal64Type{Precision: 12, Scale: 2}},
		"decimal256": {Type: &arrow.Decimal256Type{Precision: 40, Scale: 2}},
		"string_manifest_decimal": {Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "DECIMAL", "databricks.type_precision": "10", "databricks.type_scale": "2",
		})},
		"integer_manifest_decimal": {Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "DECIMAL", "databricks.type_precision": "10", "databricks.type_scale": "2",
		})},
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

func TestDecimalMetadataKernelUsesNativeType(t *testing.T) {
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
			require.Equal(t, int64(10), precision)
			require.Equal(t, int64(2), scale)
		})
	}
}
