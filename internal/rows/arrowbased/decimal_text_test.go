package arrowbased

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/stretchr/testify/require"
)

func decimalTextScanner(tb testing.TB, value string, scale *int32, dbType cli_service.TTypeId) *arrowRowScanner {
	tb.Helper()
	qualifiers := &cli_service.TTypeQualifiers{Qualifiers: map[string]*cli_service.TTypeQualifierValue{}}
	if scale != nil {
		qualifiers.Qualifiers["scale"] = &cli_service.TTypeQualifierValue{I32Value: scale}
	}
	schema := &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
		ColumnName: "amount",
		TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
			PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: dbType, TypeQualifiers: qualifiers},
		}}},
	}}}
	rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{RowCount: 2}}}
	scanner, err := NewArrowRowScanner(getMetadataResp(schema), rowSet, &config.Config{}, nil, context.Background(), nil)
	require.NoError(tb, err)
	result := scanner.(*arrowRowScanner)
	tb.Cleanup(result.Close)
	delimiter := rowscanner.NewDelimiter(0, 2)
	require.NoError(tb, result.makeColumnValuesContainers(result, delimiter))
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Append(value)
	builder.AppendNull()
	values := builder.NewStringArray()
	defer values.Release()
	require.NoError(tb, result.rowValues.SetColumnValues(0, values.Data()))
	return result
}

func TestScanRowDecimalText(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		scale int32
		want  string
	}{
		{"zero", "0E-10", 2, "0.00"},
		{"sub_one", "0.5", 2, "0.50"},
		{"negative_sub_one", "-0.05", 2, "-0.05"},
		{"small_exponent", "1E-7", 7, "0.0000001"},
		{"trailing_zeros", "100.0", 2, "100.00"},
		{"wide", "123456789012345678901234567.89", 2, "123456789012345678901234567.89"},
		{"wide_exponent", "1.2345678901234567890123456789E+26", 2, "123456789012345678901234567.89"},
		{"negative_exponent", "-1e-7", 7, "-0.0000001"},
		{"max_precision", "99999999999999999999999999999999999999", 0, "99999999999999999999999999999999999999"},
		{"negative_max_precision", "-99999999999999999999999999999999999999", 0, "-99999999999999999999999999999999999999"},
		{"max_scale", "1E-38", 38, "0.00000000000000000000000000000000000001"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scanner := decimalTextScanner(t, test.value, &test.scale, cli_service.TTypeId_DECIMAL_TYPE)
			destination := make([]driver.Value, 1)
			require.NoError(t, scanner.ScanRow(destination, 0))
			require.Equal(t, test.want, destination[0])
			raw, err := scanner.rowValues.Value(0, 0)
			require.NoError(t, err)
			require.Equal(t, test.value, raw)
			require.NoError(t, scanner.ScanRow(destination, 1))
			require.Nil(t, destination[0])
		})
	}
}

func TestScanRowDecimalTextWithoutScale(t *testing.T) {
	for _, test := range []struct{ value, want string }{
		{"1E-7", "0.0000001"},
		{"1.2300E+5", "123000"},
		{"-1.2300e-2", "-0.012300"},
		{"100.00", "100.00"},
	} {
		t.Run(test.value, func(t *testing.T) {
			scanner := decimalTextScanner(t, test.value, nil, cli_service.TTypeId_DECIMAL_TYPE)
			destination := make([]driver.Value, 1)
			require.NoError(t, scanner.ScanRow(destination, 0))
			require.Equal(t, test.want, destination[0])
		})
	}
}

func TestScanRowNonDecimalTextUnchanged(t *testing.T) {
	scale := int32(7)
	scanner := decimalTextScanner(t, "1E-7", &scale, cli_service.TTypeId_STRING_TYPE)
	destination := make([]driver.Value, 1)
	require.NoError(t, scanner.ScanRow(destination, 0))
	require.Equal(t, "1E-7", destination[0])
	column := scanner.rowValues.(*rowValues).columnValueHolders[0]
	require.Equal(t, arrow.STRING, scanner.colInfo[0].arrowType.ID())
	raw, err := column.Value(0)
	require.NoError(t, err)
	require.Equal(t, "1E-7", raw)
}

func TestScanRowDecimalTextReportsConversionError(t *testing.T) {
	scale := int32(2)
	scanner := decimalTextScanner(t, "1E-3", &scale, cli_service.TTypeId_DECIMAL_TYPE)
	destination := make([]driver.Value, 1)
	err := scanner.ScanRow(destination, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "amount")
	require.NotContains(t, err.Error(), "1E-3")
}

func TestDecimalScaleQualifiersOptional(t *testing.T) {
	for _, qualifiers := range []*cli_service.TTypeQualifiers{
		nil,
		{},
		{Qualifiers: map[string]*cli_service.TTypeQualifierValue{"scale": nil}},
		{Qualifiers: map[string]*cli_service.TTypeQualifierValue{"scale": {}}},
	} {
		schema := &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
			TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
				PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_DECIMAL_TYPE, TypeQualifiers: qualifiers},
			}}},
		}}}
		arrowSchema, err := tTableSchemaToArrowSchema(schema, &config.ArrowConfig{})
		require.NoError(t, err)
		columns := getColumnInfo(arrowSchema, schema)
		require.Len(t, columns, 1)
		require.Nil(t, columns[0].decimalScale)
	}
}

func BenchmarkScanRowDecimalText(b *testing.B) {
	for _, test := range []struct {
		value string
		scale int32
	}{
		{"19.99", 2},
		{"1E-7", 7},
		{"0E-18", 18},
		{"123456789012345678901234567.89", 2},
	} {
		b.Run(test.value, func(b *testing.B) {
			scanner := decimalTextScanner(b, test.value, &test.scale, cli_service.TTypeId_DECIMAL_TYPE)
			destination := make([]driver.Value, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := scanner.ScanRow(destination, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkScanRowNonDecimalText(b *testing.B) {
	scanner := decimalTextScanner(b, "19.99", nil, cli_service.TTypeId_STRING_TYPE)
	destination := make([]driver.Value, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := scanner.ScanRow(destination, 0); err != nil {
			b.Fatal(err)
		}
	}
}
