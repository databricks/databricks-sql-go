package arrowbased

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

func TestSchemaBytesFallbackTypes(t *testing.T) {
	columns := []struct {
		name      string
		typeID    cli_service.TTypeId
		arrowType arrow.DataType
	}{
		{"boolean", cli_service.TTypeId_BOOLEAN_TYPE, arrow.FixedWidthTypes.Boolean},
		{"tinyint", cli_service.TTypeId_TINYINT_TYPE, arrow.PrimitiveTypes.Int8},
		{"smallint", cli_service.TTypeId_SMALLINT_TYPE, arrow.PrimitiveTypes.Int16},
		{"int", cli_service.TTypeId_INT_TYPE, arrow.PrimitiveTypes.Int32},
		{"bigint", cli_service.TTypeId_BIGINT_TYPE, arrow.PrimitiveTypes.Int64},
		{"float", cli_service.TTypeId_FLOAT_TYPE, arrow.PrimitiveTypes.Float32},
		{"double", cli_service.TTypeId_DOUBLE_TYPE, arrow.PrimitiveTypes.Float64},
		{"string", cli_service.TTypeId_STRING_TYPE, arrow.BinaryTypes.String},
		{"char", cli_service.TTypeId_CHAR_TYPE, arrow.BinaryTypes.String},
		{"varchar", cli_service.TTypeId_VARCHAR_TYPE, arrow.BinaryTypes.String},
		{"binary", cli_service.TTypeId_BINARY_TYPE, arrow.BinaryTypes.Binary},
		{"date", cli_service.TTypeId_DATE_TYPE, arrow.FixedWidthTypes.Date32},
		{"null", cli_service.TTypeId_NULL_TYPE, arrow.Null},
		{"decimal", cli_service.TTypeId_DECIMAL_TYPE, arrow.BinaryTypes.String},
		{"timestamp", cli_service.TTypeId_TIMESTAMP_TYPE, arrow.BinaryTypes.String},
		{"array", cli_service.TTypeId_ARRAY_TYPE, arrow.BinaryTypes.String},
		{"map", cli_service.TTypeId_MAP_TYPE, arrow.BinaryTypes.String},
		{"struct", cli_service.TTypeId_STRUCT_TYPE, arrow.BinaryTypes.String},
		{"interval", cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE, arrow.BinaryTypes.String},
	}
	for _, nativeDecimal := range []bool{false, true} {
		for _, nativeTimestamp := range []bool{false, true} {
			t.Run("decimal="+strconv.FormatBool(nativeDecimal)+"/timestamp="+strconv.FormatBool(nativeTimestamp), func(t *testing.T) {
				cfg := config.ArrowConfig{UseArrowNativeDecimal: nativeDecimal, UseArrowNativeTimestamp: nativeTimestamp}
				metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{}}
				fields := make([]arrow.Field, len(columns))
				for index, column := range columns {
					entry := &cli_service.TPrimitiveTypeEntry{Type: column.typeID}
					dataType := column.arrowType
					if column.typeID == cli_service.TTypeId_DECIMAL_TYPE {
						precision, scale := int32(38), int32(9)
						entry.TypeQualifiers = &cli_service.TTypeQualifiers{Qualifiers: map[string]*cli_service.TTypeQualifierValue{
							"precision": {I32Value: &precision}, "scale": {I32Value: &scale},
						}}
						if nativeDecimal {
							dataType = &arrow.Decimal128Type{Precision: precision, Scale: scale}
						}
					}
					if column.typeID == cli_service.TTypeId_TIMESTAMP_TYPE && nativeTimestamp {
						dataType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Etc/UTC"}
					}
					metadata.Schema.Columns = append(metadata.Schema.Columns, &cli_service.TColumnDesc{
						ColumnName: column.name,
						TypeDesc:   &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: entry}}},
					})
					fields[index] = arrow.Field{Name: column.name, Type: dataType, Nullable: true}
				}
				expected := arrow.NewSchema(fields, nil)
				for _, schemaBytes := range [][]byte{nil, {}} {
					metadata.ArrowSchema = schemaBytes
					serialized, schemaErr := GetArrowSchemaBytes(context.Background(), metadata, cfg)
					require.NoError(t, schemaErr)
					reader, err := ipc.NewReader(bytes.NewReader(serialized))
					require.NoError(t, err)
					require.True(t, expected.Equal(reader.Schema()), "expected=%s actual=%s", expected, reader.Schema())
					reader.Release()
				}
				var server bytes.Buffer
				writer := ipc.NewWriter(&server, ipc.WithSchema(expected))
				require.NoError(t, writer.Close())
				metadata.ArrowSchema = server.Bytes()
				serialized, err := GetArrowSchemaBytes(context.Background(), metadata, cfg)
				require.NoError(t, err)
				require.Equal(t, server.Bytes(), serialized)
			})
		}
	}
}

func TestSchemaBytesFallbackError(t *testing.T) {
	metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
		ColumnName: "amount",
		TypeDesc:   &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_DECIMAL_TYPE}}}},
	}}}}
	schemaBytes, err := GetArrowSchemaBytes(context.Background(), metadata, config.ArrowConfig{UseArrowNativeDecimal: true})
	require.Error(t, err)
	require.Empty(t, schemaBytes)
}

func TestSchemaBytesNativeTypeLimitations(t *testing.T) {
	for _, typeID := range []cli_service.TTypeId{cli_service.TTypeId_ARRAY_TYPE, cli_service.TTypeId_MAP_TYPE, cli_service.TTypeId_STRUCT_TYPE, cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE, cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE} {
		t.Run(typeID.String(), func(t *testing.T) {
			column := &cli_service.TColumnDesc{ColumnName: "value", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: typeID}}}}}
			cfg := config.ArrowConfig{UseArrowNativeComplexTypes: true, UseArrowNativeIntervalTypes: true}
			_, expectedErr := tColumnDescToArrowDataType(column, &cfg)
			require.Error(t, expectedErr)
			metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{column}}}
			schemaBytes, err := GetArrowSchemaBytes(context.Background(), metadata, cfg)
			require.ErrorContains(t, err, expectedErr.Error())
			require.Empty(t, schemaBytes)
		})
	}
}

func TestSchemaPeekLeavesRecordBodyUnread(t *testing.T) {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(make([]int64, 10000), nil)
	values := builder.NewArray()
	defer values.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 10000)
	defer record.Release()
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	header, _, err := peekIPCSchema(bytes.NewReader(output.Bytes()))
	require.NoError(t, err)
	require.NotEmpty(t, header)
	payload := output.Bytes()[len(header):]
	source := bytes.NewReader(payload)
	header, replay, err := peekIPCSchema(source)
	require.NoError(t, err)
	require.Empty(t, header)
	require.Greater(t, source.Len(), 80000, "only message metadata may be read")
	actual, err := io.ReadAll(replay)
	require.NoError(t, err)
	require.Equal(t, payload, actual)
}
