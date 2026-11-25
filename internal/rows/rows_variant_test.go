package rows

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnTypeDatabaseTypeNameVariant covers the Thrift VARIANT type name. The server
// sends VARIANT as a string column tagged with Spark:DataType:SqlName=VARIANT in the
// Arrow result schema. VARIANT values are JSON text on every result format, so the tag
// alone decides the name, while TIMESTAMP_NTZ keeps its Arrow-decoding requirement.
// No row is fetched, so this also covers a zero-row result set.
func TestColumnTypeDatabaseTypeNameVariant(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"Spark:DataType:SqlName": "VARIANT",
		})},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "ntz", Type: arrow.FixedWidthTypes.Timestamp_us, Metadata: arrow.MetadataFrom(map[string]string{
			"Spark:DataType:SqlName": "TIMESTAMP_NTZ",
		})},
	}, nil)
	var schemaBytes bytes.Buffer
	writer := ipc.NewWriter(&schemaBytes, ipc.WithSchema(arrowSchema))
	require.NoError(t, writer.Close())

	schema := &cli_service.TTableSchema{}
	for i, typeID := range []cli_service.TTypeId{
		cli_service.TTypeId_STRING_TYPE,
		cli_service.TTypeId_STRING_TYPE,
		cli_service.TTypeId_TIMESTAMP_TYPE,
	} {
		schema.Columns = append(schema.Columns, &cli_service.TColumnDesc{
			ColumnName: arrowSchema.Field(i).Name,
			TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
				PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: typeID},
			}}},
		})
	}

	format := func(f cli_service.TSparkRowSetType) *cli_service.TSparkRowSetType { return &f }
	noArrowConfig := config.WithDefaults()
	noArrowConfig.UseArrowBatches = false

	for _, tc := range []struct {
		name        string
		cfg         *config.Config
		arrowSchema []byte
		format      *cli_service.TSparkRowSetType
		want        []string
	}{
		{"arrow result, format unset", config.WithDefaults(), schemaBytes.Bytes(), nil,
			[]string{"VARIANT", "STRING", "TIMESTAMP_NTZ"}},
		{"arrow based set", config.WithDefaults(), schemaBytes.Bytes(), format(cli_service.TSparkRowSetType_ARROW_BASED_SET),
			[]string{"VARIANT", "STRING", "TIMESTAMP_NTZ"}},
		{"url based set", config.WithDefaults(), schemaBytes.Bytes(), format(cli_service.TSparkRowSetType_URL_BASED_SET),
			[]string{"VARIANT", "STRING", "TIMESTAMP_NTZ"}},
		{"column based set", config.WithDefaults(), schemaBytes.Bytes(), format(cli_service.TSparkRowSetType_COLUMN_BASED_SET),
			[]string{"VARIANT", "STRING", "TIMESTAMP"}},
		{"arrow batches disabled", noArrowConfig, schemaBytes.Bytes(), nil,
			[]string{"VARIANT", "STRING", "TIMESTAMP"}},
		{"no arrow schema", config.WithDefaults(), nil, nil,
			[]string{"STRING", "STRING", "TIMESTAMP"}},
		{"invalid arrow schema", config.WithDefaults(), []byte("invalid arrow schema"), nil,
			[]string{"STRING", "STRING", "TIMESTAMP"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rowSet := &rows{
				client: &client.TestClient{},
				config: tc.cfg,
				schema: schema,
				resultSetMetadata: &cli_service.TGetResultSetMetadataResp{
					Schema:       schema,
					ArrowSchema:  tc.arrowSchema,
					ResultFormat: tc.format,
				},
			}
			for i, want := range tc.want {
				assert.Equal(t, want, rowSet.ColumnTypeDatabaseTypeName(i), "column %d", i)
			}
			// The VARIANT column scans as a string, so it must keep the STRING scan
			// type and length.
			assert.Equal(t, rowSet.ColumnTypeScanType(1), rowSet.ColumnTypeScanType(0), "scan type")
			variantLen, variantHasLen := rowSet.ColumnTypeLength(0)
			stringLen, stringHasLen := rowSet.ColumnTypeLength(1)
			assert.Equal(t, stringLen, variantLen, "length")
			assert.Equal(t, stringHasLen, variantHasLen, "length presence")
		})
	}
}
