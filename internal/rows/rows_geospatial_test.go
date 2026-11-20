package rows

import (
	"bytes"
	"reflect"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

func TestColumnTypeDatabaseTypeNameGeospatial(t *testing.T) {
	fields := []arrow.Field{}
	for _, name := range []string{"GEOMETRY(0)", "GEOGRAPHY(4326)", "GEOMETRY(3857)", "GEOMETRY(ANY)"} {
		fields = append(fields, arrow.Field{Name: "duplicate", Type: arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{"Spark:DataType:SqlName": name})})
	}
	fields = append(fields, arrow.Field{Name: "plain", Type: arrow.BinaryTypes.String})
	fields = append(fields, arrow.Field{Name: "nested", Type: arrow.StructOf(fields[0])})
	var schemaBytes bytes.Buffer
	writer := ipc.NewWriter(&schemaBytes, ipc.WithSchema(arrow.NewSchema(fields, nil)))
	require.NoError(t, writer.Close())
	schema := &cli_service.TTableSchema{}
	for _, field := range fields {
		typeID := cli_service.TTypeId_STRING_TYPE
		if field.Type.ID() == arrow.STRUCT {
			typeID = cli_service.TTypeId_STRUCT_TYPE
		}
		schema.Columns = append(schema.Columns, &cli_service.TColumnDesc{
			ColumnName: field.Name,
			TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
				PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: typeID},
			}}},
		})
	}
	format := func(value cli_service.TSparkRowSetType) *cli_service.TSparkRowSetType { return &value }
	for _, test := range []struct {
		name     string
		format   *cli_service.TSparkRowSetType
		schema   []byte
		noArrow  bool
		fallback bool
	}{
		{name: "format unset", schema: schemaBytes.Bytes()},
		{name: "inline", schema: schemaBytes.Bytes(), format: format(cli_service.TSparkRowSetType_ARROW_BASED_SET)},
		{name: "CloudFetch", schema: schemaBytes.Bytes(), format: format(cli_service.TSparkRowSetType_URL_BASED_SET)},
		{name: "column based", schema: schemaBytes.Bytes(), format: format(cli_service.TSparkRowSetType_COLUMN_BASED_SET)},
		{name: "Arrow disabled", schema: schemaBytes.Bytes(), noArrow: true},
		{name: "missing schema", fallback: true},
		{name: "invalid schema", schema: []byte("invalid Arrow schema"), fallback: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.WithDefaults()
			cfg.UseArrowBatches = !test.noArrow
			resultRows := &rows{client: &client.TestClient{}, config: cfg, schema: schema,
				resultSetMetadata: &cli_service.TGetResultSetMetadataResp{
					Schema: schema, ArrowSchema: test.schema, ResultFormat: test.format,
				}}
			var readers sync.WaitGroup
			for range 8 {
				readers.Go(func() {
					for index := range 4 {
						want := fields[index].Metadata.Values()[0]
						if test.fallback {
							want = "STRING"
						}
						if got := resultRows.ColumnTypeDatabaseTypeName(index); got != want {
							t.Errorf("column %d = %q, want %q", index, got, want)
						}
					}
				})
			}
			readers.Wait()
			for index := range 4 {
				require.Equal(t, reflect.TypeOf(""), resultRows.ColumnTypeScanType(index))
				length, hasLength := resultRows.ColumnTypeLength(index)
				plainLength, plainHasLength := resultRows.ColumnTypeLength(4)
				require.Equal(t, plainLength, length)
				require.Equal(t, plainHasLength, hasLength)
			}
			require.Equal(t, "STRING", resultRows.ColumnTypeDatabaseTypeName(4))
			require.Equal(t, "STRUCT", resultRows.ColumnTypeDatabaseTypeName(5))
		})
	}
}
