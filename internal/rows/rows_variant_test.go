package rows

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnTypeDatabaseTypeNameVariant covers the Thrift opt-in. The server sends
// VARIANT as a string column tagged with Spark:DataType:SqlName=VARIANT in the Arrow
// result schema. With the option off the column reports STRING; with it on, VARIANT.
// Scan type and length must not change either way. No row is fetched, so this also
// covers a zero-row result set.
func TestColumnTypeDatabaseTypeNameVariant(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"Spark:DataType:SqlName": "VARIANT",
		})},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
	var schemaBytes bytes.Buffer
	writer := ipc.NewWriter(&schemaBytes, ipc.WithSchema(arrowSchema))
	require.NoError(t, writer.Close())

	schema := &cli_service.TTableSchema{}
	for _, name := range []string{"v", "s"} {
		schema.Columns = append(schema.Columns, &cli_service.TColumnDesc{
			ColumnName: name,
			TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
				PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_STRING_TYPE},
			}}},
		})
	}

	type columnReport struct {
		name     string
		scanType reflect.Type
		length   int64
		hasLen   bool
	}
	reports := map[bool][]columnReport{}
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enableVariantTypeName=%t", enabled), func(t *testing.T) {
			cfg := config.WithDefaults()
			cfg.EnableVariantTypeName = enabled
			rowSet := &rows{
				client: &client.TestClient{},
				config: cfg,
				schema: schema,
				resultSetMetadata: &cli_service.TGetResultSetMetadataResp{
					Schema:      schema,
					ArrowSchema: schemaBytes.Bytes(),
				},
			}
			var got []columnReport
			for i := range schema.Columns {
				length, hasLen := rowSet.ColumnTypeLength(i)
				got = append(got, columnReport{
					name:     rowSet.ColumnTypeDatabaseTypeName(i),
					scanType: rowSet.ColumnTypeScanType(i),
					length:   length,
					hasLen:   hasLen,
				})
			}
			reports[enabled] = got

			wantVariant := "STRING"
			if enabled {
				wantVariant = "VARIANT"
			}
			assert.Equal(t, wantVariant, got[0].name, "VARIANT column type name")
			assert.Equal(t, "STRING", got[1].name, "plain string column must stay STRING")
		})
	}

	require.Len(t, reports[false], 2)
	require.Len(t, reports[true], 2)
	for i := range reports[false] {
		off, on := reports[false][i], reports[true][i]
		assert.Equal(t, off.scanType, on.scanType, "column %d scan type must not change with the option", i)
		assert.Equal(t, off.length, on.length, "column %d length must not change with the option", i)
		assert.Equal(t, off.hasLen, on.hasLen, "column %d length presence must not change with the option", i)
	}
}
