package rows

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
)

func BenchmarkGeospatialDatabaseTypeName(benchmark *testing.B) {
	field := arrow.Field{Name: "geo", Type: arrow.BinaryTypes.String,
		Metadata: arrow.MetadataFrom(map[string]string{"Spark:DataType:SqlName": "GEOMETRY(3857)"})}
	var schemaBytes bytes.Buffer
	writer := ipc.NewWriter(&schemaBytes, ipc.WithSchema(arrow.NewSchema([]arrow.Field{field}, nil)))
	if err := writer.Close(); err != nil {
		benchmark.Fatal(err)
	}
	metadata := &cli_service.TGetResultSetMetadataResp{ArrowSchema: schemaBytes.Bytes()}
	schema := &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
		ColumnName: "geo", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
			PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_STRING_TYPE},
		}}},
	}}}
	newRows := func() *rows {
		return &rows{client: &client.TestClient{}, config: config.WithDefaults(), schema: schema, resultSetMetadata: metadata}
	}
	benchmark.Run("cold", func(benchmark *testing.B) {
		benchmark.ReportAllocs()
		for range benchmark.N {
			if newRows().ColumnTypeDatabaseTypeName(0) == "" {
				benchmark.Fatal("missing type name")
			}
		}
	})
	benchmark.Run("cached", func(benchmark *testing.B) {
		resultRows := newRows()
		resultRows.ColumnTypeDatabaseTypeName(0)
		benchmark.ReportAllocs()
		benchmark.ResetTimer()
		for range benchmark.N {
			if resultRows.ColumnTypeDatabaseTypeName(0) == "" {
				benchmark.Fatal("missing type name")
			}
		}
	})
}
