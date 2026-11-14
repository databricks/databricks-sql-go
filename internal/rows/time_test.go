package rows

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/stretchr/testify/require"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
)

func TestTimeColumnMetadata(t *testing.T) {
	for _, dataType := range []arrow.DataType{arrow.FixedWidthTypes.Time32s, arrow.FixedWidthTypes.Time32ms, arrow.FixedWidthTypes.Time64us, arrow.FixedWidthTypes.Time64ns} {
		t.Run(dataType.String(), func(t *testing.T) {
			var schemaBytes bytes.Buffer
			writer := ipc.NewWriter(&schemaBytes, ipc.WithSchema(arrow.NewSchema([]arrow.Field{{Name: "clock", Type: dataType, Nullable: true}}, nil)))
			require.NoError(t, writer.Close())
			for _, format := range []cli_service.TSparkRowSetType{cli_service.TSparkRowSetType_ARROW_BASED_SET, cli_service.TSparkRowSetType_URL_BASED_SET} {
				rowSet := &rows{
					client: &client.TestClient{}, config: config.WithDefaults(), ctx: context.Background(),
					schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
						ColumnName: "clock", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_STRING_TYPE},
						}}},
					}}},
					resultSetMetadata: &cli_service.TGetResultSetMetadataResp{ArrowSchema: schemaBytes.Bytes(), ResultFormat: &format},
				}
				var readers sync.WaitGroup
				for range 8 {
					readers.Go(func() {
						if got := rowSet.ColumnTypeScanType(0); got != scanTypeDateTime {
							t.Errorf("scan type = %v, want time.Time", got)
						}
						if got := rowSet.ColumnTypeDatabaseTypeName(0); got != "TIME" {
							t.Errorf("database type = %q, want TIME", got)
						}
					})
				}
				readers.Wait()
				require.Equal(t, "TIME", rowSet.ColumnTypeDatabaseTypeName(0))
				length, hasLength := rowSet.ColumnTypeLength(0)
				require.Zero(t, length)
				require.False(t, hasLength)
				for _, fallback := range []string{"no Arrow", "columnar", "no schema"} {
					fallbackRows := &rows{client: rowSet.client, schema: rowSet.schema, ctx: rowSet.ctx, config: config.WithDefaults(), resultSetMetadata: &cli_service.TGetResultSetMetadataResp{ArrowSchema: schemaBytes.Bytes()}}
					switch fallback {
					case "no Arrow":
						fallbackRows.config.UseArrowBatches = false
					case "columnar":
						columnar := cli_service.TSparkRowSetType_COLUMN_BASED_SET
						fallbackRows.resultSetMetadata.ResultFormat = &columnar
					case "no schema":
						fallbackRows.resultSetMetadata.ArrowSchema = nil
					}
					require.Equal(t, "STRING", fallbackRows.ColumnTypeDatabaseTypeName(0), fallback)
					require.Equal(t, scanTypeString, fallbackRows.ColumnTypeScanType(0), fallback)
				}
				require.Equal(t, reflect.TypeOf(time.Time{}), rowSet.ColumnTypeScanType(0))
			}
		})
	}
}
