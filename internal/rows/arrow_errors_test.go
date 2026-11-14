package rows

import (
	"context"
	"database/sql/driver"
	"io"
	"sync"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestArrowScannerFailureDoesNotPanic(t *testing.T) {
	for _, shape := range []string{"native-array", "invalid-decimal", "invalid-schema"} {
		for _, accessor := range []string{"ipc", "batches", "next"} {
			if shape == "invalid-schema" && accessor != "next" {
				continue
			}
			t.Run(shape+"/"+accessor, func(t *testing.T) {
				var page cli_service.TFetchResultsResp
				loadTestData(t, "multipleFetch/FetchResults1.json", &page)
				page.HasMoreRows = new(bool)
				page.ResultSetMetadata.ArrowSchema = nil
				typeID := cli_service.TTypeId_ARRAY_TYPE
				if shape == "invalid-decimal" {
					typeID = cli_service.TTypeId_DECIMAL_TYPE
				}
				page.ResultSetMetadata.Schema = &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{ColumnName: "value", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: typeID}}}}}}}
				if shape == "invalid-schema" {
					page.ResultSetMetadata.ArrowSchema = []byte("invalid Arrow schema")
				}
				cli := &client.TestClient{
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						return &page, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				cfg := config.WithDefaults()
				cfg.UseArrowNativeDecimal = true
				result, err := NewRows(context.Background(), nil, cli, cfg, nil, nil)
				require.NoError(t, err)
				require.NotPanics(t, func() {
					switch accessor {
					case "ipc":
						iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
						require.NoError(t, err)
						defer iterator.Close()
						_, err = iterator.SchemaBytes()
						require.Error(t, err)
						_, err = iterator.Next()
						require.Error(t, err)
					case "batches":
						iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
						require.NoError(t, err)
						defer iterator.Close()
						_, err = iterator.Schema()
						require.Error(t, err)
						_, err = iterator.Next()
						require.Error(t, err)
					case "next":
						require.Error(t, result.Next(make([]driver.Value, 1)))
						require.ErrorIs(t, result.Next(make([]driver.Value, 1)), io.EOF)
					}
					require.NoError(t, result.Close())
				})
			})
		}
	}
}

func TestArrowMetadataPublicationDuringResultFormat(t *testing.T) {
	r := &rows{}
	format := cli_service.TSparkRowSetType_ARROW_BASED_SET
	metadata := &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{}, ResultFormat: &format}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r.useServerArrowSchema(metadata) }()
	go func() {
		defer wg.Done()
		for range 1000 {
			r.ResultFormat()
		}
	}()
	wg.Wait()
	require.Equal(t, "INLINE_ARROW", r.ResultFormat())
}
