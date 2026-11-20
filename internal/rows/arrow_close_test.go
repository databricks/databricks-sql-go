package rows

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestArrowCloseBeforeFirstUse(test *testing.T) {
	for _, accessor := range []string{"ipc", "batches"} {
		for _, direct := range []bool{false, true} {
			test.Run(fmt.Sprintf("%s/direct=%t", accessor, direct), func(test *testing.T) {
				closes := 0
				cli := &client.TestClient{FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					closes++
					return &cli_service.TCloseOperationResp{}, nil
				}}
				var downloads atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { downloads.Add(1) }))
				defer server.Close()
				var initial *cli_service.TSparkDirectResults
				if direct {
					format := cli_service.TSparkRowSetType_URL_BASED_SET
					initial = &cli_service.TSparkDirectResults{
						ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{ResultFormat: &format, Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{decimalMetadataColumn(cli_service.TTypeId_INT_TYPE, nil, nil)}}},
						ResultSet:         &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: 1, ExpiryTime: time.Now().Add(time.Hour).Unix()}}}},
					}
				}
				result, rowsErr := NewRows(context.Background(), nil, cli, config.WithDefaults(), initial, nil)
				require.NoError(test, rowsErr)
				defer func() { require.NoError(test, result.Close()) }()
				if accessor == "batches" {
					iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
					require.NoError(test, err)
					iterator.Close()
					iterator.Close()
					require.False(test, iterator.HasNext())
					_, err = iterator.Next()
					require.ErrorIs(test, err, io.EOF)
					require.Equal(test, 1, closes)
				} else {
					iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
					require.NoError(test, err)
					iterator.Close()
					iterator.Close()
					require.False(test, iterator.HasNext())
					_, err = iterator.Next()
					require.ErrorIs(test, err, io.EOF)
					require.Zero(test, closes)
				}
				require.NoError(test, result.Close())
				require.Equal(test, 1, closes)
				require.Zero(test, downloads.Load())
			})
		}
	}
}
