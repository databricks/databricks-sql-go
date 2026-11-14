package rows

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowAPIsCompleteDirectResults(t *testing.T) {
	for _, delivery := range []string{"inline", "cloudfetch"} {
		for _, api := range []string{"batches", "ipc streams"} {
			t.Run(delivery+"/"+api, func(t *testing.T) {
				var execute cli_service.TExecuteStatementResp
				loadTestData(t, "directResultsMultipleFetch/ExecuteStatement.json", &execute)
				direct := execute.DirectResults
				noMoreRows := false
				direct.ResultSet.HasMoreRows = &noMoreRows
				direct.CloseOperation = nil
				var wantCounts []int64
				for _, batch := range direct.ResultSet.Results.ArrowBatches {
					wantCounts = append(wantCounts, batch.RowCount)
				}

				if delivery == "cloudfetch" {
					files := make(map[string][]byte)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						data, ok := files[r.URL.Path]
						if !ok {
							http.NotFound(w, r)
							return
						}
						_, _ = w.Write(data)
					}))
					defer server.Close()
					var start int64
					for i, batch := range direct.ResultSet.Results.ArrowBatches {
						path := "/" + strconv.Itoa(i)
						files[path] = bytes.Join([][]byte{direct.ResultSetMetadata.ArrowSchema, batch.Batch}, nil)
						direct.ResultSet.Results.ResultLinks = append(direct.ResultSet.Results.ResultLinks, &cli_service.TSparkArrowResultLink{
							FileLink: server.URL + path, StartRowOffset: start, RowCount: batch.RowCount,
							ExpiryTime: time.Now().Add(time.Hour).Unix(),
						})
						start += batch.RowCount
					}
					direct.ResultSet.Results.ArrowBatches = nil
					format := cli_service.TSparkRowSetType_URL_BASED_SET
					direct.ResultSetMetadata.ResultFormat = &format
				}

				fetches, closes := 0, 0
				op := &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{GUID: make([]byte, 16)}}
				testClient := &client.TestClient{
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						fetches++
						return nil, io.ErrUnexpectedEOF
					},
					FnCloseOperation: func(_ context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						closes++
						assert.Same(t, op, req.OperationHandle)
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				dr, err := NewRows(context.Background(), op, testClient, config.WithDefaults(), direct, nil)
				require.NoError(t, err)
				defer dr.Close() //nolint:errcheck
				publicRows := dr.(dbsqlrows.Rows)
				var gotCounts []int64
				if api == "batches" {
					batches, batchErr := publicRows.GetArrowBatches(context.Background())
					require.NoError(t, batchErr)
					defer batches.Close()
					for batches.HasNext() {
						record, nextErr := batches.Next()
						require.NoError(t, nextErr)
						gotCounts = append(gotCounts, record.NumRows())
						record.Release()
						if len(gotCounts) < len(wantCounts) {
							assert.Zero(t, closes, "local data must be consumed before cleanup")
						}
					}
					_, nextErr := batches.Next()
					assert.ErrorIs(t, nextErr, io.EOF)
					assert.False(t, batches.HasNext())
					batches.Close()
				} else {
					streams, streamErr := publicRows.GetArrowIPCStreams(context.Background())
					require.NoError(t, streamErr)
					defer streams.Close()
					for streams.HasNext() {
						stream, nextErr := streams.Next()
						require.NoError(t, nextErr)
						reader, readErr := ipc.NewReader(stream)
						require.NoError(t, readErr)
						for reader.Next() {
							gotCounts = append(gotCounts, reader.RecordBatch().NumRows())
						}
						readErr = reader.Err()
						reader.Release()
						require.NoError(t, readErr)
						assert.Zero(t, closes, "local data must be consumed before cleanup")
					}
					_, nextErr := streams.Next()
					assert.ErrorIs(t, nextErr, io.EOF)
					assert.False(t, streams.HasNext())
					streams.Close()
				}
				assert.Equal(t, wantCounts, gotCounts)
				assert.Zero(t, fetches)
				assert.Equal(t, 1, closes, "exhaustion must close the server operation")
				require.NoError(t, dr.Close())
				assert.Equal(t, 1, closes, "parent cleanup must not close the operation twice")
				assert.Zero(t, fetches)
			})
		}
	}
}
