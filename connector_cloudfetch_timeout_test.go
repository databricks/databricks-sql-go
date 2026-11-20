package dbsql

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	driverrows "github.com/databricks/databricks-sql-go/internal/rows"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFetchClientTimeout(t *testing.T) {
	for _, transport := range []string{"default", "WithTransport"} {
		for _, stall := range []string{"headers", "body"} {
			for _, api := range []string{"rows", "batches", "streams"} {
				t.Run(transport+"/"+stall+"/"+api, func(t *testing.T) {
					release := make(chan struct{})
					cancelled := make(chan struct{}, 2)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if stall == "body" {
							w.Header().Set("Content-Length", "100")
							w.WriteHeader(http.StatusOK)
							w.(http.Flusher).Flush()
						}
						select {
						case <-r.Context().Done():
							cancelled <- struct{}{}
						case <-release:
						}
					}))
					defer server.Close()
					defer close(release)

					options := []ConnOption{func(cfg *config.Config) {
						cfg.ClientTimeout = 100 * time.Millisecond
						cfg.RetryMax = 0
						cfg.UseLz4Compression = false
						cfg.MaxDownloadThreads = 2
					}}
					if transport == "WithTransport" {
						options = append(options, WithTransport(http.DefaultTransport))
					}
					con, err := NewConnector(options...)
					require.NoError(t, err)
					cfg := con.(*connector).cfg
					fixture, err := os.ReadFile("internal/rows/testdata/directResultsMultipleFetch/ExecuteStatement.json")
					require.NoError(t, err)
					var execute cli_service.TExecuteStatementResp
					require.NoError(t, json.Unmarshal(fixture, &execute))
					direct := execute.DirectResults
					direct.ResultSet.Results.ArrowBatches = nil
					direct.ResultSet.Results.ResultLinks = []*cli_service.TSparkArrowResultLink{
						{FileLink: server.URL, ExpiryTime: time.Now().Add(time.Hour).Unix(), RowCount: 1},
						{FileLink: server.URL, ExpiryTime: time.Now().Add(time.Hour).Unix(), RowCount: 1, StartRowOffset: 1},
					}
					format := cli_service.TSparkRowSetType_URL_BASED_SET
					direct.ResultSetMetadata.ResultFormat = &format
					testClient := &client.TestClient{
						FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
							return &cli_service.TCloseOperationResp{}, nil
						},
					}
					rowSet, err := driverrows.NewRows(context.Background(), nil, testClient, cfg, direct, nil)
					require.NoError(t, err)
					defer rowSet.Close() //nolint:errcheck

					var next func() error
					switch api {
					case "rows":
						next = func() error { return rowSet.Next(make([]driver.Value, len(rowSet.Columns()))) }
					case "batches":
						batches, batchErr := rowSet.(dbsqlrows.Rows).GetArrowBatches(context.Background())
						require.NoError(t, batchErr)
						defer batches.Close()
						next = func() error {
							record, nextErr := batches.Next()
							if record != nil {
								record.Release()
							}
							return nextErr
						}
					case "streams":
						streams, streamErr := rowSet.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
						require.NoError(t, streamErr)
						defer streams.Close()
						next = func() error { _, nextErr := streams.Next(); return nextErr }
					}

					result := make(chan error, 1)
					start := time.Now()
					go func() { result <- next() }()
					select {
					case nextErr := <-result:
						assert.ErrorIs(t, nextErr, dbsqlerr.DriverError)
						assert.ErrorIs(t, nextErr, context.DeadlineExceeded)
						assert.GreaterOrEqual(t, time.Since(start), cfg.ClientTimeout)
					case <-time.After(2 * time.Second):
						server.CloseClientConnections()
						select {
						case nextErr := <-result:
							assert.Error(t, nextErr)
						case <-time.After(2 * time.Second):
							t.Fatal("CloudFetch read did not exit after connections closed")
						}
						t.Fatal("CloudFetch read did not honor ClientTimeout")
					}
					for range 2 {
						select {
						case <-cancelled:
						case <-time.After(2 * time.Second):
							t.Fatal("in-flight CloudFetch request was not cancelled")
						}
					}
				})
			}
		}
	}
}

func TestCloudFetchWithTransportPreservesHTTPClient(t *testing.T) {
	for _, timeout := range []time.Duration{0, 5 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			customClient := &http.Client{Timeout: timeout}
			con, err := NewConnector(func(cfg *config.Config) { cfg.HTTPClient = customClient }, WithTransport(http.DefaultTransport))
			require.NoError(t, err)
			assert.Same(t, customClient, con.(*connector).cfg.HTTPClient)
			assert.Equal(t, timeout, customClient.Timeout)
		})
	}
}
