package rows

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

func TestCloudFetchServerShapedExpiry(t *testing.T) {
	for _, api := range []string{"rows", "batches", "ipc"} {
		for _, mode := range []string{"whole page", "one link", "closed near expiry", "open near expiry", "403 during download"} {
			t.Run(api+"/"+mode, func(t *testing.T) {
				fixture := newCloudFetchFixture(t)
				payloads := make(map[string][]byte)
				for _, link := range fixture.links {
					response, err := http.Get(link.FileLink)
					require.NoError(t, err)
					data, err := io.ReadAll(response.Body)
					require.NoError(t, response.Body.Close())
					require.NoError(t, err)
					payloads["/"+strconv.FormatInt(link.StartRowOffset, 10)] = data
				}
				var active, peak, oldGETs atomic.Int64
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					if request.URL.RawQuery == "old" {
						oldGETs.Add(1)
						if mode == "403 during download" {
							time.Sleep(1200 * time.Millisecond)
						}
						if mode != "closed near expiry" {
							writer.WriteHeader(http.StatusForbidden)
							return
						}
					}
					current := active.Add(1)
					defer active.Add(-1)
					for previous := peak.Load(); previous < current; previous = peak.Load() {
						if peak.CompareAndSwap(previous, current) {
							break
						}
					}
					time.Sleep(100 * time.Millisecond)
					_, _ = writer.Write(payloads[request.URL.Path])
				}))
				defer server.Close()
				for _, link := range fixture.links {
					link.FileLink = fmt.Sprintf("%s/%d?old", server.URL, link.StartRowOffset)
					remaining := -time.Minute
					if mode == "closed near expiry" || mode == "open near expiry" {
						remaining = 20 * time.Second
					}
					if mode == "403 during download" {
						remaining = 31 * time.Second
					}
					link.ExpiryTime = time.Now().Add(remaining).UnixMilli()
				}
				calls, closes := 0, 0
				cli := &client.TestClient{
					FnFetchResults: func(_ context.Context, request *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						calls++
						var fresh []*cli_service.TSparkArrowResultLink
						for _, original := range fixture.links {
							if original.StartRowOffset < *request.StartRowOffset {
								continue
							}
							link := *original
							link.FileLink = fmt.Sprintf("%s/%d?fresh", server.URL, link.StartRowOffset)
							link.ExpiryTime = time.Now().Add(15 * time.Minute).UnixMilli()
							fresh = append(fresh, &link)
							if mode == "one link" {
								break
							}
						}
						return &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{StartRowOffset: *request.StartRowOffset, ResultLinks: fresh}}, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						closes++
						return &cli_service.TCloseOperationResp{}, nil
					},
				}
				more := false
				direct := &cli_service.TSparkDirectResults{ResultSetMetadata: fixture.metadata,
					ResultSet: &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links}}}
				if mode == "closed near expiry" {
					direct.CloseOperation = &cli_service.TCloseOperationResp{}
				}
				cfg := config.WithDefaults()
				cfg.MaxDownloadThreads = 6
				cfg.UseLz4Compression = false
				result, rowsErr := NewRows(context.Background(), nil, cli, cfg, direct, nil)
				require.NoError(t, rowsErr)
				defer func() { require.NoError(t, result.Close()) }()
				started := time.Now()
				values, err := consumeCloudFetch(t, result, api)
				require.NoError(t, err)
				var expected []int64
				for value := int64(0); value < 18; value++ {
					expected = append(expected, value)
				}
				require.Equal(t, expected, values)
				wantCalls := 1
				if mode == "one link" {
					wantCalls = 6
				}
				if mode == "closed near expiry" {
					wantCalls = 0
				}
				require.Equal(t, wantCalls, calls)
				if mode == "whole page" {
					require.Equal(t, int64(6), peak.Load(), "renewed queued links must download in parallel")
					require.Zero(t, oldGETs.Load())
				}
				if mode == "403 during download" {
					require.Positive(t, oldGETs.Load())
				}
				require.NoError(t, result.Close())
				if mode == "closed near expiry" {
					require.Zero(t, closes)
				} else {
					require.Equal(t, 1, closes)
				}
				t.Logf("rows=%d RPCs=%d peak_GETs=%d elapsed=%s", len(values), calls, peak.Load(), time.Since(started))
			})
		}
	}
}
