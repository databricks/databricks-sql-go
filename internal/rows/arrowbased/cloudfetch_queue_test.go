package arrowbased

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/require"
)

func TestCloudFetchRenewalPreservesCompletedDownloads(t *testing.T) {
	var gets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		gets.Add(1)
		_, _ = writer.Write([]byte("payload"))
	}))
	defer server.Close()
	var links []*cli_service.TSparkArrowResultLink
	for index := int64(0); index < 6; index++ {
		links = append(links, &cli_service.TSparkArrowResultLink{FileLink: server.URL, StartRowOffset: 3 * index, RowCount: 3, ExpiryTime: time.Now().Add(time.Hour).UnixMilli()})
	}
	links[0].ExpiryTime = time.Now().Add(-time.Minute).UnixMilli()
	var cloud *cloudIPCStreamIterator
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			require.Eventually(t, func() bool {
				cloud.mu.Lock()
				defer cloud.mu.Unlock()
				complete := true
				for remaining := cloud.downloadTasks.Len(); remaining > 0; remaining-- {
					task := cloud.downloadTasks.Dequeue()
					complete = complete && task.downloaded.Load()
					cloud.downloadTasks.Enqueue(task)
				}
				return complete
			}, 5*time.Second, time.Millisecond)
			var fresh []*cli_service.TSparkArrowResultLink
			for _, original := range links {
				link := *original
				link.ExpiryTime = time.Now().Add(time.Hour).UnixMilli()
				fresh = append(fresh, &link)
			}
			return &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{ResultLinks: fresh}}, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	pages := rowscanner.NewResultPageIterator(context.Background(), rowscanner.NewDelimiter(0, 18), 10, nil, nil, false, cli, dbsqllog.Logger)
	defer func() { require.NoError(t, pages.Close()) }()
	cfg := config.WithDefaults()
	cfg.MaxDownloadThreads = 6
	cfg.UseLz4Compression = false
	iterator, err := NewCloudIPCStreamIterator(rowscanner.WithCloudFetchLinkRefresher(context.Background(), pages), links, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	cloud = iterator.(*cloudIPCStreamIterator)
	for range links {
		stream, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		data, readErr := io.ReadAll(stream)
		require.NoError(t, readErr)
		require.Equal(t, "payload", string(data))
	}
	require.Equal(t, int64(6), gets.Load(), "completed queued downloads must not be fetched again")
}

func TestCloudFetchRenewalFailureIsVisibleAndRedacted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	rpcErr := errors.New("Invalid OperationHandle: operation was reaped https://storage/file?sig=hidden")
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return nil, rpcErr
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	pages := rowscanner.NewResultPageIterator(context.Background(), rowscanner.NewDelimiter(0, 3), 10, nil, nil, false, cli, dbsqllog.Logger)
	defer func() { require.NoError(t, pages.Close()) }()
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	iterator, err := NewCloudIPCStreamIterator(rowscanner.WithCloudFetchLinkRefresher(context.Background(), pages), []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, ExpiryTime: time.Now().Add(-time.Minute).UnixMilli(), RowCount: 3}}, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	_, downloadErr := iterator.Next()
	require.ErrorContains(t, downloadErr, "Invalid OperationHandle: operation was reaped")
	require.NotContains(t, downloadErr.Error(), "hidden")
	require.NotContains(t, downloadErr.Error(), "?sig")
	require.ErrorIs(t, downloadErr, rpcErr)
	require.ErrorIs(t, downloadErr, errLinkExpired)
	require.Same(t, rpcErr, combineCloudFetchErrors(rpcErr, rpcErr))
	require.LessOrEqual(t, len(redactCloudFetchRenewalError(errors.New(strings.Repeat("x", 2000)))), 515)
}
