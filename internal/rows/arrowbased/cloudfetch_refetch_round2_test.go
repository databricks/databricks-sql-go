package arrowbased

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/require"
)

func TestCloudFetchExpiryMilliseconds(t *testing.T) {
	future := time.Now().Add(15 * time.Minute).UnixMilli()
	require.Greater(t, future, int64(1e12))
	require.False(t, isLinkExpired(future, 30*time.Second))
	require.True(t, isLinkExpired(future, 16*time.Minute))
	require.True(t, isLinkExpired(time.Now().Add(-time.Minute).UnixMilli(), 0))
	require.True(t, isLinkExpired(time.Now().Unix(), 0), "the IDL and JDBC use milliseconds, without a seconds heuristic")
}

func TestCloudFetchNoRefresherIgnoresBuffer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("downloaded"))
	}))
	defer server.Close()
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MinTimeToExpiry = time.Hour
	iterator, err := NewCloudIPCStreamIterator(context.Background(), []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, ExpiryTime: time.Now().Add(20 * time.Second).UnixMilli(), RowCount: 3}}, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	stream, streamErr := iterator.Next()
	require.NoError(t, streamErr)
	data, readErr := io.ReadAll(stream)
	require.NoError(t, readErr)
	require.Equal(t, "downloaded", string(data))
}

func TestCloudFetchNearExpiryRPCFailureFallsBack(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("downloaded"))
	}))
	defer server.Close()
	calls := 0
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			calls++
			return nil, errors.New("renewal unavailable")
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	pages := rowscanner.NewResultPageIterator(context.Background(), rowscanner.NewDelimiter(0, 3), 10, nil, nil, false, cli, dbsqllog.Logger)
	defer func() { require.NoError(t, pages.Close()) }()
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	iterator, err := NewCloudIPCStreamIterator(rowscanner.WithCloudFetchLinkRefresher(context.Background(), pages), []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, ExpiryTime: time.Now().Add(20 * time.Second).UnixMilli(), RowCount: 3}}, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	stream, streamErr := iterator.Next()
	require.NoError(t, streamErr)
	data, readErr := io.ReadAll(stream)
	require.NoError(t, readErr)
	require.Equal(t, "downloaded", string(data))
	require.Equal(t, 1, calls)
}

func TestCloudFetchRenewalExhaustionKeepsDownloadCause(t *testing.T) {
	for _, status := range []int{0, http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusForbidden} {
		t.Run(fmt.Sprintf("status=%d", status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if status == 0 {
					<-request.Context().Done()
					return
				}
				writer.Header().Set("Retry-After", "1")
				writer.WriteHeader(status)
			}))
			defer server.Close()
			link := &cli_service.TSparkArrowResultLink{FileLink: server.URL + "?signature=do-not-print", ExpiryTime: time.Now().Add(100 * time.Millisecond).UnixMilli(), RowCount: 3}
			calls := 0
			cli := &client.TestClient{
				FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					calls++
					expired := *link
					expired.ExpiryTime = time.Now().Add(-time.Minute).UnixMilli()
					return &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{&expired}}}, nil
				},
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			pages := rowscanner.NewResultPageIterator(context.Background(), rowscanner.NewDelimiter(0, 3), 10, nil, nil, false, cli, dbsqllog.Logger)
			defer func() { require.NoError(t, pages.Close()) }()
			ctx := rowscanner.WithCloudFetchLinkRefresher(context.Background(), pages)
			cfg := config.WithDefaults()
			cfg.UseLz4Compression = false
			cfg.MaxDownloadThreads = 1
			cfg.HTTPClient = &http.Client{Timeout: 150 * time.Millisecond}
			cfg.RetryMax = 1
			if status == 0 || status == http.StatusForbidden {
				cfg.RetryMax = 0
			}
			iterator, rowsErr := NewCloudIPCStreamIterator(ctx, []*cli_service.TSparkArrowResultLink{link}, 0, cfg, nil)
			require.NoError(t, rowsErr)
			defer iterator.Close()
			cloud := iterator.(*cloudIPCStreamIterator)
			cloud.pendingLinks.Clear()
			cloud.downloadTasks.Enqueue(cloud.newDownloadTask(link, true))
			_, err := iterator.Next()
			require.ErrorIs(t, err, errLinkExpired)
			require.ErrorIs(t, err, dbsqlerr.DriverError)
			require.Equal(t, dbsqlerrint.CategoryChunkDownload, dbsqlerrint.CategoryFromError(err))
			require.NotContains(t, err.Error(), "do-not-print")
			require.Equal(t, 3, calls)
			if status == 0 {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.ErrorContains(t, err, fmt.Sprintf("HTTP error %d", status))
			}
		})
	}
}

func TestCloudFetchUnexpired403IsTerminal(t *testing.T) {
	var requests atomic.Int64
	renewals := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			renewals++
			return nil, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	pages := rowscanner.NewResultPageIterator(context.Background(), rowscanner.NewDelimiter(0, 3), 10, nil, nil, false, cli, dbsqllog.Logger)
	defer func() { require.NoError(t, pages.Close()) }()
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	iterator, err := NewCloudIPCStreamIterator(rowscanner.WithCloudFetchLinkRefresher(context.Background(), pages), []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, ExpiryTime: time.Now().Add(time.Hour).UnixMilli(), RowCount: 3}}, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	_, downloadErr := iterator.Next()
	require.ErrorContains(t, downloadErr, "HTTP error 403")
	require.NotErrorIs(t, downloadErr, errLinkExpired)
	require.Equal(t, int64(1), requests.Load())
	require.Zero(t, renewals)
}
