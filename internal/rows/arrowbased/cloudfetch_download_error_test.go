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

	dbsqlerr "github.com/databricks/databricks-sql-go/v2/errors"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/v2/internal/errors"
	"github.com/databricks/databricks-sql-go/v2/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/v2/logger"
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
	for _, remaining := range []time.Duration{20 * time.Second, 15*time.Minute - 20*time.Minute} {
		t.Run(remaining.String(), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte("downloaded"))
			}))
			defer server.Close()
			cfg := config.WithDefaults()
			cfg.UseLz4Compression = false
			cfg.MinTimeToExpiry = time.Hour
			iterator, err := NewCloudIPCStreamIterator(context.Background(), []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, ExpiryTime: time.Now().Add(remaining).UnixMilli(), RowCount: 3}}, 0, cfg, nil)
			require.NoError(t, err)
			defer iterator.Close()
			stream, streamErr := iterator.Next()
			require.NoError(t, streamErr)
			data, readErr := io.ReadAll(stream)
			require.NoError(t, readErr)
			require.Equal(t, "downloaded", string(data))
		})
	}
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
			cfg.RetryWaitMin, cfg.RetryWaitMax = time.Millisecond, time.Millisecond
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
				require.ErrorContains(t, err, "Client.Timeout")
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

func TestCloudFetchRenewalCallerDeadlineIsNotChunkError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cli := &client.TestClient{
		FnFetchResults: func(ctx context.Context, _ *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	pages := rowscanner.NewResultPageIterator(ctx, rowscanner.NewDelimiter(0, 3), 10, nil, nil, false, cli, dbsqllog.Logger)
	defer func() { require.NoError(t, pages.Close()) }()
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	iterator, err := NewCloudIPCStreamIterator(rowscanner.WithCloudFetchLinkRefresher(ctx, pages), []*cli_service.TSparkArrowResultLink{{ExpiryTime: time.Now().Add(-time.Minute).UnixMilli(), RowCount: 3}}, 0, cfg, nil)
	require.NoError(t, err)
	defer iterator.Close()
	_, downloadErr := iterator.Next()
	require.ErrorIs(t, downloadErr, context.DeadlineExceeded)
	require.Empty(t, dbsqlerrint.CategoryFromError(downloadErr))
}

func TestCloudFetchRenewalDeadlineCauseOrdering(t *testing.T) {
	for _, stage := range []string{"caller before RPC", "fetch before RPC", "fetch during RPC"} {
		t.Run(stage, func(t *testing.T) {
			baseCtx, finish := context.WithTimeout(context.Background(), 5*time.Second)
			defer finish()
			callerCtx, cancelCaller := context.WithCancelCause(baseCtx)
			defer cancelCaller(nil)
			pageCtx, cancelPage := context.WithCancelCause(baseCtx)
			defer cancelPage(nil)
			cli := &client.TestClient{
				FnFetchResults: func(rpcCtx context.Context, _ *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					require.Equal(t, "fetch during RPC", stage)
					require.NoError(t, callerCtx.Err())
					cancelPage(context.DeadlineExceeded)
					<-rpcCtx.Done()
					require.NoError(t, callerCtx.Err())
					return nil, rpcCtx.Err()
				},
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			pages := rowscanner.NewResultPageIterator(pageCtx, rowscanner.NewDelimiter(0, 3), 10, nil, nil, false, cli, dbsqllog.Logger)
			defer func() { require.NoError(t, pages.Close()) }()
			refetch := rowscanner.CloudFetchLinkRefresherFromContext(rowscanner.WithCloudFetchLinkRefresher(callerCtx, pages))
			require.NotNil(t, refetch)
			switch stage {
			case "caller before RPC":
				cancelCaller(context.DeadlineExceeded)
			case "fetch before RPC":
				cancelPage(context.DeadlineExceeded)
			}
			_, err := refetch(callerCtx, &cli_service.TSparkArrowResultLink{RowCount: 3})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NotErrorIs(t, err, context.Canceled)
			require.Empty(t, dbsqlerrint.CategoryFromError(err))
		})
	}
}
