package arrowbased

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/v2/internal/errors"
	"github.com/databricks/databricks-sql-go/v2/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/v2/logger"
	"github.com/stretchr/testify/require"
)

func TestExpiredLinkStackAtFailure(t *testing.T) {
	_, err := fetchBatchBytes(context.Background(), &cli_service.TSparkArrowResultLink{ExpiryTime: time.Now().Add(-time.Minute).UnixMilli()}, time.Minute, time.Minute, 0, http.DefaultClient, 0, 0, 0)
	require.ErrorIs(t, err, errLinkExpired)
	wrapped := dbsqlerrint.NewDriverError(context.Background(), "download failed", err)
	require.NotEmpty(t, wrapped.StackTrace())
	frame := fmt.Sprintf("%+v", wrapped.StackTrace()[0])
	require.Contains(t, frame, "newCloudFetchExpiryError")
	require.Contains(t, fmt.Sprintf("%+v", wrapped.StackTrace()[1]), "fetchBatchBytes")
	require.NotContains(t, frame, "arrowbased.init")
}

func TestCloudFetchDownloadTimingExcludesRenewal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("payload"))
	}))
	defer server.Close()
	expired := []*cli_service.TSparkArrowResultLink{
		{StartRowOffset: 0, RowCount: 3, FileLink: server.URL, ExpiryTime: time.Now().Add(-time.Hour).UnixMilli()},
		{StartRowOffset: 3, RowCount: 3, FileLink: server.URL, ExpiryTime: time.Now().Add(-time.Hour).UnixMilli()},
	}
	calls := 0
	cli := &client.TestClient{
		FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			calls++
			time.Sleep(250 * time.Millisecond)
			var fresh []*cli_service.TSparkArrowResultLink
			for _, original := range expired {
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
	pages := rowscanner.NewResultPageIterator(context.Background(), rowscanner.NewDelimiter(0, 6), 10, nil, nil, false, cli, dbsqllog.Logger)
	defer func() { require.NoError(t, pages.Close()) }()
	ctx := rowscanner.WithCloudFetchLinkRefresher(context.Background(), pages)
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 2
	var durations []int64
	iterator, err := NewCloudIPCStreamIterator(ctx, expired, 0, cfg, func(duration int64) { durations = append(durations, duration) })
	require.NoError(t, err)
	defer iterator.Close()
	started := time.Now()
	for range expired {
		stream, err := iterator.Next()
		require.NoError(t, err)
		data, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.Equal(t, "payload", string(data))
	}
	require.GreaterOrEqual(t, time.Since(started), 250*time.Millisecond)
	require.Equal(t, 1, calls)
	require.Len(t, durations, 2)
	for _, duration := range durations {
		require.Less(t, duration, int64(250), "download timing must exclude the 250ms renewal")
	}
}
