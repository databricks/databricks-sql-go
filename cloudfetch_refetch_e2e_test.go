package dbsql

import (
	"context"
	"database/sql/driver"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	internalrows "github.com/databricks/databricks-sql-go/v2/internal/rows"
	dbsqlrows "github.com/databricks/databricks-sql-go/v2/rows"
	"github.com/stretchr/testify/require"
)

func openCloudFetchOperation(t *testing.T) (context.Context, cli_service.TCLIService, *cli_service.TOperationHandle) {
	return openCloudFetchOperationForQuery(t, "SELECT id, repeat('x', 64) AS pad FROM range(2000000)")
}

func openCloudFetchOperationForQuery(t *testing.T, statement string) (context.Context, cli_service.TCLIService, *cli_service.TOperationHandle) {
	t.Helper()
	host, path, token := pecoTestingCreds(t)
	driverConnector, err := NewConnector(WithServerHostname(host), WithPort(443), WithHTTPPath(path), WithAccessToken(token))
	require.NoError(t, err)
	configured := driverConnector.(*connector)
	thriftClient, err := client.InitThriftClient(configured.cfg, configured.client)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	protocol := int64(configured.cfg.ThriftProtocolVersion)
	session, err := thriftClient.OpenSession(ctx, &cli_service.TOpenSessionReq{ClientProtocolI64: &protocol})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, closeErr := thriftClient.CloseSession(ctx, &cli_service.TCloseSessionReq{SessionHandle: session.SessionHandle})
		require.NoError(t, closeErr)
	})
	enabled := true
	execution, err := thriftClient.ExecuteStatement(ctx, &cli_service.TExecuteStatementReq{
		SessionHandle:       session.SessionHandle,
		Statement:           statement,
		CanReadArrowResult_: &enabled,
		CanDownloadResult_:  &enabled,
	})
	require.NoError(t, err)
	return ctx, thriftClient, execution.OperationHandle
}

type cloudFetchRefetchCounter struct {
	cli_service.TCLIService
	refetches    atomic.Int64
	restores     atomic.Int64
	plainFetches atomic.Int64
	closes       atomic.Int64
}

func (counter *cloudFetchRefetchCounter) FetchResults(ctx context.Context, request *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
	if request.StartRowOffset != nil {
		if request.GetIncludeResultSetMetadata() {
			counter.restores.Add(1)
		} else {
			counter.refetches.Add(1)
		}
	} else {
		counter.plainFetches.Add(1)
	}
	return counter.TCLIService.FetchResults(ctx, request)
}

func (counter *cloudFetchRefetchCounter) CloseOperation(ctx context.Context, request *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
	counter.closes.Add(1)
	return counter.TCLIService.CloseOperation(ctx, request)
}

func TestE2ECloudFetchRefetchExpiredLinks(t *testing.T) {
	for _, api := range []string{"rows", "batches", "ipc"} {
		t.Run(api, func(t *testing.T) {
			var baselineChecksum int64
			for _, expire := range []bool{false, true} {
				ctx, thriftClient, handle := openCloudFetchOperation(t)
				metadata := true
				initial, err := thriftClient.FetchResults(ctx, &cli_service.TFetchResultsReq{
					OperationHandle: handle, MaxRows: 500000, IncludeResultSetMetadata: &metadata,
				})
				require.NoError(t, err)
				require.Greater(t, len(initial.Results.ResultLinks), 1)
				if expire {
					for _, link := range initial.Results.ResultLinks {
						link.ExpiryTime = time.Now().Add(-time.Second).UnixMilli()
					}
				}
				counter := &cloudFetchRefetchCounter{TCLIService: thriftClient}
				cfg := config.WithDefaults()
				cfg.MaxDownloadThreads = 4
				cfg.UseLz4Compression = false
				result, rowsErr := internalrows.NewRows(ctx, handle, counter, cfg, &cli_service.TSparkDirectResults{
					ResultSet: initial, ResultSetMetadata: initial.ResultSetMetadata,
				}, nil)
				require.NoError(t, rowsErr)
				t.Cleanup(func() { require.NoError(t, result.Close()) })
				count, checksum := drainCloudFetchLive(t, result, api)
				require.NoError(t, result.Close())
				require.Equal(t, int64(2000000), count)
				require.Equal(t, int64(1999999000000), checksum)
				if expire {
					require.Equal(t, baselineChecksum, checksum)
					require.GreaterOrEqual(t, counter.refetches.Load(), int64(1))
				} else {
					baselineChecksum = checksum
					require.Zero(t, counter.refetches.Load())
				}
				require.Equal(t, int64(1), counter.closes.Load())
				t.Logf("forced_expiry=%t rows=%d checksum=%d initial_files=%d refetches=%d (shared warehouse)", expire, count, checksum, len(initial.Results.ResultLinks), counter.refetches.Load())
			}
		})
	}
}

func TestE2ECloudFetchRefetchMultiplePages(t *testing.T) {
	ctx, thriftClient, handle := openCloudFetchOperationForQuery(t, "SELECT id, repeat('x', 256) AS pad FROM range(8500000)")
	metadata := true
	initial, err := thriftClient.FetchResults(ctx, &cli_service.TFetchResultsReq{
		OperationHandle: handle, MaxRows: 500000, IncludeResultSetMetadata: &metadata,
	})
	require.NoError(t, err)
	require.True(t, initial.GetHasMoreRows(), "the regression must exercise cursor restoration across pages")
	require.Greater(t, len(initial.Results.ResultLinks), 1)
	for _, link := range initial.Results.ResultLinks {
		link.ExpiryTime = time.Now().Add(20 * time.Second).UnixMilli()
	}
	counter := &cloudFetchRefetchCounter{TCLIService: thriftClient}
	cfg := config.WithDefaults()
	cfg.MaxDownloadThreads = 4
	cfg.UseLz4Compression = false
	result, rowsErr := internalrows.NewRows(ctx, handle, counter, cfg, &cli_service.TSparkDirectResults{
		ResultSet: initial, ResultSetMetadata: initial.ResultSetMetadata,
	}, nil)
	require.NoError(t, rowsErr)
	t.Cleanup(func() { require.NoError(t, result.Close()) })
	count, checksum := drainCloudFetchLive(t, result, "ipc")
	require.NoError(t, result.Close())
	require.Equal(t, int64(8500000), count)
	require.Equal(t, int64(36124995750000), checksum)
	require.GreaterOrEqual(t, counter.refetches.Load(), int64(1))
	require.GreaterOrEqual(t, counter.restores.Load(), int64(1))
	require.GreaterOrEqual(t, counter.plainFetches.Load(), int64(1))
	require.Equal(t, int64(1), counter.closes.Load())
	t.Logf("rows=%d checksum=%d initial_files=%d renewals=%d restores=%d plain_fetches=%d closes=%d (shared warehouse)", count, checksum, len(initial.Results.ResultLinks), counter.refetches.Load(), counter.restores.Load(), counter.plainFetches.Load(), counter.closes.Load())
}

func TestE2ECloudFetchRefetchRealExpiryBuffer(t *testing.T) {
	ctx, thriftClient, handle := openCloudFetchOperation(t)
	metadata := true
	initial, err := thriftClient.FetchResults(ctx, &cli_service.TFetchResultsReq{
		OperationHandle: handle, MaxRows: 500000, IncludeResultSetMetadata: &metadata,
	})
	require.NoError(t, err)
	require.Greater(t, len(initial.Results.ResultLinks), 1)
	expiry := initial.Results.ResultLinks[0].ExpiryTime
	remaining := time.Until(time.UnixMilli(expiry))
	counter := &cloudFetchRefetchCounter{TCLIService: thriftClient}
	cfg := config.WithDefaults()
	cfg.MaxDownloadThreads = len(initial.Results.ResultLinks)
	cfg.UseLz4Compression = false
	cfg.MinTimeToExpiry = max(30*time.Second, remaining+time.Minute)
	result, rowsErr := internalrows.NewRows(ctx, handle, counter, cfg, &cli_service.TSparkDirectResults{
		ResultSet: initial, ResultSetMetadata: initial.ResultSetMetadata,
	}, nil)
	require.NoError(t, rowsErr)
	t.Cleanup(func() { require.NoError(t, result.Close()) })
	count, checksum := drainCloudFetchLive(t, result, "ipc")
	require.NoError(t, result.Close())
	require.Equal(t, expiry, initial.Results.ResultLinks[0].ExpiryTime, "use the server's expiry unchanged")
	require.Equal(t, int64(2000000), count)
	require.Equal(t, int64(1999999000000), checksum)
	require.GreaterOrEqual(t, counter.refetches.Load(), int64(1))
	require.Equal(t, int64(1), counter.closes.Load())
	t.Logf("server_expiry_ms=%d remaining=%s buffer=%s rows=%d checksum=%d renewals=%d (shared warehouse)", expiry, remaining, cfg.MinTimeToExpiry, count, checksum, counter.refetches.Load())
}

func drainCloudFetchLive(t *testing.T, result driver.Rows, api string) (int64, int64) {
	t.Helper()
	var count, checksum int64
	appendValue := func(value int64) {
		require.Equal(t, count, value)
		count++
		checksum += value
	}
	appendRecord := func(record arrow.RecordBatch) {
		for _, value := range record.Column(0).(*array.Int64).Int64Values() {
			appendValue(value)
		}
	}
	if api == "rows" {
		for {
			values := make([]driver.Value, 2)
			err := result.Next(values)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			appendValue(values[0].(int64))
		}
	} else if api == "batches" {
		iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
		require.NoError(t, err)
		defer iterator.Close()
		for {
			record, err := iterator.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			appendRecord(record)
			record.Release()
		}
	} else {
		iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
		require.NoError(t, err)
		defer iterator.Close()
		for {
			stream, err := iterator.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			reader, err := ipc.NewReader(stream)
			require.NoError(t, err)
			for reader.Next() {
				appendRecord(reader.RecordBatch())
			}
			require.NoError(t, reader.Err())
			reader.Release()
		}
	}
	return count, checksum
}
