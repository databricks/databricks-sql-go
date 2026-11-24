package rows

import (
	"context"
	"database/sql/driver"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/v2/auth/noop"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/v2/rows"
	"github.com/stretchr/testify/require"
)

func cloudFetchConsumer(t *testing.T, result driver.Rows, api string) (func() error, func()) {
	t.Helper()
	if api == "rows" {
		return func() error { return result.Next(make([]driver.Value, 1)) }, func() { require.NoError(t, result.Close()) }
	}
	if api == "batches" {
		iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
		require.NoError(t, err)
		return func() error {
			record, err := iterator.Next()
			if record != nil {
				record.Release()
			}
			return err
		}, iterator.Close
	}
	iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
	require.NoError(t, err)
	return func() error {
		stream, err := iterator.Next()
		if err == nil {
			_, err = io.Copy(io.Discard, stream)
		}
		return err
	}, iterator.Close
}

func TestCloudFetchOperationClosesOnce(t *testing.T) {
	for _, api := range []string{"rows", "batches", "ipc"} {
		for _, finish := range []string{"eof", "rows close", "iterator close", "unused iterator close"} {
			t.Run(api+"/"+finish, func(t *testing.T) {
				fixture := newCloudFetchFixture(t)
				for _, link := range fixture.links {
					link.ExpiryTime = time.Now().Add(time.Hour).UnixMilli()
				}
				closes := 0
				cli := &client.TestClient{FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					closes++
					return &cli_service.TCloseOperationResp{}, nil
				}}
				more := false
				cfg := config.WithDefaults()
				cfg.UseLz4Compression = false
				result, err := NewRows(context.Background(), nil, cli, cfg, &cli_service.TSparkDirectResults{
					ResultSetMetadata: fixture.metadata,
					ResultSet:         &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links}},
				}, nil)
				require.NoError(t, err)
				defer func() { require.NoError(t, result.Close()) }()
				next, closeIterator := cloudFetchConsumer(t, result, api)
				if finish != "unused iterator close" {
					require.NoError(t, next())
					require.Zero(t, closes, "final-page operation must remain available during downloads")
				}
				switch finish {
				case "eof":
					for {
						err := next()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)
					}
				case "rows close":
					require.NoError(t, result.Close())
				default:
					closeIterator()
				}
				require.Equal(t, 1, closes)
				closeIterator()
				closeIterator()
				require.NoError(t, result.Close())
				require.Equal(t, 1, closes)
			})
		}
	}
}

func TestCloudFetchRefetchNPlusOneSameThriftClient(t *testing.T) {
	fixture := newCloudFetchFixture(t)
	fixture.links[0].ExpiryTime = time.Now().Add(time.Hour).UnixMilli()
	var inDriverCall atomic.Bool
	var backgroundRPCs atomic.Int64
	var renewals atomic.Int64
	success := &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}
	fixture.metadata.Status = success
	more := false
	service := &client.TestClient{
		FnGetResultSetMetadata: func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			return fixture.metadata, nil
		},
		FnFetchResults: func(_ context.Context, request *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			if request.StartRowOffset != nil {
				renewals.Add(1)
				time.Sleep(10 * time.Millisecond)
			}
			if !inDriverCall.Load() {
				backgroundRPCs.Add(1)
			}
			var links []*cli_service.TSparkArrowResultLink
			for _, original := range fixture.links {
				if request.StartRowOffset == nil || original.StartRowOffset >= *request.StartRowOffset {
					fresh := *original
					fresh.ExpiryTime = time.Now().Add(time.Hour).UnixMilli()
					links = append(links, &fresh)
				}
			}
			return &cli_service.TFetchResultsResp{Status: success, HasMoreRows: &more, ResultSetMetadata: fixture.metadata,
				Results: &cli_service.TRowSet{StartRowOffset: links[0].StartRowOffset, ResultLinks: links}}, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{Status: success}, nil
		},
	}
	protocol := thrift.NewTBinaryProtocolFactoryConf(&thrift.TConfiguration{})
	server := httptest.NewServer(http.HandlerFunc(thrift.NewThriftHandlerFunc(cli_service.NewTCLIServiceProcessor(service), protocol, protocol)))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	cfg := config.WithDefaults()
	cfg.Authenticator = &noop.NoopAuth{}
	cfg.Protocol = endpoint.Scheme
	cfg.Host = endpoint.Hostname()
	cfg.Port, err = strconv.Atoi(endpoint.Port())
	require.NoError(t, err)
	cfg.HTTPPath = "/sql/1.0/warehouses/test"
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 6
	thriftClient, err := client.InitThriftClient(cfg, server.Client())
	require.NoError(t, err)
	handle := func(id byte) *cli_service.TOperationHandle {
		identifier := make([]byte, 16)
		identifier[0] = id
		return &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{GUID: identifier, Secret: identifier}, HasResultSet: true}
	}
	first, err := NewRows(context.Background(), handle(1), thriftClient, cfg, &cli_service.TSparkDirectResults{
		ResultSetMetadata: fixture.metadata,
		ResultSet:         &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links}},
	}, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	second, err := NewRows(context.Background(), handle(2), thriftClient, cfg, nil, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()
	for expected := int64(0); expected < 18; expected++ {
		inDriverCall.Store(true)
		values := make([]driver.Value, 1)
		require.NoError(t, first.Next(values))
		require.Equal(t, expected, values[0])
		inDriverCall.Store(false)
		inDriverCall.Store(true)
		require.NoError(t, second.Next(values))
		require.Equal(t, expected, values[0])
		inDriverCall.Store(false)
	}
	require.Equal(t, int64(1), renewals.Load())
	require.Zero(t, backgroundRPCs.Load())
}
