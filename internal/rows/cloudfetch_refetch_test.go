package rows

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

type cloudFetchFixture struct {
	metadata *cli_service.TGetResultSetMetadataResp
	links    []*cli_service.TSparkArrowResultLink
	server   *httptest.Server
}

func newCloudFetchFixture(t testing.TB) *cloudFetchFixture {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	var schemaBytes bytes.Buffer
	writer := ipc.NewWriter(&schemaBytes, ipc.WithSchema(schema))
	require.NoError(t, writer.Close())
	fixture := &cloudFetchFixture{metadata: &cli_service.TGetResultSetMetadataResp{
		ArrowSchema: schemaBytes.Bytes()[:schemaBytes.Len()-8],
		Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
			ColumnName: "id", TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
				PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_BIGINT_TYPE},
			}}},
		}}},
	}}
	files := make(map[string][]byte)
	for start := int64(0); start < 18; start += 3 {
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues([]int64{start, start + 1, start + 2}, nil)
		values := builder.NewArray()
		builder.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{values}, 3)
		values.Release()
		var data bytes.Buffer
		writer := ipc.NewWriter(&data, ipc.WithSchema(schema))
		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())
		record.Release()
		path := "/" + strconv.FormatInt(start, 10)
		files[path] = data.Bytes()
		fixture.links = append(fixture.links, &cli_service.TSparkArrowResultLink{
			FileLink: path, StartRowOffset: start, RowCount: 3, ExpiryTime: time.Now().Add(-time.Hour).Unix(),
		})
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, exists := files[request.URL.Path]
		if !exists {
			http.NotFound(writer, request)
			return
		}
		_, _ = writer.Write(data)
	}))
	t.Cleanup(fixture.server.Close)
	for _, link := range fixture.links {
		link.FileLink = fixture.server.URL + link.FileLink
	}
	return fixture
}

func consumeCloudFetch(t testing.TB, result driver.Rows, api string) ([]int64, error) {
	t.Helper()
	var values []int64
	if api == "rows" {
		for {
			row := make([]driver.Value, 1)
			err := result.Next(row)
			if err == io.EOF {
				return values, nil
			}
			if err != nil {
				return values, err
			}
			values = append(values, row[0].(int64))
		}
	}
	appendRecord := func(record arrow.RecordBatch) {
		values = append(values, record.Column(0).(*array.Int64).Int64Values()...)
	}
	if api == "batches" {
		iterator, err := result.(dbsqlrows.Rows).GetArrowBatches(context.Background())
		if err != nil {
			return nil, err
		}
		defer iterator.Close()
		for {
			record, err := iterator.Next()
			if err == io.EOF {
				return values, nil
			}
			if err != nil {
				return values, err
			}
			appendRecord(record)
			record.Release()
		}
	}
	iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
	if err != nil {
		return nil, err
	}
	defer iterator.Close()
	for {
		stream, err := iterator.Next()
		if err == io.EOF {
			return values, nil
		}
		if err != nil {
			return values, err
		}
		reader, err := ipc.NewReader(stream)
		if err != nil {
			return values, err
		}
		for reader.Next() {
			appendRecord(reader.RecordBatch())
		}
		err = reader.Err()
		reader.Release()
		if err != nil {
			return values, err
		}
	}
}

func TestCloudFetchRefetch(t *testing.T) {
	for _, api := range []string{"rows", "batches", "ipc"} {
		for _, threads := range []int{1, 4} {
			for _, initialPage := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/threads=%d/direct=%t", api, threads, initialPage), func(t *testing.T) {
					fixture := newCloudFetchFixture(t)
					cfg := config.WithDefaults()
					cfg.MaxDownloadThreads = threads
					cfg.UseLz4Compression = false
					var mutex sync.Mutex
					var refreshed []int64
					closed := false
					cursor := int64(0)
					page := func(start int64, count int) *cli_service.TFetchResultsResp {
						end := min(int(start/3)+count, len(fixture.links))
						more := end < len(fixture.links)
						cursor = int64(end * 3)
						return &cli_service.TFetchResultsResp{
							HasMoreRows: &more, ResultSetMetadata: fixture.metadata,
							Results: &cli_service.TRowSet{StartRowOffset: start, ResultLinks: fixture.links[start/3 : end]},
						}
					}
					cli := &client.TestClient{FnFetchResults: func(_ context.Context, request *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						mutex.Lock()
						defer mutex.Unlock()
						if closed {
							return nil, errors.New("fetch after operation close")
						}
						if request.MaxRows == 3 && request.StartRowOffset != nil {
							start := *request.StartRowOffset
							refreshed = append(refreshed, start)
							response := page(start, 3)
							response.Results.ResultLinks = append([]*cli_service.TSparkArrowResultLink(nil), response.Results.ResultLinks...)
							for index, expired := range response.Results.ResultLinks {
								fresh := *expired
								fresh.ExpiryTime = time.Now().Add(time.Hour).Unix()
								response.Results.ResultLinks[index] = &fresh
							}
							return response, nil
						}
						if request.StartRowOffset != nil {
							cursor = *request.StartRowOffset
						}
						return page(cursor, 3), nil
					}}
					cli.FnGetResultSetMetadata = func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
						return fixture.metadata, nil
					}
					cli.FnCloseOperation = func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						mutex.Lock()
						require.False(t, closed)
						closed = true
						mutex.Unlock()
						return &cli_service.TCloseOperationResp{}, nil
					}
					var direct *cli_service.TSparkDirectResults
					if initialPage {
						direct = &cli_service.TSparkDirectResults{ResultSet: page(0, 3), ResultSetMetadata: fixture.metadata}
					}
					result, newRowsErr := NewRows(context.Background(), nil, cli, cfg, direct, nil)
					require.NoError(t, newRowsErr)
					defer func() { require.NoError(t, result.Close()) }()
					actual, err := consumeCloudFetch(t, result, api)
					require.NoError(t, err)
					var expected []int64
					for value := int64(0); value < 18; value++ {
						expected = append(expected, value)
					}
					require.Equal(t, expected, actual)
					mutex.Lock()
					defer mutex.Unlock()
					require.True(t, closed)
					require.Equal(t, []int64{0, 9}, refreshed)
				})
			}
		}
	}
}

func TestCloudFetchRefetchFailures(t *testing.T) {
	for _, scenario := range []string{"expired", "rpc failure", "wrong range", "wrong subsequent range", "missing pending range", "duplicate range", "empty response", "closed operation"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newCloudFetchFixture(t)
			calls := 0
			cli := &client.TestClient{FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
				calls++
				if scenario == "rpc failure" {
					return nil, errors.New("refetch RPC failed")
				}
				if scenario == "empty response" {
					return &cli_service.TFetchResultsResp{}, nil
				}
				links := append([]*cli_service.TSparkArrowResultLink(nil), fixture.links[:3]...)
				switch scenario {
				case "wrong range", "wrong subsequent range":
					index := 0
					if scenario == "wrong subsequent range" {
						index = 1
					}
					link := *links[index]
					link.RowCount++
					links[index] = &link
				case "missing pending range":
					links = links[:2]
				case "duplicate range":
					links = append(links, links[0])
				}
				return &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{ResultLinks: links}}, nil
			}}
			cli.FnCloseOperation = func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
				return &cli_service.TCloseOperationResp{}, nil
			}
			more := false
			direct := &cli_service.TSparkDirectResults{
				ResultSetMetadata: fixture.metadata,
				ResultSet:         &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links[:3]}},
			}
			if scenario == "closed operation" {
				direct.CloseOperation = &cli_service.TCloseOperationResp{}
			}
			cfg := config.WithDefaults()
			cfg.UseLz4Compression = false
			result, newRowsErr := NewRows(context.Background(), nil, cli, cfg, direct, nil)
			require.NoError(t, newRowsErr)
			defer func() { require.NoError(t, result.Close()) }()
			_, err := consumeCloudFetch(t, result, "rows")
			require.ErrorContains(t, err, dbsqlerr.ErrLinkExpired)
			want := 1
			switch scenario {
			case "expired":
				want = 3
			case "closed operation":
				want = 0
			}
			require.Equal(t, want, calls)
		})
	}
}

func TestCloudFetchCloseDuringRefetch(t *testing.T) {
	for _, api := range []string{"batches", "ipc"} {
		t.Run(api, func(t *testing.T) {
			fixture := newCloudFetchFixture(t)
			started := make(chan struct{})
			stopped := make(chan struct{})
			cli := &client.TestClient{
				FnFetchResults: func(ctx context.Context, _ *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					close(started)
					<-ctx.Done()
					close(stopped)
					return nil, ctx.Err()
				},
				FnCloseOperation: func(ctx context.Context, _ *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, ctx.Err()
				},
			}
			more := false
			direct := &cli_service.TSparkDirectResults{
				ResultSetMetadata: fixture.metadata,
				ResultSet:         &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links[:1]}},
			}
			cfg := config.WithDefaults()
			cfg.UseLz4Compression = false
			result, newRowsErr := NewRows(context.Background(), nil, cli, cfg, direct, nil)
			require.NoError(t, newRowsErr)
			t.Cleanup(func() { require.NoError(t, result.Close()) })
			finished := make(chan error, 1)
			go func() {
				_, err := consumeCloudFetch(t, result, api)
				finished <- err
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("refetch did not start")
			}
			require.NoError(t, result.Close())
			select {
			case err := <-finished:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not stop refetch")
			}
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("refetch RPC did not stop")
			}
		})
	}
}

func TestCloudFetchRefetchExpiryBuffer(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		buffer    time.Duration
		remaining time.Duration
		calls     int
	}{
		{"default", 0, 20 * time.Second, 1},
		{"smaller", 10 * time.Second, 20 * time.Second, 1},
		{"larger", time.Minute, 50 * time.Second, 1},
		{"no renewal", 0, time.Minute, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newCloudFetchFixture(t)
			fixture.links[0].ExpiryTime = time.Now().Add(scenario.remaining).Unix()
			calls := 0
			cli := &client.TestClient{
				FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
					calls++
					fresh := *fixture.links[0]
					fresh.ExpiryTime = time.Now().Add(time.Hour).Unix()
					return &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{&fresh}}}, nil
				},
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			more := false
			direct := &cli_service.TSparkDirectResults{
				ResultSetMetadata: fixture.metadata,
				ResultSet:         &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links[:1]}},
			}
			cfg := config.WithDefaults()
			cfg.UseLz4Compression = false
			cfg.MinTimeToExpiry = scenario.buffer
			result, newRowsErr := NewRows(context.Background(), nil, cli, cfg, direct, nil)
			require.NoError(t, newRowsErr)
			defer func() { require.NoError(t, result.Close()) }()
			actual, err := consumeCloudFetch(t, result, "rows")
			require.NoError(t, err)
			require.Equal(t, []int64{0, 1, 2}, actual)
			require.Equal(t, scenario.calls, calls)
		})
	}
}

func BenchmarkCloudFetchNoExpiry(benchmark *testing.B) {
	fixture := newCloudFetchFixture(benchmark)
	for _, link := range fixture.links {
		link.ExpiryTime = time.Now().Add(time.Hour).Unix()
	}
	cfg := config.WithDefaults()
	cfg.UseLz4Compression = false
	cfg.MaxDownloadThreads = 4
	cfg.CloudFetchSpeedThresholdMbps = 0
	cli := &client.TestClient{
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
	more := false
	direct := &cli_service.TSparkDirectResults{
		ResultSetMetadata: fixture.metadata,
		ResultSet:         &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links}},
	}
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for benchmark.Loop() {
		result, err := NewRows(context.Background(), nil, cli, cfg, direct, nil)
		require.NoError(benchmark, err)
		actual, consumeErr := consumeCloudFetch(benchmark, result, "batches")
		require.NoError(benchmark, consumeErr)
		require.Len(benchmark, actual, 18)
		require.NoError(benchmark, result.Close())
	}
}
