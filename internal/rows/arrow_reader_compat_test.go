package rows

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"testing/iotest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/stretchr/testify/require"
)

func TestArrowCloudReaderInterfaces(test *testing.T) {
	for _, aliases := range []bool{false, true} {
		for _, discovered := range []bool{false, true} {
			test.Run(fmt.Sprintf("aliases=%t/discovered=%t", aliases, discovered), func(test *testing.T) {
				var page cli_service.TFetchResultsResp
				loadTestData(test, "multipleFetch/FetchResults1.json", &page)
				originalHeader := page.ResultSetMetadata.ArrowSchema
				payload := bytes.Join([][]byte{originalHeader, page.Results.ArrowBatches[0].Batch}, nil)
				header := originalHeader
				if aliases {
					reader, err := ipc.NewReader(bytes.NewReader(header))
					require.NoError(test, err)
					fields := reader.Schema().Fields()
					reader.Release()
					for index := range fields {
						fields[index].Name = fmt.Sprintf("alias_%d", index)
						page.ResultSetMetadata.Schema.Columns[index].ColumnName = fields[index].Name
					}
					var output bytes.Buffer
					writer := ipc.NewWriter(&output, ipc.WithSchema(arrow.NewSchema(fields, nil)))
					require.NoError(test, writer.Close())
					header = output.Bytes()[:output.Len()-8]
				}
				page.ResultSetMetadata.ArrowSchema = header
				if discovered {
					page.ResultSetMetadata.ArrowSchema = nil
				}
				server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { _, _ = response.Write(payload) }))
				defer server.Close()
				page.Results = &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: 4096, ExpiryTime: time.Now().Add(time.Hour).Unix()}}}
				page.HasMoreRows = new(bool)
				format := cli_service.TSparkRowSetType_URL_BASED_SET
				page.ResultSetMetadata.ResultFormat = &format
				direct := &cli_service.TSparkDirectResults{ResultSet: &page, ResultSetMetadata: page.ResultSetMetadata, CloseOperation: &cli_service.TCloseOperationResp{}}
				result, rowsErr := NewRows(context.Background(), nil, &client.TestClient{}, config.WithDefaults(), direct, nil)
				require.NoError(test, rowsErr)
				defer func() { require.NoError(test, result.Close()) }()
				iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
				require.NoError(test, err)
				defer iterator.Close()
				stream, err := iterator.Next()
				require.NoError(test, err)
				require.Implements(test, (*io.WriterTo)(nil), stream)
				require.Implements(test, (*io.Seeker)(nil), stream)
				require.Implements(test, (*io.ReaderAt)(nil), stream)
				require.Implements(test, (*interface{ Len() int })(nil), stream)
				if !aliases {
					require.IsType(test, &bytes.Reader{}, stream)
				}
				expected := payload
				if aliases {
					actualHeader, err := iterator.SchemaBytes()
					require.NoError(test, err)
					expected = bytes.Join([][]byte{actualHeader, payload[len(originalHeader):]}, nil)
				}
				require.NoError(test, iotest.TestReader(stream, expected))
			})
		}
	}
}

func BenchmarkArrowCloudIPCForward(benchmark *testing.B) {
	for _, aliases := range []bool{false, true} {
		benchmark.Run(fmt.Sprintf("aliases=%t", aliases), func(benchmark *testing.B) {
			data, err := os.ReadFile("testdata/multipleFetch/FetchResults1.json")
			require.NoError(benchmark, err)
			var page cli_service.TFetchResultsResp
			require.NoError(benchmark, json.Unmarshal(data, &page))
			originalHeader := page.ResultSetMetadata.ArrowSchema
			batch := page.Results.ArrowBatches[0]
			payload := bytes.Join([][]byte{originalHeader, bytes.Repeat(batch.Batch, 32)}, nil)
			if aliases {
				reader, err := ipc.NewReader(bytes.NewReader(originalHeader))
				require.NoError(benchmark, err)
				fields := reader.Schema().Fields()
				reader.Release()
				for index := range fields {
					fields[index].Name = fmt.Sprintf("alias_%d", index)
					page.ResultSetMetadata.Schema.Columns[index].ColumnName = fields[index].Name
				}
				var header bytes.Buffer
				writer := ipc.NewWriter(&header, ipc.WithSchema(arrow.NewSchema(fields, nil)))
				require.NoError(benchmark, writer.Close())
				page.ResultSetMetadata.ArrowSchema = header.Bytes()[:header.Len()-8]
			}
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) { _, _ = response.Write(payload) }))
			defer server.Close()
			page.Results = &cli_service.TRowSet{ResultLinks: []*cli_service.TSparkArrowResultLink{{FileLink: server.URL, RowCount: batch.RowCount * 32, ExpiryTime: time.Now().Add(time.Hour).Unix()}}}
			page.HasMoreRows = new(bool)
			format := cli_service.TSparkRowSetType_URL_BASED_SET
			page.ResultSetMetadata.ResultFormat = &format
			direct := &cli_service.TSparkDirectResults{ResultSet: &page, ResultSetMetadata: page.ResultSetMetadata, CloseOperation: &cli_service.TCloseOperationResp{}}
			cfg := config.WithDefaults()
			cfg.MaxDownloadThreads = 1
			benchmark.ReportAllocs()
			benchmark.SetBytes(int64(len(payload)))
			benchmark.ResetTimer()
			for benchmark.Loop() {
				result, rowsErr := NewRows(context.Background(), nil, &client.TestClient{}, cfg, direct, nil)
				require.NoError(benchmark, rowsErr)
				iterator, err := result.(dbsqlrows.Rows).GetArrowIPCStreams(context.Background())
				require.NoError(benchmark, err)
				stream, err := iterator.Next()
				require.NoError(benchmark, err)
				var output bytes.Buffer
				copied, err := io.Copy(&output, stream)
				require.NoError(benchmark, err)
				require.GreaterOrEqual(benchmark, copied, int64(len(payload)-len(originalHeader)))
				require.EqualValues(benchmark, output.Len(), copied)
				iterator.Close()
				require.NoError(benchmark, result.Close())
			}
		})
	}
}
