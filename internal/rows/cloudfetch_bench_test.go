package rows

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	"github.com/stretchr/testify/require"
)

type cloudFetchMemoryTransport map[string][]byte

func (transport cloudFetchMemoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	data := transport[request.URL.Path]
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data)), ContentLength: int64(len(data)), Request: request}, nil
}

func BenchmarkCloudFetchDownloadMatrix(benchmark *testing.B) {
	for _, transport := range []string{"mem", "httptest"} {
		for _, dimensions := range [][2]int{{6, 3}, {32, 1000}} {
			for _, api := range []string{"rows", "batches", "ipc"} {
				benchmark.Run(fmt.Sprintf("%s/%dx%d/%s", transport, dimensions[0], dimensions[1], api), func(benchmark *testing.B) {
					fixture := newCloudFetchFixture(benchmark, dimensions[0], dimensions[1])
					cfg := config.WithDefaults()
					cfg.MaxDownloadThreads = 4
					cfg.UseLz4Compression = false
					cfg.CloudFetchSpeedThresholdMbps = 0
					if transport == "mem" {
						payloads := make(cloudFetchMemoryTransport)
						for _, link := range fixture.links {
							response, err := http.Get(link.FileLink)
							require.NoError(benchmark, err)
							data, err := io.ReadAll(response.Body)
							require.NoError(benchmark, response.Body.Close())
							require.NoError(benchmark, err)
							location, err := url.Parse(link.FileLink)
							require.NoError(benchmark, err)
							payloads[location.Path] = data
						}
						cfg.HTTPClient = &http.Client{Transport: payloads}
					}
					for _, link := range fixture.links {
						link.ExpiryTime = time.Now().Add(time.Hour).UnixMilli()
					}
					cli := &client.TestClient{FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						return &cli_service.TCloseOperationResp{}, nil
					}}
					more := false
					direct := &cli_service.TSparkDirectResults{ResultSetMetadata: fixture.metadata,
						ResultSet: &cli_service.TFetchResultsResp{HasMoreRows: &more, Results: &cli_service.TRowSet{ResultLinks: fixture.links}}}
					benchmark.ReportAllocs()
					benchmark.ResetTimer()
					for benchmark.Loop() {
						result, err := NewRows(context.Background(), nil, cli, cfg, direct, nil)
						require.NoError(benchmark, err)
						values, consumeErr := consumeCloudFetch(benchmark, result, api)
						require.NoError(benchmark, consumeErr)
						require.Len(benchmark, values, dimensions[0]*dimensions[1])
						require.NoError(benchmark, result.Close())
					}
				})
			}
		}
	}
}
