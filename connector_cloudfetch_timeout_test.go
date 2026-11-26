package dbsql

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/v2/errors"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/client"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/v2/internal/errors"
	driverrows "github.com/databricks/databricks-sql-go/v2/internal/rows"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cloudFetchStallingTransport struct {
	cancelled chan<- struct{}
	release   <-chan struct{}
}

func (transport cloudFetchStallingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	select {
	case <-request.Context().Done():
		transport.cancelled <- struct{}{}
		return nil, request.Context().Err()
	case <-transport.release:
		return nil, context.Canceled
	}
}

func TestCloudFetchClientTimeout(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		stallBody     bool
		withTransport bool
	}{
		{name: "default/headers"},
		{name: "default/body", stallBody: true},
		{name: "WithTransport/headers", withTransport: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			release := make(chan struct{})
			cancelled := make(chan struct{}, 2)
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if testCase.withTransport {
					response.WriteHeader(http.StatusNoContent)
					return
				}
				if testCase.stallBody {
					response.Header().Set("Content-Length", "100")
					response.WriteHeader(http.StatusOK)
					response.(http.Flusher).Flush()
				}
				select {
				case <-request.Context().Done():
					cancelled <- struct{}{}
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)

			options := []ConnOption{func(cfg *config.Config) {
				cfg.ClientTimeout = 100 * time.Millisecond
				cfg.RetryMax = 0
				cfg.UseLz4Compression = false
				cfg.MaxDownloadThreads = 2
			}}
			if testCase.withTransport {
				options = append(options, WithTransport(cloudFetchStallingTransport{cancelled: cancelled, release: release}))
			}
			con, err := NewConnector(options...)
			require.NoError(t, err)
			cfg := con.(*connector).cfg
			fixture, err := os.ReadFile("internal/rows/testdata/directResultsMultipleFetch/ExecuteStatement.json")
			require.NoError(t, err)
			var execute cli_service.TExecuteStatementResp
			require.NoError(t, json.Unmarshal(fixture, &execute))
			direct := execute.DirectResults
			direct.ResultSet.Results.ArrowBatches = nil
			expiry := time.Now().Add(time.Hour).UnixMilli()
			direct.ResultSet.Results.ResultLinks = []*cli_service.TSparkArrowResultLink{
				{FileLink: server.URL + "/file?sig=SECRET_SIGNATURE", ExpiryTime: expiry, RowCount: 1},
				{FileLink: server.URL + "/file?sig=SECRET_SIGNATURE", ExpiryTime: expiry, RowCount: 1, StartRowOffset: 1},
			}
			format := cli_service.TSparkRowSetType_URL_BASED_SET
			direct.ResultSetMetadata.ResultFormat = &format
			testClient := &client.TestClient{
				FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
					return &cli_service.TCloseOperationResp{}, nil
				},
			}
			rowSet, err := driverrows.NewRows(context.Background(), nil, testClient, cfg, direct, nil)
			require.NoError(t, err)
			defer rowSet.Close() //nolint:errcheck

			result := make(chan error, 1)
			start := time.Now()
			go func() { result <- rowSet.Next(make([]driver.Value, len(rowSet.Columns()))) }()
			select {
			case nextErr := <-result:
				require.Error(t, nextErr)
				assert.ErrorIs(t, nextErr, dbsqlerr.DriverError)
				assert.ErrorIs(t, nextErr, context.DeadlineExceeded)
				assert.Equal(t, dbsqlerrint.CategoryChunkDownload, dbsqlerrint.CategoryFromError(nextErr))
				assert.NotContains(t, nextErr.Error(), "SECRET_SIGNATURE")
				assert.GreaterOrEqual(t, time.Since(start), cfg.ClientTimeout)
			case <-time.After(cfg.ClientTimeout + 2*time.Second):
				server.CloseClientConnections()
				select {
				case nextErr := <-result:
					assert.Error(t, nextErr)
				case <-time.After(2 * time.Second):
					t.Fatal("CloudFetch read did not exit after connections closed")
				}
				t.Fatal("CloudFetch read did not honor ClientTimeout")
			}
			for range 2 {
				select {
				case <-cancelled:
				case <-time.After(2 * time.Second):
					t.Fatal("in-flight CloudFetch request was not cancelled")
				}
			}
		})
	}
}

func TestCloudFetchWithTransportClientTimeoutOptionOrder(t *testing.T) {
	for _, timeout := range []time.Duration{0, 100 * time.Millisecond, 5 * time.Second} {
		for _, order := range []string{"before", "after"} {
			t.Run(timeout.String()+"/"+order, func(t *testing.T) {
				options := []ConnOption{func(cfg *config.Config) { cfg.ClientTimeout = timeout }, WithTransport(http.DefaultTransport)}
				if order == "after" {
					options[0], options[1] = options[1], options[0]
				}
				con, err := NewConnector(options...)
				require.NoError(t, err)
				require.NotNil(t, con.(*connector).cfg.HTTPClient)
				assert.Equal(t, timeout, con.(*connector).cfg.HTTPClient.Timeout)
				assert.Same(t, http.DefaultTransport, con.(*connector).cfg.HTTPClient.Transport)
			})
		}
	}
}
