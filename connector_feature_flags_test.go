package dbsql

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/featureflags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type flagRoundTripper func(*http.Request) (*http.Response, error)

func (f flagRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConnectionOwnsFlagsWithTelemetryDisabled(t *testing.T) {
	var fetches atomic.Int32
	transport := flagRoundTripper(func(r *http.Request) (*http.Response, error) {
		fetches.Add(1)
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		assert.Equal(t, "123", r.Header.Get("X-Databricks-Org-Id"))
		assert.Equal(t, "/api/2.0/connector-service/feature-flags/GOLANG/"+DriverVersion, r.URL.Path)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
			`{"flags":[{"name":"sampleLimit","value":"42"}]}`)), Header: make(http.Header)}, nil
	})
	driverConnector, err := NewConnector(
		WithServerHostname("flags.example"), WithHTTPPath("/sql/1.0/warehouses/test?o=123"),
		WithAccessToken("test-token"), WithTransport(transport),
		func(cfg *config.Config) { cfg.EnableTelemetry = config.NewConfigValue(false) },
	)
	require.NoError(t, err)
	c := driverConnector.(*connector)
	ctx := context.Background()
	flags := featureflags.GetCache()
	request := c.featureFlagRequest()
	c.thriftBackendFactory = func(ctx context.Context, cfg *config.Config, _ *http.Client) (backend.Backend, error) {
		return thrift.NewForTest(&client.TestClient{
			FnOpenSession: func(context.Context, *cli_service.TOpenSessionReq) (*cli_service.TOpenSessionResp, error) {
				return getTestSession(), nil
			},
			FnCloseSession: func(context.Context, *cli_service.TCloseSessionReq) (*cli_service.TCloseSessionResp, error) {
				return &cli_service.TCloseSessionResp{Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}}, nil
			},
		}, getTestSession(), cfg), nil
	}
	first, err := c.Connect(ctx)
	require.NoError(t, err)
	second, err := c.Connect(ctx)
	require.NoError(t, err)
	require.Nil(t, first.(*conn).telemetry)
	require.Zero(t, fetches.Load(), "unused flags must not add connection requests")
	value, err := flags.GetInt32(ctx, *first.(*conn).featureFlags, "sampleLimit")
	require.NoError(t, err)
	require.EqualValues(t, 42, value)
	require.NoError(t, first.Close())
	value, err = flags.GetInt32(ctx, *second.(*conn).featureFlags, "sampleLimit")
	require.NoError(t, err)
	require.EqualValues(t, 42, value)
	require.EqualValues(t, 1, fetches.Load())
	require.NoError(t, second.Close())
	flags.Acquire(request.Host, request.WorkspaceID)
	defer flags.Release(request.Host, request.WorkspaceID)
	_, err = flags.GetInt32(ctx, request, "sampleLimit")
	require.NoError(t, err)
	require.EqualValues(t, 2, fetches.Load(), "last close releases the workspace cache")
}

func TestConnectionFlagFailureAndKernelPaths(t *testing.T) {
	for _, scenario := range []string{"flag fetch fails", "backend fails", "explicit kernel"} {
		t.Run(scenario, func(t *testing.T) {
			var fetches atomic.Int32
			c, err := NewConnector(WithServerHostname("flags.example"), WithAccessToken("test-token"),
				WithTransport(flagRoundTripper(func(*http.Request) (*http.Response, error) {
					fetches.Add(1)
					status := http.StatusOK
					if scenario == "flag fetch fails" {
						status = http.StatusServiceUnavailable
					}
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(`{"flags":[]}`)), Header: make(http.Header)}, nil
				})), WithUseKernel(scenario == "explicit kernel"))
			require.NoError(t, err)
			connector := c.(*connector)
			connector.thriftBackendFactory = func(context.Context, *config.Config, *http.Client) (backend.Backend, error) {
				if scenario == "backend fails" {
					return nil, errors.New("backend failed")
				}
				return &fakeThriftBackend{sessionID: "test-session"}, nil
			}
			connector.kernelBackendFactory = func(context.Context, *config.Config) (backend.Backend, error) {
				return &fakeKernelBackend{}, nil
			}
			connection, err := c.Connect(context.Background())
			if scenario == "backend fails" {
				require.ErrorContains(t, err, "backend failed")
			} else {
				require.NoError(t, err)
				require.Zero(t, fetches.Load())
				if scenario == "flag fetch fails" {
					value, err := featureflags.GetCache().GetBool(context.Background(), *connection.(*conn).featureFlags, "missing")
					require.Error(t, err)
					require.False(t, value)
				}
				require.NoError(t, connection.Close())
			}
			// No consumer is left after a failed open or close; a getter cannot fetch.
			value, err := featureflags.GetCache().GetBool(context.Background(), connector.featureFlagRequest(), "missing")
			require.NoError(t, err)
			require.False(t, value)
			if scenario == "flag fetch fails" {
				require.EqualValues(t, 1, fetches.Load())
			} else {
				require.Zero(t, fetches.Load())
			}
		})
	}
}
