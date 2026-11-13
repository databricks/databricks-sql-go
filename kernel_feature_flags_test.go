//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/warehouse_cache"
	"github.com/stretchr/testify/require"
)

// kernelFlagWorkspaceSeq gives every fixture its own workspace ID. The kernel's
// process-wide flag cache otherwise keys on host:port, and httptest can reuse a
// port within the flag TTL, turning a cold fetch into a cache hit.
var kernelFlagWorkspaceSeq atomic.Int64

// The kernel prefetches connector-service flags before CreateSession whenever
// the driver identity is set. Telemetry is the kernel's only flag consumer, so
// an explicit opt-out must not cost that request, while unset still defers to
// the server flag (#499).
func TestKernelFeatureFlagRequestsFollowTelemetrySetting(t *testing.T) {
	for _, tc := range []struct {
		name          string
		telemetry     *bool
		serverFlag    bool
		recovery      bool
		wantFlagGets  int64
		wantTelemetry bool
	}{
		{name: "explicit false", telemetry: boolPtr(false), serverFlag: true},
		{name: "explicit false after automatic recovery", telemetry: boolPtr(false), serverFlag: true, recovery: true},
		{name: "unset with server flag on", serverFlag: true, wantFlagGets: 1, wantTelemetry: true},
		{name: "unset with server flag off", wantFlagGets: 1},
		{name: "explicit true", telemetry: boolPtr(true), wantFlagGets: 1, wantTelemetry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer warehouse_cache.ClearCache()
			workspaceID := strconv.FormatInt(4990000000+kernelFlagWorkspaceSeq.Add(1), 10)
			var flagGets, telemetryPosts, sessions atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasPrefix(r.URL.Path, "/api/2.0/connector-service/feature-flags/"):
					flagGets.Add(1)
					require.Equal(t, http.MethodGet, r.Method)
					require.Equal(t, "/api/2.0/connector-service/feature-flags/GOLANG/"+DriverVersion, r.URL.Path)
					require.Equal(t, workspaceID, r.Header.Get("X-Databricks-Org-Id"))
					value := "false"
					if tc.serverFlag {
						value = "true"
					}
					_, _ = io.WriteString(w, `{"flags":[{"name":"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForKernel","value":"`+
						value+`"}],"ttl_seconds":300}`)
				case r.URL.Path == "/telemetry-ext":
					telemetryPosts.Add(1)
					_, _ = io.WriteString(w, `{}`)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sessions"):
					sessions.Add(1)
					_, _ = io.WriteString(w, `{"session_id":"flags-session"}`)
				default:
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			defer server.Close()

			options := []ConnOption{
				WithServerHostname(server.URL), WithHTTPPath("/sql/1.0/warehouses/test?o=" + workspaceID),
				WithAccessToken("test-token"), WithUseKernel(true), WithSkipTLSHostVerify(),
				WithRetries(-1, time.Millisecond, time.Millisecond),
			}
			if tc.telemetry != nil {
				enabled := *tc.telemetry
				options = append(options, func(cfg *config.Config) { cfg.EnableTelemetry = config.NewConfigValue(enabled) })
			}
			dbConnector, err := NewConnector(options...)
			require.NoError(t, err)
			if tc.recovery {
				c := dbConnector.(*connector)
				c.cfg.UseKernel = false
				c.thriftBackendFactory = func(context.Context, *config.Config, *http.Client) (backend.Backend, error) {
					sqlState := "KP001"
					return &fakeThriftBackend{openSessionErr: client.CheckOpenSessionStatus(&cli_service.TOpenSessionResp{
						Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_ERROR_STATUS, SqlState: &sqlState},
					})}, nil
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			connection, err := dbConnector.Connect(ctx)
			require.NoError(t, err)
			require.NoError(t, connection.Close())

			require.Equal(t, int64(1), sessions.Load())
			require.Equal(t, tc.wantFlagGets, flagGets.Load(), "connector-service feature-flag GETs")
			require.Equal(t, tc.wantTelemetry, telemetryPosts.Load() > 0, "kernel telemetry export")
		})
	}
}

func boolPtr(v bool) *bool { return &v }
