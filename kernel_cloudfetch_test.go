//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/warehouse_cache"
	"github.com/stretchr/testify/require"
)

func TestKernelCloudFetchDisabledPreservesUnsupportedServerError(t *testing.T) {
	for _, automaticRecovery := range []bool{false, true} {
		for _, rejectSession := range []bool{false, true} {
			t.Run(fmt.Sprintf("recovery=%t/session_rejected=%t", automaticRecovery, rejectSession), func(t *testing.T) {
				testKernelCloudFetchDisabledUnsupportedServer(t, automaticRecovery, rejectSession)
			})
		}
	}
}

func testKernelCloudFetchDisabledUnsupportedServer(t *testing.T, automaticRecovery, rejectSession bool) {
	t.Helper()
	defer warehouse_cache.ClearCache()
	var sessions atomic.Int64
	var executions atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sessions"):
			sessions.Add(1)
			var body struct {
				SessionConfs map[string]string `json:"session_confs"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SessionConfs["can_cloud_download"] != "false" {
				http.Error(w, "missing disabled download capability", http.StatusBadRequest)
				return
			}
			for key := range body.SessionConfs {
				if strings.EqualFold(key, "cloudfetch_enabled") || (strings.EqualFold(key, "can_cloud_download") && key != "can_cloud_download") {
					http.Error(w, "conflicting download parameter", http.StatusBadRequest)
					return
				}
			}
			if rejectSession {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error_code":"CONFIG_NOT_AVAILABLE","message":"Configuration can_cloud_download is not available. SQLSTATE: 42K0I"}`)
				return
			}
			_, _ = io.WriteString(w, `{"session_id":"test-session"}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/statements"):
			executions.Add(1)
			_, _ = io.WriteString(w, `{"statement_id":"unsupported-inline","status":{"state":"FAILED","sql_state":"42K0I","error":{"error_code":"CONFIG_NOT_AVAILABLE","message":"Configuration can_cloud_download is not available"}}}`)
		default:
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer server.Close()
	dbConnector, err := NewConnector(
		WithServerHostname(server.URL), WithHTTPPath("/sql/1.0/warehouses/test"),
		WithAccessToken("test-token"), WithUseKernel(true), WithCloudFetch(false),
		WithSessionParams(map[string]string{"CAN_CLOUD_DOWNLOAD": "true"}),
		WithSkipTLSHostVerify(), WithRetries(-1, time.Millisecond, time.Millisecond),
	)
	require.NoError(t, err)
	var thriftAttempts atomic.Int64
	if automaticRecovery {
		c := dbConnector.(*connector)
		c.cfg.UseKernel = false
		c.thriftBackendFactory = func(context.Context, *config.Config, *http.Client) (backend.Backend, error) {
			thriftAttempts.Add(1)
			sqlState := "KP001"
			return &fakeThriftBackend{openSessionErr: client.CheckOpenSessionStatus(&cli_service.TOpenSessionResp{
				Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_ERROR_STATUS, SqlState: &sqlState},
			})}, nil
		}
	}
	db := sql.OpenDB(dbConnector)
	defer db.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := db.Conn(ctx)
	if rejectSession {
		require.Error(t, err)
		require.Contains(t, err.Error(), "CONFIG_NOT_AVAILABLE")
		require.Contains(t, err.Error(), "42K0I")
		require.Equal(t, int64(0), executions.Load())
	} else {
		require.NoError(t, err)
		defer connection.Close() //nolint:errcheck
		_, err = connection.ExecContext(ctx, "SELECT 1")
		require.Error(t, err)
		var executionError dbsqlerr.DBExecutionError
		require.ErrorAs(t, err, &executionError)
		require.Equal(t, "42K0I", executionError.SqlState())
		require.Contains(t, err.Error(), "CONFIG_NOT_AVAILABLE")
		require.Equal(t, int64(1), executions.Load(), "unsupported delivery must not fall back")
	}
	require.Equal(t, int64(1), sessions.Load(), "unsupported delivery must not reopen the session")
	if automaticRecovery {
		require.Equal(t, int64(1), thriftAttempts.Load(), "backend recovery must not repeat")
	}
}
