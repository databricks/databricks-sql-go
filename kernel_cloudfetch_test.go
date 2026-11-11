//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"database/sql"
	"encoding/json"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestKernelCloudFetchDisabledPreservesUnsupportedServerError(t *testing.T) {
	var executions atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/sessions"):
			var body struct {
				SessionConfs map[string]string `json:"session_confs"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.SessionConfs["can_cloud_download"] != "false" {
				http.Error(w, "missing disabled download capability", http.StatusBadRequest)
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
	connector, err := NewConnector(
		WithServerHostname(server.URL), WithHTTPPath("/sql/1.0/warehouses/test"),
		WithAccessToken("test-token"), WithUseKernel(true), WithCloudFetch(false),
		WithSkipTLSHostVerify(), WithRetries(-1, time.Millisecond, time.Millisecond),
	)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := db.Conn(ctx)
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
