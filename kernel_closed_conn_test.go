//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKernelClosedConnRejectsStatementsWithoutRequests is the hermetic SEA leg
// of SESSION-021: a fake SEA endpoint counts CreateSession / ExecuteStatement
// while a closed kernel-backed conn rejects Prepare, PrepareContext and direct
// execution, then a new conn from the same connector opens its own session.
func TestKernelClosedConnRejectsStatementsWithoutRequests(t *testing.T) {
	ca, caKey, caPEM := makeMTLSTestCA(t)
	serverCert := makeMTLSTestLeaf(t, ca, caKey, "127.0.0.1", net.ParseIP("127.0.0.1"))

	var creates, deletes, statements atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimSuffix(r.URL.Path, "/")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/sessions"):
			creates.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"session_id":"session-021"}`)
		case r.Method == http.MethodDelete && strings.HasSuffix(path, "/sessions/session-021"):
			deletes.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{}`)
		case strings.Contains(path, "/statements"):
			statements.Add(1)
			http.Error(w, "unexpected statement request", http.StatusInternalServerError)
		default: // feature flags
			http.NotFound(w, r)
		}
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert.tlsCert}, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	connector, err := NewConnector(
		WithServerHostname(srv.Listener.Addr().String()),
		WithHTTPPath("/sql/1.0/warehouses/hermetic"),
		WithAccessToken("dapi-hermetic-placeholder"),
		WithUseKernel(true),
		WithRetries(-1, 0, 0),
		WithKernelTrustedCerts(caPEM),
	)
	require.NoError(t, err)
	ctx := context.Background()

	a, err := connector.Connect(ctx)
	require.NoError(t, err)
	require.NoError(t, a.Close())
	require.Equal(t, int32(1), creates.Load())
	require.Equal(t, int32(1), deletes.Load())

	requireClosed := func(err error) {
		t.Helper()
		require.ErrorIs(t, err, driver.ErrBadConn)
		require.ErrorContains(t, err, "connection is closed")
	}
	_, err = a.Prepare("SELECT 1")
	requireClosed(err)
	_, err = a.(driver.ConnPrepareContext).PrepareContext(ctx, "SELECT 1")
	requireClosed(err)
	_, err = a.(driver.QueryerContext).QueryContext(ctx, "SELECT 1", nil)
	requireClosed(err)
	_, err = a.(driver.ExecerContext).ExecContext(ctx, "SELECT 1", nil)
	requireClosed(err)
	assert.Equal(t, int32(1), creates.Load(), "rejected calls must not open a session")
	assert.Equal(t, int32(0), statements.Load(), "rejected calls must not execute a statement")

	b, err := connector.Connect(ctx)
	require.NoError(t, err)
	assert.Equal(t, int32(2), creates.Load(), "a later conn must open its own session")
	s, err := b.Prepare("SELECT 1")
	require.NoError(t, err)
	require.NoError(t, s.Close())
	require.NoError(t, b.Close())
	assert.Equal(t, int32(0), statements.Load())
}

func TestKernelDeadSessionRejectsPinnedConnWithoutRequests(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		message string
	}{
		{"timed out session", http.StatusBadRequest, `{"error_code":"BAD_REQUEST","message":"Invalid SessionHandle: test-session"}`, "Invalid SessionHandle"},
		{"missing session", http.StatusNotFound, `{"error_code":"NOT_FOUND","message":"Session does not exist: test-session"}`, "Session does not exist"},
	}
	for _, tc := range cases {
		for _, nextCall := range []string{"exec", "query"} {
			t.Run(tc.name+"/"+nextCall, func(t *testing.T) {
				ca, caKey, caPEM := makeMTLSTestCA(t)
				serverCert := makeMTLSTestLeaf(t, ca, caKey, "127.0.0.1", net.ParseIP("127.0.0.1"))
				var statements atomic.Int32
				srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					path := strings.TrimSuffix(r.URL.Path, "/")
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.Method == http.MethodPost && strings.HasSuffix(path, "/sessions"):
						_, _ = io.WriteString(w, `{"session_id":"test-session"}`)
					case r.Method == http.MethodDelete && strings.HasSuffix(path, "/sessions/test-session"):
						_, _ = io.WriteString(w, `{}`)
					case strings.Contains(path, "/statements"):
						statements.Add(1)
						w.WriteHeader(tc.status)
						_, _ = io.WriteString(w, tc.body)
					default:
						http.NotFound(w, r)
					}
				}))
				srv.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert.tlsCert}, MinVersion: tls.VersionTLS12}
				srv.StartTLS()
				t.Cleanup(srv.Close)

				connector, err := NewConnector(
					WithServerHostname(srv.Listener.Addr().String()),
					WithHTTPPath("/sql/1.0/warehouses/hermetic"),
					WithAccessToken("dapi-hermetic-placeholder"),
					WithUseKernel(true),
					WithRetries(-1, 0, 0),
					WithKernelTrustedCerts(caPEM),
				)
				require.NoError(t, err)
				db := sql.OpenDB(connector)
				t.Cleanup(func() { _ = db.Close() })
				ctx := context.Background()
				pinned, err := db.Conn(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { _ = pinned.Close() })

				_, err = pinned.ExecContext(ctx, "UPDATE test SET value = value + 1")
				require.ErrorContains(t, err, tc.message)
				require.NotErrorIs(t, err, driver.ErrBadConn)
				require.Equal(t, int32(1), statements.Load(), "the failed statement must not be replayed")
				require.NoError(t, pinned.Raw(func(dc any) error {
					require.False(t, dc.(driver.Validator).IsValid())
					return nil
				}))

				if nextCall == "exec" {
					_, err = pinned.ExecContext(ctx, "SELECT 1")
				} else {
					var rows *sql.Rows
					rows, err = pinned.QueryContext(ctx, "SELECT 1")
					if rows != nil {
						_ = rows.Close()
					}
				}
				require.ErrorIs(t, err, driver.ErrBadConn)
				require.Equal(t, int32(1), statements.Load(), "the invalid connection must reject requests locally")
				_, err = pinned.ExecContext(ctx, "SELECT 1")
				require.ErrorIs(t, err, sql.ErrConnDone)
			})
		}
	}
}
