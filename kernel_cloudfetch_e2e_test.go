//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestE2EKernelCloudFetchDisabledLargeResults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large inline result in -short mode")
	}
	host, httpPath, token := pecoTestingCreds(t)
	var workspaceConnections, blockedConnections atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != net.JoinHostPort(host, "443") {
			blockedConnections.Add(1)
			http.Error(w, "Only the SQL workspace is reachable", http.StatusForbidden)
			return
		}
		upstream, err := net.DialTimeout("tcp", r.Host, 10*time.Second)
		if err != nil {
			http.Error(w, "Workspace connection failed", http.StatusBadGateway)
			return
		}
		defer upstream.Close() //nolint:errcheck
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Proxy tunneling is unavailable", http.StatusInternalServerError)
			return
		}
		client, buffered, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer client.Close() //nolint:errcheck
		workspaceConnections.Add(1)
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		if err := buffered.Flush(); err != nil {
			return
		}
		go func() { _, _ = io.Copy(upstream, buffered) }()
		_, _ = io.Copy(client, upstream)
	}))
	defer proxy.Close()
	connector, err := NewConnector(
		WithServerHostname(host), WithHTTPPath(httpPath), WithAccessToken(token),
		WithUseKernel(true), WithCloudFetch(false),
		WithSessionParams(map[string]string{"cloudfetch_enabled": "false"}),
		WithKernelProxy(KernelProxy{URL: proxy.URL}),
		WithRetries(-1, time.Millisecond, time.Millisecond),
	)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT id, sha2(CAST(id AS STRING), 512) AS payload FROM range(250000) ORDER BY id")
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var count int64
	var payloadBytes int
	digest := sha256.New()
	for rows.Next() {
		var id int64
		var payload string
		require.NoError(t, rows.Scan(&id, &payload))
		require.Equal(t, count, id)
		require.Equal(t, fmt.Sprintf("%x", sha512.Sum512([]byte(strconv.FormatInt(id, 10)))), payload)
		_, _ = fmt.Fprintf(digest, "%d\t%s\n", id, payload)
		payloadBytes += len(payload)
		count++
	}
	require.NoError(t, rows.Err())
	require.Positive(t, workspaceConnections.Load(), "SQL traffic must use the restricted proxy")
	require.Zero(t, blockedConnections.Load(), "no non-workspace connections are allowed")
	require.Equal(t, int64(250000), count)
	require.Equal(t, 32000000, payloadBytes)
	require.Equal(t, "a25ada4c6c476b53e97eb6e8a945e9ce4c14cc432c7ad672dc6fb607d660c86a", fmt.Sprintf("%x", digest.Sum(nil)))
}
