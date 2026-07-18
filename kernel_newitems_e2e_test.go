//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"testing"
	"time"
)

// This file holds the live end-to-end tests for the "new-items" kernel features
// (advanced proxy, Geography, configurable backoff, conn/error telemetry). They
// run against the same DATABRICKS_PECOTESTING_* warehouse as the rest of the
// kernel E2E suite (via kernelTestDBWith / pecoTestingCreds) and self-skip when
// those credentials are unset.

// TestKernelE2EProxyRejectsBadURL proves the advanced-proxy setter (WithKernelProxy)
// is actually applied on the kernel path. No proxy is provisioned for the staging
// warehouse, so a real round-trip can't be asserted; instead we route through an
// unreachable proxy and require the connect to FAIL. An ignored proxy setter would
// let the connection succeed directly, so a failure here is the proof the URL
// reached the kernel's HTTP stack. A bounded context keeps a hung dial from
// stalling the test.
func TestKernelE2EProxyRejectsBadURL(t *testing.T) {
	// A routable-but-dead proxy address: connections are refused/time out rather
	// than silently bypassed. 127.0.0.1:1 has no listener.
	db := kernelTestDBWith(t, WithKernelProxy("http://127.0.0.1:1", "", "", ""))
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var got int64
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&got)
	if err == nil {
		t.Fatal("expected the query to fail when routed through an unreachable proxy " +
			"(an ignored proxy setter would connect directly), got success")
	}
	t.Logf("connect through unreachable proxy failed as expected: %v", err)
}
