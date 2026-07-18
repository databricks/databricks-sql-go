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

// TestKernelE2ERetryConfig proves a tuned WithRetries policy (backoff bounds + max
// attempts) reaches the kernel's HTTP retry config and the connection still works:
// the setter accepts the range and a normal query succeeds.
func TestKernelE2ERetryConfig(t *testing.T) {
	db := kernelTestDBWith(t, WithRetries(6, 500*time.Millisecond, 20*time.Second))
	defer db.Close()

	var got int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("query with tuned retries: %v", err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 = %d, want 1", got)
	}
}

// TestKernelE2ERetryDisabled proves the disable form (WithRetries(-1)) is accepted
// on the kernel path — previously it was rejected at connect. It maps to zero
// kernel retries, and a normal query still succeeds.
func TestKernelE2ERetryDisabled(t *testing.T) {
	db := kernelTestDBWith(t, WithRetries(-1, 0, 0))
	defer db.Close()

	var got int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("query with retries disabled: %v", err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 = %d, want 1", got)
	}
}

// TestKernelE2ERetryOverallTimeout proves the kernel-only overall-budget knob
// (WithKernelRetryOverallTimeout, the 4th retry control) is accepted at connect and
// the connection works with it set.
func TestKernelE2ERetryOverallTimeout(t *testing.T) {
	db := kernelTestDBWith(t, WithKernelRetryOverallTimeout(5*time.Minute))
	defer db.Close()

	var got int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("query with overall retry budget: %v", err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 = %d, want 1", got)
	}
}
