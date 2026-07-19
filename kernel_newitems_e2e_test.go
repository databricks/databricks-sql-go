//go:build cgo && databricks_kernel

package dbsql

import (
	"bufio"
	"context"
	"encoding/base64"
	"net"
	"net/http"
	"sync"
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

// TestKernelE2EProxyForwardsCredentials proves WithKernelProxy's basic-auth
// credentials reach the kernel in the RIGHT slots — the gap TestSetProxy can't cover,
// since it only asserts the setter returns OK and the kernel's C config is opaque
// (no readback), so a username↔password swap or a dropped bypass would pass every
// unit test yet break proxy basic-auth in production.
//
// A local CONNECT proxy captures the Proxy-Authorization header the kernel's HTTP
// stack sends, then refuses to tunnel. The connect is EXPECTED to fail (nothing is
// tunneled), but the captured header is the proof: we assert it decodes to exactly
// "user:pass" (a slot swap would decode to "pass:user"). Observing the credential on
// the wire needs no real warehouse — only that the kernel forwarded it correctly.
func TestKernelE2EProxyForwardsCredentials(t *testing.T) {
	const wantUser, wantPass = "proxyuser", "proxypass"

	var mu sync.Mutex
	var gotAuth string
	sawConnect := make(chan struct{}, 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				br := bufio.NewReader(c)
				// Handle both proxy-auth timings on one connection: a client that sends
				// Proxy-Authorization preemptively, and one that waits for a 407
				// challenge before resending. Loop so a same-connection retry after our
				// 407 is captured; clients that open a fresh connection are covered by
				// the accept loop.
				for {
					req, err := http.ReadRequest(br)
					if err != nil {
						return
					}
					if auth := req.Header.Get("Proxy-Authorization"); auth != "" {
						mu.Lock()
						gotAuth = auth
						mu.Unlock()
						select {
						case sawConnect <- struct{}{}:
						default:
						}
						// Captured — refuse to tunnel so the connect fails fast.
						_, _ = c.Write([]byte("HTTP/1.1 502 Bad Gateway\r\n\r\n"))
						return
					}
					// No credentials yet — challenge, then read the retry on this conn.
					_, _ = c.Write([]byte("HTTP/1.1 407 Proxy Authentication Required\r\n" +
						"Proxy-Authenticate: Basic realm=\"test\"\r\n" +
						"Content-Length: 0\r\n\r\n"))
				}
			}(conn)
		}
	}()

	proxyURL := "http://" + ln.Addr().String()
	db := kernelTestDBWith(t, WithKernelProxy(proxyURL, wantUser, wantPass, ""))
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The query is expected to fail (the proxy never tunnels); we only need the
	// kernel to have attempted CONNECT through the proxy so the header is captured.
	var got int64
	_ = db.QueryRowContext(ctx, "SELECT 1").Scan(&got)

	select {
	case <-sawConnect:
	case <-time.After(30 * time.Second):
		t.Fatal("kernel never issued a CONNECT through the configured proxy")
	}

	mu.Lock()
	auth := gotAuth
	mu.Unlock()
	if auth == "" {
		t.Fatal("proxy saw no Proxy-Authorization header — credentials were dropped")
	}
	const prefix = "Basic "
	if len(auth) <= len(prefix) || auth[:len(prefix)] != prefix {
		t.Fatalf("Proxy-Authorization = %q, want a Basic credential", auth)
	}
	decoded, err := base64.StdEncoding.DecodeString(auth[len(prefix):])
	if err != nil {
		t.Fatalf("decode Proxy-Authorization: %v", err)
	}
	if want := wantUser + ":" + wantPass; string(decoded) != want {
		t.Errorf("proxy credentials = %q, want %q (a username/password slot swap would show %q)",
			decoded, want, wantPass+":"+wantUser)
	}
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
