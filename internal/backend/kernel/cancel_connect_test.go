//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// blackHoleListener accepts TCP connections and then holds them open forever
// without ever writing a byte, so a client's TLS handshake blocks waiting for the
// ServerHello. It is the deterministic stand-in for a warehouse whose connect
// hangs (a cold-start stall or a network black hole), which is what the
// mid-connect cancel path must be able to interrupt. Returns the "host:port" to
// point the kernel at, and a stop func that closes the listener and every accepted
// connection. Accepted conns are retained so the runtime can't close them early.
func blackHoleListener(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	// mu guards conns: the accept goroutine appends while stop() ranges over it.
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			mu.Lock()
			conns = append(conns, c) // keep it open (and referenced): never respond
			mu.Unlock()
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	return ln.Addr().String(), func() {
		close(done)
		_ = ln.Close() // unblocks Accept → the goroutine returns, no more appends
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	}
}

// TestOpenSessionHonorsCtxDeadlineMidConnect is the connect-side counterpart to the
// mid-fetch cancel test: it proves the ctxWatcher fires the kernel cancel token
// WHILE kernel_session_open_cancellable is blocked inside a connect, so a caller's
// deadline is honored mid-connect rather than the connect running until the kernel
// gives up on its own.
//
// It drives the real cgo OpenSession (not database/sql, which can short-circuit an
// already-cancelled ctx before the driver is ever called and can wrap the result in
// its own retry error) against a black-hole server whose TLS handshake never
// completes. A short ctx deadline must then interrupt the blocking connect via the
// token and surface as a "session_open cancelled" error matching
// context.DeadlineExceeded. Without the cancellable wiring the connect would block
// until the kernel's own timeout, so the goroutine guard below fails loudly instead
// of letting a regression hang the suite.
func TestOpenSessionHonorsCtxDeadlineMidConnect(t *testing.T) {
	addr, stop := blackHoleListener(t)
	defer stop()

	k := New(Config{
		// normalise_host prepends https://, so the kernel dials this host:port over
		// TLS and blocks on the handshake the black-hole server never answers.
		Host:        addr,
		WarehouseID: "wh-test",
		Auth:        Auth{Mode: AuthPAT, Token: "dapi-not-a-real-token"},
		// Relax cert checks so nothing rejects the (never-arriving) server cert before
		// the deadline can fire; the block is at the handshake read regardless.
		TLSSkipVerify: true,
	})

	const deadline = 750 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	type result struct {
		err     error
		elapsed time.Duration
	}
	resCh := make(chan result, 1)
	start := time.Now()
	go func() {
		err := k.OpenSession(ctx)
		resCh <- result{err, time.Since(start)}
	}()

	select {
	case res := <-resCh:
		if res.err == nil {
			t.Fatal("OpenSession succeeded against a black-hole server, want a cancellation error")
		}
		// The deadline — not an unrelated connect failure — must be what ended it, and
		// the connect-cancel branch must wrap it as "session_open cancelled".
		if !errors.Is(res.err, context.DeadlineExceeded) {
			t.Errorf("OpenSession err = %v, want it to match context.DeadlineExceeded", res.err)
		}
		if !strings.Contains(res.err.Error(), "session_open cancelled") {
			t.Errorf("OpenSession err = %q, want the mid-connect %q wrap (proves the cancellable path, "+
				"not a pre-connect guard, handled it)", res.err, "session_open cancelled")
		}
		// It must return promptly after the deadline, not run to some far-off connect
		// timeout — the whole point of the mid-connect cancel.
		if res.elapsed > 20*time.Second {
			t.Errorf("OpenSession took %v after a %v deadline; the mid-connect cancel was not honored promptly", res.elapsed, deadline)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("OpenSession did not return within 30s of a 750ms deadline — the mid-connect cancel token was not fired")
	}
}
