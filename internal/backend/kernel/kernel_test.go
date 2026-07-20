//go:build cgo && databricks_kernel

package kernel

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
	"github.com/databricks/databricks-sql-go/logger"
)

// setAuth maps each Auth mode to exactly one kernel_session_config_set_auth_*
// value-setter. These are pure config setters (no network), so we can assert the
// call succeeds against a freshly allocated config for every mode — exercising the
// real cgo path (arg marshaling, NULL-for-empty on the optional U2M args) end to
// end via the trySetAuth test helper (cgo cannot be used directly in a _test.go
// file). A failure here means the mode→setter wiring or the C signature drifted.
func TestSetAuthByMode(t *testing.T) {
	cases := []struct {
		name string
		auth Auth
	}{
		{"PAT", Auth{Mode: AuthPAT, Token: "dapi-x"}},
		{"M2M", Auth{Mode: AuthM2M, ClientID: "cid", ClientSecret: "sec"}},
		// "U2M full" populates Scopes/RedirectPort, which no production path sets today
		// (resolveKernelAuth sources only the client id — see kernel.Auth docs). It is
		// kept deliberately to pin the marshalling of those optional set_auth_u2m args
		// (joinScopes + uint16 port), so the dormant wiring stays correct for a future
		// U2M scopes/port option.
		{"U2M full", Auth{Mode: AuthU2M, ClientID: "u2m-cid", Scopes: []string{"sql", "offline_access"}, RedirectPort: 8030}},
		// U2M with everything defaulted (the production shape): empty client id / no
		// scopes / port 0 must pass NULL / 0 so the kernel applies its own defaults
		// (exercises newCStrOrNull).
		{"U2M defaults", Auth{Mode: AuthU2M}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := trySetAuth(c.auth); err != nil {
				t.Errorf("setAuth(%s) = %v, want nil", c.name, err)
			}
		})
	}
}

// TestSetKernelTLS exercises the real cgo setters for the experimental kernel-only
// TLS knobs (the byte-buffer trusted-CA bundle + the hostname-skip bool) via the
// trySetKernelTLS seam — proving the (*C.uint8_t, C.size_t) marshalling and the C
// signatures. A failure here means the field→setter wiring or a C signature
// drifted.
func TestSetKernelTLS(t *testing.T) {
	ca := []byte("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----\n")
	cert := []byte("-----BEGIN CERTIFICATE-----\nleaf\n-----END CERTIFICATE-----\n")
	// Assemble the key's PEM marker at runtime so the repo's secret scanner
	// doesn't flag a literal BEGIN-PRIVATE-KEY (the bytes are opaque to the
	// marshalling path under test).
	key := []byte("-----BEGIN " + "PRIVATE" + " KEY-----\nkey\n-----END PRIVATE KEY-----\n")
	cases := []struct {
		name string
		cfg  Config
	}{
		{"trusted certs only", Config{TLSTrustedCertsPEM: ca}},
		{"skip hostname only", Config{TLSSkipHostnameVerify: true}},
		{"both together", Config{TLSTrustedCertsPEM: ca, TLSSkipHostnameVerify: true}},
		{"client certificate (mTLS)", Config{TLSClientCertPEM: cert, TLSClientKeyPEM: key}},
		{"all together", Config{TLSTrustedCertsPEM: ca, TLSSkipHostnameVerify: true, TLSClientCertPEM: cert, TLSClientKeyPEM: key}},
		{"none (no-op)", Config{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := trySetKernelTLS(c.cfg); err != nil {
				t.Errorf("applyKernelTLS(%s) = %v, want nil", c.name, err)
			}
		})
	}
}

// TestABIVersionMatches asserts the linked kernel library's C-ABI version
// matches the header the driver compiled against — the same handshake
// checkABIVersion runs at connect. It exercises the real cgo symbols
// (kernel_abi_version + the DATABRICKS_KERNEL_ABI_VERSION macro), so a stale
// .a-vs-header pairing fails here at test time rather than misreading status
// codes at runtime. It also asserts checkABIVersion() returns nil for the
// matched pair the test binary links.
func TestABIVersionMatches(t *testing.T) {
	got, want := abiVersions()
	if got != want {
		t.Fatalf("kernel_abi_version() = %d, header DATABRICKS_KERNEL_ABI_VERSION = %d", got, want)
	}
	if err := checkABIVersion(); err != nil {
		t.Errorf("checkABIVersion() = %v, want nil for the linked (matching) library", err)
	}
}

// TestKernelLogLevel and TestResolveKernelLogArg — the pure level-resolution tests —
// live in the untagged logging_level_test.go so they run under CGO_ENABLED=0. The
// tests below exercise klog/klogCtx and so need the cgo build.

// klog / klogCtx must be allocation-free at the default (above-Debug) log level —
// the "no hot-path cost, safe during benchmarks" guarantee. klogCtx is the one that
// matters: logger.WithContext eagerly allocates (zerolog's With() does
// make([]byte, 0, 500)) BEFORE the .Debug() gate, so without the up-front
// kernelDebugOff() guard this would allocate ~500 B per call — per Arrow batch on the
// nextBatch hot path. Guards against reintroducing that.
func TestKernelLogNoAllocWhenOff(t *testing.T) {
	prev := logger.Logger.GetLevel()
	if err := logger.SetLogLevel("warn"); err != nil { // the default level
		t.Fatal(err)
	}
	defer logger.SetLogLevel(prev.String()) //nolint:errcheck // restoring a known-good level

	ctx := driverctx.NewContextWithConnId(context.Background(), "conn-1")
	if n := testing.AllocsPerRun(100, func() {
		klog("hot path %d", 1)
	}); n != 0 {
		t.Errorf("klog allocated %v times at Warn level, want 0", n)
	}
	if n := testing.AllocsPerRun(100, func() {
		klogCtx(ctx, "hot path %d", 1)
	}); n != 0 {
		t.Errorf("klogCtx allocated %v times at Warn level, want 0", n)
	}
}

// TestKernelLogCtxEmitsCorrelation proves the positive half of the contract the
// alloc test can't see: at debug level klogCtx actually EMITS through the shared
// logger AND attaches connId/corrId/queryId, and at the default warn level it emits
// nothing. Without this, a regression that silently stopped emitting or stopped
// attaching the fields would still pass TestKernelLogNoAllocWhenOff (0 allocs is
// also what "emit nothing" looks like).
func TestKernelLogCtxEmitsCorrelation(t *testing.T) {
	prev := logger.Logger.GetLevel()
	var buf bytes.Buffer
	logger.SetLogOutput(&buf)
	// Restore the package defaults (stderr sink, prior level) so we don't leak the
	// buffer/level into sibling tests.
	defer func() {
		logger.SetLogOutput(os.Stderr)
		_ = logger.SetLogLevel(prev.String())
	}()

	ctx := driverctx.NewContextWithConnId(context.Background(), "conn-1")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corr-2")
	ctx = driverctx.NewContextWithQueryId(ctx, "query-3")

	// Debug level: the line is emitted and carries all three correlation fields.
	if err := logger.SetLogLevel("debug"); err != nil {
		t.Fatal(err)
	}
	klogCtx(ctx, "step %s", "execute")

	var rec struct {
		Level   string `json:"level"`
		Message string `json:"message"`
		ConnID  string `json:"connId"`
		CorrID  string `json:"corrId"`
		QueryID string `json:"queryId"`
	}
	line := bytes.TrimSpace(buf.Bytes())
	if len(line) == 0 {
		t.Fatal("klogCtx emitted nothing at debug level, want one line")
	}
	if err := json.Unmarshal(line, &rec); err != nil {
		t.Fatalf("klogCtx output is not the expected JSON log line: %v (line: %s)", err, line)
	}
	if rec.Level != "debug" {
		t.Errorf("level = %q, want %q", rec.Level, "debug")
	}
	if rec.Message != "[kernel] step execute" {
		t.Errorf("message = %q, want %q", rec.Message, "[kernel] step execute")
	}
	if rec.ConnID != "conn-1" || rec.CorrID != "corr-2" || rec.QueryID != "query-3" {
		t.Errorf("correlation fields = {connId:%q corrId:%q queryId:%q}, want {conn-1 corr-2 query-3}",
			rec.ConnID, rec.CorrID, rec.QueryID)
	}

	// Warn level (the default): nothing is emitted.
	buf.Reset()
	if err := logger.SetLogLevel("warn"); err != nil {
		t.Fatal(err)
	}
	klogCtx(ctx, "step %s", "execute")
	if got := bytes.TrimSpace(buf.Bytes()); len(got) != 0 {
		t.Errorf("klogCtx emitted %q at warn level, want nothing", got)
	}
}

// The pure error-classifier tests (TestIsBadConnection, TestIsSessionFatal,
// TestToConnError, TestToStatementErrorNeverBadConn) live in the untagged
// errors_classify_test.go so they run under CGO_ENABLED=0. The tests below need a
// *KernelBackend, so they stay tagged.

// evictIfSessionFatal flips SessionValid()→false on a session-fatal error (so the
// pool discards the conn) WITHOUT the error being driver.ErrBadConn (so the
// statement is never transparently re-run).
func TestEvictIfSessionFatal(t *testing.T) {
	// valid tracks the session-dead flag SessionValid() gates on; the opaque
	// session pointer is orthogonal here (can't construct the incomplete C type),
	// so assert on k.valid directly.
	k := &KernelBackend{valid: true}

	// Non-fatal (e.g. a SQL error) leaves the session valid.
	k.evictIfSessionFatal(&KernelError{Code: statusSqlError})
	if !k.valid {
		t.Error("a SQL error must not evict the session")
	}

	// A session-fatal error marks the session dead, and the surfaced statement-path
	// error is NOT driver.ErrBadConn (so database/sql won't re-run the statement).
	fatal := &KernelError{Code: statusUnavailable, Message: "session gone"}
	k.evictIfSessionFatal(fatal)
	if k.valid {
		t.Error("a session-fatal error must evict the session (valid=false)")
	}
	if errors.Is(toStatementError(fatal), driver.ErrBadConn) {
		t.Error("the statement-path error must not be driver.ErrBadConn (no replay)")
	}
}

// Bound parameters are rejected up front by Execute with a clear error, before
// any session/C work — so this runs on a zero-value backend. The returned
// Operation must be non-nil (Backend contract) and its Close must report
// closed=false, since no server statement was ever created (a phantom
// CLOSE_STATEMENT would otherwise be recorded for it).
// When Execute fails before it acquires a statement handle (here: a nil session
// makes new_statement fail), it must still honor the Backend contract — a non-nil,
// handle-less Operation that Closes as a no-op (closed=false, no CLOSE_STATEMENT)
// and reports zero AffectedRows. (A nil-session unit test can't reach the bind
// path: the param mapping is unit-tested hermetically in TestParamBindArg, and
// exercised live end-to-end in TestKernelParamsVsThrift.)
func TestExecuteHandleLessOpContract(t *testing.T) {
	k := &KernelBackend{} // nil session → new_statement fails
	op, err := k.Execute(context.Background(), backend.ExecRequest{
		Query:  "SELECT ?",
		Params: []backend.Param{{Name: "x", Type: "STRING", Value: strPtr("v")}},
	})
	if err == nil {
		t.Fatal("expected an error from Execute on a nil-session backend, got nil")
	}
	if op == nil {
		t.Fatal("Execute must return a non-nil Operation per the Backend contract")
	}
	closed, closeErr := op.Close(context.Background())
	if closeErr != nil {
		t.Errorf("Close error = %v, want nil", closeErr)
	}
	if closed {
		t.Error("Close on a handle-less op must report closed=false (no CLOSE_STATEMENT)")
	}
	if got := op.AffectedRows(); got != 0 {
		t.Errorf("AffectedRows on a handle-less op = %d, want 0", got)
	}
}

// TestExecuteRejectsStaging drives Execute with a staging statement (not just the
// isStagingStatement detector in isolation) to pin the detector→Execute wiring: a
// refactor that dropped or reordered the check would silently reopen the
// silent-no-op data-loss path. Mirrors TestExecuteRejectsParams.
func TestExecuteRejectsStaging(t *testing.T) {
	k := &KernelBackend{}
	op, err := k.Execute(context.Background(), backend.ExecRequest{
		Query: "PUT '/tmp/f' INTO '/Volumes/main/s/e/f.csv'",
	})
	if err == nil {
		t.Fatal("expected an error for a staging statement, got nil")
	}
	if !errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
		t.Errorf("staging rejection should wrap ErrNotSupportedByKernel, got %v", err)
	}
	if op == nil {
		t.Fatal("Execute must return a non-nil Operation per the Backend contract")
	}
	closed, closeErr := op.Close(context.Background())
	if closeErr != nil {
		t.Errorf("Close error = %v, want nil", closeErr)
	}
	if closed {
		t.Error("Close on a handle-less op must report closed=false (no CLOSE_STATEMENT)")
	}
}

// ExecutionError must satisfy the same public contract as the Thrift path so the
// errors.Is → errors.As → SqlState()/QueryId() recipe documented in doc.go works
// on the kernel backend too (it previously returned a bare *KernelError that
// matched none of it).
func TestExecutionErrorContract(t *testing.T) {
	o := &kernelOp{}

	if got := o.ExecutionError(context.Background(), nil); got != nil {
		t.Errorf("ExecutionError(nil) = %v, want nil", got)
	}

	cause := &KernelError{Code: statusSqlError, Message: "boom", SQLState: "42000", QueryID: "q-123"}
	err := o.ExecutionError(context.Background(), cause)
	if err == nil {
		t.Fatal("ExecutionError(cause) should not be nil")
	}
	if !errors.Is(err, dbsqlerr.ExecutionError) {
		t.Errorf("kernel execution error should match dbsqlerr.ExecutionError; got %v", err)
	}
	var dbExec dbsqlerr.DBExecutionError
	if !errors.As(err, &dbExec) {
		t.Fatalf("kernel execution error should be a DBExecutionError; got %T", err)
	}
	if dbExec.SqlState() != "42000" {
		t.Errorf("SqlState() = %q, want 42000 (from the KernelError)", dbExec.SqlState())
	}
	// QueryId must come from the KernelError, not the (empty) ctx query id — the
	// kernel path's StatementID() is "", so relying on ctx would drop the one
	// server-side correlation handle.
	if dbExec.QueryId() != "q-123" {
		t.Errorf("QueryId() = %q, want q-123 (from the KernelError)", dbExec.QueryId())
	}
	// The *KernelError cause stays reachable via Unwrap.
	var ke *KernelError
	if !errors.As(err, &ke) {
		t.Error("the *KernelError cause should remain reachable via errors.As")
	}
}

// The execute error path must NEVER report retryable, even when the kernel marks
// the failure retryable. This is the post-submission surface (toStatementError
// refuses driver.ErrBadConn here for the same reason): a network/unavailable
// failure seen after the statement was sent may have already committed a
// non-idempotent INSERT/UPDATE/MERGE, so an app keying retry on IsRetryable() would
// double-write. It also matches the Thrift path, which always builds a
// non-retryable execution error. sqlState/queryId must still come through.
func TestExecutionErrorNeverRetryable(t *testing.T) {
	o := &kernelOp{}
	cause := &KernelError{Code: statusUnavailable, Message: "try again", SQLState: "08000", QueryID: "q-9", Retryable: true}
	err := o.ExecutionError(context.Background(), cause)
	if err == nil {
		t.Fatal("ExecutionError(cause) should not be nil")
	}

	var dbExec dbsqlerr.DBExecutionError
	if !errors.As(err, &dbExec) {
		t.Fatalf("kernel execution error should be a DBExecutionError; got %T", err)
	}
	// Even though the KernelError is Retryable, the execute path must report false:
	// the statement may have committed, so replay is unsafe.
	if dbExec.IsRetryable() {
		t.Error("IsRetryable() = true on the execute path; want false (a sent statement may have committed — no replay)")
	}
	// Dropping the retryable signal must not drop sqlState/queryId or the cause.
	if dbExec.SqlState() != "08000" {
		t.Errorf("SqlState() = %q, want 08000", dbExec.SqlState())
	}
	if dbExec.QueryId() != "q-9" {
		t.Errorf("QueryId() = %q, want q-9", dbExec.QueryId())
	}
	var ke *KernelError
	if !errors.As(err, &ke) {
		t.Error("the *KernelError cause should remain reachable via errors.As")
	}
}

func strPtr(s string) *string { return &s }

// The cell/nested rendering (ScanCell and the JSON grammar) now lives in the
// untagged internal/arrowscan package, where its tests run in the default
// CGO_ENABLED=0 build; see arrowscan_test.go. The decimal formatter lives in
// internal/decimalfmt. This file keeps the kernel-specific tests: error mapping,
// bad-connection classification, and the bound-params rejection.

// kernelRows.Close() must fire the OnClose telemetry callback so the kernel path
// records CLOSE_STATEMENT / latency / statement success-or-failure — conn gates
// that recording on OnClose being called, and the Thrift path fires it. Before this
// wiring, kernel queries emitted no close telemetry (a production blind spot). A bare
// kernelRows is safe: Close() nil-guards cur/stream/op.
func TestKernelRowsCloseFiresOnClose(t *testing.T) {
	t.Run("success path reports nil iterErr", func(t *testing.T) {
		var got struct {
			called            bool
			chunkCount        int
			iterErr, closeErr error
		}
		r := &kernelRows{
			chunkCount: 3,
			callbacks: &dbsqlrows.TelemetryCallbacks{
				OnClose: func(latencyMs int64, chunkCount int, iterErr, closeErr error) {
					got.called, got.chunkCount, got.iterErr, got.closeErr = true, chunkCount, iterErr, closeErr
				},
			},
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}
		if !got.called {
			t.Fatal("OnClose was not fired")
		}
		if got.chunkCount != 3 {
			t.Errorf("OnClose chunkCount = %d, want 3", got.chunkCount)
		}
		if got.iterErr != nil || got.closeErr != nil {
			t.Errorf("OnClose errs = (%v, %v), want (nil, nil)", got.iterErr, got.closeErr)
		}
	})

	t.Run("iterationErr is reported", func(t *testing.T) {
		sentinel := errors.New("boom")
		var gotIter error
		fired := 0
		r := &kernelRows{
			iterationErr: sentinel,
			callbacks: &dbsqlrows.TelemetryCallbacks{
				OnClose: func(_ int64, _ int, iterErr, _ error) { fired++; gotIter = iterErr },
			},
		}
		_ = r.Close()
		if !errors.Is(gotIter, sentinel) {
			t.Errorf("OnClose iterErr = %v, want %v", gotIter, sentinel)
		}
		// Idempotent: a second Close must not re-fire (conn/database-sql may double-close).
		_ = r.Close()
		if fired != 1 {
			t.Errorf("OnClose fired %d times across two Close() calls, want 1", fired)
		}
	})

	t.Run("nil callbacks is safe", func(t *testing.T) {
		r := &kernelRows{} // no callbacks
		if err := r.Close(); err != nil {
			t.Errorf("Close() with nil callbacks = %v, want nil", err)
		}
	})

	t.Run("construction-failure Close must not fire a success OnClose", func(t *testing.T) {
		// Drive the real newKernelRows construction-failure path: a nil result
		// stream makes kernel_result_stream_get_schema return a defined
		// InvalidArgument error (the kernel null-checks the handle — never UB), so
		// newKernelRows takes its cleanup r.Close() branch and returns an error. The
		// callback must NOT have been armed yet, so the supplied OnClose must not
		// fire a (falsely successful) CLOSE_STATEMENT for a statement that produced
		// no rows. This is the invariant that keeping r.callbacks unset until after
		// a successful build guarantees.
		fired := false
		cb := &dbsqlrows.TelemetryCallbacks{
			OnClose: func(int64, int, error, error) { fired = true },
		}
		op := &kernelOp{backend: &KernelBackend{}} // for evictIfSessionFatal on the error path
		rows, err := newKernelRows(context.Background(), op, nil /* stream */, cb)
		if err == nil {
			t.Fatal("newKernelRows(nil stream) = nil error, want a construction failure")
		}
		if rows != nil {
			t.Errorf("newKernelRows on failure = %v rows, want nil", rows)
		}
		if fired {
			t.Error("OnClose fired during construction-failure cleanup — callback armed too early")
		}
	})
}
