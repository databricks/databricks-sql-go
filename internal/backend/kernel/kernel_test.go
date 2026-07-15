//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
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
