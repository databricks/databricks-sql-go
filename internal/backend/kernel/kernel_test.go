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
func TestExecuteRejectsParams(t *testing.T) {
	k := &KernelBackend{}
	op, err := k.Execute(context.Background(), backend.ExecRequest{
		Query:  "SELECT ?",
		Params: []backend.Param{{Name: "x"}},
	})
	if err == nil {
		t.Fatal("expected an error for bound parameters, got nil")
	}
	if !errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
		t.Errorf("params rejection should wrap ErrNotSupportedByKernel, got %v", err)
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

// The cell/nested rendering (ScanCell and the JSON grammar) now lives in the
// untagged internal/arrowscan package, where its tests run in the default
// CGO_ENABLED=0 build; see arrowscan_test.go. The decimal formatter lives in
// internal/decimalfmt. This file keeps the kernel-specific tests: error mapping,
// bad-connection classification, and the bound-params rejection.
