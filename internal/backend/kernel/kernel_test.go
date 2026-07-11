//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend"
)

// The pure error-classifier tests (TestIsBadConnection, TestIsSessionFatal,
// TestToConnError, TestToStatementErrorNeverBadConn) live in the untagged
// errors_classify_test.go so they run under CGO_ENABLED=0. The tests below need a
// *KernelBackend, so they stay tagged.

// evictIfSessionFatal flips SessionValid()→false on a session-fatal error (so the
// pool discards the conn) WITHOUT the error being driver.ErrBadConn (so the
// statement is never transparently re-run — the H1 constraint).
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

// The cell/nested rendering (ScanCell and the JSON grammar) now lives in the
// untagged internal/arrowscan package, where its tests run in the default
// CGO_ENABLED=0 build; see arrowscan_test.go. The decimal formatter lives in
// internal/decimalfmt. This file keeps the kernel-specific tests: error mapping,
// bad-connection classification, and the bound-params rejection.
