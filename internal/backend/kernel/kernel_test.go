//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend"
)

// isBadConnection is the TRANSIENT-failure set (retrying a fresh connect could
// succeed) — used to produce driver.ErrBadConn on the session-lifecycle path.
// Unauthenticated is excluded: a wrong/expired PAT is permanent, not retryable.
func TestIsBadConnection(t *testing.T) {
	transient := []int{statusUnavailable, statusNetworkError}
	for _, code := range transient {
		if !isBadConnection(code) {
			t.Errorf("code %d should be a (transient) bad connection", code)
		}
	}
	// Unauthenticated is session-fatal but NOT retryable, so it is not bad-conn.
	notBad := []int{statusUnauthenticated, statusInvalidArgument, statusSqlError, statusTimeout}
	for _, code := range notBad {
		if isBadConnection(code) {
			t.Errorf("code %d should not be a bad connection", code)
		}
	}
}

// isSessionFatal is the broader "session is dead, evict the conn" set — it adds
// Unauthenticated (an expired token kills the session) on top of the transient set.
func TestIsSessionFatal(t *testing.T) {
	fatal := []int{statusUnauthenticated, statusUnavailable, statusNetworkError}
	for _, code := range fatal {
		if !isSessionFatal(code) {
			t.Errorf("code %d should be session-fatal", code)
		}
	}
	notFatal := []int{statusInvalidArgument, statusSqlError, statusTimeout}
	for _, code := range notFatal {
		if isSessionFatal(code) {
			t.Errorf("code %d should not be session-fatal", code)
		}
	}
}

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

// toConnError (session-lifecycle path) wraps a session-unusable KernelError as
// driver.ErrBadConn so database/sql evicts the conn, and leaves other errors and
// their sqlstate intact.
func TestToConnError(t *testing.T) {
	if toConnError(nil) != nil {
		t.Fatal("nil should map to nil")
	}

	badConn := &KernelError{Code: statusUnavailable, Message: "gone"}
	if !errors.Is(toConnError(badConn), driver.ErrBadConn) {
		t.Errorf("unavailable kernel error on the session path should identify as driver.ErrBadConn")
	}

	sqlErr := &KernelError{Code: statusSqlError, Message: "boom", SQLState: "42703"}
	ke, ok := toConnError(sqlErr).(*KernelError)
	if !ok {
		t.Fatalf("sql error should remain a *KernelError, got %T", toConnError(sqlErr))
	}
	if ke.SQLState != "42703" {
		t.Errorf("sqlstate lost: got %q", ke.SQLState)
	}
}

// toStatementError (execute/read path) must NEVER return driver.ErrBadConn — even
// for a network/unavailable status — so database/sql cannot transparently re-run a
// statement that may have already executed server-side (silent duplicate write).
func TestToStatementErrorNeverBadConn(t *testing.T) {
	if toStatementError(nil) != nil {
		t.Fatal("nil should map to nil")
	}

	for _, code := range []int{statusUnavailable, statusNetworkError, statusUnauthenticated} {
		err := toStatementError(&KernelError{Code: code, Message: "post-execute failure"})
		if errors.Is(err, driver.ErrBadConn) {
			t.Errorf("statement-path error (code=%d) must NOT identify as driver.ErrBadConn "+
				"(would let database/sql re-run a possibly-committed statement)", code)
		}
	}

	// sqlstate still preserved.
	sqlErr := &KernelError{Code: statusSqlError, Message: "boom", SQLState: "42703"}
	ke, ok := toStatementError(sqlErr).(*KernelError)
	if !ok {
		t.Fatalf("sql error should remain a *KernelError, got %T", toStatementError(sqlErr))
	}
	if ke.SQLState != "42703" {
		t.Errorf("sqlstate lost: got %q", ke.SQLState)
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
