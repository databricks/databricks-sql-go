//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend"
)

// isBadConnection maps the session-unusable status codes so the pool evicts the
// conn; every other code stays a plain kernel error.
func TestIsBadConnection(t *testing.T) {
	bad := []int{statusUnauthenticated, statusUnavailable, statusNetworkError}
	for _, code := range bad {
		if !isBadConnection(code) {
			t.Errorf("code %d should be a bad connection", code)
		}
	}
	notBad := []int{statusInvalidArgument, statusSqlError, statusTimeout}
	for _, code := range notBad {
		if isBadConnection(code) {
			t.Errorf("code %d should not be a bad connection", code)
		}
	}
}

// toDriverError wraps a session-unusable KernelError as driver.ErrBadConn (so
// database/sql evicts the conn) and leaves other errors, and their sqlstate,
// intact.
func TestToDriverError(t *testing.T) {
	if toDriverError(nil) != nil {
		t.Fatal("nil should map to nil")
	}

	badConn := &KernelError{Code: statusUnavailable, Message: "gone"}
	if !errors.Is(toDriverError(badConn), driver.ErrBadConn) {
		t.Errorf("unavailable kernel error should identify as driver.ErrBadConn")
	}

	sqlErr := &KernelError{Code: statusSqlError, Message: "boom", SQLState: "42703"}
	got := toDriverError(sqlErr)
	ke, ok := got.(*KernelError)
	if !ok {
		t.Fatalf("sql error should remain a *KernelError, got %T", got)
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
