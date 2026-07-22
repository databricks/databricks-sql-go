package kernel

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

// These guard the safety contract (which errors may trigger database/sql's
// transparent statement replay, and which permanent failures must not be retried).
// They live in an untagged file so they run under CGO_ENABLED=0 — a future edit
// reintroducing the duplicate-write bug or the auth-retry storm fails CI here,
// rather than shipping green because the tagged tests never ran.

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

// isUserFault picks the log level for a kernel-call failure: user/query faults
// (bad SQL, bad argument) log at Debug so they don't inflate the WARN rate;
// infra codes stay at Warn.
func TestIsUserFault(t *testing.T) {
	faults := []int{statusSqlError, statusInvalidArgument}
	for _, code := range faults {
		if !isUserFault(code) {
			t.Errorf("code %d should be a user fault (log at Debug)", code)
		}
	}
	infra := []int{statusUnavailable, statusNetworkError, statusTimeout, statusUnauthenticated}
	for _, code := range infra {
		if isUserFault(code) {
			t.Errorf("code %d is infra-side and should NOT be a user fault (stays Warn)", code)
		}
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

// KernelError.Error() includes sqlstate and the server queryId when present.
func TestKernelErrorString(t *testing.T) {
	withState := (&KernelError{Code: statusSqlError, Message: "boom", SQLState: "42703", QueryID: "q-1"}).Error()
	for _, want := range []string{"boom", "sqlstate=42703", "code=13", "queryId=q-1"} {
		if !contains(withState, want) {
			t.Errorf("Error() = %q, missing %q", withState, want)
		}
	}
	noState := (&KernelError{Code: statusUnavailable, Message: "gone"}).Error()
	if contains(noState, "sqlstate=") || contains(noState, "queryId=") {
		t.Errorf("Error() = %q, should omit empty sqlstate/queryId", noState)
	}
}

// statementIDFromError feeds the execute-error telemetry gate: it must pull the
// query id off a KernelError (even when wrapped) and return "" otherwise.
func TestStatementIDFromError(t *testing.T) {
	if got := statementIDFromError(&KernelError{QueryID: "q-1"}); got != "q-1" {
		t.Errorf("KernelError: got %q, want q-1", got)
	}
	wrapped := fmt.Errorf("kernel: execute: %w", &KernelError{QueryID: "q-2"})
	if got := statementIDFromError(wrapped); got != "q-2" {
		t.Errorf("wrapped KernelError: got %q, want q-2", got)
	}
	if got := statementIDFromError(errors.New("plain")); got != "" {
		t.Errorf("non-KernelError: got %q, want \"\"", got)
	}
	if got := statementIDFromError(nil); got != "" {
		t.Errorf("nil: got %q, want \"\"", got)
	}
}

// Category maps the kernel code to a telemetry category (read via
// CategoryFromError), so a kernel failure reports its authoritative code instead of
// a message-inferred guess. An unmapped code returns "" to keep the message fallback.
func TestKernelErrorCategory(t *testing.T) {
	cases := []struct {
		code int
		want dbsqlerrint.ErrorCategory
	}{
		{statusInvalidArgument, dbsqlerrint.CategoryInvalidRequest},
		{statusUnauthenticated, dbsqlerrint.CategoryAuthError},
		{statusPermissionDenied, dbsqlerrint.CategoryPermissionError},
		{statusNotFound, dbsqlerrint.CategoryNotFound},
		{statusResourceExhausted, dbsqlerrint.CategoryRateLimitExceeded},
		{statusUnavailable, dbsqlerrint.CategoryConnectionError},
		{statusTimeout, dbsqlerrint.CategoryTimeout},
		{statusCancelled, dbsqlerrint.CategoryCancelled},
		{statusDataLoss, dbsqlerrint.CategoryResultSet},
		{statusInternal, dbsqlerrint.CategoryGeneric},
		{statusInvalidStmtHandle, dbsqlerrint.CategoryStatementClosed},
		{statusNetworkError, dbsqlerrint.CategoryConnectionError},
		{statusSqlError, dbsqlerrint.CategoryExecuteStatement},
		{9999, ""}, // outside the enum → "" so classifyError falls back to the message
	}
	for _, c := range cases {
		if got := (&KernelError{Code: c.code}).Category(); got != c.want {
			t.Errorf("code %d: Category() = %q, want %q", c.code, got, c.want)
		}
	}
}

// The category must survive the fmt.Errorf("%w") wrapping the execute/read paths
// apply, since CategoryFromError walks the chain to reach the *KernelError. This is
// the property the telemetry classifier depends on end to end.
func TestKernelErrorCategoryThroughWrap(t *testing.T) {
	wrapped := fmt.Errorf("kernel: execute: %w", &KernelError{Code: statusSqlError, Message: "boom"})
	if got := dbsqlerrint.CategoryFromError(wrapped); got != dbsqlerrint.CategoryExecuteStatement {
		t.Errorf("wrapped kernel error: CategoryFromError = %q, want %q", got, dbsqlerrint.CategoryExecuteStatement)
	}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0) }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
