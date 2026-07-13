package kernel

import (
	"database/sql/driver"
	"errors"
	"testing"
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

func contains(s, sub string) bool { return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0) }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
