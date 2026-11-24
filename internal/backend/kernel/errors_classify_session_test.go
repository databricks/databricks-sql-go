package kernel

import (
	"database/sql/driver"
	"errors"
	"testing"
)

// expiredSessionError is the error the kernel returns for a statement on a session
// the server has already expired, as observed in production.
func expiredSessionError() *KernelError {
	return &KernelError{
		Code:       statusNotFound,
		Message:    "HTTP 404: Session does not exist: 01f0c0de-0000-0000-0000-000000000000; error_code=NOT_FOUND",
		HTTPStatus: 404,
	}
}

func TestIsSessionFatalErrorMatchesExpiredSession(t *testing.T) {
	if !isSessionFatalError(expiredSessionError()) {
		t.Fatal("a NotFound for the session itself must be session-fatal so the conn is evicted")
	}
}

func TestIsSessionFatalErrorIgnoresOtherNotFound(t *testing.T) {
	for _, ke := range []*KernelError{
		{Code: statusNotFound, Message: "HTTP 404: Statement does not exist: 01f0c0de; error_code=NOT_FOUND", HTTPStatus: 404},
		{Code: statusNotFound, Message: "[TABLE_OR_VIEW_NOT_FOUND] The table or view `main`.`default`.`missing` cannot be found.", SQLState: "42P01"},
		{Code: statusNotFound, Message: "volume path does not exist"},
		// The session phrase under a non-NotFound code (e.g. echoed in SQL error
		// text) is not the server reporting a dead session.
		{Code: statusSqlError, Message: "Session does not exist"},
	} {
		if isSessionFatalError(ke) {
			t.Errorf("isSessionFatalError(%q, code=%d) = true; the session is still usable", ke.Message, ke.Code)
		}
	}
}

func TestIsSessionFatalErrorKeepsSessionFatalCodes(t *testing.T) {
	for _, code := range []int{statusUnauthenticated, statusUnavailable, statusNetworkError} {
		if !isSessionFatalError(&KernelError{Code: code}) {
			t.Errorf("code %d must stay session-fatal", code)
		}
	}
	if isSessionFatalError(&KernelError{Code: statusSqlError}) {
		t.Error("a SQL error must not be session-fatal")
	}
}

// An expired session must evict the conn without database/sql replaying the
// statement or retrying the connect, so it can't turn into a retry loop.
func TestExpiredSessionIsNotBadConn(t *testing.T) {
	if err := toStatementError(expiredSessionError()); errors.Is(err, driver.ErrBadConn) {
		t.Errorf("toStatementError = %v; must not be driver.ErrBadConn", err)
	}
	if err := toConnError(expiredSessionError()); errors.Is(err, driver.ErrBadConn) {
		t.Errorf("toConnError = %v; must not be driver.ErrBadConn", err)
	}
}
