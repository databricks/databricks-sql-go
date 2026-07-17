package kernel

import (
	"errors"
	"fmt"

	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// It holds the kernel error type + the connection-classification logic that
// enforces two safety-critical guarantees — a statement-path error must never
// become driver.ErrBadConn, so database/sql cannot silently re-run a possibly-
// committed statement, and a permanent auth failure is not retried. Keeping
// it pure Go lets its tests run in the default CGO_ENABLED=0 build; a future edit
// that reintroduced the duplicate-write bug (or the auth-retry storm) would then
// fail CI instead of shipping green. The cgo file (cgo.go) populates KernelError
// from the C struct and asserts these status constants stay in lockstep with the
// C enum at compile time (see the compile-time assertion block in cgo.go).

// Status codes mirrored from the kernel C enum (KernelStatusCode in
// databricks_kernel.h) as plain Go ints, so this non-cgo file can classify errors
// without the C import. cgo.go asserts each equals its C.KernelStatusCode_* value
// at compile time, so drift from the header is a build error, not a latent bug.
const (
	statusInvalidArgument = 1
	statusUnauthenticated = 2
	statusUnavailable     = 6
	statusTimeout         = 7
	statusNetworkError    = 12
	statusSqlError        = 13
)

// KernelError is the Go-side structured error mapped from the kernel's KernelError
// struct. It carries the sqlstate so the backend's ExecutionError can attach it,
// matching the Thrift error surface.
type KernelError struct {
	Code       int
	Message    string
	SQLState   string
	VendorCode int32
	HTTPStatus uint16
	Retryable  bool
	QueryID    string
}

func (e *KernelError) Error() string {
	// Append the server query id when present — it is the one correlation handle
	// to server-side query history, and on the execute-error path (no exec handle)
	// the error itself is where kernelOp.StatementID() reads it from.
	q := ""
	if e.QueryID != "" {
		q = fmt.Sprintf(", queryId=%s", e.QueryID)
	}
	if e.SQLState != "" {
		return fmt.Sprintf("kernel: %s (sqlstate=%s, code=%d%s)", e.Message, e.SQLState, e.Code, q)
	}
	return fmt.Sprintf("kernel: %s (code=%d%s)", e.Message, e.Code, q)
}

// statementIDFromError returns the server query id carried on a KernelError, or ""
// for a nil / non-KernelError error or one with no id. The execute-error path uses
// it to set kernelOp.statementID (there is no exec handle to read it from), so the
// conn's StatementID()-gated failure telemetry fires as it does on the Thrift path.
func statementIDFromError(err error) string {
	var ke *KernelError
	if errors.As(err, &ke) {
		return ke.QueryID
	}
	return ""
}

// isBadConnection reports whether a status code is a *transient* connection
// failure — one where retrying on a fresh connection could succeed. On the
// session-lifecycle path this is wrapped as driver.ErrBadConn so database/sql
// retries connect. Unauthenticated is deliberately excluded: a wrong/expired PAT
// is permanent, so retrying it just burns connect attempts (and can worsen
// server-side auth rate-limiting) and fails identically — matching Thrift, which
// only treats an invalid session handle (a liveness signal), not a 401, as
// bad-conn. (Auth failure still marks the session dead for pool eviction — see
// isSessionFatal.)
func isBadConnection(code int) bool {
	switch code {
	case statusUnavailable, statusNetworkError:
		return true
	default:
		return false
	}
}

// isSessionFatal reports whether a status code means the server-side session is no
// longer usable, so the conn must be evicted from the pool rather than reused for
// the next query. Broader than isBadConnection: it also covers Unauthenticated (an
// expired/revoked token kills the session) — but unlike isBadConnection it is NOT
// used to produce driver.ErrBadConn on the statement path, so eviction happens
// without database/sql replaying the statement (see toStatementError + the
// KernelBackend.markSessionDead call sites).
func isSessionFatal(code int) bool {
	switch code {
	case statusUnauthenticated, statusUnavailable, statusNetworkError:
		return true
	default:
		return false
	}
}

// isUserFault reports whether a status code is a user/query fault (a bad SQL
// statement or a bad argument) rather than an infrastructure problem. Used to
// pick the log level for a kernel-call failure: user faults are routine — a
// fat-fingered query should not raise the driver's WARN rate and page on-call —
// so they log at Debug, while infra codes (Unavailable / NetworkError / Timeout /
// Unauthenticated) stay at Warn. Mirrors the Thrift path, which keeps user SQL
// errors out of operational-noise logging.
func isUserFault(code int) bool {
	switch code {
	case statusSqlError, statusInvalidArgument:
		return true
	default:
		return false
	}
}

// toConnError classifies a kernel error on a SESSION-lifecycle path (open/close/
// config, where nothing has executed): a status that means the session is unusable
// is wrapped as a bad-connection error, which identifies as driver.ErrBadConn so
// database/sql evicts the conn from the pool. Safe here because no statement ran,
// so there is nothing for database/sql to unsafely re-run. Other kernel errors —
// and plain (non-KernelError) errors — are returned unchanged, carrying sqlstate.
func toConnError(err error) error {
	if err == nil {
		return nil
	}
	ke, ok := err.(*KernelError)
	if !ok {
		return err
	}
	if isBadConnection(ke.Code) {
		return dbsqlerrint.NewBadConnectionError(ke)
	}
	return ke
}

// toStatementError classifies a kernel error on the STATEMENT path (execute and
// result read). It NEVER returns driver.ErrBadConn: once a statement has been
// sent, a network/unavailable failure surfaced afterward may have committed
// server-side, and driver.ErrBadConn would make database/sql transparently
// re-run the statement — a silent duplicate write for a non-idempotent
// INSERT/UPDATE/MERGE. This mirrors the kernel's own retry contract
// (ExecuteStatement is NonIdempotent, retried only on connect-phase failures) and
// the Thrift backend (ExecuteStatement is non-retryable), and honors Go's
// driver.ErrBadConn rule ("never return ErrBadConn if the server might have
// performed the operation"). The kernel has already exhausted its safe internal
// retries by the time we see the error. Returns the KernelError (or plain error)
// unchanged, carrying sqlstate.
func toStatementError(err error) error {
	if err == nil {
		return nil
	}
	if ke, ok := err.(*KernelError); ok {
		return ke
	}
	return err
}
