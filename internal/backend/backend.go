// Package backend defines the execution-backend abstraction for the
// databricks-sql-go driver. A connection holds one Backend, created by the
// connector at connect time, and drives session lifecycle and statement
// execution through it without depending on a concrete protocol.
//
// The interfaces are expressed in neutral terms — plain Go request structs, a
// driver.Rows result, and small accessors for statement id, affected rows, and
// sqlstate-aware error wrapping — so that only a Backend implementation imports
// its protocol's client (e.g. the Thrift backend imports internal/cli_service).
package backend

import (
	"context"
	"database/sql/driver"

	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
)

// Backend is the per-connection execution backend. Exactly one Backend backs one
// conn, which (via database/sql's pool) is used by one goroutine at a time — so
// implementations need not be safe for concurrent use by multiple goroutines.
type Backend interface {
	// OpenSession establishes the server-side session. Called once by the
	// connector factory at connect time, before the conn is handed to the pool.
	OpenSession(ctx context.Context) error

	// CloseSession tears down the server-side session. Called from conn.Close.
	CloseSession(ctx context.Context) error

	// SessionValid backs driver.Validator (conn.IsValid): it reports whether the
	// session is still usable, so a broken conn is evicted from the pool rather
	// than reused. It does no I/O — it inspects state captured at OpenSession.
	SessionValid() bool

	// SessionID is the formatted server session id (conn.id). Valid only after a
	// successful OpenSession; empty otherwise.
	SessionID() string

	// Execute runs a statement to a terminal state and returns an Operation
	// describing the outcome.
	//
	// An implementation MUST return a non-nil Operation, even when err is non-nil:
	// the caller reads StatementID, wraps the error via ExecutionError, and closes
	// the operation via Close on both the success and failure paths. When the
	// server returned no handle, the accessors return zero values (StatementID "",
	// Close closed=false) rather than panicking.
	Execute(ctx context.Context, req ExecRequest) (Operation, error)
}

// Operation is a submitted statement that has reached (or attempted to reach) a
// terminal state. It exposes only neutral accessors, keeping the concrete
// protocol types inside the backend implementation.
type Operation interface {
	// StatementID is the server statement/operation id (empty if the server
	// returned no handle).
	StatementID() string

	// AffectedRows is the modified-row count for ExecContext results. Unlike the
	// other accessors it is defined only on the success path — the caller reads it
	// only after Execute returned a nil error — so an implementation need not make
	// it safe to call on a failed/handle-less operation.
	AffectedRows() int64

	// Results builds the driver.Rows for the operation's result set. The callbacks
	// carry the telemetry hooks (chunk timing, close, CloudFetch file); the caller
	// supplies whichever subset it needs (a full query wires all three, the
	// staging path wires only chunk timing).
	Results(ctx context.Context, callbacks *dbsqlrows.TelemetryCallbacks) (driver.Rows, error)

	// IsStaging reports whether this is a staging (PUT/GET/REMOVE) operation. It
	// may issue a metadata lookup if the answer is not already available from
	// direct results.
	IsStaging(ctx context.Context) (bool, error)

	// Close best-effort closes the server-side operation, if one is still open.
	// It is idempotent and safe to call regardless of whether execution
	// succeeded. closed reports whether a close RPC was actually issued (false
	// when the operation had no handle or was already closed), so the caller can
	// record CLOSE_STATEMENT telemetry only when a real close happened.
	Close(ctx context.Context) (closed bool, err error)

	// ExecutionError wraps cause as the driver's execution error, attaching this
	// operation's terminal sqlstate. Returns nil when cause is nil.
	ExecutionError(ctx context.Context, cause error) error
}

// ExecRequest is a backend-neutral statement-execution request. Per-backend wire
// options (Arrow flags, CloudFetch, direct results, query tags, timeouts) are
// derived from the config and context inside the backend implementation.
type ExecRequest struct {
	// Query is the SQL text to execute.
	Query string
	// Params are the bound query parameters, already type-inferred and rendered
	// to their string wire form (see Param); empty when the statement has none.
	// Inference lives in the dbsql package because it depends on the public
	// dbsql.Parameter/SqlType types; each backend maps these neutral params to its
	// own wire type.
	Params []Param
}

// Param is one bound query parameter in backend-neutral form: the name (empty
// for a positional parameter), the SQL type name as the server expects it (e.g.
// "BIGINT", "DECIMAL(2,1)", "VOID"), and the value already rendered to its
// string wire representation. A nil Value denotes a SQL NULL (type "VOID").
type Param struct {
	Name  string
	Type  string
	Value *string
}
