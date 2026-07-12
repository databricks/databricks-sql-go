//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
)

// execute runs one statement to a terminal state on the blocking-execute path,
// with out-of-band cancellation from a watcher goroutine:
//  1. new_statement + set_sql
//  2. canceller_new BEFORE execute, so it can observe the server statement id
//  3. a watcher goroutine that fires the canceller on ctx.Done()
//  4. the single blocking kernel_statement_execute (inline/CloudFetch and
//     long-query polling all happen inside the kernel, invisibly)
//  5. drain the watcher before returning, so a late cancel cannot land on a
//     statement that reuses this handle
//
// Executes SQL text only; bound parameters are rejected up front by Execute.
func (k *KernelBackend) execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	// Log the SQL length, not the text: query bodies can carry PII/secrets in
	// WHERE/INSERT/SET, and this goes to stderr. Matches the driver's own
	// debuglog convention (conn.ExecContext logs sql.len=%d).
	klog("Execute sql.len=%d", len(req.Query))

	var stmt *C.kernel_statement_t
	if err := call(func() C.KernelStatusCode {
		return C.kernel_session_new_statement(k.session, &stmt)
	}); err != nil {
		k.evictIfSessionFatal(err)
		return &kernelOp{}, fmt.Errorf("kernel: new_statement: %w", toStatementError(err))
	}

	sql := newCStr(req.Query)
	if err := call(func() C.KernelStatusCode {
		return C.kernel_statement_set_sql(stmt, sql.c)
	}); err != nil {
		sql.free()
		C.kernel_statement_close(stmt)
		k.evictIfSessionFatal(err)
		return &kernelOp{}, fmt.Errorf("kernel: set_sql: %w", toStatementError(err))
	}
	sql.free()

	// Bind parameters. The driver hands us backend.Param{Name, Type, Value}: the
	// value is already stringified, Type is the Databricks SQL type name, and a nil
	// Value is SQL NULL (Type "VOID"). Each maps 1:1 onto
	// kernel_statement_bind_parameter, which builds the SEA wire parameter directly
	// (name/ordinal + type + string), matching the Thrift path's toSparkParameters.
	if err := bindParams(stmt, req.Params); err != nil {
		C.kernel_statement_close(stmt)
		k.evictIfSessionFatal(err)
		return &kernelOp{}, fmt.Errorf("kernel: bind params: %w", toStatementError(err))
	}

	// Detached canceller, obtained before execute so it observes the server
	// statement id the moment execute publishes it. Non-fatal on failure: proceed
	// without cancellation rather than failing the query.
	var canceller *C.kernel_statement_canceller_t
	if err := call(func() C.KernelStatusCode {
		return C.kernel_statement_canceller_new(stmt, &canceller)
	}); err != nil {
		klog("canceller_new failed (proceeding without cancel): %v", err)
		canceller = nil
	}

	// Watcher goroutine (only when there is both a cancellable ctx and a
	// canceller). The server publishes the statement id to the canceller's
	// inflight slot only when the initial POST returns — held up to the server's
	// inline wait even for a long query — so a cancel fired before that is
	// a no-op. Re-fire every 250ms after ctx.Done until the kernel reports the
	// cancel RPC was actually dispatched (dispatched=true, i.e. the id appeared
	// and the RPC went out), then stop: one real cancel is enough, and further
	// fires would only hammer the server. Falls back to firing until execute
	// returns (done) if the RPC never dispatches.
	done := make(chan struct{})
	var watcherWg sync.WaitGroup
	if canceller != nil && ctx.Done() != nil {
		watcherWg.Add(1)
		go func() {
			defer watcherWg.Done()
			select {
			case <-ctx.Done():
			case <-done:
				return
			}
			klog("ctx.Done (%v) → firing canceller (with retry until dispatched)", ctx.Err())
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			if fireCancel(canceller) {
				klog("cancel dispatched on first fire")
				return
			}
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					if fireCancel(canceller) {
						klog("cancel dispatched, watcher stopping")
						return
					}
				}
			}
		}()
	}

	// The one blocking call. inline vs CloudFetch and long-query polling are all
	// resolved inside the kernel; Go just waits here.
	var exec *C.kernel_executed_statement_t
	execErr := call(func() C.KernelStatusCode {
		return C.kernel_statement_execute(stmt, &exec)
	})

	// Drain the watcher before returning so a late canceller fire cannot land on
	// a subsequent statement reusing this handle.
	close(done)
	watcherWg.Wait()
	if canceller != nil {
		C.kernel_statement_canceller_free(canceller)
	}

	op := &kernelOp{backend: k, stmt: stmt, location: k.cfg.Location}
	if execErr != nil {
		// Prefer the caller's ctx error when the ctx was cancelled (database/sql
		// convention), keeping the kernel error as the cause.
		if ctx.Err() != nil {
			klog("Execute failed under cancelled ctx: kernelErr=%v ctxErr=%v", execErr, ctx.Err())
			op.close()
			return op, fmt.Errorf("kernel: execute cancelled: %w", ctx.Err())
		}
		klog("Execute failed: %v", execErr)
		// A session-fatal status (expired token, dropped/unavailable session) means
		// this conn is unusable: evict it so the pool doesn't hand it out again. We
		// still return the PLAIN error (toStatementError, never ErrBadConn), so the
		// conn is discarded without database/sql re-running the statement.
		k.evictIfSessionFatal(execErr)
		op.close()
		return op, fmt.Errorf("kernel: execute: %w", toStatementError(execErr))
	}
	op.exec = exec
	// Capture the modified-row count now, while exec is live — the operation is
	// closed (nulling exec) before AffectedRows is read on the ExecContext path.
	op.affectedRows = int64(C.kernel_executed_statement_num_modified_rows(exec))
	klog("Execute OK stmt=%p exec=%p affectedRows=%d", stmt, exec, op.affectedRows)
	return op, nil
}

// bindParams binds the driver's backend.Param list onto the statement via the
// kernel's raw-param bind. Each Param is already stringified with its Databricks
// SQL type name; an empty Name is a positional param (ordinal assigned kernel-side
// in push order) and a nil Value is SQL NULL (Type "VOID"). Runs before execute,
// so the params are set on the fresh statement (set_sql clears any prior binds).
func bindParams(stmt *C.kernel_statement_t, params []backend.Param) error {
	for i, p := range params {
		name := newCStrOrNull(p.Name) // empty Name → NULL → positional
		typ := newCStr(p.Type)
		val := newCStrOrNull("")
		if p.Value != nil {
			val = newCStr(*p.Value) // non-nil, possibly empty string, is a real value
		}
		err := call(func() C.KernelStatusCode {
			return C.kernel_statement_bind_parameter(stmt, name.c, typ.c, val.c)
		})
		name.free()
		typ.free()
		val.free()
		if err != nil {
			return fmt.Errorf("param %d (name=%q type=%q): %w", i, p.Name, p.Type, err)
		}
		klog("bound param %d name=%q type=%q null=%v", i, p.Name, p.Type, p.Value == nil)
	}
	return nil
}

// kernelOp implements backend.Operation over a sync executed statement.
type kernelOp struct {
	// backend is the owning connection's backend, held so a session-fatal error on
	// the result-read path can evict the conn (markSessionDead), mirroring how the
	// Thrift thriftOperation holds its *Backend.
	backend *KernelBackend
	stmt    *C.kernel_statement_t
	exec    *C.kernel_executed_statement_t
	closed  bool
	// affectedRows is the modified-row count captured at execute time. It is
	// cached (not read live from exec) because the caller closes the operation —
	// which nulls exec — before reading AffectedRows (see conn.ExecContext).
	affectedRows int64
	// location renders DATE / TIMESTAMP values in the session time zone, matching
	// the Thrift path; nil means UTC. Carried onto the rows built by Results.
	location *time.Location
}

var _ backend.Operation = (*kernelOp)(nil)

// StatementID returns "": the kernel C ABI exposes no success-path statement/query
// id accessor (only the error path carries a query_id, on KernelError). Because
// conn.ExecContext/QueryContext gate per-statement telemetry on
// StatementID() != "", kernel queries currently emit no EXECUTE_STATEMENT metric
// and their query-id log field is empty — there is no session-id fallback on that
// gate. Surfacing an id needs a kernel accessor (e.g.
// kernel_executed_statement_query_id); tracked as a follow-up.
func (o *kernelOp) StatementID() string { return "" }

// AffectedRows is the modified-row count for ExecContext. It returns the value
// cached at execute time, so it is correct even after the operation is closed
// (the ExecContext path closes the op before reading this).
func (o *kernelOp) AffectedRows() int64 {
	return o.affectedRows
}

// Results builds the driver.Rows over the executed statement's result stream.
// On the query path the returned Rows owns closing the server-side operation.
func (o *kernelOp) Results(ctx context.Context, callbacks *dbsqlrows.TelemetryCallbacks) (driver.Rows, error) {
	if o.exec == nil {
		return nil, fmt.Errorf("kernel: no executed statement")
	}
	var stream *C.kernel_result_stream_t
	if err := call(func() C.KernelStatusCode {
		return C.kernel_executed_statement_get_result_stream(o.exec, &stream)
	}); err != nil {
		// No Rows is returned to own teardown, and the query path does not call
		// Operation.Close on a Results error — so close the handles here to avoid
		// leaking the statement / executed handle (and its server operation).
		o.backend.evictIfSessionFatal(err)
		o.close()
		return nil, fmt.Errorf("kernel: get_result_stream: %w", toStatementError(err))
	}
	return newKernelRows(ctx, o, stream, callbacks)
}

// IsStaging reports whether this is a staging (PUT/GET/REMOVE) operation. The
// kernel backend does not support staging, so this is always false.
func (o *kernelOp) IsStaging(ctx context.Context) (bool, error) { return false, nil }

// Close best-effort closes the executed statement and its statement handle. It is
// idempotent (a second call, or a call after Rows.Close already tore the
// operation down, is a no-op) per the backend.Operation contract. closed reports
// whether a close was actually issued.
func (o *kernelOp) Close(ctx context.Context) (bool, error) {
	return o.close(), nil
}

// close is the shared idempotent teardown used by both Operation.Close and
// Rows.Close. Closing the executed handle first, then the statement, matches the
// C ABI teardown order (result stream is closed by Rows before this runs).
func (o *kernelOp) close() bool {
	if o.closed {
		return false
	}
	o.closed = true
	// Report closed=true only when there was actually a handle to tear down. A
	// handle-less op (e.g. the &kernelOp{} returned on the bound-params error
	// path) reaches here via conn.ExecContext's unconditional Close; returning
	// true would record a phantom CLOSE_STATEMENT for a statement that never hit
	// the server. Matches the backend.Operation contract (closed=false when the
	// operation had no handle) and the Thrift backend's !hasHandle() behavior.
	didClose := o.exec != nil || o.stmt != nil
	if o.exec != nil {
		C.kernel_executed_statement_close(o.exec)
		o.exec = nil
	}
	if o.stmt != nil {
		C.kernel_statement_close(o.stmt)
		o.stmt = nil
	}
	klog("kernelOp closed (didClose=%v)", didClose)
	return didClose
}

// ExecutionError wraps cause as the driver's execution error, attaching the
// kernel's sqlstate so kernel query failures surface with the same
// DBExecutionError shape (SqlState(), QueryId(), IsRetryable()) as the Thrift
// path — the parity property consumers rely on via errors.As. The sqlstate is
// dug out of the underlying *KernelError when present; the query id still comes
// from ctx (StatementID() is "" on this backend until the kernel exposes a
// success-path accessor). Returns nil when cause is nil.
func (o *kernelOp) ExecutionError(ctx context.Context, cause error) error {
	if cause == nil {
		return nil
	}
	sqlState := ""
	var ke *KernelError
	if errors.As(cause, &ke) {
		sqlState = ke.SQLState
	}
	return dbsqlerrint.NewExecutionErrorWithState(ctx, dbsqlerr.ErrQueryExecution, cause, sqlState)
}
