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
	"fmt"
	"sync"
	"time"

	"github.com/databricks/databricks-sql-go/internal/backend"
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
// Executes SQL text only; req.Params (bound parameters) are not yet wired.
func (k *KernelBackend) execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	klog("Execute sql=%q", truncate(req.Query, 120))

	var stmt *C.kernel_statement_t
	if err := call(func() C.KernelStatusCode {
		return C.kernel_session_new_statement(k.session, &stmt)
	}); err != nil {
		return &kernelOp{}, fmt.Errorf("kernel: new_statement: %w", toDriverError(err))
	}

	sql := newCStr(req.Query)
	if err := call(func() C.KernelStatusCode {
		return C.kernel_statement_set_sql(stmt, sql.c)
	}); err != nil {
		sql.free()
		C.kernel_statement_close(stmt)
		return &kernelOp{}, fmt.Errorf("kernel: set_sql: %w", toDriverError(err))
	}
	sql.free()

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
	// inline wait (~10s) even for a long query — so a cancel fired before that is
	// a no-op. Re-fire every 250ms after ctx.Done until execute returns, so the
	// cancel takes effect once the id appears, with no kernel change.
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
			klog("ctx.Done (%v) → firing canceller (with retry)", ctx.Err())
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			C.kernel_statement_canceller_cancel(canceller)
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					C.kernel_statement_canceller_cancel(canceller)
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

	op := &kernelOp{stmt: stmt}
	if execErr != nil {
		// Prefer the caller's ctx error when the ctx was cancelled (database/sql
		// convention), keeping the kernel error as the cause.
		if ctx.Err() != nil {
			klog("Execute failed under cancelled ctx: kernelErr=%v ctxErr=%v", execErr, ctx.Err())
			op.close()
			return op, fmt.Errorf("kernel: execute cancelled: %w", ctx.Err())
		}
		klog("Execute failed: %v", execErr)
		op.close()
		return op, fmt.Errorf("kernel: execute: %w", toDriverError(execErr))
	}
	op.exec = exec
	klog("Execute OK stmt=%p exec=%p", stmt, exec)
	return op, nil
}

// kernelOp implements backend.Operation over a sync executed statement.
type kernelOp struct {
	stmt   *C.kernel_statement_t
	exec   *C.kernel_executed_statement_t
	closed bool
}

var _ backend.Operation = (*kernelOp)(nil)

// StatementID returns "": the C ABI exposes no server statement id accessor. The
// id is used only for logging/telemetry correlation, which falls back to the
// session id.
func (o *kernelOp) StatementID() string { return "" }

// AffectedRows is the modified-row count for ExecContext.
func (o *kernelOp) AffectedRows() int64 {
	if o.exec == nil {
		return 0
	}
	return int64(C.kernel_executed_statement_num_modified_rows(o.exec))
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
		return nil, fmt.Errorf("kernel: get_result_stream: %w", toDriverError(err))
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
	if o.exec != nil {
		C.kernel_executed_statement_close(o.exec)
		o.exec = nil
	}
	if o.stmt != nil {
		C.kernel_statement_close(o.stmt)
		o.stmt = nil
	}
	klog("kernelOp closed")
	return true
}

// ExecutionError wraps cause as the driver's execution error. The kernel error
// already carries the sqlstate (see KernelError), so this returns cause as-is
// (nil when cause is nil), matching the neutral contract.
func (o *kernelOp) ExecutionError(ctx context.Context, cause error) error {
	return toDriverError(cause)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
