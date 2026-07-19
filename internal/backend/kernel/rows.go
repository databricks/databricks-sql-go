//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"
// Forward-declare the Arrow C Data Interface structs so cgo can take their
// addresses; arrow-go's cdata package reinterprets these via unsafe.Pointer.
struct ArrowSchema;
struct ArrowArray;
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/cdata"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
)

var _ driver.Rows = (*kernelRows)(nil)

// kernelRows implements driver.Rows over the kernel result stream. It pulls one
// Arrow RecordBatch at a time via kernel_result_stream_next_batch (inline and
// CloudFetch are transparent below the C ABI), imports it zero-copy through the
// Arrow C Data Interface, and scans rows out on demand.
//
// The batch/row split mirrors the kernel's own ResultStream: next_batch does the
// per-batch network/decode work once; row reads walk the already-imported
// arrow.Record with O(1) indexing.
type kernelRows struct {
	ctx       context.Context
	op        *kernelOp
	stream    *C.kernel_result_stream_t
	callbacks *dbsqlrows.TelemetryCallbacks
	// watcher bridges r.ctx onto ONE kernel cancel token for the whole result set,
	// created once here rather than per nextBatch: a cancellable request-scoped ctx
	// is the common service case, and a multi-GB CloudFetch stream is thousands of
	// batches, so a fresh token + goroutine + teardown per batch would be pure churn
	// on the driver's known-sensitive large-result path. Amortizing to one watcher
	// matches the execute path's once-per-statement canceller. Firing the token
	// aborts the in-flight fetch and, since it stays fired, any subsequent one — which
	// is correct: iteration stops on the returned error. nil for a non-cancellable ctx
	// (NULL token → the plain fetch path, zero overhead). Stopped in Close.
	watcher *ctxWatcher

	cols       []string
	cur        arrow.Record // current batch (nil until first Next)
	rowInCur   int          // next row index within cur
	chunkCount int          // cumulative batches fetched, for OnChunkFetched
	closed     bool
	eof        bool
	// iterationErr is the first non-EOF error seen during Next(), reported to the
	// OnClose telemetry callback so a failed statement is recorded (matching the
	// Thrift path's rows.iterationErr). io.EOF is normal termination, not an error.
	iterationErr error
	// keyCache memoizes struct field-name JSON keys for this result set so
	// per-row rendering doesn't re-marshal constant names. Scoped to this Rows
	// (freed with it) — not a process-global, which would leak.
	keyCache *arrowscan.StructKeyCache
}

// newKernelRows fetches the schema up front (for Columns()) and returns the row
// iterator; batches are pulled lazily on Next.
func newKernelRows(ctx context.Context, op *kernelOp, stream *C.kernel_result_stream_t, cb *dbsqlrows.TelemetryCallbacks) (driver.Rows, error) {
	// The telemetry callback is deliberately NOT set on r yet: the two cleanup
	// r.Close() calls below run when construction FAILS (schema fetch/import), and a
	// Close() with the callback set would fire OnClose as a *successful* close for a
	// statement that never produced rows — masking the failure in CLOSE_STATEMENT
	// telemetry. Assign it only on the success path so cleanup Close() on a
	// schema/import failure does not record a falsely successful CLOSE_STATEMENT; the
	// construction error itself is surfaced to and recorded by the conn execute path.
	r := &kernelRows{ctx: ctx, op: op, stream: stream, keyCache: arrowscan.NewStructKeyCache()}
	// One cancel-token watcher for the whole result set (see kernelRows.watcher),
	// nil on a non-cancellable ctx. Created before the fallible schema fetch so the
	// two construction-failure r.Close() calls below tear it down too; Close is
	// idempotent and stop() is nil-safe.
	r.watcher = newCtxWatcher(ctx)

	var csch C.struct_ArrowSchema
	if err := call(func() C.KernelStatusCode {
		return C.kernel_result_stream_get_schema(stream, &csch)
	}); err != nil {
		op.backend.evictIfSessionFatal(err)
		r.Close()
		return nil, fmt.Errorf("kernel: get_schema: %w", toStatementError(err))
	}
	sch, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(&csch)))
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("kernel: import schema: %w", err)
	}
	fields := sch.Fields()
	r.cols = make([]string, len(fields))
	for i, f := range fields {
		r.cols[i] = f.Name
	}
	// Construction succeeded — now arm the close telemetry callback so a normal
	// Close() (after row iteration) records CLOSE_STATEMENT.
	r.callbacks = cb
	klogCtx(ctx, "newKernelRows: %d columns", len(r.cols))
	return r, nil
}

// Columns returns the result-set column names.
func (r *kernelRows) Columns() []string { return r.cols }

// Close releases the current batch, the kernel result stream, and (query-path
// ownership) the server operation. Idempotent.
func (r *kernelRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	closeStart := time.Now()
	// Drain the ctx watcher and free its token first: it may be mid-fire (inside
	// kernel_cancel_token_cancel) and its token must not be freed out from under a
	// concurrent fire. stop() is nil-safe (non-cancellable ctx).
	r.watcher.stop()
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.stream != nil {
		C.kernel_result_stream_close(r.stream)
		r.stream = nil
	}
	if r.op != nil {
		r.op.close()
	}
	// Fire the close telemetry callback so the kernel path records CLOSE_STATEMENT /
	// execution latency / statement success-or-failure like the Thrift path does
	// (conn gates this on OnClose being called). The kernel teardown has no fallible
	// close RPC — the C stream/statement closes don't surface an error — so closeErr
	// is nil; iterationErr carries any failure seen during Next().
	if r.callbacks != nil && r.callbacks.OnClose != nil {
		r.callbacks.OnClose(time.Since(closeStart).Milliseconds(), r.chunkCount, r.iterationErr, nil)
	}
	klogCtx(r.ctx, "kernelRows closed")
	return nil
}

// Next fills dest with the next row's values, advancing across batches. Returns
// io.EOF when the stream is drained.
func (r *kernelRows) Next(dest []driver.Value) error {
	err := r.next(dest)
	// Record the first non-EOF error for the OnClose telemetry callback (io.EOF is
	// normal drain, not a failure). Mirrors the Thrift path's iterationErr capture.
	if err != nil && err != io.EOF && r.iterationErr == nil {
		r.iterationErr = err
	}
	return err
}

func (r *kernelRows) next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}
	for r.cur == nil || r.rowInCur >= int(r.cur.NumRows()) {
		if r.eof {
			return io.EOF
		}
		if err := r.nextBatch(); err != nil {
			return err
		}
	}
	rec := r.cur
	for c := 0; c < len(dest); c++ {
		v, err := arrowscan.ScanCellCached(rec.Column(c), r.rowInCur, r.op.location, r.keyCache)
		if err != nil {
			return fmt.Errorf("kernel: scan col %d (%s): %w", c, r.cols[c], err)
		}
		dest[c] = v
	}
	r.rowInCur++
	return nil
}

// nextBatch pulls the next Arrow batch. A released array (release==NULL) is the
// kernel's end-of-stream sentinel.
func (r *kernelRows) nextBatch() error {
	// Honor cancellation both at the batch boundary AND mid-fetch. The pre-fetch
	// ctx check fast-fails an already-cancelled ctx without dialing; the result-set
	// cancel token (r.watcher, created once in newKernelRows) then bridges the
	// deadline into the kernel so a fetch already in flight — a hung CloudFetch chunk
	// (a wedged S3 / pre-signed-URL GET) — no longer blocks Next past the deadline.
	// Firing the token unblocks this call promptly (it stops waiting on the batch);
	// the kernel's background download of that chunk continues to completion or its
	// own read-timeout (~60s) rather than being torn down mid-flight, so the caller's
	// deadline is honored while the socket is reclaimed shortly after — see the scope
	// note on kernel_result_stream_next_batch_cancellable in databricks_kernel.h. A
	// NULL token (uncancellable ctx) makes the cancellable fetch behave exactly like
	// the plain kernel_result_stream_next_batch, so there is no watcher overhead on
	// the common background-context path.
	if r.ctx != nil {
		if err := r.ctx.Err(); err != nil {
			return err
		}
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	var carr C.struct_ArrowArray
	var csch C.struct_ArrowSchema
	if err := call(func() C.KernelStatusCode {
		return C.kernel_result_stream_next_batch_cancellable(r.stream, &carr, &csch, r.watcher.tokenPtr())
	}); err != nil {
		// Evict a session-fatal conn BEFORE the ctx-cancelled branch below: a
		// session-fatal fetch failure (expired token, dropped/unavailable session)
		// racing a ctx deadline/cancel is still session-fatal, so returning the ctx
		// error first would leave a dead conn marked valid in the pool. Mirrors the
		// execute path's evict-before-ctx ordering in operation.go.
		r.op.backend.evictIfSessionFatal(err)
		// Prefer the caller's ctx error when the fetch was interrupted; cancelledErr
		// holds the shared dual-%w wrap (see its doc) so errors.Is still matches the
		// ctx error AND the *KernelError stays reachable via errors.As.
		if r.ctx != nil && r.ctx.Err() != nil {
			klogCtx(r.ctx, "nextBatch interrupted by ctx: kernelErr=%v ctxErr=%v", err, r.ctx.Err())
			return cancelledErr("next_batch", r.ctx.Err(), toStatementError(err))
		}
		return fmt.Errorf("kernel: next_batch: %w", toStatementError(err))
	}
	if carr.release == nil {
		r.eof = true
		klogCtx(r.ctx, "nextBatch: EOF")
		return io.EOF
	}
	// Zero-copy import. The kernel exports self-contained batches (Rust to_ffi
	// moves the Arc-owned buffers in), so the arrow.Record safely outlives the
	// stream; we still Release each batch explicitly as we advance.
	rec, err := cdata.ImportCRecordBatch(
		(*cdata.CArrowArray)(unsafe.Pointer(&carr)),
		(*cdata.CArrowSchema)(unsafe.Pointer(&csch)))
	if err != nil {
		return fmt.Errorf("kernel: import batch: %w", err)
	}
	r.cur = rec
	r.rowInCur = 0
	// Scope the struct-key cache to this batch: the C Data import mints a fresh
	// *StructType per batch, so the prior batch's cached prefixes can never be hit
	// again — resetting keeps the cache from growing one entry per batch over the
	// whole Rows lifetime (the intra-batch memoization win is unaffected).
	r.keyCache.Reset()
	r.chunkCount++
	if r.callbacks != nil && r.callbacks.OnChunkFetched != nil {
		// chunkCount is cumulative (per the callback contract). bytesDownloaded,
		// chunkIndex, and latency are left 0: the kernel does CloudFetch/decompress
		// internally and hands back ready Arrow batches, so the Go side never sees
		// the compressed wire bytes or per-chunk fetch latency the Thrift path
		// reports — a fabricated number would be worse than a truthful zero.
		r.callbacks.OnChunkFetched(r.chunkCount, 0, 0, 0, 0)
	}
	klogCtx(r.ctx, "nextBatch: %d rows (chunk %d)", rec.NumRows(), r.chunkCount)
	return nil
}
