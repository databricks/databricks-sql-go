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
	"reflect"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/cdata"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	context2 "github.com/databricks/databricks-sql-go/internal/compat/context"
	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
	dbrows "github.com/databricks/databricks-sql-go/rows"
)

// kernelRows implements the same optional column-type interfaces the Thrift path
// (internal/rows.rows) does, so a caller sees identical result-set metadata on
// either backend. Without these, database/sql falls back to "" / interface{} for
// every column — see PECOBLR-3692.
var (
	_ driver.Rows                           = (*kernelRows)(nil)
	_ driver.RowsColumnTypeScanType         = (*kernelRows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*kernelRows)(nil)
	_ driver.RowsColumnTypeNullable         = (*kernelRows)(nil)
	_ driver.RowsColumnTypeLength           = (*kernelRows)(nil)
	// Public batch API, matching the Thrift path (rows.rows). GetArrowBatches is
	// real; GetArrowIPCStreams rejects (kernel C ABI exports C Data, not IPC bytes).
	_ dbrows.Rows = (*kernelRows)(nil)
)

// kernelRows implements driver.Rows over the kernel result stream. It pulls one
// Arrow RecordBatch at a time via kernel_result_stream_next_batch (inline and
// CloudFetch are transparent below the C ABI), imports it zero-copy through the
// Arrow C Data Interface, and scans rows out on demand.
//
// The batch/row split mirrors the kernel's own ResultStream: next_batch does the
// per-batch network/decode work once; row reads walk the already-imported
// arrow.Record with O(1) indexing.
type kernelRows struct {
	// ctx is detached from the caller's QueryContext (via context2.WithoutCancel)
	// so a submit-gating deadline can't truncate a large CloudFetch stream, matching
	// the Thrift path (see ES-1934053); cancel makes it abortable from Close.
	ctx       context.Context
	cancel    context.CancelFunc
	op        *kernelOp
	stream    *C.kernel_result_stream_t
	callbacks *dbsqlrows.TelemetryCallbacks

	cols       []string
	colTypes   []arrowscan.ColumnTypeInfo // per-column type metadata (PECOBLR-3692)
	schema     *arrow.Schema              // result-set schema, for GetArrowBatches().Schema()
	cur        arrow.Record               // current batch (nil until first Next)
	rowInCur   int                        // next row index within cur
	chunkCount int                        // cumulative batches fetched, for OnChunkFetched
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
	// Detach from the caller's cancellation but keep its values (auth/logging), so a
	// deadline that only gated statement submission can't truncate the result stream.
	resultsCtx, resultsCancel := context.WithCancel(context2.WithoutCancel(ctx))
	r := &kernelRows{ctx: resultsCtx, cancel: resultsCancel, op: op, stream: stream, keyCache: arrowscan.NewStructKeyCache()}

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
	r.schema = sch
	fields := sch.Fields()
	r.cols = make([]string, len(fields))
	// Derive per-column type metadata from the Arrow schema up front (the same
	// schema Columns() is built from), so the RowsColumnType* interfaces report
	// the Databricks type name / scan type / length matching the Thrift path
	// (PECOBLR-3692) with no per-call work. Kept in lockstep with the value scanner
	// (ScanCellCached) via the shared arrowscan.ColumnTypeInfoFor mapper.
	r.colTypes = make([]arrowscan.ColumnTypeInfo, len(fields))
	for i, f := range fields {
		r.cols[i] = f.Name
		r.colTypes[i] = arrowscan.ColumnTypeInfoFor(f.Type)
	}
	// Construction succeeded — now arm the close telemetry callback so a normal
	// Close() (after row iteration) records CLOSE_STATEMENT.
	r.callbacks = cb
	klogCtx(ctx, "newKernelRows: %d columns", len(r.cols))
	return r, nil
}

// Columns returns the result-set column names.
func (r *kernelRows) Columns() []string { return r.cols }

// ColumnTypeScanType returns the Go type a column is best scanned into, matching
// the Thrift path (PECOBLR-3692). An out-of-range index returns nil, as the
// Thrift path does on a metadata lookup failure.
func (r *kernelRows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.colTypes) {
		return nil
	}
	return r.colTypes[index].ScanType
}

// ColumnTypeDatabaseTypeName returns the Databricks type name for a column (e.g.
// "BIGINT", "DECIMAL", "ARRAY"), matching the Thrift path. An out-of-range index
// returns "".
func (r *kernelRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.colTypes) {
		return ""
	}
	return r.colTypes[index].DatabaseTypeName
}

// ColumnTypeNullable reports whether a column is nullable. The kernel result
// schema does not carry a reliable per-column nullability flag, so — exactly like
// the Thrift path — this always returns ok=false (nullability unknown).
func (r *kernelRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, false
}

// ColumnTypeLength returns a variable-length column's length (math.MaxInt64,
// unbounded) for string/binary/nested/interval types and (0, false) for
// fixed-width types, matching the Thrift path. An out-of-range index returns
// (0, false).
func (r *kernelRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.colTypes) {
		return 0, false
	}
	ct := r.colTypes[index]
	return ct.Length, ct.HasLength
}

// Close releases the current batch, the kernel result stream, and (query-path
// ownership) the server operation. Idempotent.
func (r *kernelRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	closeStart := time.Now()
	// Release the detached results context so an aborted Close doesn't leak it.
	if r.cancel != nil {
		r.cancel()
	}
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
		v, err := arrowscan.ScanCellCachedDecimalFloat(rec.Column(c), r.rowInCur, r.op.location, r.keyCache, r.op.decimalAsFloat)
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
	// Stop pulling once Close cancels r.ctx; r.ctx is detached from the caller's
	// deadline, so this fires on abort, not on a submit-gating timeout. It does NOT
	// interrupt an in-flight C fetch (the kernel exposes no per-download timeout).
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
		return C.kernel_result_stream_next_batch(r.stream, &carr, &csch)
	}); err != nil {
		r.op.backend.evictIfSessionFatal(err)
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

// GetArrowBatches exposes the kernel stream as an arrow.Record iterator (the
// public zero-copy batch API), reusing the row scanner's next_batch pull. Callers
// MUST Release() each record.
func (r *kernelRows) GetArrowBatches(context.Context) (dbrows.ArrowBatchIterator, error) {
	return &kernelBatchIterator{r: r}, nil
}

// GetArrowIPCStreams is rejected on the kernel path: the C ABI exports Arrow via
// the C Data Interface (next_batch), not IPC bytes. Use GetArrowBatches, or the
// Thrift backend for IPC streams.
func (r *kernelRows) GetArrowIPCStreams(context.Context) (dbrows.ArrowIPCStreamIterator, error) {
	return nil, fmt.Errorf("databricks: GetArrowIPCStreams is %w (kernel exports Arrow C Data, not IPC bytes); use GetArrowBatches",
		dbsqlerr.ErrNotSupportedByKernel)
}

// kernelBatchIterator adapts the kernelRows pull loop to the public
// ArrowBatchIterator: it prefetches one batch (so HasNext is exact) and transfers
// each record's ownership to the caller, who must Release it.
type kernelBatchIterator struct {
	r       *kernelRows
	pending arrow.Record // prefetched, not yet handed out
	done    bool         // stream drained (io.EOF seen)
	err     error        // sticky fetch error, surfaced by Next
}

var _ dbrows.ArrowBatchIterator = (*kernelBatchIterator)(nil)

// fill buffers one batch into pending, or sets done/err. It re-uses nextBatch and
// takes ownership of r.cur so the row scanner and Close can't double-release it.
func (it *kernelBatchIterator) fill() {
	if it.pending != nil || it.done || it.err != nil {
		return
	}
	if it.r.closed || it.r.eof { // nothing left to pull
		it.done = true
		return
	}
	switch err := it.r.nextBatch(); err {
	case nil:
		it.pending, it.r.cur = it.r.cur, nil
	case io.EOF:
		it.done = true
	default:
		it.err = err
		// The batch path bypasses Next(), so seed iterationErr here too; else OnClose
		// records the failed stream as a successful statement in telemetry.
		if it.r.iterationErr == nil {
			it.r.iterationErr = err
		}
	}
}

// Next returns the next record (caller owns it) or io.EOF when the stream drains.
func (it *kernelBatchIterator) Next() (arrow.Record, error) {
	it.fill()
	if it.err != nil {
		return nil, it.err
	}
	if it.pending == nil {
		return nil, io.EOF
	}
	rec := it.pending
	it.pending = nil
	return rec, nil
}

// HasNext reports whether a following Next would yield a record or an error.
func (it *kernelBatchIterator) HasNext() bool {
	it.fill()
	return it.pending != nil || it.err != nil
}

// Close releases any buffered record and tears down the underlying stream.
func (it *kernelBatchIterator) Close() {
	if it.pending != nil {
		it.pending.Release()
		it.pending = nil
	}
	it.r.Close() //nolint:errcheck
}

// Schema returns the result-set schema captured at construction.
func (it *kernelBatchIterator) Schema() (*arrow.Schema, error) {
	if it.r.schema == nil {
		return nil, fmt.Errorf("kernel: no schema available")
	}
	return it.r.schema, nil
}
