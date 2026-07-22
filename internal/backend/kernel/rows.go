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
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
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
	ctx       context.Context
	op        *kernelOp
	stream    *C.kernel_result_stream_t
	callbacks *dbsqlrows.TelemetryCallbacks

	cols       []string
	colTypes   []arrowscan.ColumnTypeInfo // per-column type metadata (PECOBLR-3692)
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
	r := &kernelRows{ctx: ctx, op: op, stream: stream, keyCache: arrowscan.NewStructKeyCache()}

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
	// Honor cancellation at batch boundaries: check ctx before entering the
	// blocking C fetch (which cannot itself observe ctx). This does NOT interrupt
	// a fetch already in flight — and database/sql's own cancel watcher can't
	// either: its Rows.Close takes rs.closemu.Lock(), which blocks until the
	// in-progress Next (holding the RLock) returns, so the stream close waits for
	// the C call to finish on its own. A single hung CloudFetch batch is therefore
	// uninterruptible (the kernel exposes no per-download timeout); mid-fetch
	// cancellation would need the execute path's watcher/canceller applied here.
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
