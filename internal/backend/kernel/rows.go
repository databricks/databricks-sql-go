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

	cols       []string
	cur        arrow.Record // current batch (nil until first Next)
	rowInCur   int          // next row index within cur
	chunkCount int          // cumulative batches fetched, for OnChunkFetched
	closed     bool
	eof        bool
}

// newKernelRows fetches the schema up front (for Columns()) and returns the row
// iterator; batches are pulled lazily on Next.
func newKernelRows(ctx context.Context, op *kernelOp, stream *C.kernel_result_stream_t, cb *dbsqlrows.TelemetryCallbacks) (driver.Rows, error) {
	r := &kernelRows{ctx: ctx, op: op, stream: stream, callbacks: cb}

	var csch C.struct_ArrowSchema
	if err := call(func() C.KernelStatusCode {
		return C.kernel_result_stream_get_schema(stream, &csch)
	}); err != nil {
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
	klog("newKernelRows: %d columns", len(r.cols))
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
	klog("kernelRows closed")
	return nil
}

// Next fills dest with the next row's values, advancing across batches. Returns
// io.EOF when the stream is drained.
func (r *kernelRows) Next(dest []driver.Value) error {
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
		v, err := arrowscan.ScanCell(rec.Column(c), r.rowInCur, r.op.location)
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
		return fmt.Errorf("kernel: next_batch: %w", toStatementError(err))
	}
	if carr.release == nil {
		r.eof = true
		klog("nextBatch: EOF")
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
	r.chunkCount++
	if r.callbacks != nil && r.callbacks.OnChunkFetched != nil {
		// chunkCount is cumulative (per the callback contract). bytesDownloaded,
		// chunkIndex, and latency are left 0: the kernel does CloudFetch/decompress
		// internally and hands back ready Arrow batches, so the Go side never sees
		// the compressed wire bytes or per-chunk fetch latency the Thrift path
		// reports — a fabricated number would be worse than a truthful zero.
		r.callbacks.OnChunkFetched(r.chunkCount, 0, 0, 0, 0)
	}
	klog("nextBatch: %d rows (chunk %d)", rec.NumRows(), r.chunkCount)
	return nil
}
