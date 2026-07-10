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
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/cdata"
	"github.com/databricks/databricks-sql-go/internal/decimalfmt"
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
		return nil, fmt.Errorf("kernel: get_schema: %w", toDriverError(err))
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
		v, err := scanCell(rec.Column(c), r.rowInCur, r.op.location)
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
	// Fail fast if the caller's context is already done rather than entering the
	// blocking C fetch (which cannot itself observe ctx). database/sql also runs
	// its own watcher that calls Rows.Close on cancellation, so an in-flight fetch
	// is still torn down; this just avoids starting a new one under a dead ctx.
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
		return fmt.Errorf("kernel: next_batch: %w", toDriverError(err))
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

// scanCell extracts one cell as a driver.Value. Scalars map to their Go value:
// bool, all int/uint widths, float, string, binary, date, timestamp, and
// top-level decimal (as an exact fixed-point string, matching the Thrift path —
// a float64 would lose precision beyond ~17 digits; see databricks-sql-go#274).
// Nested types (List/Map/Struct, and VARIANT which arrives nested) render to a
// JSON string byte-identical to the Thrift path (see scan_nested.go); GEOMETRY
// arrives as a WKB/WKT string and is handled by the string arm. NULLs map to
// nil. A genuinely unhandled type (e.g. interval/duration) returns an error
// rather than a silently wrong value.
func scanCell(col arrow.Array, row int, loc *time.Location) (driver.Value, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch c := col.(type) {
	case *array.Null:
		return nil, nil
	case *array.Boolean:
		return c.Value(row), nil
	case *array.Int8:
		return int64(c.Value(row)), nil
	case *array.Int16:
		return int64(c.Value(row)), nil
	case *array.Int32:
		return int64(c.Value(row)), nil
	case *array.Int64:
		return c.Value(row), nil
	case *array.Uint8:
		return int64(c.Value(row)), nil
	case *array.Uint16:
		return int64(c.Value(row)), nil
	case *array.Uint32:
		return int64(c.Value(row)), nil
	case *array.Uint64:
		return int64(c.Value(row)), nil
	case *array.Float32:
		// Return the native float32, NOT a widened float64: the Thrift path returns
		// a float32 driver.Value for a bare FLOAT column, and database/sql's
		// asString formats it at bit-size 32 — so widening here would render
		// CAST(0.1 AS FLOAT) as "0.10000000149011612" vs Thrift's "0.1".
		return c.Value(row), nil
	case *array.Float64:
		return c.Value(row), nil
	case *array.String:
		return c.Value(row), nil
	case *array.LargeString:
		return c.Value(row), nil
	case *array.Binary:
		return c.Value(row), nil
	case *array.Date32:
		return inLocation(c.Value(row).ToTime(), loc), nil
	case *array.Date64:
		return inLocation(c.Value(row).ToTime(), loc), nil
	case *array.Timestamp:
		dt, ok := col.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("timestamp column has unexpected datatype %s", col.DataType())
		}
		return inLocation(c.Value(row).ToTime(dt.Unit), loc), nil
	case *array.Decimal128:
		dt := col.DataType().(*arrow.Decimal128Type)
		return decimalfmt.ExactString(c.Value(row), dt.Scale), nil
	case *array.List, *array.LargeList, *array.FixedSizeList, *array.Map, *array.Struct:
		// Nested types (and VARIANT, which arrives as a nested value) render to a
		// JSON string matching the Thrift path.
		return renderJSONString(col, row, loc)
	default:
		return nil, fmt.Errorf("kernel: scanning arrow type %s is not supported "+
			"(intervals are not yet handled)", col.DataType())
	}
}

// inLocation renders t in loc, matching the Thrift path's .In(location); a nil
// loc leaves the value in UTC (arrow's ToTime default).
func inLocation(t time.Time, loc *time.Location) time.Time {
	if loc == nil {
		return t
	}
	return t.In(loc)
}
