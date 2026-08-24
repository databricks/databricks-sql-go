//go:build databricks_kernel_dynamic

package kernel

import (
	"database/sql/driver"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

// dynRows is a driver.Rows over a kernel result stream, using the pure-Go
// C-Data importer (cdata_pure.go) instead of the cgo cdata package. It pulls one
// Arrow batch at a time via the dlopen'd kernel_result_stream_next_batch, imports
// it, and scans cells with the SAME arrowscan scanner the cgo rows.go uses — so
// values are identical to the cgo backend by construction.
//
// This is the data-plane proof for the dynamic-loading approach: everything from
// the result stream to a driver.Value runs with CGO_ENABLED=0.
type dynRows struct {
	l        *dynLib
	stream   uintptr
	location *time.Location

	cols     []string
	colTypes []arrowscan.ColumnTypeInfo
	schema   *arrow.Schema

	cur      arrow.Record
	rowInCur int
	keyCache *arrowscan.StructKeyCache
	closed   bool
	eof      bool
}

var _ driver.Rows = (*dynRows)(nil)

// newDynRows fetches the schema up front and returns the row iterator.
func newDynRows(l *dynLib, stream uintptr, loc *time.Location) (*dynRows, error) {
	r := &dynRows{l: l, stream: stream, location: loc, keyCache: arrowscan.NewStructKeyCache()}

	var csch cArrowSchema
	if err := l.callDyn(func() int32 { return l.streamGetSchema(stream, &csch) }); err != nil {
		r.Close()
		return nil, fmt.Errorf("kernel(dyn): get_schema: %w", err)
	}
	sch, err := importCArrowSchema(&csch)
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("kernel(dyn): import schema: %w", err)
	}
	r.schema = sch
	fields := sch.Fields()
	r.cols = make([]string, len(fields))
	r.colTypes = make([]arrowscan.ColumnTypeInfo, len(fields))
	for i, f := range fields {
		r.cols[i] = f.Name
		r.colTypes[i] = arrowscan.ColumnTypeInfoFor(f.Type)
	}
	return r, nil
}

func (r *dynRows) Columns() []string { return r.cols }

func (r *dynRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.stream != 0 {
		_ = r.l.callDyn(func() int32 { return r.l.streamClose(r.stream) })
		r.stream = 0
	}
	return nil
}

func (r *dynRows) Next(dest []driver.Value) error {
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
		v, err := arrowscan.ScanCellCached(rec.Column(c), r.rowInCur, r.location, r.keyCache)
		if err != nil {
			return fmt.Errorf("kernel(dyn): scan col %d (%s): %w", c, r.cols[c], err)
		}
		dest[c] = v
	}
	r.rowInCur++
	return nil
}

// nextBatch pulls the next Arrow batch via the dlopen'd next_batch and imports
// it with the pure-Go importer. A released array (release==0) is EOF.
func (r *dynRows) nextBatch() error {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	var carr cArrowArray
	var csch cArrowSchema
	if err := r.l.callDyn(func() int32 { return r.l.streamNextBatch(r.stream, &carr, &csch) }); err != nil {
		return fmt.Errorf("kernel(dyn): next_batch: %w", err)
	}
	if carr.release == 0 {
		r.eof = true
		return io.EOF
	}
	rec, err := importCRecordBatch(&carr, &csch)
	if err != nil {
		return fmt.Errorf("kernel(dyn): import batch: %w", err)
	}
	r.cur = rec
	r.rowInCur = 0
	r.keyCache.Reset()
	return nil
}

// importCRecordBatch imports a batch given both array and schema (schema is a
// struct whose fields are the columns). Mirrors cdata.ImportCRecordBatch.
func importCRecordBatch(arr *cArrowArray, sc *cArrowSchema) (arrow.Record, error) {
	field, err := importSchema(sc)
	if err != nil {
		return nil, err
	}
	st, ok := field.Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("kernel(dyn): recordbatch import must be struct type")
	}
	return importCRecordBatchWithSchema(arr, arrow.NewSchema(st.Fields(), &field.Metadata))
}
