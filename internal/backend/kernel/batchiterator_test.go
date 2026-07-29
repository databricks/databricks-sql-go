//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	dbsqlrows "github.com/databricks/databricks-sql-go/internal/rows"
	dbrows "github.com/databricks/databricks-sql-go/rows"
)

// TestGetArrowIPCStreamsRejected pins the discoverable sentinel so callers can
// branch on it: the kernel C ABI exports Arrow C Data, not IPC bytes.
func TestGetArrowIPCStreamsRejected(t *testing.T) {
	r := &kernelRows{}
	it, err := r.GetArrowIPCStreams(context.Background())
	if it != nil {
		t.Errorf("GetArrowIPCStreams = %v iterator, want nil", it)
	}
	if !errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
		t.Errorf("GetArrowIPCStreams err = %v, want wrapping ErrNotSupportedByKernel", err)
	}
}

// TestBatchIteratorSchema returns the schema captured at construction, and errors
// (rather than panics) when none was captured.
func TestBatchIteratorSchema(t *testing.T) {
	sch := arrow.NewSchema([]arrow.Field{{Name: "c", Type: arrow.PrimitiveTypes.Int64}}, nil)
	it := &kernelBatchIterator{r: &kernelRows{schema: sch}}
	if got, err := it.Schema(); err != nil || got != sch {
		t.Errorf("Schema() = (%v, %v), want (%v, nil)", got, err, sch)
	}
	itNil := &kernelBatchIterator{r: &kernelRows{}}
	if _, err := itNil.Schema(); err == nil {
		t.Error("Schema() with no schema = nil error, want a failure")
	}
}

// TestBatchIteratorDrainedStream: once the stream is drained (eof), fill reports
// done, so HasNext is false and Next returns io.EOF without touching the C stream.
func TestBatchIteratorDrainedStream(t *testing.T) {
	it := &kernelBatchIterator{r: &kernelRows{eof: true}}
	if it.HasNext() {
		t.Error("HasNext on a drained stream = true, want false")
	}
	if rec, err := it.Next(); rec != nil || err != io.EOF {
		t.Errorf("Next on a drained stream = (%v, %v), want (nil, io.EOF)", rec, err)
	}
}

// TestBatchIteratorFetchErrorSeedsIterationErr is the regression for the telemetry
// gap: the batch path bypasses Next(), so a fetch error must still seed iterationErr
// or OnClose records the failed stream as a successful statement. A cancelled r.ctx
// makes nextBatch return a non-EOF error at the boundary without a live C stream.
func TestBatchIteratorFetchErrorSeedsIterationErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var gotIter error
	r := &kernelRows{
		ctx:    ctx,
		cancel: cancel,
		callbacks: &dbsqlrows.TelemetryCallbacks{
			OnClose: func(_ int64, _ int, iterErr, _ error) { gotIter = iterErr },
		},
	}
	it := &kernelBatchIterator{r: r}

	_, err := it.Next()
	if err == nil {
		t.Fatal("Next with a cancelled ctx = nil error, want the fetch error")
	}
	if r.iterationErr == nil {
		t.Error("iterationErr not seeded on batch-path fetch error — OnClose would report success")
	}
	it.Close()
	if !errors.Is(gotIter, err) {
		t.Errorf("OnClose iterErr = %v, want the fetch error %v", gotIter, err)
	}
}

// TestBatchIteratorFetchErrorTerminates is the regression for peco-review-bot F1:
// a fetch error must surface exactly once, then the iterator terminates — HasNext
// goes false and a further Next returns io.EOF — so a caller that logs-and-continues
// on a non-EOF error can't spin forever. (A cancelled r.ctx makes nextBatch return a
// non-EOF error at the boundary without a live C stream.)
func TestBatchIteratorFetchErrorTerminates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it := &kernelBatchIterator{r: &kernelRows{ctx: ctx, cancel: cancel}}

	if !it.HasNext() {
		t.Fatal("HasNext before the error = false, want true (a Next would yield the error)")
	}
	_, err := it.Next()
	if err == nil {
		t.Fatal("Next with a cancelled ctx = nil error, want the fetch error")
	}
	// After the error is surfaced once, the iterator is done: no infinite loop.
	if it.HasNext() {
		t.Error("HasNext after the surfaced error = true, want false (would spin forever)")
	}
	if rec, err2 := it.Next(); rec != nil || err2 != io.EOF {
		t.Errorf("Next after the surfaced error = (%v, %v), want (nil, io.EOF)", rec, err2)
	}
}

// compile-time guard: kernelBatchIterator must satisfy the public iterator surface.
var _ dbrows.ArrowBatchIterator = (*kernelBatchIterator)(nil)
