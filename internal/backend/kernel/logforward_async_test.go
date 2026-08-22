package kernel

// These exercise the pure-Go async forwarding pipeline (logforward_async.go) and
// are intentionally untagged, so the FIFO-flush, drop-policy, and panic-containment
// guarantees run in the default CGO_ENABLED=0 build — not only the kernel-linked
// lane. The cgo trampoline that feeds this pipeline is covered by the end-to-end
// test in log_callback_test.go.

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

// The forward path enqueues an owned record onto the bounded channel. (The cgo
// trampoline copies the borrowed C strings via C.GoString, then calls this; that
// tiny boundary can't be driven from a _test.go file because import "C" is
// disallowed there, so the testable logic lives in enqueueKernelLog.)
func TestEnqueueKernelLog(t *testing.T) {
	ch := make(chan kernelLogRecord, 1)
	prev := logQueue.Swap(&ch)
	t.Cleanup(func() { logQueue.Store(prev) })

	emittedAt := time.Now()
	enqueueKernelLog(emittedAt, "debug", "databricks::sql::kernel", "callback probe")
	select {
	case got := <-ch:
		if got.level != "debug" || got.target != "databricks::sql::kernel" || got.message != "callback probe" {
			t.Fatalf("enqueued record = %#v", got)
		}
		if !got.emittedAt.Equal(emittedAt) {
			t.Fatalf("emittedAt = %v, want %v", got.emittedAt, emittedAt)
		}
	default:
		t.Fatal("record was not enqueued")
	}
}

// A full buffer drops the record and counts it rather than blocking the kernel
// thread. A nil queue (logging not installed) is a safe no-op.
func TestEnqueueKernelLogDropsWhenFullAndNoopWhenUnset(t *testing.T) {
	// Unset queue: must not block or panic.
	prev := logQueue.Swap(nil)
	t.Cleanup(func() { logQueue.Store(prev) })
	enqueueKernelLog(time.Now(), "warn", "t", "before install")

	ch := make(chan kernelLogRecord, 1)
	logQueue.Store(&ch)
	before := kernelLogDropped()
	enqueueKernelLog(time.Now(), "warn", "t", "keeps the one slot") // fills the buffer
	enqueueKernelLog(time.Now(), "warn", "t", "must be dropped")    // buffer full → dropped
	if got := kernelLogDropped() - before; got != 1 {
		t.Fatalf("dropped delta = %d, want 1", got)
	}
	if len(ch) != 1 {
		t.Fatalf("channel len = %d, want 1 (non-blocking drop)", len(ch))
	}
}

// A panicking writer must not kill the drain goroutine (an unrecovered goroutine
// panic is fatal to the process); the drain contains it and keeps processing.
func TestDrainRecoversFromWriterPanic(t *testing.T) {
	t.Cleanup(func() { logger.SetLogOutput(os.Stderr) })
	logger.SetLogOutput(io.Discard) // discard the sink's forwarded records
	done := make(chan string, 1)
	sink := newLogSink()
	sink.observe = func(_, _, message string) {
		if message == "boom" {
			panic("writer failure")
		}
		done <- message
	}

	ch := make(chan kernelLogRecord, 2)
	go drainKernelLogs(ch, sink)
	defer close(ch)

	ch <- kernelLogRecord{level: "error", target: "t", message: "boom"} // triggers the panic
	ch <- kernelLogRecord{level: "info", target: "t", message: "after"} // must still be delivered
	select {
	case got := <-done:
		if got != "after" {
			t.Fatalf("delivered %q, want %q", got, "after")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain goroutine did not survive a writer panic")
	}
}

// flushKernelLogs returns only after every record queued before it is written.
func TestFlushKernelLogsWaitsForQueued(t *testing.T) {
	t.Cleanup(func() { logger.SetLogOutput(os.Stderr) })
	logger.SetLogOutput(io.Discard) // discard the sink's forwarded records
	seen := make(chan string, 8)
	sink := newLogSink()
	sink.observe = func(_, _, message string) { seen <- message }

	ch := make(chan kernelLogRecord, 8)
	prev := logQueue.Swap(&ch)
	t.Cleanup(func() { logQueue.Store(prev) })
	go drainKernelLogs(ch, sink)
	defer close(ch)

	enqueueKernelLog(time.Now(), "info", "t", "one")
	enqueueKernelLog(time.Now(), "info", "t", "two")
	if !flushKernelLogs(2 * time.Second) {
		t.Fatal("flush timed out")
	}
	// The barrier is FIFO-ordered behind both records, so both are delivered.
	if got := len(seen); got != 2 {
		t.Fatalf("after flush, delivered %d records, want 2", got)
	}
}

// flushKernelLogs is a no-op returning true when logging was never installed.
func TestFlushKernelLogsNoopWhenUnset(t *testing.T) {
	prev := logQueue.Swap(nil)
	t.Cleanup(func() { logQueue.Store(prev) })
	if !flushKernelLogs(time.Second) {
		t.Fatal("flush with no queue should be a no-op returning true")
	}
}

type panicWriter struct{}

func (panicWriter) Write([]byte) (int, error) { panic("writer always panics") }

// A panicking writer combined with dropped records must not crash the drain: the
// forwarded record AND the one-shot drop warning both write to that writer, and
// both must be contained. observe advances the drop counter past the drain's
// baseline (so the warning fires) and panics (so forward is exercised too); the
// shared output is the panicking writer (so the warning write panics).
func TestDrainContainsPanicFromForwardAndDropWarning(t *testing.T) {
	prevLevel := logger.Logger.GetLevel()
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		logger.Logger.Logger = logger.Logger.Level(prevLevel)
	})
	if err := logger.SetLogLevel("warn"); err != nil {
		t.Fatal(err)
	}
	logger.SetLogOutput(panicWriter{})

	sink := newLogSink()
	sink.observe = func(_, _, _ string) {
		logDropped.Add(1) // advance past the drain's baseline, deterministically
		panic("forward failure")
	}

	ch := make(chan kernelLogRecord, 4)
	prev := logQueue.Swap(&ch)
	t.Cleanup(func() { logQueue.Store(prev) })
	go drainKernelLogs(ch, sink)
	defer close(ch)

	enqueueKernelLog(time.Now(), "info", "databricks::sql::kernel", "boom")

	// The drain reaches the flush barrier only if it survived both panics.
	if !flushKernelLogs(2 * time.Second) {
		t.Fatal("drain did not survive a panicking writer combined with dropped records")
	}
}
