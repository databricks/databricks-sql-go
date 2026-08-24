package kernel

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag
// (matching logging_level.go and logforward.go). It holds the pure-Go async
// forwarding pipeline — the bounded hand-off queue, its drain, the flush barrier,
// drop accounting, and panic containment. Only the cgo trampoline and the
// kernel_init_logging_callback call actually need cgo; keeping the rest untagged
// lets its tests (FIFO flush, drop policy, panic containment) run in the default
// CGO_ENABLED=0 build rather than only in the kernel-linked lane.

import (
	"sync/atomic"
	"time"
)

// kernelLogRecord is an owned copy of one kernel tracing record. The C strings are
// valid only for the duration of the callback, so the trampoline copies them before
// the record leaves the kernel thread. A record with a non-nil done channel is a
// flush barrier: the drain closes done (in FIFO order, after every earlier record is
// written) and forwards nothing — see flushKernelLogs.
type kernelLogRecord struct {
	emittedAt time.Time
	level     string
	target    string
	message   string
	done      chan struct{}
}

var (
	// logQueue publishes the bounded hand-off channel to the trampoline. It is an
	// atomic pointer so the read on a kernel thread synchronizes with the write on
	// the installing goroutine; nil until (and unless) logging installs.
	logQueue atomic.Pointer[chan kernelLogRecord]
	// logDropped counts records discarded because the buffer was full — a growing
	// value means the sink cannot keep up. Exposed via kernelLogDropped.
	logDropped atomic.Uint64
)

// kernelLogDropped reports how many forwarded kernel records were dropped because
// the bounded hand-off buffer was full. Safe to call at any time.
func kernelLogDropped() uint64 { return logDropped.Load() }

// enqueueKernelLog hands one already-owned record to the drain goroutine without
// blocking. Split out of the cgo trampoline so the enqueue/drop policy is testable
// without cgo (import "C" is not allowed in _test.go files).
func enqueueKernelLog(emittedAt time.Time, level, target, message string) {
	qp := logQueue.Load()
	if qp == nil {
		return
	}
	rec := kernelLogRecord{emittedAt: emittedAt, level: level, target: target, message: message}
	// Non-blocking hand-off: never stall a kernel thread on a slow, contended, or
	// re-entrant user writer. A full buffer drops the record and counts it.
	select {
	case *qp <- rec:
	default:
		logDropped.Add(1)
	}
}

// drainKernelLogs is the single goroutine that moves records off kernel threads and
// into the shared logger. Running the arbitrary user writer here — not in the
// trampoline — keeps user I/O (and any driver re-entry it triggers) off the kernel
// thread, honoring the C ABI's "return promptly / no re-entry" contract.
func drainKernelLogs(ch <-chan kernelLogRecord, sink *logSink) {
	baselineDrops := logDropped.Load()
	warnedDrop := false
	for rec := range ch {
		// A flush barrier carries no record: closing done signals that every earlier
		// record has been written (channel + drain are FIFO).
		if rec.done != nil {
			close(rec.done)
			continue
		}
		// Every write below goes to the user's writer, which may panic — an
		// unrecovered goroutine panic is fatal to the process, so contain each one.
		contain(func() {
			sink.forward(rec.emittedAt, rec.level, rec.target, rec.message)
		})
		// Surface log loss the first time the sink falls behind. Routed through the
		// sink's own (immutable) logger, not logger.Logger, so this long-lived
		// goroutine never races SetLogLevel's reassignment of Logger.Logger. One-shot
		// so a burst can't turn into log spam; the total stays in kernelLogDropped().
		if !warnedDrop && logDropped.Load() > baselineDrops {
			warnedDrop = true
			dropped := logDropped.Load() - baselineDrops
			contain(func() { sink.warnDropped(dropped) })
		}
	}
}

// contain runs fn, swallowing any panic. A misbehaving user writer (reached via the
// sink) must never take down the drain goroutine.
func contain(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

// flushKernelLogs blocks until every kernel record already queued has been written,
// or until timeout elapses; it returns whether the flush completed. It is a no-op
// returning true when kernel logging was never installed.
//
// It drains the asynchronous hand-off so records are not lost or misrouted when the
// log writer is closed, output is retargeted, or the process exits. There is
// currently no public entry point that calls it — only the end-to-end test does;
// exposing a supported flush API (or wiring it into a shutdown path) is a separate
// change. Best-effort: records already dropped for a full buffer are gone, and
// records enqueued after this call are not waited on.
func flushKernelLogs(timeout time.Duration) bool {
	qp := logQueue.Load()
	if qp == nil {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{})
	// Enqueue the barrier behind everything already queued. A full buffer means the
	// drain is behind; wait (bounded) for room rather than dropping the barrier.
	select {
	case *qp <- kernelLogRecord{done: done}:
	case <-timer.C:
		return false
	}
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
