//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"

// The Go export below is generated with mutable char* parameters, but
// KernelLogCallback lends read-only const char*. kernelLogAdapter has the exact
// KernelLogCallback signature and forwards to the Go export, so the function
// pointer handed to the kernel needs no incompatible function-pointer cast.
void kernelLogTrampoline(char* level, char* target, char* message, void* user_data);
static void kernelLogAdapter(const char* level, const char* target,
                             const char* message, void* user_data) {
    kernelLogTrampoline((char*)level, (char*)target, (char*)message, user_data);
}
static KernelLogCallback kernel_log_cb(void) { return kernelLogAdapter; }
*/
import "C"

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/databricks/databricks-sql-go/logger"
)

// kernelLogRecord is an owned copy of one kernel tracing record. The C strings
// are valid only for the duration of the callback, so the trampoline copies them
// before the record leaves the kernel thread. A record with a non-nil done
// channel is a flush barrier: the drain closes done (in FIFO order, after every
// earlier record is written) and forwards nothing — see flushKernelLogs.
type kernelLogRecord struct {
	emittedAt time.Time
	level     string
	target    string
	message   string
	done      chan struct{}
}

// kernelLogChannelCapacity bounds the hand-off buffer between kernel threads and
// the drain goroutine. Bursts beyond this are dropped rather than blocking a
// kernel thread — logs are advisory and must never back-pressure a kernel path.
const kernelLogChannelCapacity = 4096

var (
	logCallbackOnce sync.Once
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

//export kernelLogTrampoline
func kernelLogTrampoline(level, target, message *C.char, _ unsafe.Pointer) {
	// A panic must never cross the C ABI. user_data is deliberately unused: the
	// kernel is given NULL, and the destination is reached through logQueue, so no
	// Go pointer is ever fabricated into a C void* (which the GC could fault on).
	defer func() { _ = recover() }()
	// time.Now() here is the emission time — the callback fires synchronously on
	// the kernel thread as the event is logged. C.GoString copies each borrowed
	// string into owned Go memory before the record can outlive the callback; the
	// rest is pure Go (see enqueueKernelLog).
	enqueueKernelLog(time.Now(), C.GoString(level), C.GoString(target), C.GoString(message))
}

// enqueueKernelLog hands one already-owned record to the drain goroutine without
// blocking. Split out of the cgo trampoline so the enqueue/drop policy is
// testable without cgo (import "C" is not allowed in _test.go files).
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

// drainKernelLogs is the single goroutine that moves records off kernel threads
// and into the shared logger. Running the arbitrary user writer here — not in the
// trampoline — keeps user I/O (and any driver re-entry it triggers) off the
// kernel thread, honoring the C ABI's "return promptly / no re-entry" contract.
func drainKernelLogs(ch <-chan kernelLogRecord, sink *logSink) {
	baselineDrops := logDropped.Load()
	warnedDrop := false
	for rec := range ch {
		// A flush barrier carries no record: closing done signals that every
		// earlier record has been written (channel + drain are FIFO).
		if rec.done != nil {
			close(rec.done)
			continue
		}
		// Every write below goes to the user's writer, which may panic — an
		// unrecovered goroutine panic is fatal to the process, so contain each one.
		contain(func() {
			sink.forward(rec.emittedAt, rec.level, rec.target, rec.message)
		})
		// Surface log loss the first time the sink falls behind — from this
		// goroutine, never the kernel thread. One-shot so a burst can't turn into
		// log spam; the running total stays available via kernelLogDropped(). This
		// writes to the same destination as forward, so it is contained too.
		if !warnedDrop && logDropped.Load() > baselineDrops {
			warnedDrop = true
			dropped := logDropped.Load() - baselineDrops
			contain(func() {
				logger.Logger.Warn().Uint64("dropped", dropped).Msg(
					"[kernel] kernel log records dropped; the log sink is not keeping up " +
						"(raise capacity or lower kernel verbosity)")
			})
		}
	}
}

// contain runs fn, swallowing any panic. A misbehaving user writer (reached via
// the shared logger) must never take down the drain goroutine.
func contain(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

// flushKernelLogs blocks until every kernel record already queued has been
// written, or until timeout elapses; it returns whether the flush completed. It
// is a no-op returning true when kernel logging was never installed.
//
// It drains the asynchronous hand-off so records are not lost or misrouted when
// the log writer is closed, output is retargeted, or the process exits. There is
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
	// Enqueue the barrier behind everything already queued. A full buffer means
	// the drain is behind; wait (bounded) for room rather than dropping the barrier.
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

func installKernelLogCallback(level string, useNULL bool) {
	logCallbackOnce.Do(func() {
		// OFF intentionally installs no subscriber and starts no drain.
		if !useNULL && level == "OFF" {
			return
		}

		ch := make(chan kernelLogRecord, kernelLogChannelCapacity)
		// Publish before installing so a callback that fires during
		// kernel_init_logging_callback already has a channel to enqueue onto;
		// records buffer until the drain starts just below.
		logQueue.Store(&ch)

		var clevel cStr
		if !useNULL {
			clevel = newCStr(level)
			defer clevel.free()
		}
		// NULL user_data: the drain goroutine owns the sink, so nothing
		// Go-managed crosses into C as a pointer.
		if err := call(func() C.KernelStatusCode {
			return C.kernel_init_logging_callback(clevel.c, C.kernel_log_cb(), nil)
		}); err != nil {
			// Install failed, so the callback layer was not installed. Unpublish the
			// channel; no drain was started and nothing references it, so it is simply
			// collected — no close (and thus no send-on-closed race to reason about).
			logQueue.Store(nil)
			logger.Logger.Warn().Msgf(
				"databricks: kernel_init_logging_callback: %v (kernel logs not forwarded; proceeding)", err)
			return
		}
		// Installed: start the single drain goroutine. Any records enqueued during
		// the call above are buffered and delivered once it runs.
		go drainKernelLogs(ch, newLogSink())
	})
}
