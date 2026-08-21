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
	"unsafe"

	"github.com/databricks/databricks-sql-go/logger"
)

// kernelLogRecord is an owned copy of one kernel tracing record. The C strings
// are valid only for the duration of the callback, so the trampoline copies them
// before the record leaves the kernel thread.
type kernelLogRecord struct {
	level   string
	target  string
	message string
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
	// C.GoString copies each borrowed string into owned Go memory before the
	// record can outlive the callback; the rest is pure Go (see enqueueKernelLog).
	enqueueKernelLog(C.GoString(level), C.GoString(target), C.GoString(message))
}

// enqueueKernelLog hands one already-owned record to the drain goroutine without
// blocking. Split out of the cgo trampoline so the enqueue/drop policy is
// testable without cgo (import "C" is not allowed in _test.go files).
func enqueueKernelLog(level, target, message string) {
	qp := logQueue.Load()
	if qp == nil {
		return
	}
	rec := kernelLogRecord{level: level, target: target, message: message}
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
	for rec := range ch {
		// A writer panic must not kill the drain goroutine — an unrecovered
		// goroutine panic is fatal to the process. Contain it per record.
		func() {
			defer func() { _ = recover() }()
			sink.forward(rec.level, rec.target, rec.message)
		}()
	}
}

func installKernelLogCallback(level string, useNULL bool) {
	logCallbackOnce.Do(func() {
		// OFF intentionally installs no subscriber and starts no drain.
		if !useNULL && level == "OFF" {
			return
		}

		ch := make(chan kernelLogRecord, kernelLogChannelCapacity)
		go drainKernelLogs(ch, newLogSink())
		// Publish before installing the subscriber so a callback that fires during
		// kernel_init_logging_callback already sees the channel.
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
			// The subscriber did not install (commonly: a global subscriber is
			// already set), so no callback will ever fire. Retire the drain:
			// unpublish the channel first, then close it so the goroutine exits.
			// Safe because no producer exists on this path.
			logQueue.Store(nil)
			close(ch)
			logger.Logger.Warn().Msgf(
				"databricks: kernel_init_logging_callback: %v (kernel logs not forwarded; proceeding)", err)
		}
	})
}
