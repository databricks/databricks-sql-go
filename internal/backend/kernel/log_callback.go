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
	"time"
	"unsafe"

	"github.com/databricks/databricks-sql-go/logger"
)

// This file is the thin cgo layer of the kernel log bridge: the exported callback
// trampoline and the one-time kernel_init_logging_callback install. The pure-Go
// pipeline it drives (queue, drain, flush, drop accounting) lives untagged in
// logforward_async.go so its tests run in the default CGO_ENABLED=0 build.

// kernelLogChannelCapacity bounds the hand-off buffer between kernel threads and the
// drain goroutine. Bursts beyond this are dropped rather than blocking a kernel
// thread — logs are advisory and must never back-pressure a kernel path.
const kernelLogChannelCapacity = 4096

// logCallbackOnce guards the process-wide, first-call-wins install.
var logCallbackOnce sync.Once

//export kernelLogTrampoline
func kernelLogTrampoline(level, target, message *C.char, _ unsafe.Pointer) {
	// A panic must never cross the C ABI. user_data is deliberately unused: the
	// kernel is given NULL, and the destination is reached through logQueue, so no
	// Go pointer is ever fabricated into a C void* (which the GC could fault on).
	defer func() { _ = recover() }()
	// time.Now() here is the emission time — the callback fires synchronously on the
	// kernel thread as the event is logged. C.GoString copies each borrowed string
	// into owned Go memory before the record can outlive the callback; the rest is
	// pure Go (see enqueueKernelLog).
	enqueueKernelLog(time.Now(), C.GoString(level), C.GoString(target), C.GoString(message))
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
		// NULL user_data: the drain goroutine owns the sink, so nothing Go-managed
		// crosses into C as a pointer.
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
