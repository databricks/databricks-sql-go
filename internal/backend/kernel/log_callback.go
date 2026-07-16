//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"

// Adapter matching the kernel_log_callback typedef (const char*) that forwards
// to the //export'd Go trampoline. cgo generates the trampoline's prototype in
// _cgo_export.h with non-const char* params, which don't match the typedef, so
// this shim bridges the two and gives kernel_set_log_callback one clean address
// to take. Forward-declared with the SAME (char*) signature cgo generates to
// avoid a conflicting-declaration error; the kernel only ever reads the strings.
void kernelLogTrampoline(void* ctx, int level, char* target, char* message);
// Hand kernel_set_log_callback the trampoline's address as a kernel_log_callback.
// The //export'd trampoline's generated signature uses non-const char* while the
// typedef uses const char*, which are function-pointer compatible (a const-only
// difference); do the cast once here, on the C side, so the Go call site stays
// clean and there is no separately-linked adapter symbol.
static kernel_log_callback kernel_log_cb(void) {
    return (kernel_log_callback)kernelLogTrampoline;
}
// Cast a cgo.Handle (an integer token) to the opaque void* ctx on the C side,
// so Go never does unsafe.Pointer(uintptr) directly (which `go vet` flags).
static void* kernel_handle_to_ctx(uintptr_t h) { return (void*)h; }
// Test-only: invoke the trampoline exactly as the kernel drain thread would.
// (A C func-pointer value can't be called directly from Go, so a helper does it.)
static void kernel_invoke_trampoline_for_test(void* ctx, int level, char* target, char* message) {
    kernelLogTrampoline(ctx, level, target, message);
}
*/
import "C"

import (
	"runtime/cgo"
	"sync"
	"unsafe"

	"github.com/databricks/databricks-sql-go/logger"
)

// This file holds the reverse-call machinery for the kernel log callback (K4):
// a cgo-exported Go function the kernel invokes from its log-drain thread, made
// safe with a recover() panic firewall (a panic across the cgo boundary would
// abort the process) and a runtime/cgo.Handle so a Go pointer (the *logSink) can
// round-trip through the C void* ctx under cgo's pointer rules. This is the same
// machinery a kernel→host token-provider callback (OAuth U2M external creds)
// would need.

// logCallbackOnce guards the one-time callback registration; logCallbackHandle
// keeps the cgo.Handle alive for the process (the kernel holds its numeric value
// as the opaque ctx and there is no detach in the v0 C ABI, so we never Delete
// it — a deliberate process-lifetime pin, not a leak).
var (
	logCallbackOnce   sync.Once
	logCallbackHandle cgo.Handle
)

// invokeLogTrampolineForTest drives the C→Go trampoline exactly as the kernel's
// drain thread would — allocating C strings, casting a cgo.Handle to void*, and
// calling through kernel_log_cb() — so a test can assert the full reverse-call
// round-trip (handle unwrap + recover firewall + sink dispatch) without a live
// kernel event. Not used in production.
func invokeLogTrampolineForTest(h cgo.Handle, level int, target, message string) {
	ct := C.CString(target)
	defer C.free(unsafe.Pointer(ct))
	cm := C.CString(message)
	defer C.free(unsafe.Pointer(cm))
	ctx := C.kernel_handle_to_ctx(C.uintptr_t(h))
	C.kernel_invoke_trampoline_for_test(ctx, C.int(level), ct, cm)
}

//export kernelLogTrampoline
func kernelLogTrampoline(ctx unsafe.Pointer, level C.int, target, message *C.char) {
	// Panic firewall: a Go panic unwinding across the cgo boundary aborts the
	// process. Convert any panic in the sink routing into a dropped log line.
	defer func() { _ = recover() }()
	if ctx == nil {
		return
	}
	sink, ok := cgo.Handle(uintptr(ctx)).Value().(*logSink)
	if !ok || sink == nil {
		return
	}
	// Copy the borrowed C strings out immediately — they are valid only for the
	// duration of this call.
	sink.forward(int(level), C.GoString(target), C.GoString(message))
}

// installKernelLogCallback registers the trampoline as the kernel's log sink,
// once per process, wrapping a *logSink in a cgo.Handle passed as the opaque ctx.
// The kernel filters forwarded events at `level` (an OFF/ERROR/WARN/INFO/DEBUG/
// TRACE string, or "" → NULL to defer to RUST_LOG), which the caller maps from
// the driver's own log level so kernel Rust lines follow the one
// DATABRICKS_LOG_LEVEL knob — the same string kernel_init_logging would have
// received (see initKernelLogging).
//
// Best-effort: a non-Success status (e.g. Internal because a global tracing
// subscriber was already installed) is logged and ignored — kernel logs simply
// won't flow through the callback, which is a documented, benign outcome. Returns
// nothing; the caller does not gate connect on it.
func installKernelLogCallback(level string) {
	logCallbackOnce.Do(func() {
		// Snapshot the driver logger at install (TraceLevel, immutable) so the
		// kernel drain thread never re-gates already-approved events and never
		// reads the mutable global logger.Logger (see logSink.log).
		logCallbackHandle = cgo.NewHandle(newLogSink())
		// Pass the handle (an integer token) as the opaque ctx via a C-side cast,
		// so Go doesn't do unsafe.Pointer(uintptr) directly.
		ctx := C.kernel_handle_to_ctx(C.uintptr_t(logCallbackHandle))
		// An empty level maps to a NULL char* (kernel defers to RUST_LOG); a
		// non-empty level is passed as a C string the kernel parses with the same
		// precedence as kernel_init_logging (explicit wins > RUST_LOG > warn).
		clevel := newCStrOrNull(level)
		defer clevel.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_set_log_callback(C.kernel_log_cb(), ctx, clevel.c)
		}); err != nil {
			// On a non-Success return the kernel installed no subscriber and did
			// NOT retain ctx (it stores the sink only on the success path), so the
			// process-lifetime pin at logCallbackHandle would be a true leak here —
			// free it. (On success we deliberately never Delete: the kernel holds
			// the handle value as its opaque ctx for the process lifetime.)
			logCallbackHandle.Delete()
			logCallbackHandle = 0
			// Surface at Warn (visible at the default level), NOT klog: klog is
			// Debug-gated and no-ops at the default, and this install runs once so
			// a later level change can't re-surface it. The message names the
			// benign common cause (a global subscriber already installed) so
			// on-call doesn't mistake a logging no-op for a connect failure.
			logger.Logger.Warn().Msgf(
				"databricks: kernel_set_log_callback: %v (kernel logs not forwarded; proceeding)", err)
		}
	})
}
