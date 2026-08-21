//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"

// cgo generates this declaration with mutable char pointers; the kernel lends
// read-only strings, so cast once in the C adapter to match KernelLogCallback.
void kernelLogTrampoline(char* level, char* target, char* message, void* user_data);
static KernelLogCallback kernel_log_cb(void) {
    return (KernelLogCallback)kernelLogTrampoline;
}
static void* kernel_handle_to_ctx(uintptr_t h) { return (void*)h; }
static void kernel_invoke_log_trampoline_for_test(
    char* level, char* target, char* message, void* user_data) {
    kernelLogTrampoline(level, target, message, user_data);
}
*/
import "C"

import (
	"runtime/cgo"
	"sync"
	"unsafe"

	"github.com/databricks/databricks-sql-go/logger"
)

var (
	logCallbackOnce   sync.Once
	logCallbackHandle cgo.Handle
)

//export kernelLogTrampoline
func kernelLogTrampoline(level, target, message *C.char, userData unsafe.Pointer) {
	// A panic must not cross the C ABI. Treat a malformed handle or sink failure
	// as a dropped diagnostic; logging must never fail a query.
	defer func() { _ = recover() }()
	if userData == nil {
		return
	}
	sink, ok := cgo.Handle(uintptr(userData)).Value().(*logSink)
	if !ok || sink == nil {
		return
	}
	sink.forward(C.GoString(level), C.GoString(target), C.GoString(message))
}

func invokeLogTrampolineForTest(h cgo.Handle, level, target, message string) {
	clevel := C.CString(level)
	defer C.free(unsafe.Pointer(clevel))
	ctarget := C.CString(target)
	defer C.free(unsafe.Pointer(ctarget))
	cmessage := C.CString(message)
	defer C.free(unsafe.Pointer(cmessage))
	ctx := C.kernel_handle_to_ctx(C.uintptr_t(h))
	C.kernel_invoke_log_trampoline_for_test(clevel, ctarget, cmessage, ctx)
}

func installKernelLogCallback(level string, useNULL bool) {
	logCallbackOnce.Do(func() {
		// OFF intentionally installs no subscriber and retains no callback state.
		if !useNULL && level == "OFF" {
			return
		}

		logCallbackHandle = cgo.NewHandle(newLogSink())
		ctx := C.kernel_handle_to_ctx(C.uintptr_t(logCallbackHandle))
		var clevel cStr
		if !useNULL {
			clevel = newCStr(level)
			defer clevel.free()
		}
		if err := call(func() C.KernelStatusCode {
			return C.kernel_init_logging_callback(clevel.c, C.kernel_log_cb(), ctx)
		}); err != nil {
			logCallbackHandle.Delete()
			logCallbackHandle = 0
			logger.Logger.Warn().Msgf(
				"databricks: kernel_init_logging_callback: %v (kernel logs not forwarded; proceeding)", err)
		}
	})
}
