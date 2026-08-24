//go:build databricks_kernel_dynamic

package kernel

import (
	"runtime"

	"github.com/apache/arrow/go/v12/arrow"
)

// setArrayDataFinalizer attaches a finalizer to the imported ArrayData that
// releases the C-side ArrowArray (via its release callback) when the Go GC
// reclaims the data. This is the pure-Go analogue of the cgo importer's
// runtime.SetFinalizer(imp.data, ...) that calls ArrowArrayRelease + free.
//
// releaseCArray is idempotent (it clears the release pointer after firing), so
// this backstop never double-frees if the buffers were already released. The
// kernel exports self-contained batches, so releasing here touches nothing
// session-scoped — safe even if the finalizer runs after the session closes.
//
// runtime.SetFinalizer requires a pointer to an object the GC tracks; arrow-go
// implements ArrayData as *array.Data, so the interface value is a pointer and
// SetFinalizer accepts it directly.
func setArrayDataFinalizer(data arrow.ArrayData, arr *cArrowArray) {
	runtime.SetFinalizer(data, func(arrow.ArrayData) {
		releaseCArray(arr)
	})
}
