//go:build databricks_kernel_dynamic

// Package-level PoC: a PURE-GO (CGO_ENABLED=0) loader for the Databricks SQL
// kernel's C ABI, using ebitengine/purego to dlopen the kernel SHARED library
// (.so/.dylib/.dll) at run time instead of static-linking a .a at build time
// through cgo.
//
// Why this exists (see the driver's SEA/kernel release design). The shipped cgo
// backend (cgo.go + siblings, //go:build cgo && databricks_kernel) links a
// static libdatabricks_sql_kernel.a at build time. That forces CGO_ENABLED=1, a
// C toolchain on every builder, and it breaks Go's free cross-compilation — the
// three things that block SEA from ever becoming the default backend. This file
// proves the alternative: load the kernel as a shared library at run time with
// NO cgo, so the Go side keeps CGO_ENABLED=0 and cross-compiles freely. It is
// the model gosnowflake uses for its own closed-source native core.
//
// Scope of this PoC (deliberately narrow, so it is reviewable):
//   - CONTROL plane only: dlopen -> config -> session open -> execute ->
//     query-id / affected-rows -> teardown. This is the whole happy path for
//     DML/DDL and any non-result statement.
//   - The DATA plane (Arrow result batches) is NOT here. arrow-go/v12's cdata
//     package — the zero-copy C-Data importer the cgo rows.go uses — is itself a
//     cgo package (every non-test file does `import "C"`), so a fully
//     CGO_ENABLED=0 result-fetch path needs a separate decision (a purego-based
//     C-Data import, or arrow-go v18). Called out as the documented follow-up;
//     see dynamic_loader_test.go and the PR description.
//
// Build/run this PoC (nothing static, no C compiler):
//
//	CGO_ENABLED=0 go build -tags databricks_kernel_dynamic ./internal/backend/kernel/
//	DBX_KERNEL_DYLIB=/abs/path/to/libdatabricks_sql_kernel.dylib \
//	  CGO_ENABLED=0 go test -tags databricks_kernel_dynamic \
//	  -run TestDynamicLoaderControlPlane ./internal/backend/kernel/ -v
//
// Memory model, mirrored from the cgo path:
//   - Strings handed to the kernel are copied into C memory for the call and
//     freed right after; the kernel copies them into owned Rust memory on
//     receipt, so freeing immediately is safe (same contract as cgo cStr).
//   - Every fallible call is wrapped so the kernel's thread-local last error is
//     read on the SAME OS thread (runtime.LockOSThread), closing the same
//     goroutine-migration window the cgo `call` helper documents.
package kernel

import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/ebitengine/purego"
)

// kernelStatusSuccess is KernelStatusCode_Success (0). The full enum lives in
// errors_classify.go as plain ints; this loader only needs the success sentinel
// plus the classifier those constants feed.
const kernelStatusSuccess = 0

// dynLib holds the dlopen handle plus the kernel C ABI functions bound as Go
// func values. Only the control-plane subset needed for the PoC is bound.
//
// purego.RegisterLibFunc maps a Go signature onto a C symbol. The mapping rules
// used here: C pointer/opaque-handle types become uintptr; `const char*`
// becomes a Go string on the ARGUMENT side (purego marshals it to a C string
// for the duration of the call); KernelStatusCode (an int enum) becomes int32.
type dynLib struct {
	handle uintptr

	// Lifecycle + config (KernelStatusCode kernel_*(...))
	// initLogging's file_path is a uintptr, not a string, so the caller can pass
	// 0 (a real C NULL → log to stderr). purego marshals a Go "" to a non-null
	// empty C string, which the kernel would treat as a filename to open — this
	// is the same NULL-vs-empty distinction the cgo path handles with
	// newCStrOrNull.
	initLogging      func(level string, filePath uintptr) int32
	configNew        func(out *uintptr) int32
	configFree       func(config uintptr)
	configSetHTTPath func(config uintptr, host, httpPath string) int32
	configSetWH      func(config uintptr, host, warehouseID string) int32
	configSetAuthPAT func(config uintptr, token string) int32
	sessionOpen      func(config uintptr, out *uintptr) int32
	sessionClose     func(session uintptr) int32
	newStatement     func(session uintptr, out *uintptr) int32
	setSQL           func(stmt uintptr, sql string) int32
	execute          func(stmt uintptr, out *uintptr) int32
	statementClose   func(stmt uintptr) int32

	// Executed-statement result metadata (control plane).
	execQueryID func(executed uintptr) uintptr // returns const char* (0 if none)
	execNumRows func(executed uintptr) int64
	execClose   func(executed uintptr) int32

	// Result stream (data plane): pull Arrow C-Data batches.
	getResultStream func(executed uintptr, out *uintptr) int32
	streamGetSchema func(stream uintptr, out *cArrowSchema) int32
	streamNextBatch func(stream uintptr, outArray *cArrowArray, outSchema *cArrowSchema) int32
	streamClose     func(stream uintptr) int32

	// Error surface: KernelError is read back through an out-param struct.
	getLastError func(out *cKernelError) bool
}

// cKernelError mirrors the C `KernelError` struct byte-for-byte (64-bit ABI) so
// purego can fill it via an out-pointer. The string fields are C `char*`
// (uintptr here), valid only until the next FFI call on this thread — copied
// out immediately in readLastError, exactly as the cgo lastError does.
//
// Layout matches databricks_kernel.h exactly (offsets are for 64-bit, 8-byte
// pointer alignment):
//
//	int32_t     code;         // 0
//	                          // 4  (pad: next field is an 8-byte pointer)
//	const char* message;      // 8
//	const char* sql_state;    // 16
//	int32_t     vendor_code;  // 24
//	uint16_t    http_status;  // 28
//	bool        retryable;    // 30
//	                          // 31 (pad: next field is an 8-byte pointer)
//	const char* query_id;     // 32
//	                          // total size 40
//
// The order here is code, message, sql_state, vendor_code, http_status,
// retryable, query_id — NOT grouped by type. A drift from the header would
// misread the struct; the cgo path guards its enum with compile-time asserts,
// and the real PR would add an unsafe.Sizeof/Offsetof layout check here.
type cKernelError struct {
	code       int32
	_          [4]byte // pad to 8-align message
	message    uintptr // const char*
	sqlState   uintptr // const char*
	vendorCode int32
	httpStatus uint16
	retryable  bool
	_          [1]byte // pad to 8-align queryID
	queryID    uintptr // const char*
}

// openDynLib dlopens the kernel shared library and binds the control-plane ABI.
// path is an absolute path to libdatabricks_sql_kernel.{so,dylib,dll}. In a
// real build this would be resolved next to the executable (rpath) or from a
// documented env var; the PoC takes it explicitly.
func openDynLib(path string) (*dynLib, error) {
	h, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, fmt.Errorf("databricks: kernel dlopen(%q): %w", path, err)
	}
	l := &dynLib{handle: h}
	// RegisterLibFunc panics if a symbol is missing, which is what we want at
	// load time — a missing symbol means an ABI/rev mismatch, a hard failure.
	purego.RegisterLibFunc(&l.initLogging, h, "kernel_init_logging")
	purego.RegisterLibFunc(&l.configNew, h, "kernel_session_config_new")
	purego.RegisterLibFunc(&l.configFree, h, "kernel_session_config_free")
	purego.RegisterLibFunc(&l.configSetHTTPath, h, "kernel_session_config_set_http_path")
	purego.RegisterLibFunc(&l.configSetWH, h, "kernel_session_config_set_warehouse")
	purego.RegisterLibFunc(&l.configSetAuthPAT, h, "kernel_session_config_set_auth_pat")
	purego.RegisterLibFunc(&l.sessionOpen, h, "kernel_session_open")
	purego.RegisterLibFunc(&l.sessionClose, h, "kernel_session_close")
	purego.RegisterLibFunc(&l.newStatement, h, "kernel_session_new_statement")
	purego.RegisterLibFunc(&l.setSQL, h, "kernel_statement_set_sql")
	purego.RegisterLibFunc(&l.execute, h, "kernel_statement_execute")
	purego.RegisterLibFunc(&l.statementClose, h, "kernel_statement_close")
	purego.RegisterLibFunc(&l.execQueryID, h, "kernel_executed_statement_query_id")
	purego.RegisterLibFunc(&l.execNumRows, h, "kernel_executed_statement_num_modified_rows")
	purego.RegisterLibFunc(&l.execClose, h, "kernel_executed_statement_close")
	purego.RegisterLibFunc(&l.getResultStream, h, "kernel_executed_statement_get_result_stream")
	purego.RegisterLibFunc(&l.streamGetSchema, h, "kernel_result_stream_get_schema")
	purego.RegisterLibFunc(&l.streamNextBatch, h, "kernel_result_stream_next_batch")
	purego.RegisterLibFunc(&l.streamClose, h, "kernel_result_stream_close")
	purego.RegisterLibFunc(&l.getLastError, h, "kernel_get_last_error")
	return l, nil
}

// callDyn runs a fallible kernel entry point on a pinned OS thread and, on a
// non-Success status, reads the kernel's thread-local last error. This is the
// purego twin of cgo.go's `call`: the LockOSThread pin is what makes the
// separate get_last_error read observe the right thread's buffer.
func (l *dynLib) callDyn(fn func() int32) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	st := fn()
	if st == kernelStatusSuccess {
		return nil
	}
	return l.readLastError(int(st))
}

// readLastError copies the kernel's thread-local last error into a Go
// *KernelError. Must run on the same OS thread as the failing call (callDyn
// guarantees it). String fields are copied out of C memory immediately because
// they are invalidated by the next FFI call.
func (l *dynLib) readLastError(code int) *KernelError {
	var e cKernelError
	if !l.getLastError(&e) {
		return &KernelError{Code: code, Message: fmt.Sprintf("kernel status %d (no detail)", code)}
	}
	ke := &KernelError{
		Code:       int(e.code),
		Message:    goStringFromC(e.message),
		VendorCode: e.vendorCode,
		HTTPStatus: e.httpStatus,
		Retryable:  e.retryable,
		SQLState:   goStringFromC(e.sqlState),
		QueryID:    goStringFromC(e.queryID),
	}
	return ke
}

// goStringFromC copies a NUL-terminated C string at the given address into a Go
// string. A 0 address yields "". This is the CGO_ENABLED=0 stand-in for
// C.GoString.
//
// It walks bytes off an unsafe.Pointer base with unsafe.Add (the vet-approved
// idiom — arithmetic stays on unsafe.Pointer, never on a bare uintptr, so the
// GC's pointer accounting is never fooled), finds the NUL, then copies out. The
// copy is deliberate: the source bytes live in kernel/C memory and are only
// valid until the next FFI call, so the returned string must not alias them.
func goStringFromC(p uintptr) string {
	if p == 0 {
		return ""
	}
	// p is a C address returned across the FFI boundary, not a Go pointer, so
	// this uintptr->unsafe.Pointer conversion is the documented-safe FFI case
	// (go/analysis flags it heuristically; purego relies on the same pattern).
	base := unsafe.Pointer(p) //nolint:govet // FFI C pointer, not GC-managed

	var n int
	for *(*byte)(unsafe.Add(base, n)) != 0 {
		n++
	}
	if n == 0 {
		return ""
	}
	return string(unsafe.Slice((*byte)(base), n))
}
