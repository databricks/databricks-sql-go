//go:build cgo && databricks_kernel

// Package kernel implements backend.Backend over the Databricks SQL kernel's C
// ABI (databricks-sql-kernel, src/c_abi). It is build-tag-gated behind
// `databricks_kernel` so the default driver build stays pure-Go (CGO_ENABLED=0,
// go-gettable, cross-compilable); only a build that opts in with
// `-tags databricks_kernel` and CGO_ENABLED=1 links the kernel static lib.
//
// This file holds the cgo plumbing: the C ABI include/link directives, the
// fallible-call helper that makes the kernel's thread-local last error readable,
// the error mapping to the driver's error surface, and the gated step logger.
// The backend, operation, and rows layers live in sibling files.
//
// Link contract (${SRCDIR}-relative, machine-independent). The header is
// included from ${SRCDIR}/include and the static lib is linked from
// ${SRCDIR}/lib/<os>_<arch>; the per-platform link flags live in the
// cgo_<os>.go files beside this one. Both directories are produced by the
// build step (`make kernel-lib`), which checks out the kernel at the commit
// pinned in the repo-root KERNEL_REV file and `cargo build`s a static lib —
// so the kernel revision is a reviewable pin, never baked into a #cgo line
// (those expand only ${SRCDIR} and cannot run git or read env). The dirs are
// .gitignore'd; nothing kernel-built is committed. For local development
// against an existing checkout, `make kernel-lib KERNEL_LOCAL_A=<path/to>.a
// KERNEL_LOCAL_HEADER=<path/to>databricks_kernel.h` copies those in instead of
// building. The eventual release path downloads a published .a at the pinned
// rev rather than building it (see the driver's distribution design).
package kernel

/*
#cgo CFLAGS: -I${SRCDIR}/include
#include <stdlib.h>
#include "databricks_kernel.h"
*/
import "C"

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/databricks/databricks-sql-go/logger"
)

// ─── Debug logging ───────────────────────────────────────────────────────────
//
// Gated on DBSQL_KERNEL_DEBUG so it is OFF by default and, in particular, OFF
// during benchmarks (debug logging perturbs latency). Every binding step logs
// through klog when enabled — entry, status codes, handle addresses, batch
// counts — which is what makes a failing e2e cheap to diagnose. The same flag
// also installs the kernel's own Rust (tracing) logs via initKernelLogging, so
// binding and kernel logs interleave on stderr as one stream.
var kdebug = os.Getenv("DBSQL_KERNEL_DEBUG") != ""

// Deferred (tracked): klog writes raw to stderr, NOT through the driver's
// logger.Logger, so its lines carry no connId/corrId/queryId and can't be
// correlated in a multi-conn process; it's also gated on its own
// DBSQL_KERNEL_DEBUG rather than the driver's DATABRICKS_LOG_LEVEL knob. Unifying
// kernel logging onto logger.Logger (which no-ops below its level, so no
// benchmark cost) is a tracked logging-unification follow-up, kept separate from
// this PR because it changes the debug-logging surface.
func klog(format string, args ...any) {
	if !kdebug {
		return
	}
	fmt.Fprintf(os.Stderr, "[kernel] "+format+"\n", args...)
}

// KernelDebugEnabled reports whether binding-level debug logging is on (i.e.
// DBSQL_KERNEL_DEBUG is set). Exposed so a benchmark can assert the flag is off
// before measuring — debug logging perturbs latency; there is no such benchmark
// in-tree yet, so this currently has no callers.
func KernelDebugEnabled() bool { return kdebug }

// initLoggingOnce guards kernel_init_logging, which is process-wide and
// first-call-wins in the kernel. We install the kernel subscriber lazily on the
// first session open rather than in init(), so a process that never opens a
// kernel session installs nothing.
var initLoggingOnce sync.Once

// initKernelLogging turns on the kernel's own Rust (tracing) logs, gated on the
// same DBSQL_KERNEL_DEBUG flag as klog so both are OFF by default and, in
// particular, OFF during benchmarks — the subscriber is never installed in a
// benchmark run. level=NULL lets the kernel honor RUST_LOG (default warn);
// file_path=NULL sends kernel logs to stderr so they interleave with klog on one
// stream. Best-effort: Internal (e.g. the host already installed a global
// subscriber) is a documented, benign outcome — logged, never fatal to connect.
//
// Scope caveat: the kernel subscriber is PROCESS-WIDE, first-call-wins, and never
// uninstalled. Because kdebug is read once from DBSQL_KERNEL_DEBUG at package
// init, the switch is process-global, not per-connection: in a long-lived
// multi-tenant process, the first kernel session opened with the flag set
// installs the subscriber (and its stderr output) for ALL subsequent kernel
// sessions in that process, and there is no way to scope it to one connection or
// turn it off afterward. The "off during benchmarks" guarantee therefore depends
// on DBSQL_KERNEL_DEBUG being unset before package init. Making this a
// per-process runtime knob (rather than an init-time env read) is tracked with
// the logging-unification follow-up.
func initKernelLogging() {
	if !kdebug {
		return
	}
	initLoggingOnce.Do(func() {
		if err := call(func() C.KernelStatusCode {
			return C.kernel_init_logging(nil, nil)
		}); err != nil {
			klog("kernel_init_logging: %v (kernel logs unavailable; proceeding)", err)
		}
	})
}

// call runs a fallible kernel entry point and, on a non-Success status, reads
// the kernel's thread-local last error into a Go error.
//
// The kernel reports rich errors via a thread-local buffer read by a *second*
// call (kernel_get_last_error). Go's M:N scheduler can move a goroutine to a
// different OS thread between two cgo calls, so a naive call-then-read pair can
// observe the wrong thread's buffer. LockOSThread pins the goroutine to its OS
// thread across the call and its error read, closing that window.
func call(fn func() C.KernelStatusCode) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	st := fn()
	if st == C.KernelStatusCode_Success {
		return nil
	}
	return lastError(st)
}

// fireCancel dispatches a server-side cancel and reports whether the RPC was
// actually sent. The kernel returns dispatched=false while no server statement
// id has been observed yet (the window before the execute POST returns), and
// true once the cancel RPC goes out — which is the watcher's cue to stop
// re-firing.
func fireCancel(canceller *C.kernel_statement_canceller_t) bool {
	var dispatched C.bool
	C.kernel_statement_canceller_cancel(canceller, &dispatched)
	return bool(dispatched)
}

// lastError reads the kernel's thread-local last error and copies its string
// fields out immediately — the C `char*` fields are valid only until the next
// FFI call on this thread. Must run on the same OS thread as the failing call;
// call guarantees that via LockOSThread.
func lastError(code C.KernelStatusCode) *KernelError {
	var e C.KernelError
	if !bool(C.kernel_get_last_error(&e)) {
		return &KernelError{Code: int(code), Message: fmt.Sprintf("kernel status %d (no detail)", int(code))}
	}
	ke := &KernelError{
		Code:       int(e.code),
		Message:    C.GoString(e.message),
		VendorCode: int32(e.vendor_code),
		HTTPStatus: uint16(e.http_status),
		Retryable:  bool(e.retryable),
	}
	if e.sql_state != nil {
		ke.SQLState = C.GoString(e.sql_state)
	}
	if e.query_id != nil {
		ke.QueryID = C.GoString(e.query_id)
	}
	klog("kernel error: code=%d sqlstate=%q vendor=%d http=%d retryable=%v msg=%q",
		ke.Code, ke.SQLState, ke.VendorCode, ke.HTTPStatus, ke.Retryable, ke.Message)
	// Also emit through the driver's logger — no SQL text or PII, just the
	// status/sqlstate/http fields plus the server query id (a correlation token,
	// not PII) so on-call can pivot to server-side query history — so a kernel-path
	// failure is visible without DBSQL_KERNEL_DEBUG. This is the error path only
	// (never the hot per-row/per-batch path), so it does not perturb benchmarks.
	// User/query faults (bad SQL, bad argument) are routine and log at Debug so they
	// don't inflate the WARN rate on-call alerts key on; infra codes stay at Warn.
	msg := "databricks: kernel call failed: code=%d sqlstate=%q vendor=%d http=%d retryable=%v queryId=%q"
	if isUserFault(ke.Code) {
		logger.Logger.Debug().Msgf(msg, ke.Code, ke.SQLState, ke.VendorCode, ke.HTTPStatus, ke.Retryable, ke.QueryID)
	} else {
		logger.Logger.Warn().Msgf(msg, ke.Code, ke.SQLState, ke.VendorCode, ke.HTTPStatus, ke.Retryable, ke.QueryID)
	}
	return ke
}

// The plain-int status constants used by the untagged classifier logic
// (errors_classify.go) must stay in lockstep with the C enum in
// databricks_kernel.h. These compile-time assertions make a drift a build error
// under -tags databricks_kernel. Each converts BOTH directions of the difference
// to uint: if the Go constant and the C value disagree, one of a-b / b-a is a
// negative constant, and `uint(<negative constant>)` is a hard compile error
// ("constant overflows uint") — so the file won't compile whether the header
// renumbers a code up OR down. (A one-sided `[a-b]struct{}` array size only
// catches C > Go: a downward renumber makes a-b positive, a legal array, and the
// drift slips through.)
const (
	_ = uint(statusInvalidArgument-int(C.KernelStatusCode_InvalidArgument)) | uint(int(C.KernelStatusCode_InvalidArgument)-statusInvalidArgument)
	_ = uint(statusUnauthenticated-int(C.KernelStatusCode_Unauthenticated)) | uint(int(C.KernelStatusCode_Unauthenticated)-statusUnauthenticated)
	_ = uint(statusUnavailable-int(C.KernelStatusCode_Unavailable)) | uint(int(C.KernelStatusCode_Unavailable)-statusUnavailable)
	_ = uint(statusTimeout-int(C.KernelStatusCode_Timeout)) | uint(int(C.KernelStatusCode_Timeout)-statusTimeout)
	_ = uint(statusNetworkError-int(C.KernelStatusCode_NetworkError)) | uint(int(C.KernelStatusCode_NetworkError)-statusNetworkError)
	_ = uint(statusSqlError-int(C.KernelStatusCode_SqlError)) | uint(int(C.KernelStatusCode_SqlError)-statusSqlError)
)

// cStr wraps C.CString with a guaranteed free. The kernel copies strings into
// owned Rust memory on receipt, so freeing immediately after the call is safe.
// Use: cs := newCStr(s); defer cs.free(); ...C.fn(cs.c)...
type cStr struct{ c *C.char }

func newCStr(s string) cStr { return cStr{c: C.CString(s)} }

// newCStrOrNull is like newCStr but yields a NULL C pointer for an empty string,
// for kernel args whose "unset" sentinel is NULL (e.g. the optional U2M client id /
// scopes, where NULL selects the kernel's own default). C.CString("") would instead
// pass a non-NULL pointer to an empty string, which the kernel treats as a real
// (empty) value rather than "use the default".
func newCStrOrNull(s string) cStr {
	if s == "" {
		return cStr{c: nil}
	}
	return cStr{c: C.CString(s)}
}

func (s cStr) free() {
	if s.c != nil {
		C.free(unsafe.Pointer(s.c))
	}
}

// cBytes wraps C.CBytes with a guaranteed free, for the byte-buffer setters (e.g.
// a PEM CA bundle) that take a (*C.uint8_t, C.size_t) pair. The kernel copies the
// bytes into owned Rust memory on receipt, so freeing right after the call is
// safe. An empty slice yields a NULL pointer + 0 length (the setters reject that,
// which is what we want — an empty buffer is never valid).
// Use: cb := newCBytes(b); defer cb.free(); ...C.fn(cb.ptr, cb.len)...
type cBytes struct {
	ptr *C.uint8_t
	len C.size_t
}

func newCBytes(b []byte) cBytes {
	if len(b) == 0 {
		return cBytes{}
	}
	return cBytes{ptr: (*C.uint8_t)(C.CBytes(b)), len: C.size_t(len(b))}
}

func (b cBytes) free() {
	if b.ptr != nil {
		C.free(unsafe.Pointer(b.ptr))
	}
}
