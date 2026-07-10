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
// The directives below name the kernel library to link but carry no search
// paths, so the header and static lib locations are supplied at build time via
// the standard CGO_CFLAGS / CGO_LDFLAGS environment variables, e.g.:
//
//	CGO_CFLAGS="-I<kernel>/include" \
//	CGO_LDFLAGS="-L<kernel>/target/release -Wl,-rpath,<kernel>/target/release" \
//	go build -tags databricks_kernel ./...
//
// A shippable build instead links a committed per-platform prebuilt static lib
// via a ${SRCDIR}-relative path (the go-duckdb duckdb-go-bindings model); wiring
// that + a tagged CI job is a distribution follow-up.
package kernel

/*
#cgo LDFLAGS: -ldatabricks_sql_kernel -ldl -lm
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

	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
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

func klog(format string, args ...any) {
	if !kdebug {
		return
	}
	fmt.Fprintf(os.Stderr, "[kernel] "+format+"\n", args...)
}

// KernelDebugEnabled reports whether binding-level debug logging is on. Tests
// assert the flag wiring; benchmarks assert it is false before measuring.
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
	return ke
}

// KernelError is the Go-side structured error mapped from the kernel's
// KernelError struct. It carries the sqlstate so the backend's ExecutionError
// can attach it, matching the Thrift error surface.
type KernelError struct {
	Code       int
	Message    string
	SQLState   string
	VendorCode int32
	HTTPStatus uint16
	Retryable  bool
	QueryID    string
}

func (e *KernelError) Error() string {
	if e.SQLState != "" {
		return fmt.Sprintf("kernel: %s (sqlstate=%s, code=%d)", e.Message, e.SQLState, e.Code)
	}
	return fmt.Sprintf("kernel: %s (code=%d)", e.Message, e.Code)
}

// Status codes mirrored as Go ints so non-cgo code (tests, error mapping) can
// reference them without the C import. Kept in lockstep with the C enum.
const (
	statusInvalidArgument = int(C.KernelStatusCode_InvalidArgument)
	statusUnauthenticated = int(C.KernelStatusCode_Unauthenticated)
	statusUnavailable     = int(C.KernelStatusCode_Unavailable)
	statusTimeout         = int(C.KernelStatusCode_Timeout)
	statusNetworkError    = int(C.KernelStatusCode_NetworkError)
	statusSqlError        = int(C.KernelStatusCode_SqlError)
)

// isBadConnection reports whether a status code means the session is no longer
// usable, so the driver evicts the conn from the pool (via driver.ErrBadConn's
// surface) rather than reusing it.
func isBadConnection(code int) bool {
	switch code {
	case statusUnauthenticated, statusUnavailable, statusNetworkError:
		return true
	default:
		return false
	}
}

// toDriverError classifies a kernel error for the pool: a status that means the
// session is unusable is wrapped as a bad-connection error (which identifies as
// driver.ErrBadConn, so database/sql evicts the conn), matching how the Thrift
// backend surfaces connection loss. Other kernel errors — and plain
// (non-KernelError) errors — are returned unchanged, carrying their sqlstate.
func toDriverError(err error) error {
	if err == nil {
		return nil
	}
	ke, ok := err.(*KernelError)
	if !ok {
		return err
	}
	if isBadConnection(ke.Code) {
		return dbsqlerrint.NewBadConnectionError(ke)
	}
	return ke
}

// cStr wraps C.CString with a guaranteed free. The kernel copies strings into
// owned Rust memory on receipt, so freeing immediately after the call is safe.
// Use: cs := newCStr(s); defer cs.free(); ...C.fn(cs.c)...
type cStr struct{ c *C.char }

func newCStr(s string) cStr { return cStr{c: C.CString(s)} }

func (s cStr) free() {
	if s.c != nil {
		C.free(unsafe.Pointer(s.c))
	}
}
