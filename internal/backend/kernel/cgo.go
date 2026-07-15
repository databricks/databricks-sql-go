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
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// ─── Debug logging ───────────────────────────────────────────────────────────
//
// Binding-level debug lines (entry, status codes, handle addresses, batch counts)
// go through the driver's shared logger.Logger at Debug level, so the SAME knob
// that controls the rest of the driver — DATABRICKS_LOG_LEVEL / dbsql.SetLogLevel
// — turns kernel binding logs on and off, and each line carries the structured
// connId/corrId/queryId fields (via klogCtx) that let it be correlated in a
// multi-conn process. zerolog no-ops a Debug() event when the level is above debug,
// so this stays zero-work (no allocation, no formatting) at the default Warn level
// — including during benchmarks, which run at the default level.
//
// DBSQL_KERNEL_DEBUG is retained only as an advanced override for the *kernel's own
// Rust logs* (see initKernelLogging): it forces the kernel subscriber on and lets
// RUST_LOG tune the kernel's verbosity independently. It no longer gates the Go
// binding lines — those follow the driver log level.

// kernelDebugOff reports whether the driver log level is above Debug, i.e. kernel
// binding lines would be discarded. This is the cheap front gate for klog/klogCtx:
// GetLevel() is a plain field read, whereas building the event (and, for klogCtx,
// logger.WithContext → zerolog's With(), which eagerly make([]byte, 0, 500)s and
// formats the correlation keys) allocates BEFORE .Debug() consults the level. So we
// must short-circuit here to keep the hot path (per-batch nextBatch) allocation-free
// at the default Warn level — including during benchmarks. zerolog's own .Debug()
// no-op is not enough because the argument expressions run first.
func kernelDebugOff() bool { return logger.Logger.GetLevel() > zerolog.DebugLevel }

// klog emits a binding-level debug line with no request context. Prefer klogCtx
// where a ctx is in scope so the line carries connId/corrId/queryId. Gated by the
// driver log level (Debug): a no-op — no formatting, no allocation — at the default.
func klog(format string, args ...any) {
	if kernelDebugOff() {
		return
	}
	logger.Logger.Debug().Msgf("[kernel] "+format, args...)
}

// klogCtx is klog with request correlation: it pulls connId/corrId/queryId off ctx
// (the conn layer stuffs them in before calling the backend, exactly as the Thrift
// path does) so a kernel binding line can be joined to the rest of a request's logs
// in a multi-conn process. A nil ctx degrades to klog. Level-gated up front so the
// logger.WithContext allocation never happens below Debug (see kernelDebugOff).
func klogCtx(ctx context.Context, format string, args ...any) {
	if kernelDebugOff() {
		return
	}
	if ctx == nil {
		logger.Logger.Debug().Msgf("[kernel] "+format, args...)
		return
	}
	logger.WithContext(
		driverctx.ConnIdFromContext(ctx),
		driverctx.CorrelationIdFromContext(ctx),
		driverctx.QueryIdFromContext(ctx),
	).Debug().Msgf("[kernel] "+format, args...)
}

// initLoggingOnce guards kernel_init_logging, which is process-wide and
// first-call-wins in the kernel. We install the kernel subscriber lazily on the
// first session open rather than in init(), so a process that never opens a
// kernel session installs nothing.
var initLoggingOnce sync.Once

// initKernelLogging turns on the kernel's own Rust (tracing) logs and points their
// verbosity at the driver's log level, so DATABRICKS_LOG_LEVEL drives both the Go
// binding lines and the kernel's Rust lines from one knob. The mapped level is
// passed to kernel_init_logging (Go zerolog level → the kernel's OFF/ERROR/WARN/
// INFO/DEBUG/TRACE string); DBSQL_KERNEL_DEBUG forces the subscriber on with a
// NULL level so the kernel honors RUST_LOG instead (the advanced override for
// tuning kernel-only verbosity). file_path=NULL sends kernel logs to stderr — the
// kernel ABI has no sink hook, so the Rust lines always go to stderr and are NOT
// routed through logger.SetLogOutput (unlike the Go binding lines).
//
// Best-effort: an Internal return (e.g. the host already installed a global
// subscriber) is a documented, benign outcome — logged at Warn, never fatal to
// connect. The subscriber installs at whatever level is mapped in; a driver left at
// the default Warn level (benchmarks included, and never having set
// DBSQL_KERNEL_DEBUG) installs it at WARN, so the kernel emits nothing below Warn
// and there is no hot-path cost.
//
// Scope caveat: the kernel subscriber is PROCESS-WIDE, first-call-wins, and never
// uninstalled — in a long-lived multi-tenant process the first kernel session's
// level/destination applies to ALL subsequent kernel sessions, with no way to
// re-scope or turn it off afterward. That is a kernel-ABI property, not a Go one.
// A direct consequence: the driver level is sampled HERE, once, at the first kernel
// session — a later dbsql.SetLogLevel re-levels the Go binding lines (klog/klogCtx
// re-read GetLevel per call) but NOT the already-installed Rust subscriber. Set the
// level before opening the first kernel connection to govern the Rust logs.
func initKernelLogging() {
	initLoggingOnce.Do(func() {
		// DBSQL_KERNEL_DEBUG override: force the subscriber on and let RUST_LOG tune
		// it (level=NULL). Otherwise map the driver's current log level in, so the
		// one DATABRICKS_LOG_LEVEL knob governs kernel verbosity too.
		var level cStr
		if os.Getenv("DBSQL_KERNEL_DEBUG") != "" {
			level = cStr{c: nil} // NULL → kernel honors RUST_LOG
		} else {
			level = newCStr(kernelLogLevel(logger.Logger.GetLevel()))
			defer level.free()
		}
		if err := call(func() C.KernelStatusCode {
			return C.kernel_init_logging(level.c, nil)
		}); err != nil {
			// The kernel subscriber didn't install (commonly: the host already
			// installed a global tracing subscriber). Non-fatal — surface it through
			// the shared logger so it's visible without a separate stderr scrape.
			logger.Logger.Warn().Msgf("databricks: kernel_init_logging: %v (kernel logs unavailable; proceeding)", err)
		}
	})
}

// kernelLogLevel maps a zerolog level to the level string kernel_init_logging
// accepts (OFF/ERROR/WARN/INFO/DEBUG/TRACE), so the driver's log level drives the
// kernel's Rust logs. An unrecognized level falls back to WARN (the kernel's own
// default), matching the driver's default.
//
// FatalLevel/PanicLevel map to OFF, not ERROR: at those levels the Go driver
// suppresses even its own Error() lines, so the kernel's Rust subscriber must not
// be louder than the driver the user configured — the kernel has no fatal/panic
// threshold, and emitting ERROR lines there would leak stderr output a user who set
// DATABRICKS_LOG_LEVEL=fatal explicitly asked to silence.
func kernelLogLevel(l zerolog.Level) string {
	switch l {
	case zerolog.TraceLevel:
		return "TRACE"
	case zerolog.DebugLevel:
		return "DEBUG"
	case zerolog.InfoLevel:
		return "INFO"
	case zerolog.WarnLevel:
		return "WARN"
	case zerolog.ErrorLevel:
		return "ERROR"
	case zerolog.FatalLevel, zerolog.PanicLevel, zerolog.Disabled:
		return "OFF"
	default:
		return "WARN"
	}
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
