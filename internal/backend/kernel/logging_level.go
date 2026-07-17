package kernel

import (
	"os"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// It holds the pure level-resolution logic that decides what verbosity the kernel's
// Rust subscriber is initialized with — the zerolog→kernel level mapping and the
// DBSQL_KERNEL_DEBUG override. Keeping it out of the cgo file lets its tests run in
// the default CGO_ENABLED=0 build (matching errors_classify.go), so the load-bearing
// decisions — that fatal/panic collapse to OFF, and that DBSQL_KERNEL_DEBUG yields a
// NULL level so the kernel honors RUST_LOG — are pinned by CI rather than only by
// comments. cgo.go's initKernelLogging is the thin cgo caller: it turns the result
// into a C string (or NULL) and hands it to kernel_init_logging.

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

// resolveKernelLogArg decides the level string to pass to kernel_init_logging and
// whether to pass NULL instead. It is the pure core of initKernelLogging, split out
// so it can be unit-tested without cgo.
//
// DBSQL_KERNEL_DEBUG (any non-empty value) is the advanced override: it returns
// useNULL=true so the cgo caller passes a NULL level, which makes the kernel honor
// RUST_LOG for its own verbosity — independent of the driver level, and with the
// per-target filtering (e.g. the hyper/reqwest HTTP stack) that the single mapped
// level can't express. The kernel reads RUST_LOG ONLY when the level is NULL; a
// non-NULL level shadows it. So without this override, RUST_LOG is inert — the
// mapped driver level always wins. Otherwise it returns the driver's current level
// mapped via kernelLogLevel, so the one DATABRICKS_LOG_LEVEL knob governs kernel
// verbosity too. When useNULL is true the level string is unused (empty).
func resolveKernelLogArg() (level string, useNULL bool) {
	if os.Getenv("DBSQL_KERNEL_DEBUG") != "" {
		return "", true
	}
	return kernelLogLevel(logger.Logger.GetLevel()), false
}
