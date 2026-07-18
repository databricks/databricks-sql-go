package kernel

// This file is intentionally NOT behind the cgo build tag: the level mapping
// and sink-routing logic are pure Go, so their tests run under CGO_ENABLED=0.
// The cgo trampoline that calls into these lives in log_callback.go (tagged).

import (
	"os"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// Kernel log levels as delivered over the C ABI (1=ERROR..5=TRACE), mirroring
// the kernel's numeric severity. 0 is unused on the wire (the kernel never
// forwards an OFF event).
const (
	kernelLevelError = 1
	kernelLevelWarn  = 2
	kernelLevelInfo  = 3
	kernelLevelDebug = 4
	kernelLevelTrace = 5
)

// logSink receives forwarded kernel log events and routes them into the driver's
// logger, so kernel-internal diagnostics (HTTP stack, CloudFetch, retry) join the
// unified Go+kernel debug stream instead of only reaching stderr via RUST_LOG.
// A dedicated type (rather than calling the logger directly from the trampoline)
// keeps the routing pure-Go and unit-testable, and gives the cgo.Handle something
// concrete to wrap.
type logSink struct {
	// log is the destination for forwarded events. It is a SNAPSHOT of the
	// driver logger taken once at install (newLogSink), pinned to TraceLevel, and
	// never reassigned. Two reasons:
	//   - No double gating. The KERNEL already filters events against the level
	//     the driver passed to kernel_set_log_callback (DATABRICKS_LOG_LEVEL, or
	//     RUST_LOG under DBSQL_KERNEL_DEBUG); that decision is authoritative. If
	//     we routed through the live logger.Logger — gated at the driver level —
	//     a DEBUG kernel event under the DBSQL_KERNEL_DEBUG override would be
	//     re-dropped by a driver still at Warn, defeating the override. Pinning
	//     the sink at TraceLevel lets every already-approved event through.
	//   - No data race. The kernel drain thread (a non-Go OS thread) calls
	//     forward asynchronously; reading the mutable global logger.Logger here
	//     would race a concurrent dbsql.SetLogLevel/SetLogOutput. A value copy
	//     captured at install is immutable, so the drain thread reads only its
	//     own snapshot. (The writer/output is captured at install; a later
	//     SetLogOutput does not re-target already-forwarded kernel lines — the
	//     same "level fixed at first connect" caveat the kernel subscriber has.)
	log zerolog.Logger

	// nativeSeverity, when true, emits each forwarded kernel event at its own mapped
	// level (kernel ERROR→Error, WARN→Warn). When false (the default) the two
	// operationally-loud levels are demoted one step — kernel ERROR→Warn, WARN→Info —
	// so kernel-internal transient events (e.g. "retrying request" during a 503 / S3
	// storm) do not inflate the driver's shared Warn/Error stream that log-based
	// alerting keys on. INFO/DEBUG/TRACE are unaffected (already below the alerting
	// threshold). Set from DBSQL_KERNEL_DEBUG at install: a caller who opted into
	// kernel debugging wants the native severities. See newLogSink.
	nativeSeverity bool

	// observe, when non-nil, is invoked with each forwarded event before the
	// logger routing. A test seam so a round-trip test can assert the trampoline
	// unwrapped the handle and delivered the event; nil in production.
	observe func(level int, target, message string)
}

// newLogSink snapshots the current driver logger at TraceLevel for the sink to
// forward through. Called once at install so the drain thread never touches the
// mutable global logger. Kept separate from the zero value so tests can build a
// sink with an explicit writer.
//
// It also decides the severity policy: by default forwarded kernel ERROR/WARN are
// demoted one step (see logSink.nativeSeverity) so routine kernel-internal
// retries/backoff don't page on-call through the driver's own WARN/ERROR rate;
// DBSQL_KERNEL_DEBUG (the same knob that widens kernel verbosity) preserves the
// native severities for a debugging session.
func newLogSink() *logSink {
	return &logSink{
		log:            logger.Logger.Level(zerolog.TraceLevel),
		nativeSeverity: os.Getenv("DBSQL_KERNEL_DEBUG") != "",
	}
}

// forward routes one kernel log event into the sink's logger at the mapped level.
// The sink logger is a TraceLevel snapshot of the driver logger (see logSink.log),
// so events the kernel already approved are emitted without being re-gated by the
// driver level. Kept small and allocation-light: it is on the (debug-only) log
// path, not a hot query path. target and message are already Go strings (copied
// out of the C buffers by the trampoline before this is called).
func (s *logSink) forward(level int, target, message string) {
	if s == nil {
		return
	}
	if s.observe != nil {
		s.observe(level, target, message)
	}
	switch level {
	case kernelLevelError:
		// Demote to Warn by default so a kernel-internal error (e.g. a retried,
		// ultimately-recovered request) doesn't inflate the driver's Error rate;
		// DBSQL_KERNEL_DEBUG keeps the native Error severity.
		if s.nativeSeverity {
			s.log.Error().Str("target", target).Msg(message)
		} else {
			s.log.Warn().Str("target", target).Str("kernelLevel", "error").Msg(message)
		}
	case kernelLevelWarn:
		// Demote to Info by default so transient kernel WARN lines (503/S3 retry,
		// backoff) don't page on-call through the driver's WARN rate.
		if s.nativeSeverity {
			s.log.Warn().Str("target", target).Msg(message)
		} else {
			s.log.Info().Str("target", target).Str("kernelLevel", "warn").Msg(message)
		}
	case kernelLevelInfo:
		s.log.Info().Str("target", target).Msg(message)
	case kernelLevelDebug:
		s.log.Debug().Str("target", target).Msg(message)
	case kernelLevelTrace:
		s.log.Trace().Str("target", target).Msg(message)
	default:
		// Unknown level: don't drop it — surface at Debug so it's still visible
		// without being mistaken for a warning/error.
		s.log.Debug().Str("target", target).Int("kernelLevel", level).Msg(message)
	}
}
