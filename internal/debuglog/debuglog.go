// Package debuglog is the step-level debug tracer for the databricks-sql-go
// SEA/kernel integration.
//
// The SEA-via-kernel path crosses a Go-GC <-> Rust-ownership FFI boundary where
// failures are silent and non-local (thread migration, use-after-free, thread
// blowup, cross-region latency that looks like "the protocol is slow"). A Go
// stack trace stops at the cgo edge, so the way to see which step failed or
// slowed down is an explicit, ordered, timed, per-function log at each step.
//
// Emission is through the driver's existing logger (logger.DebugLogger, a
// trace-level sibling that shares the main logger's sink and format), so there
// is a single logging system and one place to redirect output. It has its own
// DBSQL_DEBUG_LOG flag (below) so it is enabled independently of the package log
// level.
//
// Every event carries a process-global monotonic sequence number (a total order
// even when timestamps tie), the conn/correlation/query ids from the context,
// the function the step is in, and — for Track — the elapsed time.
//
// Gating: OFF by default (a single atomic-bool load per call site when off).
// Enable via the DBSQL_DEBUG_LOG env var (1/true/yes/on) or SetEnabled.
package debuglog

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/logger"
)

// enabled is the single gate. Kept as an atomic so SetEnabled is safe to call
// concurrently with logging (e.g. a test flipping it while a goroutine logs).
var enabled atomic.Bool

// seq is the process-global sequence counter — the primary ordering key, since
// timestamps can tie or go backwards across cores.
var seq atomic.Uint64

// clockOverride optionally replaces the time source so tests can make timing
// deterministic. nil (the default) means use time.Now. Kept atomic — like
// enabled and seq — so an override is safe to set concurrently with logging.
var clockOverride atomic.Pointer[func() time.Time]

// now returns the current time from the override if one is set, else time.Now.
func now() time.Time {
	if f := clockOverride.Load(); f != nil {
		return (*f)()
	}
	return time.Now()
}

func init() {
	if envEnabled(os.Getenv("DBSQL_DEBUG_LOG")) {
		enabled.Store(true)
	}
}

// envEnabled reports whether an env-var value means "on". Accepts 1/true/yes/on
// (case-insensitive, trimmed); everything else (including empty) is off.
func envEnabled(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// Enabled reports whether step logging is currently on. Cheap enough to call at
// every step; callers use it to skip building expensive log arguments.
func Enabled() bool { return enabled.Load() }

// SetEnabled turns step logging on or off at runtime. Returns the previous value
// so a test can save and restore it.
func SetEnabled(on bool) (previous bool) { return enabled.Swap(on) }

// setClock overrides the time source, or resets to time.Now when f is nil
// (test-only; unexported).
func setClock(f func() time.Time) {
	if f == nil {
		clockOverride.Store(nil)
		return
	}
	clockOverride.Store(&f)
}

// Logf emits a single ordered, function-tagged step event. fn is the function
// the step is in (e.g. "thrift.Backend.Execute"); the format/args describe the
// step. No-op when logging is disabled — guard the call with Enabled() if the
// args are expensive to build.
func Logf(ctx context.Context, fn string, format string, args ...any) {
	if !enabled.Load() {
		return
	}
	event(ctx, fn, "").Msgf(format, args...)
}

// Track marks the start of a step and returns a function that logs the step's
// elapsed time. Intended for `defer`:
//
//	defer debuglog.Track(ctx, "thrift.Backend.Execute", "sql=%q", query)()
//
// The enter event is emitted immediately; the returned closure emits a matching
// done event carrying the elapsed duration. Both are no-ops when disabled, and
// the returned closure is always safe to call.
func Track(ctx context.Context, fn string, format string, args ...any) func() {
	if !enabled.Load() {
		return func() {}
	}
	start := now()
	event(ctx, fn, "enter").Msgf(format, args...)
	return func() {
		// If logging was disabled between enter and here, skip the done line. This
		// can leave an orphan enter with no done, which is acceptable for a debug
		// trace — SetEnabled is not toggled mid-query in normal use, and emitting a
		// done through a now-disabled logger would be worse.
		if !enabled.Load() {
			return
		}
		event(ctx, fn, "done").Dur("elapsed", now().Sub(start)).Msg("")
	}
}

// event builds a trace-level zerolog event pre-populated with a nanosecond
// timestamp, the sequence number, the step's function, the phase
// ("enter"/"done", or "" for a point log), and the conn/correlation/query ids
// present on the context. The caller finishes it with Msg/Msgf. Emission goes
// through logger.DebugLogger so the step trace shares the driver's single sink.
//
// The ts field is written as an explicit RFC3339Nano string rather than via
// zerolog's Time(), because zerolog's time rendering is governed by the global
// zerolog.TimeFieldFormat (RFC3339, whole seconds). Latency work needs sub-second
// resolution to correlate steps with server-side traces and to see inter-step
// gaps, so ts carries nanosecond precision independent of the shared logger's
// coarser default `time` field.
func event(ctx context.Context, fn, phase string) *zerolog.Event {
	e := logger.DebugLogger().Log().
		Str("ts", now().Format(time.RFC3339Nano)).
		Uint64("seq", seq.Add(1)).
		Str("fn", fn)
	if phase != "" {
		e = e.Str("phase", phase)
	}
	if ctx != nil {
		if v := driverctx.ConnIdFromContext(ctx); v != "" {
			e = e.Str("connId", v)
		}
		if v := driverctx.CorrelationIdFromContext(ctx); v != "" {
			e = e.Str("corrId", v)
		}
		if v := driverctx.QueryIdFromContext(ctx); v != "" {
			e = e.Str("queryId", v)
		}
	}
	return e
}
