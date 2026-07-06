// Package debuglog is the step-level debug tracer for the databricks-sql-go
// SEA/kernel integration.
//
// The SEA-via-kernel path crosses a Go-GC <-> Rust-ownership FFI boundary where
// failures are silent and non-local (thread migration, use-after-free, thread
// blowup, cross-region latency that looks like "the protocol is slow"). A Go
// stack trace stops at the cgo edge, so the way to see which step failed or
// slowed down is an explicit, ordered, timed, per-function log at each step —
// in particular an "enter" line emitted *before* a step runs, so a step that
// hangs at the FFI edge (and never returns) is still visible.
//
// This is a thin helper over the driver's single logger (logger.Logger): every
// event is an ordinary debug-level line on the same sink, in the same format
// (JSON in production, pretty in a terminal) as the rest of the driver's logs.
// There is no second logger and no separate output knob — turn it on the way
// you turn on any driver logging, with logger.SetLogLevel("debug") or
// DATABRICKS_LOG_LEVEL=debug. That keeps these lines readable next to the
// existing driver logs and lets kernel-side logs (written to the same stderr)
// interleave into one stream in execution order.
//
// Each event carries the function the step is in, the phase ("enter"/"done",
// omitted for a point log), the elapsed time (on "done"), and the
// conn/correlation/query ids from the context. The shared logger supplies the
// timestamp and level fields, so the format matches every other driver line.
package debuglog

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/logger"
)

// Enabled reports whether debug-level logging is currently on, i.e. whether
// events would be emitted. Cheap enough to call at every step; callers use it
// to skip building expensive log arguments. It simply reflects the driver's log
// level, so debug tracing follows SetLogLevel like every other driver log.
func Enabled() bool { return logger.Logger.GetLevel() <= zerolog.DebugLevel }

// Logf emits a single function-tagged step event at debug level. fn is the
// function the step is in (e.g. "thrift.Backend.Execute"); the format/args
// describe the step. No-op when debug logging is off (the underlying event is
// nil and discards cheaply) — guard the call with Enabled() if the args are
// expensive to build.
func Logf(ctx context.Context, fn string, format string, args ...any) {
	event(ctx, fn, "").Msgf(format, args...)
}

// Track marks the start of a step and returns a function that logs the step's
// elapsed time. Intended for `defer`:
//
//	defer debuglog.Track(ctx, "thrift.Backend.Execute", "sql.len=%d", len(query))()
//
// The enter event is emitted immediately (so a step that never returns is still
// visible); the returned closure emits a matching done event carrying the
// elapsed duration. Both are no-ops when debug logging is off, and the returned
// closure is always safe to call.
func Track(ctx context.Context, fn string, format string, args ...any) func() {
	if !Enabled() {
		return func() {}
	}
	start := time.Now()
	event(ctx, fn, "enter").Msgf(format, args...)
	return func() {
		event(ctx, fn, "done").Dur("elapsed", time.Now().Sub(start)).Msg("")
	}
}

// event builds a debug-level zerolog event pre-populated with the step's
// function, the phase ("enter"/"done", or "" for a point log), and the
// conn/correlation/query ids present on the context. The caller finishes it
// with Msg/Msgf. Emission goes through logger.Logger so the step trace shares
// the driver's single sink, format, and timestamp — one ordered stream that
// kernel-side stderr logs interleave into by execution order. Returns a nil
// event (a cheap no-op that ignores all chained calls) when debug logging is
// off.
func event(ctx context.Context, fn, phase string) *zerolog.Event {
	e := logger.Logger.Debug().Str("fn", fn)
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
