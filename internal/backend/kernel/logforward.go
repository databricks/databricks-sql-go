package kernel

import (
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// logSink is the Go destination for kernel tracing records. It forwards through
// logger.ForwardingSink (not a logger derived from logger.Logger), so it (a) still
// follows SetLogOutput, (b) is ungated at TraceLevel because the kernel already
// applied its configured level, (c) carries no auto-timestamp hook — forward stamps
// each record with the emission time captured on the kernel thread, not the drain
// time — and (d) cannot be round-tripped into SetLogOutput (ForwardingSink is not
// an io.Writer), which would deadlock.
type logSink struct {
	sink    *logger.ForwardingSink
	observe func(level, target, message string)
}

func newLogSink() *logSink {
	return &logSink{sink: logger.NewForwardingSink()}
}

// forward writes one kernel record. emittedAt is the time the kernel emitted the
// event (captured in the cgo callback), stamped as the record's timestamp so a
// backed-up drain does not skew kernel log times toward drain time.
func (s *logSink) forward(emittedAt time.Time, level, target, message string) {
	if s == nil {
		return
	}
	if s.observe != nil {
		s.observe(level, target, message)
	}
	s.event(level).Time(zerolog.TimestampFieldName, emittedAt).Str("target", target).Msg(message)
}

// warnDropped emits a one-shot advisory that forwarded records were dropped. It
// goes through the sink's own immutable logger — not logger.Logger, whose embedded
// value SetLogLevel reassigns — so the long-lived drain goroutine never races
// SetLogLevel. Like forwarded records it is ungated, which is what we want: log loss
// should surface regardless of the driver level.
func (s *logSink) warnDropped(dropped uint64) {
	s.sink.Event(zerolog.WarnLevel).
		Uint64("dropped", dropped).
		Time(zerolog.TimestampFieldName, time.Now()).
		Msg("[kernel] kernel log records dropped; the log sink is not keeping up " +
			"(raise capacity or lower kernel verbosity)")
}

// event picks the zerolog event for a kernel level string. An unknown level maps
// to Debug and preserves the raw kernel level as a field.
func (s *logSink) event(level string) *zerolog.Event {
	switch strings.ToLower(level) {
	case "error":
		return s.sink.Event(zerolog.ErrorLevel)
	case "warn":
		return s.sink.Event(zerolog.WarnLevel)
	case "info":
		return s.sink.Event(zerolog.InfoLevel)
	case "debug":
		return s.sink.Event(zerolog.DebugLevel)
	case "trace":
		return s.sink.Event(zerolog.TraceLevel)
	default:
		return s.sink.Event(zerolog.DebugLevel).Str("kernelLevel", level)
	}
}
