package kernel

import (
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// logSink is the Go destination for kernel tracing records. Its logger comes from
// logger.NewForwardingLogger rather than being derived from logger.Logger, so it
// (a) still follows SetLogOutput, (b) is ungated at TraceLevel because the kernel
// already applied its configured level, and (c) carries no auto-timestamp hook —
// forward stamps each record with the emission time captured on the kernel thread,
// not the drain time.
type logSink struct {
	log     zerolog.Logger
	observe func(level, target, message string)
}

func newLogSink() *logSink {
	return &logSink{log: logger.NewForwardingLogger().Level(zerolog.TraceLevel)}
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
	ev := s.event(level)
	ev.Time(zerolog.TimestampFieldName, emittedAt).Str("target", target).Msg(message)
}

// event picks the zerolog event for a kernel level string. An unknown level maps
// to Debug and preserves the raw kernel level as a field.
func (s *logSink) event(level string) *zerolog.Event {
	switch strings.ToLower(level) {
	case "error":
		return s.log.Error()
	case "warn":
		return s.log.Warn()
	case "info":
		return s.log.Info()
	case "debug":
		return s.log.Debug()
	case "trace":
		return s.log.Trace()
	default:
		return s.log.Debug().Str("kernelLevel", level)
	}
}
