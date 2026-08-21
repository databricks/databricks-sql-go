package kernel

import (
	"strings"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/rs/zerolog"
)

// logSink is the Go destination for kernel tracing records. The logger value is
// immutable and ungated because the kernel already applied its configured level.
// Its writer is the logger package's stable shared proxy, so SetLogOutput can
// safely retarget this snapshot after the first kernel connection.
type logSink struct {
	log     zerolog.Logger
	observe func(level, target, message string)
}

func newLogSink() *logSink {
	return &logSink{log: logger.Logger.Level(zerolog.TraceLevel)}
}

func (s *logSink) forward(level, target, message string) {
	if s == nil {
		return
	}
	if s.observe != nil {
		s.observe(level, target, message)
	}
	switch strings.ToLower(level) {
	case "error":
		s.log.Error().Str("target", target).Msg(message)
	case "warn":
		s.log.Warn().Str("target", target).Msg(message)
	case "info":
		s.log.Info().Str("target", target).Msg(message)
	case "debug":
		s.log.Debug().Str("target", target).Msg(message)
	case "trace":
		s.log.Trace().Str("target", target).Msg(message)
	default:
		s.log.Debug().Str("target", target).Str("kernelLevel", level).Msg(message)
	}
}
