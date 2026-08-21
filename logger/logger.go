package logger

import (
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
)

type DBSQLLogger struct {
	zerolog.Logger
}

// sharedOutput is the process-wide destination behind every driver logger,
// including logger values derived before a later SetLogOutput call. Serializing
// writes keeps Go and kernel-callback records intact when they arrive from
// different goroutines or native kernel threads.
type sharedOutput struct {
	mu sync.Mutex
	w  io.Writer
}

func (o *sharedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.w.Write(p)
}

func (o *sharedOutput) set(w io.Writer) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.w = w
}

// Track is a simple utility function to use with logger to log a message with a timestamp.
// Recommended to use in conjunction with Duration.
//
// For example:
//
//	msg, start := log.Track("Run operation")
//	defer log.Duration(msg, start)
func (l *DBSQLLogger) Track(msg string) (string, time.Time) {
	return msg, time.Now()
}

// Duration logs a debug message with the time elapsed between the provided start and the current time.
// Use in conjunction with Track.
//
// For example:
//
//	msg, start := log.Track("Run operation")
//	defer log.Duration(msg, start)
func (l *DBSQLLogger) Duration(msg string, start time.Time) {
	l.Debug().Msgf("%v elapsed time: %v", msg, time.Since(start))
}

var output = &sharedOutput{w: os.Stderr}

var Logger = &DBSQLLogger{zerolog.New(output).With().Timestamp().Logger()}

// Enable pretty printing for interactive terminals and json for production.
func init() {
	// for tty terminal enable pretty logs
	if isatty.IsTerminal(os.Stdout.Fd()) && runtime.GOOS != "windows" {
		output.set(zerolog.ConsoleWriter{Out: os.Stderr})
	}
	// by default only log warns or above
	loglvl := zerolog.WarnLevel
	if lvst := os.Getenv("DATABRICKS_LOG_LEVEL"); lvst != "" {
		if lv, err := zerolog.ParseLevel(lvst); err != nil {
			Logger.Error().Msgf("log level %s not recognized", lvst)
		} else {
			loglvl = lv
		}
	}
	Logger.Logger = Logger.Level(loglvl)
	Logger.Info().Msgf("setting log level to %s", loglvl)
}

// Sets log level. Default is "warn"
// Available levels are: "trace" "debug" "info" "warn" "error" "fatal" "panic" or "disabled"
func SetLogLevel(l string) error {
	if lv, err := zerolog.ParseLevel(l); err != nil {
		return err
	} else {
		Logger.Logger = Logger.Level(lv)
		return nil
	}
}

// Sets logging output. Default is os.Stderr. If in terminal, pretty logs are enabled.
func SetLogOutput(w io.Writer) {
	output.set(w)
}

// Sets log to trace. -1
// You must call Msg on the returned event in order to send the event.
func Trace() *zerolog.Event {
	return Logger.Trace()
}

// Sets log to debug. 0
// You must call Msg on the returned event in order to send the event.
func Debug() *zerolog.Event {
	return Logger.Debug()
}

// Sets log to info. 1
// You must call Msg on the returned event in order to send the event.
func Info() *zerolog.Event {
	return Logger.Info()
}

// Sets log to warn. 2
// You must call Msg on the returned event in order to send the event.
func Warn() *zerolog.Event {
	return Logger.Warn()
}

// Sets log to error. 3
// You must call Msg on the returned event in order to send the event.
func Error() *zerolog.Event {
	return Logger.Error()
}

// Sets log to fatal. 4
// You must call Msg on the returned event in order to send the event.
func Fatal() *zerolog.Event {
	return Logger.Fatal()
}

// Sets log to panic. 5
// You must call Msg on the returned event in order to send the event.
func Panic() *zerolog.Event {
	return Logger.Panic()
}

// Err starts a new message with error level with err as a field if not nil or with info level if err is nil.
// You must call Msg on the returned event in order to send the event.
func Err(err error) *zerolog.Event {
	return Logger.Err(err)
}

// WithContext sets connectionId, correlationId, and queryId to be used as fields.
func WithContext(connectionId string, correlationId string, queryId string) *DBSQLLogger {
	return &DBSQLLogger{Logger.With().Str("connId", connectionId).Str("corrId", correlationId).Str("queryId", queryId).Logger()}
}

// Track is a convenience function to track time spent
func Track(msg string) (string, time.Time) {
	return msg, time.Now()
}

// Duration is a convenience function to log elapsed time. Often used with Track
func Duration(msg string, start time.Time) {
	Logger.Debug().Msgf("%v elapsed time: %v", msg, time.Since(start))
}
