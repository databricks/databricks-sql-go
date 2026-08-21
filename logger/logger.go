package logger

import (
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
)

type DBSQLLogger struct {
	zerolog.Logger
}

// sharedOutput is the process-wide destination behind every driver logger,
// including logger values derived before a later SetLogOutput call. The current
// destination is held behind an atomic pointer and wrapped in a per-destination
// synchronized, level-aware writer. This gives three properties the driver's
// logging (Go, Thrift, and forwarded kernel records) relies on:
//
//   - Retargeting (set) is a single atomic store: it never waits on an in-flight
//     user Write, is never blocked by a slow/stuck writer, and never holds a lock
//     across arbitrary user code — so SetLogOutput can't self-deadlock and a stuck
//     writer can always be replaced.
//   - Concurrent records to one destination stay intact (zerolog.SyncWriter
//     serializes writes per destination).
//   - A zerolog.LevelWriter destination keeps severity-aware routing: the proxy
//     itself implements LevelWriter, so zerolog calls WriteLevel on it and it
//     forwards WriteLevel to the destination.
type sharedOutput struct {
	dst atomic.Pointer[zerolog.LevelWriter]
}

func newSharedOutput(w io.Writer) *sharedOutput {
	o := &sharedOutput{}
	o.set(w)
	return o
}

// set publishes w as the current destination. A nil writer is normalized to
// io.Discard, matching the historical zerolog.New(nil) behavior (before the proxy
// existed, SetLogOutput(nil) → Logger.Output(nil) → io.Discard); without this the
// next log would panic on a nil-interface Write.
func (o *sharedOutput) set(w io.Writer) {
	if w == nil {
		w = io.Discard
	}
	// SyncWriter serializes concurrent writes to this destination and preserves a
	// LevelWriter's WriteLevel (a plain writer is adapted). The result always
	// implements LevelWriter; keep a fallback adapter in case that ever changes.
	sw := zerolog.SyncWriter(w)
	lw, ok := sw.(zerolog.LevelWriter)
	if !ok {
		lw = plainLevelWriter{sw}
	}
	o.dst.Store(&lw)
}

func (o *sharedOutput) current() zerolog.LevelWriter {
	return *o.dst.Load()
}

func (o *sharedOutput) Write(p []byte) (int, error) {
	return o.current().Write(p)
}

func (o *sharedOutput) WriteLevel(l zerolog.Level, p []byte) (int, error) {
	return o.current().WriteLevel(l, p)
}

// plainLevelWriter adapts an io.Writer that is not a zerolog.LevelWriter, routing
// WriteLevel to Write (dropping the level, as zerolog's own adapter does).
type plainLevelWriter struct{ io.Writer }

func (p plainLevelWriter) WriteLevel(_ zerolog.Level, b []byte) (int, error) {
	return p.Write(b)
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

var output = newSharedOutput(os.Stderr)

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
