package logger

import (
	"io"
	"os"
	"runtime"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
)

var Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()

// enable pretty printing for interactive terminals and json for production.
func init() {
	// for tty terminal enable pretty logs
	if isatty.IsTerminal(os.Stdout.Fd()) && runtime.GOOS != "windows" {
		Logger = Logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		// UNIX Time is faster and smaller than most timestamps
		// If you set zerolog.TimeFieldFormat to an empty string,
		// logs will write with UNIX time.
		zerolog.TimeFieldFormat = ""
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
	Logger = Logger.Level(loglvl)
	Logger.Info().Msgf("setting log level to %s", loglvl)
}

// Sets log level
// Available levels are: "trace" "debug" "info" "warn" "error" "fatal" "panic"
func SetLogLevel(l string) error {
	if lv, err := zerolog.ParseLevel(l); err != nil {
		return err
	} else {
		Logger = Logger.Level(lv)
		return nil
	}
}

func SetLogOutput(w io.Writer) {
	Logger = Logger.Output(w)
}

// You must call Msg on the returned event in order to send the event.
func Trace() *zerolog.Event {
	return Logger.Info()
}

// You must call Msg on the returned event in order to send the event.
func Debug() *zerolog.Event {
	return Logger.Debug()
}

// You must call Msg on the returned event in order to send the event.
func Info() *zerolog.Event {
	return Logger.Info()
}

// You must call Msg on the returned event in order to send the event.
func Warn() *zerolog.Event {
	return Logger.Warn()
}

// You must call Msg on the returned event in order to send the event.
func Error() *zerolog.Event {
	return Logger.Error()
}

// You must call Msg on the returned event in order to send the event.
func Err(err error) *zerolog.Event {
	return Logger.Err(err)
}

// You must call Msg on the returned event in order to send the event.
func Fatal() *zerolog.Event {
	return Logger.Fatal()
}

// You must call Msg on the returned event in order to send the event.
func Panic() *zerolog.Event {
	return Logger.Panic()
}
