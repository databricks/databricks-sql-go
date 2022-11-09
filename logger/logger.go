package logger

import (
	"io"
	"os"
	"runtime"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
)

var Log = zerolog.New(os.Stderr).With().Timestamp().Logger()

// enable pretty printing for interactive terminals and json for production.
func init() {
	// for tty terminal enable pretty logs
	if isatty.IsTerminal(os.Stdout.Fd()) && runtime.GOOS != "windows" {
		Log = Log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		// UNIX Time is faster and smaller than most timestamps
		// If you set zerolog.TimeFieldFormat to an empty string,
		// logs will write with UNIX time.
		zerolog.TimeFieldFormat = ""
	}
	// by default only log error
	SetLogLevel(zerolog.WarnLevel)
}

func SetLogLevel(l zerolog.Level) {
	Log = Log.Level(l)
}

func SetLogOutput(w io.Writer) {
	Log = Log.Output(w)
}
