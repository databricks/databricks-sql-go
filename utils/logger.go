package utils

import (
	"os"
	"runtime"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
)

var Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()

// ConfigureGlobalLogger will configure zerolog globally. It will
// enable pretty printing for interactive terminals and json for production.
func ConfigureLogger() {
	// for tty terminal enable pretty logs
	if isatty.IsTerminal(os.Stdout.Fd()) && runtime.GOOS != "windows" {
		Logger = Logger.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	} else {
		// UNIX Time is faster and smaller than most timestamps
		// If you set zerolog.TimeFieldFormat to an empty string,
		// logs will write with UNIX time.
		zerolog.TimeFieldFormat = ""
	}
}
