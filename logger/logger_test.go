package logger

import (
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// Existing logger values must follow SetLogOutput. The kernel callback keeps an
// immutable Trace-level logger value for thread safety, so retargeting only works
// when every value writes through the same stable output proxy.
func TestSetLogOutputRetargetsExistingLogger(t *testing.T) {
	prevLevel := Logger.GetLevel()
	t.Cleanup(func() {
		SetLogOutput(os.Stderr)
		Logger.Logger = Logger.Level(prevLevel)
	})

	first, err := os.CreateTemp(t.TempDir(), "driver-first-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Errorf("close first log: %v", err)
		}
	})
	second, err := os.CreateTemp(t.TempDir(), "driver-second-*.log")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("close second log: %v", err)
		}
	})

	snapshot := Logger.Level(zerolog.TraceLevel)
	SetLogOutput(first)
	snapshot.Info().Msg("first destination")
	SetLogOutput(second)
	snapshot.Info().Msg("second destination")

	if err := first.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := second.Sync(); err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(first.Name())
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(second.Name())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(firstBytes); !strings.Contains(got, "first destination") || strings.Contains(got, "second destination") {
		t.Fatalf("first log contents = %q", got)
	}
	if got := string(secondBytes); !strings.Contains(got, "second destination") || strings.Contains(got, "first destination") {
		t.Fatalf("second log contents = %q", got)
	}
}
