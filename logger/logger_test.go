package logger

import (
	"os"
	"strings"
	"sync"
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

// SetLogOutput(nil) must normalize to io.Discard, not store a nil writer that
// panics on the next enabled log. (Before the shared proxy, zerolog.New(nil)
// handled this; the proxy must preserve it.)
func TestSetLogOutputNilDiscards(t *testing.T) {
	prevLevel := Logger.GetLevel()
	t.Cleanup(func() {
		SetLogOutput(os.Stderr)
		Logger.Logger = Logger.Level(prevLevel)
	})

	SetLogOutput(nil)
	Logger.Logger = Logger.Level(zerolog.TraceLevel)
	// An enabled log actually reaches the writer; a nil writer would panic here.
	Logger.Info().Msg("after nil output is discarded, not panicked")
}

// recordingLevelWriter records whether zerolog reaches it via WriteLevel (level
// preserved) or Write (level lost).
type recordingLevelWriter struct {
	mu     sync.Mutex
	levels []zerolog.Level
	plain  int
}

func (w *recordingLevelWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.plain++
	return len(p), nil
}

func (w *recordingLevelWriter) WriteLevel(l zerolog.Level, p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.levels = append(w.levels, l)
	return len(p), nil
}

// A LevelWriter destination supplied via SetLogOutput must keep receiving
// WriteLevel (severity-aware routing), not be flattened to Write by the proxy.
func TestSetLogOutputPreservesLevelWriter(t *testing.T) {
	prevLevel := Logger.GetLevel()
	t.Cleanup(func() {
		SetLogOutput(os.Stderr)
		Logger.Logger = Logger.Level(prevLevel)
	})

	lw := &recordingLevelWriter{}
	SetLogOutput(lw)
	Logger.Logger = Logger.Level(zerolog.TraceLevel)
	Logger.Warn().Msg("severity aware")

	lw.mu.Lock()
	defer lw.mu.Unlock()
	if lw.plain != 0 {
		t.Fatalf("destination received %d plain Write calls; expected WriteLevel only", lw.plain)
	}
	foundWarn := false
	for _, l := range lw.levels {
		if l == zerolog.WarnLevel {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Fatalf("WriteLevel not called with WarnLevel; severity routing lost (levels=%v)", lw.levels)
	}
}
