package logger

import (
	"bytes"
	"encoding/json"
	"io"
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

// A ForwardingSink must not be an io.Writer. If it were, SetLogOutput(sink) would
// store it as the destination and its own writes would route back through the
// SyncWriter that wraps it (output -> SyncWriter -> sink -> output), deadlocking
// the next record on the re-entered mutex — the same trap that made returning a
// zerolog.Logger (which implements io.Writer) unsafe.
func TestForwardingSinkIsNotAnIOWriter(t *testing.T) {
	if _, ok := any(NewForwardingSink()).(io.Writer); ok {
		t.Fatal("ForwardingSink must not implement io.Writer (would enable a self-referential SetLogOutput deadlock)")
	}
}

// countingWriter is a self-synchronized destination: it counts complete records
// and flags any that isn't a single well-formed JSON line (which is what byte
// interleaving from unsynchronized concurrent writes would produce).
type countingWriter struct {
	mu    sync.Mutex
	lines int
	bad   int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if json.Valid(bytes.TrimSpace(p)) {
		w.lines++
	} else {
		w.bad++
	}
	return len(p), nil
}

// Hot-swapping SetLogOutput under concurrent logging must not corrupt output when
// the destination is safe for concurrent use — even when the same writer is
// reapplied (each set builds a fresh SyncWriter, so the writer's own lock, not the
// driver's, is what serializes writes straddling a swap). Run under -race.
func TestConcurrentRetargetToSyncWriter(t *testing.T) {
	prevLevel := Logger.GetLevel()
	t.Cleanup(func() {
		SetLogOutput(os.Stderr)
		Logger.Logger = Logger.Level(prevLevel)
	})

	w := &countingWriter{}
	SetLogOutput(w)
	snapshot := Logger.Level(zerolog.TraceLevel)

	const writers, perWriter = 8, 200

	// Retargeter: reapply the same writer until stopped, each call replacing the
	// SyncWriter wrapper while writes are in flight.
	stop := make(chan struct{})
	retargeterDone := make(chan struct{})
	go func() {
		defer close(retargeterDone)
		for {
			select {
			case <-stop:
				return
			default:
				SetLogOutput(w)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				snapshot.Info().Int("j", j).Msg("concurrent")
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-retargeterDone

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.bad != 0 {
		t.Fatalf("observed %d corrupted (non-JSON) writes", w.bad)
	}
	if w.lines != writers*perWriter {
		t.Fatalf("wrote %d complete records, want %d", w.lines, writers*perWriter)
	}
}
