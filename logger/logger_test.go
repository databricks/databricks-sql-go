package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// restoreLogger restores the level and default output after a test that changes them.
func restoreLogger(t *testing.T) {
	t.Helper()
	prevLevel := Logger.GetLevel()
	t.Cleanup(func() {
		SetLogOutput(os.Stderr)
		Logger.Logger = Logger.Level(prevLevel)
	})
}

func TestSetLogLevelAcceptsNamesInAnyCase(t *testing.T) {
	restoreLogger(t)

	cases := []struct {
		in   string
		want zerolog.Level
	}{
		{"trace", zerolog.TraceLevel},
		{"TRACE", zerolog.TraceLevel},
		{"Trace", zerolog.TraceLevel},
		{"debug", zerolog.DebugLevel},
		{"DEBUG", zerolog.DebugLevel},
		{"Debug", zerolog.DebugLevel},
		{"INFO", zerolog.InfoLevel},
		{"Info", zerolog.InfoLevel},
		{"WARN", zerolog.WarnLevel},
		{"wArN", zerolog.WarnLevel},
		{"ERROR", zerolog.ErrorLevel},
		{"FATAL", zerolog.FatalLevel},
		{"Panic", zerolog.PanicLevel},
		{"DISABLED", zerolog.Disabled},
		// Numeric levels keep zerolog's scale (out of scope for #441).
		{"1", zerolog.InfoLevel},
		{"-1", zerolog.TraceLevel},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			Logger.Logger = Logger.Level(zerolog.WarnLevel)
			if err := SetLogLevel(c.in); err != nil {
				t.Fatalf("SetLogLevel(%q) = %v, want nil", c.in, err)
			}
			if got := Logger.GetLevel(); got != c.want {
				t.Fatalf("SetLogLevel(%q) level = %v, want %v", c.in, got, c.want)
			}
		})
	}
}

// A custom zerolog.LevelFieldMarshalFunc changes the spellings ParseLevel matches.
// The custom spelling keeps working, and the level names stay case-insensitive.
func TestSetLogLevelWithCustomLevelSpelling(t *testing.T) {
	restoreLogger(t)
	prevMarshal := zerolog.LevelFieldMarshalFunc
	t.Cleanup(func() { zerolog.LevelFieldMarshalFunc = prevMarshal })
	zerolog.LevelFieldMarshalFunc = func(l zerolog.Level) string {
		if l == zerolog.DebugLevel {
			return "DBG"
		}
		return strings.ToUpper(l.String())
	}

	for in, want := range map[string]zerolog.Level{
		"DBG": zerolog.DebugLevel, "dbg": zerolog.DebugLevel,
		"DEBUG": zerolog.DebugLevel, "Debug": zerolog.DebugLevel, "debug": zerolog.DebugLevel,
		"WARN": zerolog.WarnLevel, "warn": zerolog.WarnLevel, "Trace": zerolog.TraceLevel,
	} {
		Logger.Logger = Logger.Level(zerolog.ErrorLevel)
		if err := SetLogLevel(in); err != nil {
			t.Errorf("SetLogLevel(%q) = %v, want nil", in, err)
			continue
		}
		if got := Logger.GetLevel(); got != want {
			t.Errorf("SetLogLevel(%q) level = %v, want %v", in, got, want)
		}
	}
}

// The warning quotes at most a bounded prefix of the rejected value; the returned
// error is zerolog's, unchanged.
func TestSetLogLevelWarningIsBounded(t *testing.T) {
	restoreLogger(t)

	for name, value := range map[string]string{
		"long":    strings.Repeat("v", 1<<20),
		"escaped": strings.Repeat("\x00", 1<<20),
		"utf8":    strings.Repeat("é", 1<<19),
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			SetLogOutput(&buf)
			Logger.Logger = Logger.Level(zerolog.WarnLevel)

			_, wantErr := zerolog.ParseLevel(value)
			if err := SetLogLevel(value); err == nil || err.Error() != wantErr.Error() {
				t.Fatalf("SetLogLevel error changed: got %d bytes, want zerolog's error", len(fmt.Sprint(err)))
			}
			var rec struct {
				Level   string `json:"level"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
				t.Fatalf("record is not one JSON line (%d bytes): %v", buf.Len(), err)
			}
			if rec.Level != "warn" || buf.Len() > 1024 {
				t.Fatalf("got a %d-byte %q record, want one warning under 1 KiB", buf.Len(), rec.Level)
			}
			if want := fmt.Sprintf("(%d bytes)", len(value)); !strings.Contains(rec.Message, want) {
				t.Fatalf("message %q does not report the value length %s", rec.Message, want)
			}
		})
	}
}

func TestSetLogLevelUnrecognizedWarnsAndKeepsLevel(t *testing.T) {
	restoreLogger(t)

	cases := []struct {
		value     string
		start     zerolog.Level
		wantWarns int
	}{
		{"verbose", zerolog.WarnLevel, 1},
		{"VERBOSE", zerolog.TraceLevel, 1},
		{"de bug", zerolog.DebugLevel, 1},
		{"200", zerolog.InfoLevel, 1},
		// The current level filters the warning; it is not raised to show it.
		{"verbose", zerolog.ErrorLevel, 0},
		{"verbose", zerolog.Disabled, 0},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s@%s", c.value, c.start), func(t *testing.T) {
			var buf bytes.Buffer
			SetLogOutput(&buf)
			Logger.Logger = Logger.Level(c.start)

			_, wantErr := zerolog.ParseLevel(c.value)
			err := SetLogLevel(c.value)
			if err == nil || wantErr == nil || err.Error() != wantErr.Error() {
				t.Fatalf("SetLogLevel(%q) error = %v, want %v", c.value, err, wantErr)
			}
			if got := Logger.GetLevel(); got != c.start {
				t.Fatalf("level after SetLogLevel(%q) = %v, want unchanged %v", c.value, got, c.start)
			}

			warns := 0
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				if line == "" {
					continue
				}
				var rec struct {
					Level   string `json:"level"`
					Message string `json:"message"`
				}
				if err := json.Unmarshal([]byte(line), &rec); err != nil {
					t.Fatalf("record %q is not JSON: %v", line, err)
				}
				if rec.Level != "warn" || !strings.Contains(rec.Message, fmt.Sprintf("%q", c.value)) ||
					!strings.Contains(rec.Message, c.start.String()) {
					t.Fatalf("unexpected record %q", line)
				}
				warns++
			}
			if warns != c.wantWarns {
				t.Fatalf("got %d warning(s) in %q, want %d", warns, buf.String(), c.wantWarns)
			}
		})
	}
}

// envLevelHelper makes TestEnvLogLevel print the level init applied and return.
const envLevelHelper = "DBSQL_LOGGER_ENV_LEVEL_HELPER"

// DATABRICKS_LOG_LEVEL is read in init, so check it in a fresh process.
func TestEnvLogLevel(t *testing.T) {
	if os.Getenv(envLevelHelper) == "1" {
		fmt.Printf("level=%s\n", Logger.GetLevel())
		return
	}

	cases := []struct {
		env        string
		want       string
		wantStderr string
	}{
		{"debug", "debug", ""},
		{"DEBUG", "debug", ""},
		{"Trace", "trace", ""},
		{"ERROR", "error", ""},
		{"1", "info", ""},
		{"verbose", "warn", "log level verbose not recognized"},
	}
	for _, c := range cases {
		t.Run(c.env, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestEnvLogLevel$") //nolint:gosec // Re-executes this test binary only.
			cmd.Env = append(os.Environ(), envLevelHelper+"=1", "DATABRICKS_LOG_LEVEL="+c.env)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("helper process: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
			}
			if want := "level=" + c.want + "\n"; !strings.HasPrefix(stdout.String(), want) {
				t.Fatalf("stdout = %q, want prefix %q", stdout.String(), want)
			}
			if c.wantStderr != "" && !strings.Contains(stderr.String(), c.wantStderr) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), c.wantStderr)
			}
		})
	}
}

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
