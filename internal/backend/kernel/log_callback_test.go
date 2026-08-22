//go:build cgo && databricks_kernel

package kernel

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

const (
	logFileHelperEnv = "DBSQL_KERNEL_LOG_FILE_HELPER"
	logFilePathEnv   = "DBSQL_KERNEL_LOG_FILE_PATH"
	goLogFileProbe   = "go local-file logging probe"
	rustLogFileProbe = "retry max_wait_ms is below min_wait_ms"
)

// TestKernelCallbackWritesConfiguredFileEndToEnd proves the user-visible parity
// contract in a fresh process: the same file passed to logger.SetLogOutput gets a
// native Go record and a real Rust tracing record delivered through the C ABI.
// A subprocess is required because the kernel tracing subscriber is process-wide
// and first-call-wins.
func TestKernelCallbackWritesConfiguredFileEndToEnd(t *testing.T) {
	if os.Getenv(logFileHelperEnv) == "1" {
		path := os.Getenv(logFilePathEnv)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // Parent supplies its temp path.
		if err != nil {
			t.Fatal(err)
		}
		if err := logger.SetLogLevel("warn"); err != nil {
			t.Fatal(err)
		}
		logger.SetLogOutput(file)

		initKernelLogging()
		logger.Logger.Warn().Msg(goLogFileProbe)
		// The C ABI corrects this inverted range and emits a Rust klog::warn!,
		// giving the test a deterministic kernel-owned record without a server.
		err = trySetRetry(Config{Retry: &RetryConfig{
			MinWait:    5 * time.Second,
			MaxWait:    time.Second,
			MaxRetries: 1,
		}})
		if err != nil {
			t.Fatal(err)
		}

		// The kernel record crosses an async drain goroutine, so flush it into the
		// file before retargeting the output or closing it — otherwise the drain
		// could write to stderr (post-retarget) or after Close.
		if !flushKernelLogs(5 * time.Second) {
			t.Fatal("kernel log flush timed out")
		}

		logger.SetLogOutput(os.Stderr)
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}

	logPath := filepath.Join(t.TempDir(), "driver-and-kernel.log")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKernelCallbackWritesConfiguredFileEndToEnd$") //nolint:gosec // Re-executes this test binary only.
	cmd.Env = append(os.Environ(), logFileHelperEnv+"=1", logFilePathEnv+"="+logPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("logging helper failed: %v\n%s", err, output)
	}
	contents, err := os.ReadFile(logPath) //nolint:gosec // Test-owned temporary path.
	if err != nil {
		t.Fatal(err)
	}
	got := string(contents)
	if !strings.Contains(got, goLogFileProbe) {
		t.Errorf("local log file is missing Go record: %q", got)
	}
	if !strings.Contains(got, rustLogFileProbe) {
		t.Errorf("local log file is missing Rust kernel record: %q", got)
	}
}

// The forward path enqueues an owned record onto the bounded channel. (The cgo
// trampoline copies the borrowed C strings via C.GoString, then calls this; that
// tiny boundary can't be driven from a _test.go file because import "C" is
// disallowed there, so the testable logic lives in enqueueKernelLog.)
func TestEnqueueKernelLog(t *testing.T) {
	ch := make(chan kernelLogRecord, 1)
	prev := logQueue.Swap(&ch)
	t.Cleanup(func() { logQueue.Store(prev) })

	emittedAt := time.Now()
	enqueueKernelLog(emittedAt, "debug", "databricks::sql::kernel", "callback probe")
	select {
	case got := <-ch:
		if got.level != "debug" || got.target != "databricks::sql::kernel" || got.message != "callback probe" {
			t.Fatalf("enqueued record = %#v", got)
		}
		if !got.emittedAt.Equal(emittedAt) {
			t.Fatalf("emittedAt = %v, want %v", got.emittedAt, emittedAt)
		}
	default:
		t.Fatal("record was not enqueued")
	}
}

// A full buffer drops the record and counts it rather than blocking the kernel
// thread. A nil queue (logging not installed) is a safe no-op.
func TestEnqueueKernelLogDropsWhenFullAndNoopWhenUnset(t *testing.T) {
	// Unset queue: must not block or panic.
	prev := logQueue.Swap(nil)
	t.Cleanup(func() { logQueue.Store(prev) })
	enqueueKernelLog(time.Now(), "warn", "t", "before install")

	ch := make(chan kernelLogRecord, 1)
	logQueue.Store(&ch)
	before := kernelLogDropped()
	enqueueKernelLog(time.Now(), "warn", "t", "keeps the one slot") // fills the buffer
	enqueueKernelLog(time.Now(), "warn", "t", "must be dropped")    // buffer full → dropped
	if got := kernelLogDropped() - before; got != 1 {
		t.Fatalf("dropped delta = %d, want 1", got)
	}
	if len(ch) != 1 {
		t.Fatalf("channel len = %d, want 1 (non-blocking drop)", len(ch))
	}
}

// A panicking writer must not kill the drain goroutine (an unrecovered goroutine
// panic is fatal to the process); the drain contains it and keeps processing.
func TestDrainRecoversFromWriterPanic(t *testing.T) {
	t.Cleanup(func() { logger.SetLogOutput(os.Stderr) })
	logger.SetLogOutput(io.Discard) // discard the sink's forwarded records
	done := make(chan string, 1)
	sink := newLogSink()
	sink.observe = func(_, _, message string) {
		if message == "boom" {
			panic("writer failure")
		}
		done <- message
	}

	ch := make(chan kernelLogRecord, 2)
	go drainKernelLogs(ch, sink)
	defer close(ch)

	ch <- kernelLogRecord{level: "error", target: "t", message: "boom"} // triggers the panic
	ch <- kernelLogRecord{level: "info", target: "t", message: "after"} // must still be delivered
	select {
	case got := <-done:
		if got != "after" {
			t.Fatalf("delivered %q, want %q", got, "after")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain goroutine did not survive a writer panic")
	}
}

// flushKernelLogs returns only after every record queued before it is written.
func TestFlushKernelLogsWaitsForQueued(t *testing.T) {
	t.Cleanup(func() { logger.SetLogOutput(os.Stderr) })
	logger.SetLogOutput(io.Discard) // discard the sink's forwarded records
	seen := make(chan string, 8)
	sink := newLogSink()
	sink.observe = func(_, _, message string) { seen <- message }

	ch := make(chan kernelLogRecord, 8)
	prev := logQueue.Swap(&ch)
	t.Cleanup(func() { logQueue.Store(prev) })
	go drainKernelLogs(ch, sink)
	defer close(ch)

	enqueueKernelLog(time.Now(), "info", "t", "one")
	enqueueKernelLog(time.Now(), "info", "t", "two")
	if !flushKernelLogs(2 * time.Second) {
		t.Fatal("flush timed out")
	}
	// The barrier is FIFO-ordered behind both records, so both are delivered.
	if got := len(seen); got != 2 {
		t.Fatalf("after flush, delivered %d records, want 2", got)
	}
}

// flushKernelLogs is a no-op returning true when logging was never installed.
func TestFlushKernelLogsNoopWhenUnset(t *testing.T) {
	prev := logQueue.Swap(nil)
	t.Cleanup(func() { logQueue.Store(prev) })
	if !flushKernelLogs(time.Second) {
		t.Fatal("flush with no queue should be a no-op returning true")
	}
}

type panicWriter struct{}

func (panicWriter) Write([]byte) (int, error) { panic("writer always panics") }

// A panicking writer combined with dropped records must not crash the drain: the
// forwarded record AND the one-shot drop warning both write to that writer, and
// both must be contained. observe advances the drop counter past the drain's
// baseline (so the warning fires) and panics (so forward is exercised too); the
// shared output is the panicking writer (so the warning write panics).
func TestDrainContainsPanicFromForwardAndDropWarning(t *testing.T) {
	prevLevel := logger.Logger.GetLevel()
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		logger.Logger.Logger = logger.Logger.Level(prevLevel)
	})
	if err := logger.SetLogLevel("warn"); err != nil {
		t.Fatal(err)
	}
	logger.SetLogOutput(panicWriter{})

	sink := newLogSink()
	sink.observe = func(_, _, _ string) {
		logDropped.Add(1) // advance past the drain's baseline, deterministically
		panic("forward failure")
	}

	ch := make(chan kernelLogRecord, 4)
	prev := logQueue.Swap(&ch)
	t.Cleanup(func() { logQueue.Store(prev) })
	go drainKernelLogs(ch, sink)
	defer close(ch)

	enqueueKernelLog(time.Now(), "info", "databricks::sql::kernel", "boom")

	// The drain reaches the flush barrier only if it survived both panics.
	if !flushKernelLogs(2 * time.Second) {
		t.Fatal("drain did not survive a panicking writer combined with dropped records")
	}
}
