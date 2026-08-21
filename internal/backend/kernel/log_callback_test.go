//go:build cgo && databricks_kernel

package kernel

import (
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

		// The kernel record now crosses an async drain goroutine, so wait until it
		// lands in the file before retargeting the output or closing it — otherwise
		// the drain could write to stderr (post-retarget) or after Close. Bounded so
		// a real failure surfaces as a missing probe in the parent, not a hang.
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if b, _ := os.ReadFile(path); strings.Contains(string(b), rustLogFileProbe) { //nolint:gosec // Parent supplies its temp path.
				break
			}
			time.Sleep(10 * time.Millisecond)
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

	enqueueKernelLog("debug", "databricks::sql::kernel", "callback probe")
	select {
	case got := <-ch:
		want := kernelLogRecord{"debug", "databricks::sql::kernel", "callback probe"}
		if got != want {
			t.Fatalf("enqueued record = %#v, want %#v", got, want)
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
	enqueueKernelLog("warn", "t", "before install")

	ch := make(chan kernelLogRecord, 1)
	logQueue.Store(&ch)
	before := kernelLogDropped()
	enqueueKernelLog("warn", "t", "keeps the one slot") // fills the buffer
	enqueueKernelLog("warn", "t", "must be dropped")    // buffer full → dropped
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
	done := make(chan string, 1)
	sink := &logSink{observe: func(_, _, message string) {
		if message == "boom" {
			panic("writer failure")
		}
		done <- message
	}}

	ch := make(chan kernelLogRecord, 2)
	go drainKernelLogs(ch, sink)
	defer close(ch)

	ch <- kernelLogRecord{"error", "t", "boom"} // triggers the panic
	ch <- kernelLogRecord{"info", "t", "after"} // must still be delivered
	select {
	case got := <-done:
		if got != "after" {
			t.Fatalf("delivered %q, want %q", got, "after")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain goroutine did not survive a writer panic")
	}
}
