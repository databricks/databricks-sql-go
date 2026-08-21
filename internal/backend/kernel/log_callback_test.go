//go:build cgo && databricks_kernel

package kernel

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime/cgo"
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

func TestLogCallbackRoundTrip(t *testing.T) {
	type record struct{ level, target, message string }
	received := make(chan record, 1)
	h := cgo.NewHandle(&logSink{observe: func(level, target, message string) {
		received <- record{level, target, message}
	}})
	defer h.Delete()

	invokeLogTrampolineForTest(h, "debug", "databricks::sql::kernel", "callback probe")
	select {
	case got := <-received:
		want := record{"debug", "databricks::sql::kernel", "callback probe"}
		if got != want {
			t.Fatalf("callback record = %#v, want %#v", got, want)
		}
	default:
		t.Fatal("callback did not reach the Go sink")
	}
}

func TestLogCallbackPanicDoesNotCrossABI(t *testing.T) {
	h := cgo.NewHandle(&logSink{observe: func(string, string, string) {
		panic("sink failure")
	}})
	defer h.Delete()

	// The trampoline's recovery boundary converts a logger panic into a dropped
	// diagnostic instead of allowing it to cross cgo and terminate the process.
	invokeLogTrampolineForTest(h, "error", "databricks::sql::kernel", "boom")
}
