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

		// The kernel record crosses an async drain goroutine. Wait until that record
		// reaches the file before retargeting output and closing the test-owned file.
		waitForLogProbe(t, path, rustLogFileProbe)

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

// OFF must leave both the Go and kernel once-only slots available. A later session
// can then install the callback after the driver level is raised.
func TestKernelCallbackCanInstallAfterOff(t *testing.T) {
	if os.Getenv(logFileHelperEnv) == "1" {
		path := os.Getenv(logFilePathEnv)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // Parent supplies its temp path.
		if err != nil {
			t.Fatal(err)
		}
		logger.SetLogOutput(file)
		if err := logger.SetLogLevel("disabled"); err != nil {
			t.Fatal(err)
		}
		initKernelLogging()
		if logQueue.Load() != nil {
			t.Fatal("OFF installed a kernel log queue")
		}

		if err := logger.SetLogLevel("warn"); err != nil {
			t.Fatal(err)
		}
		initKernelLogging()
		if logQueue.Load() == nil {
			t.Fatal("WARN did not install the callback after OFF")
		}
		if err := trySetRetry(Config{Retry: &RetryConfig{
			MinWait:    5 * time.Second,
			MaxWait:    time.Second,
			MaxRetries: 1,
		}}); err != nil {
			t.Fatal(err)
		}
		waitForLogProbe(t, path, rustLogFileProbe)

		logger.SetLogOutput(os.Stderr)
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}

	logPath := filepath.Join(t.TempDir(), "kernel-off-first.log")
	cmd := exec.Command(os.Args[0], "-test.run=^TestKernelCallbackCanInstallAfterOff$") //nolint:gosec // Re-executes this test binary only.
	cmd.Env = append(os.Environ(), logFileHelperEnv+"=1", logFilePathEnv+"="+logPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("logging helper failed: %v\n%s", err, output)
	}
	contents, err := os.ReadFile(logPath) //nolint:gosec // Test-owned temporary path.
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), rustLogFileProbe) {
		t.Errorf("local log file is missing Rust kernel record: %q", contents)
	}
}

func waitForLogProbe(t *testing.T, path, probe string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		contents, err := os.ReadFile(path) //nolint:gosec // Test-owned temporary path.
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(contents), probe) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("kernel log did not reach the configured file: %q", contents)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
