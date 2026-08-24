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
		deadline := time.Now().Add(5 * time.Second)
		for {
			contents, readErr := os.ReadFile(path) //nolint:gosec // Parent supplies its temp path.
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Contains(string(contents), rustLogFileProbe) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("kernel log did not reach the configured file: %q", contents)
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
