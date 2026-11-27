//go:build cgo && databricks_kernel

package kernel

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
)

func TestMain(tests *testing.M) {
	// TODO(databricks/databricks-sql-kernel#509): Remove once the bindings block SIGPIPE on kernel runtime threads.
	signal.Ignore(syscall.SIGPIPE)
	os.Exit(tests.Run())
}
