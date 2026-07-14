//go:build !cgo || !databricks_kernel

package dbsql

import (
	"context"
	"errors"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
)

// In the default pure-Go build (no databricks_kernel tag) the kernel backend is
// not compiled in. Connecting with WithUseKernel(true) must fail loudly with a
// clear "not compiled in" error rather than silently falling back to Thrift or
// panicking on a nil backend. This guards the stub that fails closed.
func TestKernelBackendNotCompiledIn(t *testing.T) {
	connector, err := NewConnector(
		WithServerHostname("example.cloud.databricks.com"),
		WithPort(443),
		WithHTTPPath("/sql/1.0/endpoints/12346a5b5b0e123a"),
		WithAccessToken("supersecret"),
		WithUseKernel(true),
	)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	_, err = connector.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect with WithUseKernel(true) in a non-kernel build should error, got nil")
	}
	// Detect the build mismatch via the exported sentinel, not message text.
	if !errors.Is(err, dbsqlerr.ErrKernelNotCompiled) {
		t.Errorf("error should wrap ErrKernelNotCompiled; got: %v", err)
	}
}
