//go:build !cgo || !databricks_kernel

package dbsql

import (
	"context"
	"errors"

	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// newKernelBackend is the stub compiled when the kernel backend is not built in.
// It fails loudly rather than silently falling back to Thrift, so a mismatch
// between WithUseKernel and the build tags surfaces at connect time. The real
// implementation is in kernel_backend.go.
func newKernelBackend(_ context.Context, _ *config.Config) (backend.Backend, error) {
	return nil, errors.New("databricks: the SEA-via-kernel backend is not compiled into this binary; " +
		"rebuild with -tags databricks_kernel and CGO_ENABLED=1, or unset WithUseKernel")
}
