//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
)

// newKernelBackend builds a backend for a supported config and propagates a
// validation error otherwise. The exhaustive per-option reject/auth assertions
// live in the untagged TestValidateKernelConfig (kernel_config_test.go), which
// runs in the default CGO_ENABLED=0 build; this tagged smoke test just confirms
// the tagged wrapper wires validateKernelConfig + the kernel.Config assembly.
func TestNewKernelBackend(t *testing.T) {
	base := func() *config.Config {
		c := config.WithDefaults()
		c.Host = "h.databricks.com"
		c.Port = 443
		c.HTTPPath = "/sql/1.0/warehouses/abc"
		c.AccessToken = "dapi-x"
		return c
	}

	t.Run("supported config builds", func(t *testing.T) {
		if _, err := newKernelBackend(context.Background(), base()); err != nil {
			t.Errorf("a supported config should build cleanly, got %v", err)
		}
	})

	t.Run("validation error propagates", func(t *testing.T) {
		c := base()
		c.QueryTimeout = 30 * time.Second // rejected by validateKernelConfig
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("newKernelBackend should propagate the validation error")
		}
	})
}
