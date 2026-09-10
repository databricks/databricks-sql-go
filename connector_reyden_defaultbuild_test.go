//go:build !databricks_kernel

package dbsql

import (
	"context"
	"errors"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
)

// TestReydenDefaultBuildKernelNotCompiled asserts that in the default pure-Go build
// newKernelBackend reports the kernel is not compiled in. It is gated to the default
// build because under -tags databricks_kernel newKernelBackend is the real (compiled)
// implementation and does not return this error.
func TestReydenDefaultBuildKernelNotCompiled(t *testing.T) {
	t.Run("Default build's newKernelBackend returns 'not compiled' error", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.UseKernel = true

		be, err := newKernelBackend(context.Background(), cfg)

		assert.Nil(t, be)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, dbsqlerr.ErrKernelNotCompiled),
			"default build should not have kernel backend compiled in")
	})
}
