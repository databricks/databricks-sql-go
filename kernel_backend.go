//go:build cgo && databricks_kernel

package dbsql

import (
	"context"

	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// newKernelBackend builds the SEA-via-kernel backend from the driver config; the
// connector opens the session right after, matching the Thrift path. It maps the
// config fields the kernel backend currently reads — host, warehouse/http path,
// and PAT.
func newKernelBackend(_ context.Context, cfg *config.Config) (backend.Backend, error) {
	return kernel.New(kernel.Config{
		Host:        cfg.Host,
		HTTPPath:    cfg.HTTPPath,
		WarehouseID: cfg.WarehouseID,
		Token:       cfg.AccessToken,
	}), nil
}
