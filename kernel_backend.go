//go:build cgo && databricks_kernel

package dbsql

import (
	"context"

	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// newKernelBackend builds the SEA-via-kernel backend from the driver config; the
// connector opens the session right after, matching the Thrift path. It reads the
// same config fields Thrift does and translates them to the kernel's flat
// connection config, so the user-facing options are unchanged — only the routing
// differs. The public API adds nothing beyond WithUseKernel.
func newKernelBackend(_ context.Context, cfg *config.Config) (backend.Backend, error) {
	// Reject options the kernel path can't honor yet + resolve the PAT. The
	// validation is pure Go and lives in kernel_config.go (untagged) so its tests —
	// including the exhaustiveness guard against a dropped Config field — run in the
	// default CGO_ENABLED=0 build.
	token, err := validateKernelConfig(cfg)
	if err != nil {
		return nil, err
	}

	kc := kernel.Config{
		Host:        cfg.Host,
		HTTPPath:    cfg.HTTPPath,
		WarehouseID: cfg.WarehouseID,
		Token:       token,
		Location:    cfg.Location,
		// Session confs (STATEMENT_TIMEOUT, QUERY_TAGS, TIMEZONE, …) — the same
		// SessionParams map the Thrift backend forwards, so they flow to the
		// server identically with no per-backend translation. SPOG org routing
		// rides in HTTPPath's ?o= and is parsed kernel-side.
		SessionConf: cfg.SessionParams,
	}
	// TLS: the driver honors TLSConfig only for InsecureSkipVerify (see
	// internal/client), so map exactly that knob to the kernel.
	if cfg.TLSConfig != nil && cfg.TLSConfig.InsecureSkipVerify {
		kc.TLSSkipVerify = true
	}
	// Proxy: the Thrift path uses http.ProxyFromEnvironment; mirror it by reading
	// the same HTTP(S)_PROXY / NO_PROXY environment for the kernel.
	kc.ProxyURL = proxyForEndpoint(cfg)
	return kernel.New(kc), nil
}

// proxyForEndpoint (pure Go, no kernel dependency) lives in kernel_proxy.go so
// its test runs in the default CGO_ENABLED=0 build.
