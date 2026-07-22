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
	// Reject options the kernel path can't honor yet + resolve the auth form. The
	// validation is pure Go and lives in kernel_config.go (untagged) so its tests —
	// including the exhaustiveness guard against a dropped Config field — run in the
	// default CGO_ENABLED=0 build. It returns kernel.Auth directly.
	kauth, err := validateKernelConfig(cfg)
	if err != nil {
		return nil, err
	}

	// Assemble the flat kernel.Config from the driver config. The field-by-field
	// mapping (including the experimental TLS forwarding) is the pure, cgo-free
	// buildKernelConfig in kernel_config.go, unit-tested under CGO_ENABLED=0.
	// SPOG org routing rides in HTTPPath's ?o= and is parsed kernel-side.
	kc := buildKernelConfig(cfg, kauth)
	// Proxy: an explicit WithKernelProxy (carried on KernelExperimental) takes
	// precedence over the environment — an explicit proxy is a deliberate
	// override, and consulting both would be ambiguous. Otherwise mirror the
	// Thrift path, which uses http.ProxyFromEnvironment, by reading the same
	// HTTP(S)_PROXY / NO_PROXY environment for the kernel. resolveKernelProxy is
	// pure Go (untagged, in kernel_config.go) with its own unit coverage, so it
	// (and proxyForEndpoint) resolve here rather than in buildKernelConfig.
	resolveKernelProxy(cfg, &kc)
	return kernel.New(kc), nil
}

// proxyForEndpoint (pure Go, no kernel dependency) lives in kernel_proxy.go so
// its test runs in the default CGO_ENABLED=0 build.
