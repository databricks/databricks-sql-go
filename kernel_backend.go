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
	// default CGO_ENABLED=0 build.
	auth, err := validateKernelConfig(cfg)
	if err != nil {
		return nil, err
	}

	kc := kernel.Config{
		Host:        cfg.Host,
		HTTPPath:    cfg.HTTPPath,
		WarehouseID: cfg.WarehouseID,
		Auth:        toKernelAuth(auth),
		Location:    cfg.Location,
		// Initial namespace: no kernel config setter, so the kernel backend applies
		// these post-connect via USE CATALOG / USE SCHEMA.
		Catalog: cfg.Catalog,
		Schema:  cfg.Schema,
		// Session confs (STATEMENT_TIMEOUT, QUERY_TAGS, TIMEZONE, metric-view, …) —
		// the same effective params the Thrift backend forwards (user SessionParams
		// plus any option-derived conf like metric-view metadata), so they flow to
		// the server identically with no per-backend translation. SPOG org routing
		// rides in HTTPPath's ?o= and is parsed kernel-side.
		SessionConf: cfg.EffectiveSessionParams(),
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

// toKernelAuth maps the untagged auth descriptor (resolved by validateKernelConfig
// in the default build) to the kernel package's cgo-side auth struct. Kept here
// (tagged) because kernel.Auth is defined in the cgo-tagged kernel package; the
// resolution/validation itself stays untagged in kernel_config.go.
func toKernelAuth(a *kernelAuth) kernel.Auth {
	switch a.mode {
	case kernelAuthM2M:
		return kernel.Auth{Mode: kernel.AuthM2M, ClientID: a.clientID, ClientSecret: a.clientSecret}
	case kernelAuthU2M:
		// Only the client id is sourced from Go; scopes and redirect port use the
		// kernel's defaults (the Go U2M authenticator carries neither, and the kernel
		// M2M/U2M defaults match Go's — see auth.U2MCredentialsProvider).
		return kernel.Auth{Mode: kernel.AuthU2M, ClientID: a.clientID}
	default:
		return kernel.Auth{Mode: kernel.AuthPAT, Token: a.token}
	}
}

// proxyForEndpoint (pure Go, no kernel dependency) lives in kernel_proxy.go so
// its test runs in the default CGO_ENABLED=0 build.
