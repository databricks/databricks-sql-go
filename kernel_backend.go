//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"errors"
	"net/http"

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
	// A few options aren't wired for the kernel backend yet. Fail loudly rather
	// than silently ignore them (which would behave differently than Thrift):
	//   - Catalog/Schema (WithInitialNamespace): no kernel C-ABI setter yet, so
	//     the session would run in the default namespace and unqualified names
	//     would resolve differently.
	//   - EnableMetricViewMetadata: deferred — it maps to a server session conf,
	//     which we want to route backend-neutrally rather than duplicate here.
	if cfg.Catalog != "" || cfg.Schema != "" {
		return nil, errors.New("databricks: WithInitialNamespace (catalog/schema) is not yet supported by the kernel backend; " +
			"omit it or use the default (Thrift) backend")
	}
	if cfg.EnableMetricViewMetadata {
		return nil, errors.New("databricks: WithEnableMetricViewMetadata is not yet supported by the kernel backend; " +
			"omit it or use the default (Thrift) backend")
	}

	kc := kernel.Config{
		Host:        cfg.Host,
		HTTPPath:    cfg.HTTPPath,
		WarehouseID: cfg.WarehouseID,
		Token:       cfg.AccessToken,
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

// proxyForEndpoint resolves the proxy the Thrift path would use for this
// connection, via the same http.ProxyFromEnvironment (HTTP(S)_PROXY / NO_PROXY)
// the Thrift transport applies at request time. Building the endpoint request
// lets ProxyFromEnvironment apply the NO_PROXY rules for this exact host, so the
// kernel sees the same effective proxy decision — returning "" (direct) when
// NO_PROXY excludes the host or no proxy is set. No extra dependency: this is the
// stdlib function the driver already relies on.
func proxyForEndpoint(cfg *config.Config) string {
	endpoint, err := cfg.ToEndpointURL()
	if err != nil {
		return ""
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return ""
	}
	proxyURL, err := http.ProxyFromEnvironment(req)
	if err != nil || proxyURL == nil {
		return ""
	}
	return proxyURL.String()
}
