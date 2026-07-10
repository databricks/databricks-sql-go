//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/auth/pat"
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
	// Auth: the kernel backend authenticates with a PAT only (kc.Token below).
	// Any other authenticator — OAuth M2M/U2M, a token provider, external/static
	// token, federated — sets cfg.Authenticator but leaves cfg.AccessToken empty,
	// so an empty PAT would reach the kernel and fail with an opaque
	// Unauthenticated error. Reject it here so the failure names the cause, per
	// the doc.go contract. nil / NoopAuth / PATAuth are the PAT-or-none cases.
	switch cfg.Authenticator.(type) {
	case nil, *noop.NoopAuth, *pat.PATAuth:
		// PAT (or no explicit authenticator) — supported.
	default:
		return nil, errors.New("databricks: only personal access token (WithAccessToken) auth is supported by the kernel backend; " +
			"OAuth (M2M/U2M), token-provider, external/static, and federated authenticators are not yet supported — " +
			"use PAT or the default (Thrift) backend")
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
	return proxyForEndpointFunc(cfg, http.ProxyFromEnvironment)
}

// proxyForEndpointFunc is the testable core: it builds the endpoint request and
// asks resolve for the proxy, returning "" (direct) on any error, no proxy, or an
// unbuildable endpoint. resolve is http.ProxyFromEnvironment in production; tests
// inject a deterministic resolver to exercise proxy-set / NO_PROXY / direct
// without depending on http.ProxyFromEnvironment's process-wide env caching.
func proxyForEndpointFunc(cfg *config.Config, resolve func(*http.Request) (*url.URL, error)) string {
	endpoint, err := cfg.ToEndpointURL()
	if err != nil {
		return ""
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return ""
	}
	proxyURL, err := resolve(req)
	if err != nil || proxyURL == nil {
		return ""
	}
	return proxyURL.String()
}
