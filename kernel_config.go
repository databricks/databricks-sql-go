package dbsql

import (
	"fmt"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/auth/pat"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// The kernel backend's option-validation is pure Go (it reads config.Config and
// returns an error or a resolved PAT), so keeping it untagged lets its tests —
// including the reflective exhaustiveness check that guards against a future
// Config field being silently dropped — run under CGO_ENABLED=0. The tagged
// newKernelBackend calls validateKernelConfig, then assembles the cgo kernel.Config.

// validateKernelConfig enforces the kernel backend's "nothing silently ignored"
// contract: it rejects every option the kernel path can't yet honor with a clear
// error (rather than dropping it, which would behave differently than Thrift) and
// resolves the PAT the kernel authenticates with. On success it returns the token
// to use. Options it does NOT reject are either forwarded by newKernelBackend or
// intentionally accepted-but-inert (documented in doc.go and asserted by
// TestKernelConfigFieldsClassified).
//
// Every rejection wraps errors.ErrNotSupportedByKernel so a caller can detect the
// "kernel can't honor this option" case with errors.Is (e.g. to fall back to the
// default backend) instead of matching on message text.
func validateKernelConfig(cfg *config.Config) (token string, err error) {
	// Initial namespace (WithInitialNamespace): no kernel C-ABI setter yet, so the
	// session would run in the default namespace and unqualified names would
	// resolve differently than Thrift.
	if cfg.Catalog != "" || cfg.Schema != "" {
		return "", fmt.Errorf("databricks: WithInitialNamespace (catalog/schema) is %w; "+
			"omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// EnableMetricViewMetadata: maps to a server session conf we want to route
	// backend-neutrally rather than duplicate here.
	if cfg.EnableMetricViewMetadata {
		return "", fmt.Errorf("databricks: WithEnableMetricViewMetadata is %w; "+
			"omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// Port / Protocol: the kernel C ABI takes only a bare host and connects over
	// https:443; it has no port or scheme setter. The Thrift path honors a custom
	// port/scheme via ToEndpointURL, so a non-default value here would be silently
	// ignored on the kernel path (it would just hit 443) — reject it instead, per
	// the "nothing silently ignored" contract. Defaults (https/443) are fine.
	if cfg.Protocol != "" && cfg.Protocol != "https" {
		return "", fmt.Errorf("databricks: a non-https protocol is %w "+
			"(it connects over https); use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	if cfg.Port != 0 && cfg.Port != 443 {
		return "", fmt.Errorf("databricks: a non-default port (WithPort) is %w "+
			"(it connects on 443); omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// Transport (WithTransport, a custom http.RoundTripper carrying a custom CA
	// bundle / mTLS / proxy): the kernel uses its own Rust HTTP stack below the C
	// ABI and never sees a Go RoundTripper, so a custom Transport would be silently
	// ignored. Reject it per the "nothing silently ignored" contract. (The kernel
	// does honor HTTPS_PROXY and InsecureSkipVerify through their own mappings; only
	// a wholesale custom Transport is unsupported.)
	if cfg.Transport != nil {
		return "", fmt.Errorf("databricks: a custom WithTransport (RoundTripper) is %w "+
			"(the kernel uses its own HTTP stack); use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// Auth: the kernel backend authenticates with a PAT only. Any other
	// authenticator sets cfg.Authenticator but leaves cfg.AccessToken empty, so an
	// empty PAT would reach the kernel and fail with an opaque Unauthenticated
	// error. Reject it here so the failure names the cause.
	token = cfg.AccessToken
	switch a := cfg.Authenticator.(type) {
	case nil, *noop.NoopAuth:
		// No explicit authenticator — token comes from cfg.AccessToken (may be
		// empty; caught below).
	case *pat.PATAuth:
		// WithAccessToken sets both cfg.AccessToken and this authenticator, but
		// WithAuthenticator(&pat.PATAuth{...}) sets only the authenticator and leaves
		// cfg.AccessToken empty. Take the token from the authenticator when
		// cfg.AccessToken didn't carry it, so both PAT paths work.
		if token == "" {
			token = a.AccessToken
		}
	default:
		return "", fmt.Errorf("databricks: only personal access token (WithAccessToken) auth is supported by the kernel backend; "+
			"OAuth (M2M/U2M), token-provider, external/static, and federated authenticators are %w — "+
			"use PAT or the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	if token == "" {
		// Missing required config (not an unsupported-feature rejection), so this is
		// intentionally NOT wrapped with ErrNotSupportedByKernel.
		return "", fmt.Errorf("databricks: the kernel backend requires a personal access token; " +
			"set one with WithAccessToken (or a *pat.PATAuth via WithAuthenticator)")
	}
	// WithTimeout maps to a per-statement server timeout on Thrift
	// (TExecuteStatementReq.QueryTimeout); the kernel C ABI exposes no equivalent,
	// so reject it rather than run the query with no server-side timeout.
	if cfg.QueryTimeout > 0 {
		return "", fmt.Errorf("databricks: WithTimeout (server query timeout) is %w; "+
			"omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// WithRetries(-1) explicitly disables retries, but the kernel retries
	// internally below the C ABI with no user-facing toggle — so a disable request
	// would be silently violated. Reject it. Positive/default RetryMax is fine: the
	// kernel provides retries (just not user-tunable), documented in doc.go.
	if cfg.RetryMax < 0 {
		return "", fmt.Errorf("databricks: disabling retries via WithRetries is %w "+
			"(the kernel retries internally); omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	return token, nil
}
