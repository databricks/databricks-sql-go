package dbsql

import (
	"errors"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/auth/pat"
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
func validateKernelConfig(cfg *config.Config) (token string, err error) {
	// Initial namespace (WithInitialNamespace): no kernel C-ABI setter yet, so the
	// session would run in the default namespace and unqualified names would
	// resolve differently than Thrift.
	if cfg.Catalog != "" || cfg.Schema != "" {
		return "", errors.New("databricks: WithInitialNamespace (catalog/schema) is not yet supported by the kernel backend; " +
			"omit it or use the default (Thrift) backend")
	}
	// EnableMetricViewMetadata: maps to a server session conf we want to route
	// backend-neutrally rather than duplicate here.
	if cfg.EnableMetricViewMetadata {
		return "", errors.New("databricks: WithEnableMetricViewMetadata is not yet supported by the kernel backend; " +
			"omit it or use the default (Thrift) backend")
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
		return "", errors.New("databricks: only personal access token (WithAccessToken) auth is supported by the kernel backend; " +
			"OAuth (M2M/U2M), token-provider, external/static, and federated authenticators are not yet supported — " +
			"use PAT or the default (Thrift) backend")
	}
	if token == "" {
		return "", errors.New("databricks: the kernel backend requires a personal access token; " +
			"set one with WithAccessToken (or a *pat.PATAuth via WithAuthenticator)")
	}
	// WithTimeout maps to a per-statement server timeout on Thrift
	// (TExecuteStatementReq.QueryTimeout); the kernel C ABI exposes no equivalent,
	// so reject it rather than run the query with no server-side timeout.
	if cfg.QueryTimeout > 0 {
		return "", errors.New("databricks: WithTimeout (server query timeout) is not yet supported by the kernel backend; " +
			"omit it or use the default (Thrift) backend")
	}
	// WithRetries(-1) explicitly disables retries, but the kernel retries
	// internally below the C ABI with no user-facing toggle — so a disable request
	// would be silently violated. Reject it. Positive/default RetryMax is fine: the
	// kernel provides retries (just not user-tunable), documented in doc.go.
	if cfg.RetryMax < 0 {
		return "", errors.New("databricks: disabling retries via WithRetries is not supported by the kernel backend " +
			"(the kernel retries internally); omit it or use the default (Thrift) backend")
	}
	return token, nil
}
