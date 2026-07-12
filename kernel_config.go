package dbsql

import (
	"errors"

	"github.com/databricks/databricks-sql-go/auth"
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

// kernelAuthMode is the auth form the kernel backend will use for a connection.
type kernelAuthMode int

const (
	kernelAuthPAT kernelAuthMode = iota // personal access token
	kernelAuthM2M                       // OAuth client-credentials (client id + secret)
	kernelAuthU2M                       // OAuth user-to-machine (browser/PKCE, kernel-owned)
)

// kernelAuth is the resolved auth descriptor validateKernelConfig hands to
// newKernelBackend. Exactly the fields for `mode` are populated; the backend maps
// it to the matching kernel_session_config_set_auth_* setter. Kept as a value type
// (no secrets logged) — the backend zeroes nothing, matching how the PAT token was
// previously passed as a plain string.
type kernelAuth struct {
	mode         kernelAuthMode
	token        string // PAT
	clientID     string // M2M + U2M (U2M: the cloud-inferred Go client id)
	clientSecret string // M2M
}

// validateKernelConfig enforces the kernel backend's "nothing silently ignored"
// contract: it rejects every option the kernel path can't yet honor with a clear
// error (rather than dropping it, which would behave differently than Thrift) and
// resolves the auth descriptor the kernel authenticates with (PAT, or OAuth
// M2M/U2M). Options it does NOT reject are either forwarded by newKernelBackend or
// intentionally accepted-but-inert (documented in doc.go and asserted by
// TestKernelConfigFieldsClassified).
func validateKernelConfig(cfg *config.Config) (*kernelAuth, error) {
	// Initial namespace (WithInitialNamespace) is forwarded, not rejected: the
	// kernel C ABI has no catalog/schema setter, so KernelBackend.OpenSession
	// selects it post-connect with USE CATALOG / USE SCHEMA (the OSS ODBC driver's
	// workaround). No per-backend handling needed here.
	// EnableMetricViewMetadata is forwarded, not rejected: config.EffectiveSessionParams
	// folds its server conf (spark.sql.thriftserver.metadata.metricview.enabled=true)
	// into SessionConf backend-neutrally, so the kernel path sends the identical conf
	// the Thrift path does. No per-backend handling needed here.
	// Port / Protocol: the kernel C ABI takes only a bare host and connects over
	// https:443; it has no port or scheme setter. The Thrift path honors a custom
	// port/scheme via ToEndpointURL, so a non-default value here would be silently
	// ignored on the kernel path (it would just hit 443) — reject it instead, per
	// the "nothing silently ignored" contract. Defaults (https/443) are fine.
	if cfg.Protocol != "" && cfg.Protocol != "https" {
		return nil, errors.New("databricks: a non-https protocol is not supported by the kernel backend " +
			"(it connects over https); use the default (Thrift) backend")
	}
	if cfg.Port != 0 && cfg.Port != 443 {
		return nil, errors.New("databricks: a non-default port (WithPort) is not supported by the kernel backend " +
			"(it connects on 443); omit it or use the default (Thrift) backend")
	}
	auth, err := resolveKernelAuth(cfg)
	if err != nil {
		return nil, err
	}
	// WithTimeout maps to a per-statement server timeout on Thrift
	// (TExecuteStatementReq.QueryTimeout); the kernel C ABI exposes no equivalent,
	// so reject it rather than run the query with no server-side timeout.
	if cfg.QueryTimeout > 0 {
		return nil, errors.New("databricks: WithTimeout (server query timeout) is not yet supported by the kernel backend; " +
			"omit it or use the default (Thrift) backend")
	}
	// WithRetries(-1) explicitly disables retries, but the kernel retries
	// internally below the C ABI with no user-facing toggle — so a disable request
	// would be silently violated. Reject it. Positive/default RetryMax is fine: the
	// kernel provides retries (just not user-tunable), documented in doc.go.
	if cfg.RetryMax < 0 {
		return nil, errors.New("databricks: disabling retries via WithRetries is not supported by the kernel backend " +
			"(the kernel retries internally); omit it or use the default (Thrift) backend")
	}
	return auth, nil
}

// resolveKernelAuth picks the kernel auth form from the config. The kernel backend
// drives the kernel's own OAuth flow from raw credentials (mirroring pyo3/napi and
// the Node/Python kernel bindings) rather than reusing the Go authenticator's
// Authenticate method. It reads those credentials off cfg.Authenticator — the
// single source of truth for auth, so the last WithX option applied wins for both
// backends (matching Thrift's last-writer-wins on cfg.Authenticator). The M2M/U2M
// authenticator types are unexported, so it asserts the small
// auth.M2MCredentialsProvider / auth.U2MCredentialsProvider interfaces they satisfy:
//   - implements M2MCredentialsProvider → M2M (client id + secret)
//   - implements U2MCredentialsProvider → U2M (browser/PKCE; kernel-owned flow)
//   - PAT / nil / noop                  → PAT (from AccessToken or a *pat.PATAuth)
//   - anything else                     → rejected loudly (token-provider / external
//     / static / federated), so the failure names the cause instead of surfacing as
//     an opaque Unauthenticated.
func resolveKernelAuth(cfg *config.Config) (*kernelAuth, error) {
	switch a := cfg.Authenticator.(type) {
	case auth.M2MCredentialsProvider:
		clientID, clientSecret := a.M2MCredentials()
		return &kernelAuth{mode: kernelAuthM2M, clientID: clientID, clientSecret: clientSecret}, nil
	case auth.U2MCredentialsProvider:
		return &kernelAuth{mode: kernelAuthU2M, clientID: a.U2MClientID()}, nil
	case nil, *noop.NoopAuth, *pat.PATAuth:
		// PAT (or no explicit authenticator). WithAccessToken sets both
		// cfg.AccessToken and a *pat.PATAuth, but WithAuthenticator(&pat.PATAuth{...})
		// sets only the authenticator and leaves cfg.AccessToken empty — so take the
		// token from the authenticator when cfg.AccessToken didn't carry it.
		token := cfg.AccessToken
		if token == "" {
			if p, ok := a.(*pat.PATAuth); ok {
				token = p.AccessToken
			}
		}
		if token == "" {
			return nil, errors.New("databricks: the kernel backend requires a personal access token; " +
				"set one with WithAccessToken (or a *pat.PATAuth via WithAuthenticator)")
		}
		return &kernelAuth{mode: kernelAuthPAT, token: token}, nil
	default:
		return nil, errors.New("databricks: this authenticator is not supported by the kernel backend; " +
			"PAT (WithAccessToken) and OAuth M2M/U2M (WithClientCredentials / authType) are supported, but " +
			"token-provider, external/static, and federated authenticators are not — " +
			"use one of those or the default (Thrift) backend")
	}
}
