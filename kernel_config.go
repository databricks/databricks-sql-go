package dbsql

import (
	"errors"
	"fmt"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/auth/pat"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
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
// resolves the kernel.Auth descriptor the kernel authenticates with (PAT, or OAuth
// M2M/U2M). Options it does NOT reject are either forwarded by newKernelBackend or
// intentionally accepted-but-inert (documented in doc.go and asserted by
// TestKernelConfigFieldsClassified). It returns kernel.Auth directly (no dbsql-side
// duplicate) — kernel's auth types are in an untagged file, so this untagged,
// default-build code can build them without pulling in cgo.
//
// Every rejection wraps errors.ErrNotSupportedByKernel so a caller can detect the
// "kernel can't honor this option" case with errors.Is (e.g. to fall back to the
// default backend) instead of matching on message text.
func validateKernelConfig(cfg *config.Config) (kernel.Auth, error) {
	// Initial namespace (WithInitialNamespace) is forwarded, not rejected: the
	// kernel C ABI has no catalog/schema setter, so KernelBackend.OpenSession
	// selects it post-connect with USE CATALOG / USE SCHEMA. No per-backend handling
	// needed here.
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
		return kernel.Auth{}, fmt.Errorf("databricks: a non-https protocol is %w "+
			"(it connects over https); use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	if cfg.Port != 0 && cfg.Port != 443 {
		return kernel.Auth{}, fmt.Errorf("databricks: a non-default port (WithPort) is %w "+
			"(it connects on 443); omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// Transport (WithTransport, a custom http.RoundTripper carrying a custom CA
	// bundle / mTLS / proxy): the kernel uses its own Rust HTTP stack below the C
	// ABI and never sees a Go RoundTripper, so a custom Transport would be silently
	// ignored. Reject it per the "nothing silently ignored" contract. (The kernel
	// does honor HTTPS_PROXY and InsecureSkipVerify through their own mappings; only
	// a wholesale custom Transport is unsupported.)
	if cfg.Transport != nil {
		return kernel.Auth{}, fmt.Errorf("databricks: a custom WithTransport (RoundTripper) is %w "+
			"(the kernel uses its own HTTP stack); use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// Auth: resolve the kernel auth form (PAT / OAuth M2M / U2M) off cfg.Authenticator,
	// the single source of truth. resolveKernelAuth rejects unsupported authenticators
	// loudly so the failure names the cause instead of surfacing as an opaque
	// Unauthenticated.
	kauth, err := resolveKernelAuth(cfg)
	if err != nil {
		return kernel.Auth{}, err
	}
	// WithTimeout maps to a per-statement server timeout on Thrift
	// (TExecuteStatementReq.QueryTimeout); the kernel C ABI exposes no equivalent,
	// so reject it rather than run the query with no server-side timeout.
	if cfg.QueryTimeout > 0 {
		return kernel.Auth{}, fmt.Errorf("databricks: WithTimeout (server query timeout) is %w; "+
			"omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// WithRetries(-1) explicitly disables retries, but the kernel retries
	// internally below the C ABI with no user-facing toggle — so a disable request
	// would be silently violated. Reject it. Positive/default RetryMax is fine: the
	// kernel provides retries (just not user-tunable), documented in doc.go.
	if cfg.RetryMax < 0 {
		return kernel.Auth{}, fmt.Errorf("databricks: disabling retries via WithRetries is %w "+
			"(the kernel retries internally); omit it or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	return kauth, nil
}

// resolveKernelAuth picks the kernel auth form from the config. The kernel backend
// drives the kernel's own OAuth flow from raw credentials rather than reusing the Go
// authenticator's Authenticate method. It reads those credentials off
// cfg.Authenticator — the
// single source of truth for auth, so the last WithX option applied wins for both
// backends (matching Thrift's last-writer-wins on cfg.Authenticator). The M2M/U2M
// authenticator types are unexported, so it asserts the small
// kernel.M2MCredentialsProvider / kernel.U2MCredentialsProvider interfaces they
// satisfy structurally:
//   - implements M2MCredentialsProvider → M2M (client id + secret)
//   - implements U2MCredentialsProvider → U2M (browser/PKCE; kernel-owned flow)
//   - PAT / nil / noop                  → PAT (from AccessToken or a *pat.PATAuth)
//   - anything else                     → rejected loudly (token-provider / external
//     / static / federated), so the failure names the cause instead of surfacing as
//     an opaque Unauthenticated.
func resolveKernelAuth(cfg *config.Config) (kernel.Auth, error) {
	switch a := cfg.Authenticator.(type) {
	case kernel.M2MCredentialsProvider:
		// The kernel's set_auth_m2m takes no scopes and applies "all-apis" itself, so
		// a custom scope set can't be forwarded — reject it instead of silently
		// downgrading (a least-privilege caller would get broader-than-asked access).
		if !kernel.M2MScopesSupported(a.M2MScopes()) {
			return kernel.Auth{}, fmt.Errorf("databricks: custom M2M OAuth scopes are %w "+
				"(the kernel applies its default scopes); drop the custom scopes "+
				"(use m2m.NewAuthenticator) or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
		}
		clientID, clientSecret := a.M2MCredentials()
		return kernel.Auth{Mode: kernel.AuthM2M, ClientID: clientID, ClientSecret: clientSecret}, nil
	case kernel.U2MCredentialsProvider:
		// Go sources only the client id for U2M; kernel.Auth.Scopes / RedirectPort
		// are left zero so setAuth passes the kernel's defaults. Go exposes no
		// user-facing option for U2M scopes or redirect port on either backend today
		// (the native Thrift path hardcodes both), so there is nothing to forward —
		// but the kernel.Auth fields + setAuth wiring model the full set_auth_u2m
		// surface for if/when such an option is added (see kernel.Auth docs).
		return kernel.Auth{Mode: kernel.AuthU2M, ClientID: a.U2MClientID()}, nil
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
			// Missing required config (not an unsupported-feature rejection), so this is
			// intentionally NOT wrapped with ErrNotSupportedByKernel — a caller shouldn't
			// fall back to Thrift for a forgotten token, it should supply one.
			return kernel.Auth{}, errors.New("databricks: the kernel backend requires a personal access token; " +
				"set one with WithAccessToken (or a *pat.PATAuth via WithAuthenticator)")
		}
		return kernel.Auth{Mode: kernel.AuthPAT, Token: token}, nil
	default:
		// Unsupported authenticator: wrap ErrNotSupportedByKernel so a caller can
		// detect the "kernel can't honor this auth" case with errors.Is and fall back
		// to the default backend, rather than substring-matching this message. This is
		// the same contract every other unsupported kernel option in this file follows
		// and that doc.go advertises. (The empty-PAT case above is intentionally NOT
		// wrapped — a missing token is misconfiguration to fix, not a feature the
		// kernel can't honor.)
		return kernel.Auth{}, fmt.Errorf("databricks: this authenticator is %w; "+
			"PAT (WithAccessToken) and OAuth M2M/U2M (WithClientCredentials / authType) are supported, but "+
			"token-provider, external/static, and federated authenticators are not — "+
			"use one of those or the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
}
