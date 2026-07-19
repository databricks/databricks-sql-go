package dbsql

import (
	"errors"
	"fmt"
	"strconv"
	"time"

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
	// WithRetries (RetryWaitMin / RetryWaitMax / RetryMax) is honored on the kernel
	// path: newKernelBackend forwards it via kernelRetryConfig →
	// kernel_session_config_set_retry_config, and a negative RetryMax (the disable
	// form) maps to zero kernel retries. So it is neither rejected nor silently
	// ignored here.
	return kauth, nil
}

// buildKernelConfig assembles the kernel's flat connection Config from the driver
// config and the already-resolved auth descriptor. It is the pure, cgo-free half
// of newKernelBackend (which just calls this, then kernel.New + proxy resolution),
// extracted here so the field-by-field mapping — in particular the experimental
// KernelExperimental TLS forwarding, which has no other unit coverage — is
// asserted under CGO_ENABLED=0 (see TestBuildKernelConfig). Keep it in lockstep
// with newKernelBackend's kernel.Config assembly.
func buildKernelConfig(cfg *config.Config, kauth kernel.Auth) kernel.Config {
	kc := kernel.Config{
		Host:        cfg.Host,
		HTTPPath:    cfg.HTTPPath,
		WarehouseID: cfg.WarehouseID,
		Auth:        kauth,
		Location:    cfg.Location,
		// Initial namespace: no kernel config setter, so the kernel backend applies
		// these post-connect via USE CATALOG / USE SCHEMA.
		Catalog: cfg.Catalog,
		Schema:  cfg.Schema,
		// Session confs (STATEMENT_TIMEOUT, QUERY_TAGS, TIMEZONE, metric-view, …) —
		// the same effective params the Thrift backend forwards, so they flow to the
		// server identically with no per-backend translation.
		SessionConf: cfg.EffectiveSessionParams(),
	}
	// TLS: the driver honors TLSConfig only for InsecureSkipVerify (see
	// internal/client), so map exactly that knob to the kernel.
	if cfg.TLSConfig != nil && cfg.TLSConfig.InsecureSkipVerify {
		kc.TLSSkipVerify = true
	}
	// Experimental kernel-only TLS knobs (WithKernelTrustedCerts /
	// WithKernelSkipHostnameVerify), if any. These have no Thrift-path equivalent
	// (the connector rejects them on that path) and are forwarded verbatim to the
	// kernel C ABI in OpenSession.
	if ke := cfg.KernelExperimental; ke != nil {
		kc.TLSTrustedCertsPEM = ke.TLSTrustedCertsPEM
		kc.TLSSkipHostnameVerify = ke.TLSSkipHostnameVerify
		// Kernel-only CloudFetch in-memory-chunk knob (WithKernelMaxChunksInMemory).
		// Injected into the kernel backend's own SessionConf ONLY (not via
		// EffectiveSessionParams, which both backends share) as the client-only key
		// the kernel reads and strips before the SEA wire — so it never leaks to the
		// server or the Thrift path. Zero/negative keeps the kernel default (16).
		if ke.MaxChunksInMemory > 0 {
			// EffectiveSessionParams returned a fresh map, so mutating it is safe.
			kc.SessionConf[config.KernelMaxChunksInMemoryConfKey] = strconv.Itoa(ke.MaxChunksInMemory)
		}
	}
	// Retry / backoff policy from WithRetries (+ the kernel-only overall budget).
	// nil leaves the kernel's own default policy in place.
	kc.Retry = kernelRetryConfig(cfg)
	return kc
}

// kernelRetryPlaceholderWaits are the backoff bounds substituted when the caller
// gave no valid wait range but a definite attempt count to honor — the disable form
// (RetryMax < 0), or WithRetries(n, 0, 0) where WithDefaults' waits were overwritten
// to zero. The kernel setter validates the range (it rejects min == 0 / max < min),
// so a valid one must be passed even when the attempts make the backoff moot; any
// positive min<=max works, and the kernel's own defaults (1s / 60s) are the natural
// choice.
const (
	kernelRetryPlaceholderWaitMin = 1 * time.Second
	kernelRetryPlaceholderWaitMax = 60 * time.Second
)

// kernelRetryConfig resolves the driver's WithRetries policy (RetryWaitMin /
// RetryWaitMax / RetryMax) into the kernel retry descriptor so the caller's
// backoff/attempt policy is authoritative on the kernel path — matching the Thrift
// path, which applies exactly these values via go-retryablehttp, and the same
// "retries after the initial attempt" semantics the kernel setter uses. The
// connector's WithDefaults guarantees positive waits (1s / 30s) and RetryMax 4.
//
// A negative RetryMax is the WithRetries disable form (retryablehttp treats it as
// zero retries); it maps to MaxRetries == 0 and is honored EVEN when the waits are
// zero (WithRetries(-1, 0, 0) is the idiomatic disable), by substituting a valid
// placeholder range the setter accepts — the backoff is unused with no retries.
//
// A positive RetryMax with a degenerate wait range is honored the same way: the
// caller's attempt count is authoritative and the placeholder waits are substituted
// so the setter accepts the range. This case is reachable on the NORMAL option path,
// not just from a hand-built Config: WithDefaults() runs before options, so
// WithRetries(n, 0, 0) — valid per its own godoc, which promises sane wait defaults —
// overwrites the waits back to zero and lands here with the caller's RetryMax. Without
// this the caller's RetryMax would be silently dropped to the kernel's default policy.
//
// Returns nil — leaving the kernel's own default policy in place — only when RetryMax
// is also zero on a degenerate range: the zero-value signature of a Config assembled
// without WithDefaults (unusual outside tests), where there is no caller attempt count
// to preserve. Substituting placeholders there would force zero retries onto a
// hand-built config; the kernel default is the safer choice, and a stray zero can
// never fail the connect either way.
func kernelRetryConfig(cfg *config.Config) *kernel.RetryConfig {
	// Kernel-only overall retry budget (WithKernelRetryOverallTimeout), carried on
	// KernelExperimental rather than WithRetries. Zero = keep the kernel default;
	// only a positive value overrides it.
	var overall time.Duration
	if ke := cfg.KernelExperimental; ke != nil && ke.RetryOverallTimeout > 0 {
		overall = ke.RetryOverallTimeout
	}

	// Disable form: honor it regardless of the (often zero) waits. Substitute a
	// valid placeholder range so the setter accepts it; with 0 retries it's unused.
	if cfg.RetryMax < 0 {
		return &kernel.RetryConfig{
			MinWait:        kernelRetryPlaceholderWaitMin,
			MaxWait:        kernelRetryPlaceholderWaitMax,
			MaxRetries:     0,
			OverallTimeout: overall,
		}
	}

	// Degenerate wait range (WithRetries(n, 0, 0), or a Config assembled without
	// WithDefaults). If the caller asked for a positive attempt count, honor it with
	// the placeholder waits — dropping it to the kernel default would silently ignore
	// the caller's RetryMax. Only a zero RetryMax (no attempt count to preserve) falls
	// back to the kernel's default policy.
	minWait, maxWait := cfg.RetryWaitMin, cfg.RetryWaitMax
	if minWait <= 0 || maxWait < minWait {
		if cfg.RetryMax <= 0 {
			return nil
		}
		minWait, maxWait = kernelRetryPlaceholderWaitMin, kernelRetryPlaceholderWaitMax
	}
	return &kernel.RetryConfig{
		MinWait:        minWait,
		MaxWait:        maxWait,
		MaxRetries:     uint32(cfg.RetryMax), //nolint:gosec // RetryMax >= 0 here (negative handled above)
		OverallTimeout: overall,
	}
}

// resolveKernelProxy fills the kernel Config's proxy fields. An explicit
// WithKernelProxy (KernelExperimental.ProxyURL non-empty) wins verbatim,
// including its out-of-band credentials and bypass list; otherwise the
// endpoint's environment-derived proxy URL is used (with no credentials or
// bypass list — the env path folds credentials into the URL userinfo and
// consumes NO_PROXY during resolution, per Go's proxy-env convention). Kept
// untagged here (like buildKernelConfig) so the explicit-over-env precedence is
// asserted under CGO_ENABLED=0 (see TestResolveKernelProxy); newKernelBackend
// calls it after buildKernelConfig so it stays a thin assembler.
func resolveKernelProxy(cfg *config.Config, kc *kernel.Config) {
	if ke := cfg.KernelExperimental; ke != nil && ke.ProxyURL != "" {
		kc.ProxyURL = ke.ProxyURL
		kc.ProxyUsername = ke.ProxyUsername
		kc.ProxyPassword = ke.ProxyPassword
		kc.ProxyBypassHosts = ke.ProxyBypassHosts
		return
	}
	kc.ProxyURL = proxyForEndpoint(cfg)
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
