package kernel

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag:
// the Auth descriptor and the credential-provider interfaces are plain Go with no
// cgo, so they compile in the default build too. validateKernelConfig (untagged)
// resolves an Auth from the config; OpenSession (tagged) maps it to the kernel's
// set_auth_* C setters.

// AuthMode selects which kernel auth form OpenSession applies.
type AuthMode int

const (
	AuthPAT AuthMode = iota // personal access token
	AuthM2M                 // OAuth client-credentials (client id + secret)
	AuthU2M                 // OAuth user-to-machine (browser/PKCE; kernel-owned flow)
)

// Auth is the resolved auth descriptor for a kernel connection. Only the fields
// for Mode are populated. The connector fills it from the driver config (see
// validateKernelConfig); OpenSession maps it to exactly one
// kernel_session_config_set_auth_* call.
// Scopes and RedirectPort map to the optional args of set_auth_u2m and are wired
// through to it by setAuth, but no Go path populates them today: the driver exposes
// no user option for U2M scopes or redirect port on either backend (the native
// Thrift path hardcodes both), so resolveKernelAuth leaves them zero and the kernel
// applies its defaults. They are kept — rather than dropped and the setter hardcoded
// to NULL/0 — so kernel.Auth models the full set_auth_u2m surface: adding a future
// WithOAuthRedirectPort / scopes option becomes populating these, not re-plumbing
// the setter. TestSetAuthByMode's "U2M full" case pins that marshalling so the
// dormant path stays correct.
type Auth struct {
	Mode         AuthMode
	Token        string   // PAT
	ClientID     string   // M2M + U2M (U2M: the cloud-inferred Go client id)
	ClientSecret string   // M2M
	Scopes       []string // U2M — dormant (see note above); nil → kernel default scopes
	RedirectPort uint16   // U2M — dormant (see note above); 0 → kernel default port (8020)
}

// M2MCredentialsProvider is implemented by the OAuth M2M authenticator to expose
// its raw client-credentials. The kernel backend reads these to drive the kernel's
// own M2M flow (the kernel owns the token exchange), rather than using the
// authenticator's Authenticate method. So cfg.Authenticator stays the single source
// of truth for auth on both backends — the kernel selects M2M by asserting this
// interface, so the last WithX option applied wins, exactly as on Thrift.
//
// It lives in this internal package (not the public auth package) so the
// secret-reading capability is never exposed on the driver's public API; the
// unexported concrete m2m authenticator satisfies it structurally.
type M2MCredentialsProvider interface {
	// M2MCredentials returns the client id and client secret.
	M2MCredentials() (clientID, clientSecret string)
	// M2MScopes returns the configured OAuth scopes. The kernel's C-ABI M2M setter
	// takes no scopes (it applies its own default set), so resolveKernelAuth rejects
	// a custom set via M2MScopesSupported rather than silently dropping it.
	M2MScopes() []string
}

// M2MScopesSupported reports whether an M2M authenticator's scopes can be honored
// on the kernel path. The kernel's set_auth_m2m has no scopes argument and applies
// "all-apis" itself, so only an empty set or exactly {"all-apis"} is forwardable;
// any other set would silently downgrade to the kernel default, so it is rejected.
func M2MScopesSupported(scopes []string) bool {
	switch len(scopes) {
	case 0:
		return true
	case 1:
		return scopes[0] == "all-apis"
	default:
		return false
	}
}

// U2MCredentialsProvider is implemented by the OAuth U2M authenticator to expose
// the cloud-inferred client id the kernel should use for its browser/PKCE flow (so
// the kernel path uses the same client id the Thrift path would). Internal for the
// same reason as M2MCredentialsProvider; satisfied structurally by the unexported
// u2m authenticator.
type U2MCredentialsProvider interface {
	// U2MClientID returns the OAuth client id for the U2M browser flow.
	U2MClientID() string
}
