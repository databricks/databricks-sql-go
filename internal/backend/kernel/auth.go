package kernel

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag:
// the Auth descriptor is a plain data type with no cgo, so it compiles in the
// default build too (and OpenSession, which is tagged, maps it to the kernel's
// set_auth_* C setters). Its consumer joinScopes lives in the tagged backend.go
// next to setAuth so the default build's unused-func lint doesn't flag it.

// AuthMode selects which kernel auth form OpenSession applies.
type AuthMode int

const (
	AuthPAT AuthMode = iota // personal access token
	AuthM2M                 // OAuth client-credentials (client id + secret)
	AuthU2M                 // OAuth user-to-machine (browser/PKCE; kernel-owned flow)
)

// Auth is the resolved auth descriptor for a kernel connection. Only the fields
// for Mode are populated. The connector fills it from the driver config (see
// validateKernelConfig / toKernelAuth); OpenSession maps it to exactly one
// kernel_session_config_set_auth_* call.
type Auth struct {
	Mode         AuthMode
	Token        string   // PAT
	ClientID     string   // M2M + U2M (U2M: the cloud-inferred Go client id)
	ClientSecret string   // M2M
	Scopes       []string // U2M (the kernel M2M setter takes no scopes)
	RedirectPort uint16   // U2M browser-redirect port; 0 = kernel default
}
