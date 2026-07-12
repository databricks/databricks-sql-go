package auth

import (
	"net/http"
	"strings"
)

type Authenticator interface {
	Authenticate(*http.Request) error
}

// M2MCredentialsProvider is implemented by the OAuth M2M authenticator to expose
// its raw client-credentials. The SEA-via-kernel backend reads these to drive the
// kernel's own M2M flow (the kernel owns the token exchange), rather than using the
// authenticator's Authenticate method. This keeps cfg.Authenticator the single
// source of truth for auth on both backends — the kernel selects M2M by asserting
// this interface, so the last WithX option applied wins, exactly as on Thrift.
type M2MCredentialsProvider interface {
	// M2MCredentials returns the client id and client secret. (The kernel's C-ABI
	// M2M setter takes no scopes — it applies its own default scope set, matching
	// the Go authenticator's default — so scopes are not exposed here.)
	M2MCredentials() (clientID, clientSecret string)
}

// U2MCredentialsProvider is implemented by the OAuth U2M authenticator to expose
// the cloud-inferred client id the kernel should use for its browser/PKCE flow (so
// the kernel path uses the same client id the Thrift path would). See
// M2MCredentialsProvider for why this rather than a parallel config carrier.
type U2MCredentialsProvider interface {
	// U2MClientID returns the OAuth client id for the U2M browser flow.
	U2MClientID() string
}

type AuthType int

const (
	AuthTypeUnknown AuthType = iota
	AuthTypePat
	AuthTypeOauthU2M
	AuthTypeOauthM2M
)

var authTypeNames []string = []string{"Unknown", "Pat", "OauthU2M", "OauthM2M"}

func (at AuthType) String() string {
	if at >= 0 && int(at) < len(authTypeNames) {
		return authTypeNames[at]
	}

	return authTypeNames[0]
}

func ParseAuthType(typeString string) AuthType {
	typeString = strings.ToLower(typeString)
	for i, n := range authTypeNames {
		if strings.ToLower(n) == typeString {
			return AuthType(i)
		}
	}

	return AuthTypeUnknown
}
