//go:build cgo && databricks_kernel

package dbsql

import (
	"github.com/databricks/databricks-sql-go/auth/oauth/jwtm2m"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// JWTPrivateKeyM2MConfig configures OAuth machine-to-machine authentication via
// a JWT private-key client assertion (RFC 7523). A struct is used (rather than
// positional args) so the many string fields can't be transposed.
//
// KERNEL BACKEND ONLY: the assertion is signed by the native kernel, so both
// this type and WithJWTPrivateKeyM2M exist only in a build compiled with the
// kernel backend (`cgo && databricks_kernel`). A default (Thrift-only) build
// does not expose them, so a Thrift user can't configure an auth mode the
// Thrift path cannot honor.
type JWTPrivateKeyM2MConfig struct {
	// ClientID is the service principal / OAuth client id (the assertion
	// issuer and subject).
	ClientID string
	// KeyFile is the path to the PEM-encoded private key that signs the
	// assertion.
	KeyFile string
	// Kid is the key id written into the JWT header so the IdP can select the
	// registered public key.
	Kid string
	// Passphrase decrypts an encrypted PKCS#8 key; leave empty for an
	// unencrypted key.
	Passphrase string
	// Algorithm is the JWT signing algorithm (RS256/384/512, PS256/384/512,
	// ES256, ES384); empty defaults to RS256.
	Algorithm string
	// TokenURL is the OAuth IdP token endpoint. Required when the workspace's
	// OAuth authority is an external IdP (e.g. Entra ID for Azure Databricks),
	// since Databricks-native OIDC does not advertise the private_key_jwt
	// method; empty falls back to the kernel's OIDC discovery.
	TokenURL string
	// Scopes overrides the requested OAuth scopes; empty uses the kernel
	// default (all-apis).
	Scopes []string
}

// WithJWTPrivateKeyM2M sets up OAuth M2M authentication using a JWT private-key
// client assertion. See JWTPrivateKeyM2MConfig. Requires the kernel backend
// (WithUseKernel(true)); the kernel signs the assertion. This option is only
// compiled into a kernel-enabled build (`cgo && databricks_kernel`) — the
// pure-Go Thrift path has no JWT-signing implementation, so exposing it there
// would only add a divergent, non-functional surface.
func WithJWTPrivateKeyM2M(cfg JWTPrivateKeyM2MConfig) ConnOption {
	return func(c *config.Config) {
		if cfg.ClientID != "" && cfg.KeyFile != "" && cfg.Kid != "" {
			c.Authenticator = jwtm2m.NewAuthenticator(
				cfg.ClientID, cfg.KeyFile, cfg.Kid, cfg.Passphrase, cfg.Algorithm, cfg.TokenURL, cfg.Scopes,
			)
		}
	}
}
