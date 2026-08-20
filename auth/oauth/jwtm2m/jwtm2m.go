//go:build cgo && databricks_kernel

// Package jwtm2m provides an OAuth machine-to-machine authenticator that
// authenticates with a JWT private-key client assertion (RFC 7523) instead of
// a client secret.
//
// This authenticator is KERNEL-BACKEND ONLY. The JWT is signed by the native
// kernel (which owns the assertion signing + token lifecycle); the pure-Go
// Thrift path has no JWT-signing implementation, so Authenticate returns an
// error directing the caller at the kernel backend. The authenticator exists
// so cfg.Authenticator stays the single source of truth for auth on both
// backends: the kernel selects this mode by asserting the (internal)
// JWTM2MCredentialsProvider interface that this type satisfies structurally.
package jwtm2m

import (
	"fmt"
	"net/http"

	"github.com/databricks/databricks-sql-go/auth"
)

// NewAuthenticator builds a JWT private-key M2M authenticator. clientID,
// keyFile, and kid are required; passphrase (for an encrypted PKCS#8 key),
// algorithm (default RS256), tokenURL (the OAuth IdP token endpoint — required
// for an external-IdP-backed workspace such as Entra ID), and scopes are
// optional.
func NewAuthenticator(clientID, keyFile, kid, passphrase, algorithm, tokenURL string, scopes []string) auth.Authenticator {
	return &authClient{
		clientID:   clientID,
		keyFile:    keyFile,
		kid:        kid,
		passphrase: passphrase,
		algorithm:  algorithm,
		tokenURL:   tokenURL,
		scopes:     scopes,
	}
}

type authClient struct {
	clientID   string
	keyFile    string
	kid        string
	passphrase string
	algorithm  string
	tokenURL   string
	scopes     []string
}

// JWTM2MCredentials exposes the private-key assertion inputs so the kernel
// backend can drive the kernel's own JWT client-assertion flow. It structurally
// satisfies the JWTM2MCredentialsProvider interface the kernel backend asserts
// (defined in internal/backend/kernel, so the key-reading capability is not part
// of the driver's public API).
func (c *authClient) JWTM2MCredentials() (clientID, keyFile, kid, passphrase, algorithm, tokenURL string, scopes []string) {
	return c.clientID, c.keyFile, c.kid, c.passphrase, c.algorithm, c.tokenURL, c.scopes
}

// Authenticate is unsupported on the pure-Go (Thrift) path: signing a JWT
// client assertion is done by the native kernel, not the Go driver. Use the
// kernel backend (WithUseKernel(true)) with this authenticator.
func (c *authClient) Authenticate(r *http.Request) error {
	return fmt.Errorf("jwtm2m: JWT private-key M2M is only supported on the kernel backend; " +
		"enable it with WithUseKernel(true)")
}
