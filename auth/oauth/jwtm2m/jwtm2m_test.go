package jwtm2m

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJWTM2MCredentials(t *testing.T) {
	t.Run("forwards all fields verbatim", func(t *testing.T) {
		a := NewAuthenticator(
			"sp-uuid", "/keys/jwt.pem", "kid-1", "pw", "ES256",
			"https://login.microsoftonline.com/tenant/oauth2/v2.0/token",
			[]string{"resource/.default"},
		)
		clientID, keyFile, kid, passphrase, algorithm, tokenURL, scopes :=
			a.(*authClient).JWTM2MCredentials()
		assert.Equal(t, "sp-uuid", clientID)
		assert.Equal(t, "/keys/jwt.pem", keyFile)
		assert.Equal(t, "kid-1", kid)
		assert.Equal(t, "pw", passphrase)
		assert.Equal(t, "ES256", algorithm)
		assert.Equal(t, "https://login.microsoftonline.com/tenant/oauth2/v2.0/token", tokenURL)
		assert.Equal(t, []string{"resource/.default"}, scopes)
	})

	t.Run("empty optionals stay empty", func(t *testing.T) {
		a := NewAuthenticator("sp", "/k.pem", "kid", "", "", "", nil)
		_, _, _, passphrase, algorithm, tokenURL, scopes := a.(*authClient).JWTM2MCredentials()
		assert.Equal(t, "", passphrase)
		assert.Equal(t, "", algorithm)
		assert.Equal(t, "", tokenURL)
		assert.Nil(t, scopes)
	})
}

func TestAuthenticateIsKernelOnly(t *testing.T) {
	// The Go (Thrift) path can't sign a JWT client assertion — signing is the
	// kernel's job — so Authenticate must fail loudly pointing at the kernel.
	a := NewAuthenticator("sp", "/k.pem", "kid", "", "", "", nil)
	err := a.Authenticate(&http.Request{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kernel backend")
}
