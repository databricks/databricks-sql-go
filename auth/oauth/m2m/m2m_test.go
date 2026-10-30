package m2m

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/clientcredentials"
)

// Issue #475 (part 4): the M2M token exchange must be bounded, like endpoint
// discovery already is (oauth.hostConfigTimeout). A hung token endpoint must
// make Authenticate return an error instead of blocking forever.
func TestM2MTokenExchangeIsBounded(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	orig := tokenExchangeTimeout
	tokenExchangeTimeout = 200 * time.Millisecond
	defer func() { tokenExchangeTimeout = orig }()

	c := NewAuthenticator("id", "secret", "unused.example.com").(*authClient)
	c.tokenSource = GetTokenSource(clientcredentials.Config{
		ClientID:     "id",
		ClientSecret: "secret",
		TokenURL:     srv.URL + "/oidc/v1/token",
		Scopes:       c.scopes,
	})

	done := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
		done <- c.Authenticate(req)
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a hung token endpoint must surface as an error")
	case <-time.After(5 * time.Second):
		t.Fatal("Authenticate blocked on a hung token endpoint; the token exchange is unbounded")
	}
}

func TestM2MScopes(t *testing.T) {
	t.Run("default should be [all-apis]", func(t *testing.T) {
		auth := NewAuthenticator("id", "secret", "staging.cloud.company.com").(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis"}, auth.scopes)

		auth = NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", nil).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis"}, auth.scopes)

		auth = NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", []string{}).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis"}, auth.scopes)
	})

	t.Run("should not add all-apis to passed scopes", func(t *testing.T) {
		auth := NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", []string{"sql"}).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"sql"}, auth.scopes)
	})

	t.Run("should keep all-apis if already in passed scopes", func(t *testing.T) {
		auth := NewAuthenticatorWithScopes("id", "secret", "staging.cloud.company.com", []string{"all-apis", "my-scope"}).(*authClient)
		assert.Equal(t, "id", auth.clientID)
		assert.Equal(t, "secret", auth.clientSecret)
		assert.Equal(t, []string{"all-apis", "my-scope"}, auth.scopes)
	})
}
