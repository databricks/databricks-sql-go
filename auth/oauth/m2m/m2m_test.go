package m2m

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

// The first-call branch of Authenticate (no cached token source) resolves the
// config and builds the token source under c.mx, then fetches a token. A hung
// token endpoint there must also surface as an error, and must not leave the lock
// held so later calls on the same connector can proceed.
func TestM2MFirstAuthenticateIsBounded(t *testing.T) {
	release := make(chan struct{})
	var tokenHits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&tokenHits, 1)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	origTimeout := tokenExchangeTimeout
	tokenExchangeTimeout = 200 * time.Millisecond
	defer func() { tokenExchangeTimeout = origTimeout }()

	// oauth.GetEndpoint needs a Databricks hostname and a publicly trusted TLS
	// cert, so stub only the discovery step; GetTokenSource and Token() run for real.
	origGetConfig := getConfig
	var configCalls int32
	getConfig = func(_ context.Context, _, clientID, clientSecret string, scopes []string) (clientcredentials.Config, error) {
		atomic.AddInt32(&configCalls, 1)
		return clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     srv.URL + "/oidc/v1/token",
			Scopes:       scopes,
		}, nil
	}
	defer func() { getConfig = origGetConfig }()

	c := NewAuthenticator("id", "secret", "unused.example.com").(*authClient)
	require.Nil(t, c.tokenSource, "test must exercise the first-call branch")

	authenticate := func() <-chan error {
		done := make(chan error, 1)
		go func() {
			req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
			done <- c.Authenticate(req)
		}()
		return done
	}

	// oauth2's AuthStyle auto-detection may issue more than one POST per exchange,
	// so assert the endpoint was reached on each call rather than an exact count.
	var prevHits int32
	for _, name := range []string{"first call", "call after a timed-out first call"} {
		select {
		case err := <-authenticate():
			require.Error(t, err, "%s: a hung token endpoint must surface as an error", name)
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: Authenticate blocked on a hung token endpoint (unbounded exchange or c.mx never released)", name)
		}
		hits := atomic.LoadInt32(&tokenHits)
		assert.Greater(t, hits, prevHits, "%s: token endpoint should have been called", name)
		prevHits = hits
	}
	assert.EqualValues(t, 1, atomic.LoadInt32(&configCalls), "discovery should run only on the first call")
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
