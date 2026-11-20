package m2m

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

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

// tokenServer answers the first `answered` requests with a short-lived token and
// holds every later request open until the test ends.
func tokenServer(t *testing.T, answered int32) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) <= answered {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"tok","token_type":"Bearer","expires_in":1}`)
			return
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv, &hits
}

// useTokenEndpoint skips endpoint discovery, sends token requests to tokenURL,
// and shortens the request bound for the test.
func useTokenEndpoint(t *testing.T, tokenURL string, bound time.Duration) {
	t.Helper()
	oldConfig, oldBound := getConfig, tokenRequestTimeout
	getConfig = func(_ context.Context, _, clientID, clientSecret string, scopes []string) (clientcredentials.Config, error) {
		return clientcredentials.Config{ClientID: clientID, ClientSecret: clientSecret, TokenURL: tokenURL, Scopes: scopes}, nil
	}
	tokenRequestTimeout = bound
	t.Cleanup(func() { getConfig, tokenRequestTimeout = oldConfig, oldBound })
}

func newRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "https://ws.cloud.databricks.com/sql/1.0/warehouses/x", nil)
	require.NoError(t, err)
	return req
}

// authenticate fails the test if Authenticate has not returned within limit.
func authenticate(t *testing.T, authr auth.Authenticator, limit time.Duration) (*http.Request, error) {
	t.Helper()
	req := newRequest(t)
	done := make(chan error, 1)
	go func() { done <- authr.Authenticate(req) }()
	select {
	case err := <-done:
		return req, err
	case <-time.After(limit):
		t.Fatalf("Authenticate did not return within %v", limit)
		return nil, nil
	}
}

func requireTimeout(t *testing.T, err error) {
	t.Helper()
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "want a timeout error, got %v", err)
}

func TestAuthenticateBoundsHungTokenEndpoint(t *testing.T) {
	const bound = 200 * time.Millisecond
	srv, hits := tokenServer(t, 0)
	useTokenEndpoint(t, srv.URL+"/oidc/v1/token", bound)
	authr := NewAuthenticator("id", "secret", "ws.cloud.databricks.com")

	start := time.Now()
	_, err := authenticate(t, authr, 10*time.Second)
	elapsed := time.Since(start)

	requireTimeout(t, err)
	// oauth2's AuthStyle auto-detect retries a failed first request once.
	assert.LessOrEqual(t, hits.Load(), int32(2))
	assert.GreaterOrEqual(t, elapsed, bound)
	assert.Less(t, elapsed, 2*bound+time.Second)
}

func TestAuthenticateBoundsConcurrentCallers(t *testing.T) {
	const bound = 200 * time.Millisecond
	srv, _ := tokenServer(t, 0)
	useTokenEndpoint(t, srv.URL+"/oidc/v1/token", bound)
	authr := NewAuthenticator("id", "secret", "ws.cloud.databricks.com")

	start := time.Now()
	errs := make(chan error, 2)
	for range 2 {
		req := newRequest(t)
		go func() { errs <- authr.Authenticate(req) }()
	}
	for range 2 {
		select {
		case err := <-errs:
			requireTimeout(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Authenticate did not return within 10s")
		}
	}
	// The second caller waits for the first fetch, then makes its own.
	assert.Less(t, time.Since(start), 4*bound+2*time.Second)
}

func TestAuthenticateBoundsTokenRefresh(t *testing.T) {
	const bound = 200 * time.Millisecond
	srv, hits := tokenServer(t, 1)
	useTokenEndpoint(t, srv.URL+"/oidc/v1/token", bound)
	authr := NewAuthenticator("id", "secret", "ws.cloud.databricks.com")

	req, err := authenticate(t, authr, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, "Bearer tok", req.Header.Get("Authorization"))

	// The token expires inside oauth2's 10s early-expiry window, so this refreshes.
	start := time.Now()
	_, err = authenticate(t, authr, 10*time.Second)
	requireTimeout(t, err)
	assert.Less(t, time.Since(start), bound+time.Second)
	assert.Equal(t, int32(2), hits.Load(), "a refresh reuses the detected auth style")
}

func TestWithTokenRequestTimeout(t *testing.T) {
	clientOn := func(ctx context.Context) *http.Client {
		return ctx.Value(oauth2.HTTPClient).(*http.Client)
	}

	t.Run("default client gets the bound", func(t *testing.T) {
		c := clientOn(withTokenRequestTimeout(context.Background()))
		assert.Equal(t, tokenRequestTimeout, c.Timeout)
		assert.Nil(t, c.Transport)
		assert.Zero(t, http.DefaultClient.Timeout, "http.DefaultClient must not be mutated")
	})

	t.Run("supplied client keeps its transport and is capped", func(t *testing.T) {
		base := &http.Client{Transport: &http.Transport{}, Timeout: 15 * time.Minute}
		c := clientOn(withTokenRequestTimeout(context.WithValue(context.Background(), oauth2.HTTPClient, base)))
		assert.Equal(t, tokenRequestTimeout, c.Timeout)
		assert.Same(t, base.Transport, c.Transport)
		assert.Equal(t, 15*time.Minute, base.Timeout, "supplied client must not be mutated")
	})

	t.Run("shorter supplied timeout is kept", func(t *testing.T) {
		base := &http.Client{Timeout: 5 * time.Second}
		c := clientOn(withTokenRequestTimeout(context.WithValue(context.Background(), oauth2.HTTPClient, base)))
		assert.Equal(t, 5*time.Second, c.Timeout)
	})
}
