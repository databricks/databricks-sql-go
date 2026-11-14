package m2m

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type m2mCacheRoundTripper func(*http.Request) (*http.Response, error)

func (transport m2mCacheRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestSharedTokenRefreshUsesCallingClient(t *testing.T) {
	var tokenHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"access_token": request.UserAgent(), "token_type": "Bearer", "expires_in": 3600,
		})
		tokenHits.Add(1)
	}))
	t.Cleanup(server.Close)
	shared := NewAuthenticator("id", "secret", "adb-test.azuredatabricks.net").(*authClient)
	config, err := GetConfig(context.Background(), shared.hostName, shared.clientID, shared.clientSecret, shared.scopes)
	require.NoError(t, err)
	config.TokenURL = server.URL
	shared.cache.config = &config
	base := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(base.CloseIdleConnections)
	var authenticators []*authClient
	for _, partner := range []string{"first", "second"} {
		configured := shared.WithHTTPClient(&http.Client{
			Transport: m2mCacheRoundTripper(func(request *http.Request) (*http.Response, error) {
				cloned := request.Clone(request.Context())
				cloned.Header.Set("User-Agent", partner)
				return base.RoundTrip(cloned)
			}),
		}).(*authClient)
		authenticators = append(authenticators, configured)
	}
	for index, authenticator := range authenticators {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		require.NoError(t, authenticator.Authenticate(request))
		wantAuthorization := request.Header.Get("Authorization")
		assert.Equal(t, "Bearer "+[]string{"first", "second"}[index], wantAuthorization)
		errors := make(chan error, 10)
		for range 10 {
			go func() {
				cloned := request.Clone(request.Context())
				err := authenticators[0].Authenticate(cloned)
				assert.Equal(t, wantAuthorization, cloned.Header.Get("Authorization"))
				errors <- err
			}()
		}
		for range 10 {
			require.NoError(t, <-errors)
		}
		assert.Equal(t, int32(index+1), tokenHits.Load())
		shared.cache.mutex.Lock()
		shared.cache.token.Expiry = time.Now().Add(-time.Hour)
		shared.cache.mutex.Unlock()
	}
	errors := make(chan error, 20)
	for index := range 20 {
		go func() {
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
			if err == nil {
				err = authenticators[index%len(authenticators)].Authenticate(request)
			}
			errors <- err
		}()
	}
	for range 20 {
		require.NoError(t, <-errors)
	}
	assert.Equal(t, int32(3), tokenHits.Load())
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
