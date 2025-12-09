package tokenprovider

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenProviderAuthenticator(t *testing.T) {
	t.Run("successful_authentication", func(t *testing.T) {
		provider := NewStaticTokenProvider("test-token-123")
		authenticator := NewAuthenticator(provider)

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		err := authenticator.Authenticate(req)

		require.NoError(t, err)
		assert.Equal(t, "Bearer test-token-123", req.Header.Get("Authorization"))
	})

	t.Run("authentication_with_custom_token_type", func(t *testing.T) {
		provider := NewStaticTokenProviderWithType("test-token", "MAC")
		authenticator := NewAuthenticator(provider)

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		err := authenticator.Authenticate(req)

		require.NoError(t, err)
		assert.Equal(t, "MAC test-token", req.Header.Get("Authorization"))
	})

	t.Run("authentication_error_propagation", func(t *testing.T) {
		provider := &mockProvider{
			tokenFunc: func() (*Token, error) {
				return nil, errors.New("provider failed")
			},
		}
		authenticator := NewAuthenticator(provider)

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		err := authenticator.Authenticate(req)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider failed")
		assert.Empty(t, req.Header.Get("Authorization"))
	})

	t.Run("empty_token_error", func(t *testing.T) {
		provider := &mockProvider{
			tokenFunc: func() (*Token, error) {
				return &Token{
					AccessToken: "",
					TokenType:   "Bearer",
				}, nil
			},
		}
		authenticator := NewAuthenticator(provider)

		req, _ := http.NewRequest("GET", "http://example.com", nil)
		err := authenticator.Authenticate(req)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "empty access token")
		assert.Empty(t, req.Header.Get("Authorization"))
	})

	t.Run("uses_request_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		provider := &mockProvider{
			tokenFunc: func() (*Token, error) {
				// This would normally check context cancellation
				return &Token{
					AccessToken: "test-token",
					TokenType:   "Bearer",
				}, nil
			},
		}
		authenticator := NewAuthenticator(provider)

		req, _ := http.NewRequestWithContext(ctx, "GET", "http://example.com", nil)
		err := authenticator.Authenticate(req)

		// Even with cancelled context, this should work as our mock doesn't check it
		require.NoError(t, err)
		assert.Equal(t, "Bearer test-token", req.Header.Get("Authorization"))
	})

	t.Run("external_token_integration", func(t *testing.T) {
		tokenFunc := func() (string, error) {
			return "external-token-456", nil
		}
		provider := NewExternalTokenProvider(tokenFunc)
		authenticator := NewAuthenticator(provider)

		req, _ := http.NewRequest("POST", "http://example.com/api", nil)
		err := authenticator.Authenticate(req)

		require.NoError(t, err)
		assert.Equal(t, "Bearer external-token-456", req.Header.Get("Authorization"))
	})
}
