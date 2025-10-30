package tokenprovider

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToken_IsExpired(t *testing.T) {
	tests := []struct {
		name     string
		token    *Token
		expected bool
	}{
		{
			name: "token_without_expiry",
			token: &Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Time{},
			},
			expected: false,
		},
		{
			name: "token_expired",
			token: &Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Now().Add(-10 * time.Minute),
			},
			expected: true,
		},
		{
			name: "token_not_expired",
			token: &Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Now().Add(10 * time.Minute),
			},
			expected: false,
		},
		{
			name: "token_expires_within_5_minutes",
			token: &Token{
				AccessToken: "test-token",
				ExpiresAt:   time.Now().Add(3 * time.Minute),
			},
			expected: true, // Should be considered expired due to 5-minute buffer
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.token.IsExpired())
		})
	}
}

func TestToken_SetAuthHeader(t *testing.T) {
	tests := []struct {
		name           string
		token          *Token
		expectedHeader string
	}{
		{
			name: "bearer_token",
			token: &Token{
				AccessToken: "test-access-token",
				TokenType:   "Bearer",
			},
			expectedHeader: "Bearer test-access-token",
		},
		{
			name: "default_to_bearer",
			token: &Token{
				AccessToken: "test-access-token",
				TokenType:   "",
			},
			expectedHeader: "Bearer test-access-token",
		},
		{
			name: "custom_token_type",
			token: &Token{
				AccessToken: "test-access-token",
				TokenType:   "CustomAuth",
			},
			expectedHeader: "CustomAuth test-access-token",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", "http://example.com", nil)
			tt.token.SetAuthHeader(req)
			assert.Equal(t, tt.expectedHeader, req.Header.Get("Authorization"))
		})
	}
}

func TestStaticTokenProvider(t *testing.T) {
	t.Run("valid_token", func(t *testing.T) {
		provider := NewStaticTokenProvider("static-token-123")
		token, err := provider.GetToken(context.Background())

		require.NoError(t, err)
		assert.Equal(t, "static-token-123", token.AccessToken)
		assert.Equal(t, "Bearer", token.TokenType)
		assert.True(t, token.ExpiresAt.IsZero())
		assert.Equal(t, "static", provider.Name())
	})

	t.Run("empty_token_error", func(t *testing.T) {
		provider := NewStaticTokenProvider("")
		token, err := provider.GetToken(context.Background())

		assert.Error(t, err)
		assert.Nil(t, token)
		assert.Contains(t, err.Error(), "token is empty")
	})

	t.Run("custom_token_type", func(t *testing.T) {
		provider := NewStaticTokenProviderWithType("static-token", "CustomAuth")
		token, err := provider.GetToken(context.Background())

		require.NoError(t, err)
		assert.Equal(t, "static-token", token.AccessToken)
		assert.Equal(t, "CustomAuth", token.TokenType)
	})

	t.Run("multiple_calls_same_token", func(t *testing.T) {
		provider := NewStaticTokenProvider("static-token")

		token1, err1 := provider.GetToken(context.Background())
		token2, err2 := provider.GetToken(context.Background())

		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.Equal(t, token1.AccessToken, token2.AccessToken)
	})
}

func TestExternalTokenProvider(t *testing.T) {
	t.Run("successful_token_retrieval", func(t *testing.T) {
		callCount := 0
		tokenFunc := func() (string, error) {
			callCount++
			return "external-token-" + string(rune(callCount)), nil
		}

		provider := NewExternalTokenProvider(tokenFunc)
		token, err := provider.GetToken(context.Background())

		require.NoError(t, err)
		assert.Equal(t, "external-token-\x01", token.AccessToken)
		assert.Equal(t, "Bearer", token.TokenType)
		assert.Equal(t, "external", provider.Name())
	})

	t.Run("token_function_error", func(t *testing.T) {
		tokenFunc := func() (string, error) {
			return "", errors.New("failed to retrieve token")
		}

		provider := NewExternalTokenProvider(tokenFunc)
		token, err := provider.GetToken(context.Background())

		assert.Error(t, err)
		assert.Nil(t, token)
		assert.Contains(t, err.Error(), "failed to get token")
	})

	t.Run("empty_token_error", func(t *testing.T) {
		tokenFunc := func() (string, error) {
			return "", nil
		}

		provider := NewExternalTokenProvider(tokenFunc)
		token, err := provider.GetToken(context.Background())

		assert.Error(t, err)
		assert.Nil(t, token)
		assert.Contains(t, err.Error(), "empty token returned")
	})

	t.Run("nil_function_error", func(t *testing.T) {
		provider := NewExternalTokenProvider(nil)
		token, err := provider.GetToken(context.Background())

		assert.Error(t, err)
		assert.Nil(t, token)
		assert.Contains(t, err.Error(), "token function is nil")
	})

	t.Run("custom_token_type", func(t *testing.T) {
		tokenFunc := func() (string, error) {
			return "external-token", nil
		}

		provider := NewExternalTokenProviderWithType(tokenFunc, "MAC")
		token, err := provider.GetToken(context.Background())

		require.NoError(t, err)
		assert.Equal(t, "external-token", token.AccessToken)
		assert.Equal(t, "MAC", token.TokenType)
	})

	t.Run("different_token_each_call", func(t *testing.T) {
		counter := 0
		tokenFunc := func() (string, error) {
			counter++
			return "token-" + string(rune(counter)), nil
		}

		provider := NewExternalTokenProvider(tokenFunc)

		token1, err1 := provider.GetToken(context.Background())
		token2, err2 := provider.GetToken(context.Background())

		require.NoError(t, err1)
		require.NoError(t, err2)
		assert.NotEqual(t, token1.AccessToken, token2.AccessToken)
		assert.Equal(t, "token-\x01", token1.AccessToken)
		assert.Equal(t, "token-\x02", token2.AccessToken)
	})
}

// Mock provider for testing
type mockProvider struct {
	tokenFunc func() (*Token, error)
	name      string
}

func (m *mockProvider) GetToken(ctx context.Context) (*Token, error) {
	if m.tokenFunc != nil {
		return m.tokenFunc()
	}
	return nil, errors.New("not implemented")
}

func (m *mockProvider) Name() string {
	return m.name
}
