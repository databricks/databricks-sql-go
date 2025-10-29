package tokenprovider

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create JWT tokens for testing
func createTestJWT(issuer, audience string, expiryHours int) string {
	claims := jwt.MapClaims{
		"iss": issuer,
		"aud": audience,
		"exp": time.Now().Add(time.Duration(expiryHours) * time.Hour).Unix(),
		"sub": "test-user",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte("test-secret"))
	return tokenString
}

func TestFederationProvider_HostComparison(t *testing.T) {
	tests := []struct {
		name           string
		issuer         string
		databricksHost string
		shouldExchange bool
	}{
		{
			name:           "same_host_no_port",
			issuer:         "https://test.databricks.com",
			databricksHost: "test.databricks.com",
			shouldExchange: false,
		},
		{
			name:           "same_host_with_port_443",
			issuer:         "https://test.databricks.com:443",
			databricksHost: "test.databricks.com",
			shouldExchange: false,
		},
		{
			name:           "same_host_both_with_port",
			issuer:         "https://test.databricks.com:443",
			databricksHost: "test.databricks.com:443",
			shouldExchange: false,
		},
		{
			name:           "different_host_azure",
			issuer:         "https://login.microsoftonline.com/tenant-id/",
			databricksHost: "test.databricks.com",
			shouldExchange: true,
		},
		{
			name:           "different_host_google",
			issuer:         "https://accounts.google.com",
			databricksHost: "test.databricks.com",
			shouldExchange: true,
		},
		{
			name:           "different_host_aws",
			issuer:         "https://cognito-identity.amazonaws.com",
			databricksHost: "test.databricks.com",
			shouldExchange: true,
		},
		{
			name:           "different_databricks_host",
			issuer:         "https://test1.databricks.com",
			databricksHost: "test2.databricks.com",
			shouldExchange: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a JWT token with the specified issuer
			jwtToken := createTestJWT(tt.issuer, "databricks", 1)

			// Create a mock base provider
			baseProvider := NewStaticTokenProvider(jwtToken)

			// Create federation provider
			fedProvider := NewFederationProvider(baseProvider, tt.databricksHost)

			// Check if token needs exchange
			needsExchange := fedProvider.needsTokenExchange(jwtToken)
			assert.Equal(t, tt.shouldExchange, needsExchange,
				"issuer=%s, host=%s, expected shouldExchange=%v, got=%v",
				tt.issuer, tt.databricksHost, tt.shouldExchange, needsExchange)
		})
	}
}

func TestFederationProvider_TokenExchangeSuccess(t *testing.T) {
	// Create mock token exchange server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and path
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/oidc/v1/token")

		// Verify headers
		assert.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
		assert.Equal(t, "*/*", r.Header.Get("Accept"))

		// Parse form data
		err := r.ParseForm()
		require.NoError(t, err)

		// Verify form parameters
		assert.Equal(t, "urn:ietf:params:oauth:grant-type:token-exchange", r.FormValue("grant_type"))
		assert.Equal(t, "sql", r.FormValue("scope"))
		assert.Equal(t, "urn:ietf:params:oauth:token-type:jwt", r.FormValue("subject_token_type"))
		assert.NotEmpty(t, r.FormValue("subject_token"))
		assert.Equal(t, "true", r.FormValue("return_original_token_if_authenticated"))

		// Return successful token response
		response := map[string]interface{}{
			"access_token": "exchanged-databricks-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
			"scope":        "sql",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// Create external token with different issuer
	externalToken := createTestJWT("https://login.microsoftonline.com/tenant-id/", "databricks", 1)
	baseProvider := NewStaticTokenProvider(externalToken)

	// Create federation provider pointing to mock server
	// Use full URL including http:// scheme for test server
	fedProvider := NewFederationProvider(baseProvider, server.URL)

	// Get token - should trigger exchange
	ctx := context.Background()
	token, err := fedProvider.GetToken(ctx)

	require.NoError(t, err)
	assert.Equal(t, "exchanged-databricks-token", token.AccessToken)
	assert.Equal(t, "Bearer", token.TokenType)
	assert.False(t, token.ExpiresAt.IsZero())
	assert.Contains(t, token.Scopes, "sql")
}

func TestFederationProvider_TokenExchangeWithClientID(t *testing.T) {
	clientID := "test-client-id-12345"

	// Create mock server that checks for client_id
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseForm()
		require.NoError(t, err)

		// Verify client_id is present
		assert.Equal(t, clientID, r.FormValue("client_id"))

		response := map[string]interface{}{
			"access_token": "sp-wide-federation-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	externalToken := createTestJWT("https://login.microsoftonline.com/tenant-id/", "databricks", 1)
	baseProvider := NewStaticTokenProvider(externalToken)

	fedProvider := NewFederationProviderWithClientID(baseProvider, server.URL, clientID)

	ctx := context.Background()
	token, err := fedProvider.GetToken(ctx)

	require.NoError(t, err)
	assert.Equal(t, "sp-wide-federation-token", token.AccessToken)
}

func TestFederationProvider_TokenExchangeFailureFallback(t *testing.T) {
	// Create mock server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "invalid_request"}`))
	}))
	defer server.Close()

	externalToken := createTestJWT("https://login.microsoftonline.com/tenant-id/", "databricks", 1)
	baseProvider := NewStaticTokenProvider(externalToken)

	fedProvider := NewFederationProvider(baseProvider, server.URL)

	ctx := context.Background()
	token, err := fedProvider.GetToken(ctx)

	// Should not error - falls back to external token
	require.NoError(t, err)
	assert.Equal(t, externalToken, token.AccessToken, "Should fall back to original token on exchange failure")
	assert.Equal(t, "Bearer", token.TokenType)
}

func TestFederationProvider_NoExchangeWhenSameIssuer(t *testing.T) {
	// Create token with Databricks as issuer
	databricksHost := "test.databricks.com"
	databricksToken := createTestJWT("https://"+databricksHost, "databricks", 1)
	baseProvider := NewStaticTokenProvider(databricksToken)

	fedProvider := NewFederationProvider(baseProvider, databricksHost)

	ctx := context.Background()
	token, err := fedProvider.GetToken(ctx)

	// Should not exchange - just return original token
	require.NoError(t, err)
	assert.Equal(t, databricksToken, token.AccessToken, "Should use original token when issuer matches")
}

func TestFederationProvider_NonJWTToken(t *testing.T) {
	// Use a non-JWT token (e.g., opaque PAT)
	opaqueToken := "dapi1234567890abcdef"
	baseProvider := NewStaticTokenProvider(opaqueToken)

	fedProvider := NewFederationProvider(baseProvider, "test.databricks.com")

	ctx := context.Background()
	token, err := fedProvider.GetToken(ctx)

	// Should not error - just pass through non-JWT token
	require.NoError(t, err)
	assert.Equal(t, opaqueToken, token.AccessToken, "Should pass through non-JWT tokens")
}

func TestFederationProvider_ProviderName(t *testing.T) {
	baseProvider := NewStaticTokenProvider("test-token")

	t.Run("without_client_id", func(t *testing.T) {
		fedProvider := NewFederationProvider(baseProvider, "test.databricks.com")
		assert.Equal(t, "federation[static]", fedProvider.Name())
	})

	t.Run("with_client_id", func(t *testing.T) {
		fedProvider := NewFederationProviderWithClientID(baseProvider, "test.databricks.com", "client-12345678-more")
		// Should truncate client ID to first 8 chars
		assert.Equal(t, "federation[static,sp:client-1]", fedProvider.Name())
	})
}

func TestFederationProvider_CachedIntegration(t *testing.T) {
	callCount := 0
	exchangeCount := 0

	// Mock server that counts exchanges
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchangeCount++
		response := map[string]interface{}{
			"access_token": "databricks-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	// External provider that counts calls
	externalProvider := &mockProvider{
		tokenFunc: func() (*Token, error) {
			callCount++
			externalToken := createTestJWT("https://login.microsoftonline.com/tenant/", "databricks", 1)
			return &Token{
				AccessToken: externalToken,
				TokenType:   "Bearer",
				ExpiresAt:   time.Now().Add(1 * time.Hour),
			}, nil
		},
		name: "external",
	}

	fedProvider := NewFederationProvider(externalProvider, server.URL)
	cachedProvider := NewCachedTokenProvider(fedProvider)

	ctx := context.Background()

	// First call - should call external provider and exchange
	token1, err1 := cachedProvider.GetToken(ctx)
	require.NoError(t, err1)
	assert.Equal(t, "databricks-token", token1.AccessToken)
	assert.Equal(t, 1, callCount, "External provider should be called once")
	assert.Equal(t, 1, exchangeCount, "Token should be exchanged once")

	// Second call - should use cache
	token2, err2 := cachedProvider.GetToken(ctx)
	require.NoError(t, err2)
	assert.Equal(t, "databricks-token", token2.AccessToken)
	assert.Equal(t, 1, callCount, "External provider should still be called only once (cached)")
	assert.Equal(t, 1, exchangeCount, "Token should still be exchanged only once (cached)")
}

func TestFederationProvider_InvalidJWT(t *testing.T) {
	// Test with various invalid JWT formats
	testCases := []string{
		"not.a.jwt",
		"invalid-token-format",
		"",
	}

	for _, invalidToken := range testCases {
		t.Run("invalid_jwt_"+invalidToken, func(t *testing.T) {
			baseProvider := NewStaticTokenProvider(invalidToken)
			fedProvider := NewFederationProvider(baseProvider, "test.databricks.com")

			// Should not need exchange for invalid JWT
			needsExchange := fedProvider.needsTokenExchange(invalidToken)
			assert.False(t, needsExchange, "Invalid JWT should not require exchange")
		})
	}
}

func TestFederationProvider_RealWorldIssuers(t *testing.T) {
	// Test with real-world identity provider issuers
	issuers := map[string]string{
		"azure_ad":    "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/v2.0",
		"google":      "https://accounts.google.com",
		"aws_cognito": "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_example",
		"okta":        "https://dev-12345.okta.com/oauth2/default",
		"auth0":       "https://dev-12345.auth0.com/",
		"github":      "https://token.actions.githubusercontent.com",
	}

	databricksHost := "test.databricks.com"

	for name, issuer := range issuers {
		t.Run(name, func(t *testing.T) {
			jwtToken := createTestJWT(issuer, "databricks", 1)
			baseProvider := NewStaticTokenProvider(jwtToken)
			fedProvider := NewFederationProvider(baseProvider, databricksHost)

			needsExchange := fedProvider.needsTokenExchange(jwtToken)
			assert.True(t, needsExchange, "Token from %s should require exchange", name)
		})
	}
}
