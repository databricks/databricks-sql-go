package oauth

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestFileTokenCache(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "token-cache-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cachePath := filepath.Join(tempDir, "token.json")
	cache := NewFileTokenCache(cachePath)

	// Create a test token
	expiry := time.Now().Add(1 * time.Hour)
	token := &oauth2.Token{
		AccessToken:  "test-access-token",
		TokenType:    "Bearer",
		RefreshToken: "test-refresh-token",
		Expiry:       expiry,
	}

	// Test saving the token
	err = cache.Save(token)
	require.NoError(t, err)

	// Verify the file exists
	_, err = os.Stat(cachePath)
	assert.NoError(t, err)

	// Test loading the token
	loadedToken, err := cache.Load()
	require.NoError(t, err)
	assert.NotNil(t, loadedToken)
	assert.Equal(t, token.AccessToken, loadedToken.AccessToken)
	assert.Equal(t, token.TokenType, loadedToken.TokenType)
	assert.Equal(t, token.RefreshToken, loadedToken.RefreshToken)
	assert.WithinDuration(t, token.Expiry, loadedToken.Expiry, time.Second)

	// Test file permissions
	fileInfo, err := os.Stat(cachePath)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), fileInfo.Mode().Perm())
}

func TestCachingTokenSource(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "token-source-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cachePath := filepath.Join(tempDir, "token.json")
	cache := NewFileTokenCache(cachePath)

	// Create a mock token source
	mockSource := &mockTokenSource{
		token: &oauth2.Token{
			AccessToken:  "test-access-token",
			TokenType:    "Bearer",
			RefreshToken: "test-refresh-token",
			Expiry:       time.Now().Add(1 * time.Hour),
		},
	}

	// Create a caching token source
	cachingSource := NewCachingTokenSource(mockSource, cache)

	// First call should use the mock source
	token, err := cachingSource.Token()
	require.NoError(t, err)
	assert.Equal(t, mockSource.token.AccessToken, token.AccessToken)
	assert.Equal(t, 1, mockSource.callCount)

	// Second call should use the cached token
	token, err = cachingSource.Token()
	require.NoError(t, err)
	assert.Equal(t, mockSource.token.AccessToken, token.AccessToken)
	assert.Equal(t, 1, mockSource.callCount) // Call count should still be 1

	// Create a new caching token source with the same cache
	cachingSource2 := NewCachingTokenSource(mockSource, cache)

	// This should use the cached token
	token, err = cachingSource2.Token()
	require.NoError(t, err)
	assert.Equal(t, mockSource.token.AccessToken, token.AccessToken)
	assert.Equal(t, 1, mockSource.callCount) // Call count should still be 1

	// Create a token that's expired
	mockSource.token = &oauth2.Token{
		AccessToken:  "expired-token",
		TokenType:    "Bearer",
		RefreshToken: "test-refresh-token",
		Expiry:       time.Now().Add(-1 * time.Hour),
	}

	// Save the expired token to cache
	err = cache.Save(mockSource.token)
	require.NoError(t, err)

	// Create a new mock source with a fresh token
	freshMockSource := &mockTokenSource{
		token: &oauth2.Token{
			AccessToken:  "fresh-token",
			TokenType:    "Bearer",
			RefreshToken: "test-refresh-token",
			Expiry:       time.Now().Add(1 * time.Hour),
		},
	}

	// Create a new caching token source with the expired cache
	cachingSource3 := NewCachingTokenSource(freshMockSource, cache)

	// This should detect the expired token and get a fresh one
	token, err = cachingSource3.Token()
	require.NoError(t, err)
	assert.Equal(t, freshMockSource.token.AccessToken, token.AccessToken)
	assert.Equal(t, 1, freshMockSource.callCount) // Should have called the fresh source
}

func TestGetCacheFilePath(t *testing.T) {
	host := "test-host.cloud.databricks.com"
	clientID := "test-client-id"
	scopes := []string{"scope1", "scope2"}

	path1 := GetCacheFilePath(host, clientID, scopes)
	path2 := GetCacheFilePath(host, clientID, scopes)

	// Same inputs should produce the same path
	assert.Equal(t, path1, path2)

	// Different inputs should produce different paths
	path3 := GetCacheFilePath("different-host", clientID, scopes)
	assert.NotEqual(t, path1, path3)
}

// mockTokenSource is a simple implementation of oauth2.TokenSource for testing
type mockTokenSource struct {
	token     *oauth2.Token
	callCount int
}

func (m *mockTokenSource) Token() (*oauth2.Token, error) {
	m.callCount++
	return m.token, nil
}