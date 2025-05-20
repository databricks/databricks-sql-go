package oauth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

// TokenCache defines the interface for storing and retrieving OAuth tokens
type TokenCache interface {
	// Save stores a token in the cache
	Save(token *oauth2.Token) error
	
	// Load retrieves a token from the cache
	Load() (*oauth2.Token, error)
}

// FileTokenCache implements TokenCache using file-based storage
type FileTokenCache struct {
	path string
}

// NewFileTokenCache creates a new file-based token cache
func NewFileTokenCache(path string) *FileTokenCache {
	return &FileTokenCache{path: path}
}

// Save stores a token in the file cache
func (c *FileTokenCache) Save(token *oauth2.Token) error {
	// Create parent directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(c.path), 0700); err != nil {
		return fmt.Errorf("failed to create token cache directory: %w", err)
	}
	
	// Serialize token to JSON
	data, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("failed to serialize token: %w", err)
	}
	
	// Write to file with secure permissions
	if err := os.WriteFile(c.path, data, 0600); err != nil {
		return fmt.Errorf("failed to write token to cache: %w", err)
	}
	
	log.Debug().Str("path", c.path).Msg("Token saved to cache")
	return nil
}

// Load retrieves a token from the file cache
func (c *FileTokenCache) Load() (*oauth2.Token, error) {
	data, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Cache doesn't exist yet
		}
		return nil, fmt.Errorf("failed to read token from cache: %w", err)
	}
	
	var token oauth2.Token
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, fmt.Errorf("failed to deserialize token: %w", err)
	}
	
	log.Debug().Str("path", c.path).Msg("Token loaded from cache")
	return &token, nil
}

// GetCacheFilePath returns the path to the token cache file
func GetCacheFilePath(host, clientID string, scopes []string) string {
	// Create SHA-256 hash of host, client_id, and scopes
	h := sha256.New()
	h.Write([]byte(host))
	h.Write([]byte(clientID))
	h.Write([]byte(strings.Join(scopes, ",")))
	hash := hex.EncodeToString(h.Sum(nil))
	
	// Get user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}
	
	return filepath.Join(homeDir, ".config", "databricks-sql-go", "oauth", hash)
}

// CachingTokenSource wraps a TokenSource with a TokenCache
type CachingTokenSource struct {
	src   oauth2.TokenSource
	cache TokenCache
}

// NewCachingTokenSource creates a new TokenSource that caches tokens
func NewCachingTokenSource(src oauth2.TokenSource, cache TokenCache) oauth2.TokenSource {
	return &CachingTokenSource{
		src:   src,
		cache: cache,
	}
}

// Token returns a valid token from either the cache or the underlying source
func (cts *CachingTokenSource) Token() (*oauth2.Token, error) {
	// Try to get token from cache first
	if cts.cache != nil {
		token, err := cts.cache.Load()
		if err == nil && token != nil && token.Valid() {
			log.Debug().Msg("Using cached token")
			return token, nil
		}
	}
	
	// Get a new token from the source
	token, err := cts.src.Token()
	if err != nil {
		return nil, err
	}
	
	// Save the token to cache
	if cts.cache != nil {
		if err := cts.cache.Save(token); err != nil {
			log.Warn().Err(err).Msg("Failed to save token to cache")
		}
	}
	
	return token, nil
}