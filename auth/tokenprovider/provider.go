package tokenprovider

import (
	"context"
	"net/http"
	"time"
)

// TokenProvider is the interface for providing tokens from various sources
type TokenProvider interface {
	// GetToken retrieves a valid access token
	GetToken(ctx context.Context) (*Token, error)

	// Name returns the provider name for logging/debugging
	Name() string
}

// Token represents an access token with metadata
type Token struct {
	AccessToken  string
	TokenType    string
	ExpiresAt    time.Time
	RefreshToken string
	Scopes       []string
}

// IsExpired checks if the token has expired
func (t *Token) IsExpired() bool {
	if t.ExpiresAt.IsZero() {
		return false // No expiry means token doesn't expire
	}
	// Consider token expired 30 seconds before actual expiry for safety
	// This matches the standard buffer used by other Databricks SDKs
	return time.Now().Add(30 * time.Second).After(t.ExpiresAt)
}

// SetAuthHeader sets the Authorization header on an HTTP request
func (t *Token) SetAuthHeader(r *http.Request) {
	tokenType := t.TokenType
	if tokenType == "" {
		tokenType = "Bearer"
	}
	r.Header.Set("Authorization", tokenType+" "+t.AccessToken)
}
