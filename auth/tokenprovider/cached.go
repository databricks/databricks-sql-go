package tokenprovider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// CachedTokenProvider wraps another provider and caches tokens
type CachedTokenProvider struct {
	provider   TokenProvider
	cache      *Token
	mutex      sync.RWMutex
	refreshing bool // prevents thundering herd
	// RefreshThreshold determines when to refresh (default 5 minutes before expiry)
	RefreshThreshold time.Duration
}

// NewCachedTokenProvider creates a caching wrapper around any token provider
func NewCachedTokenProvider(provider TokenProvider) *CachedTokenProvider {
	return &CachedTokenProvider{
		provider:         provider,
		RefreshThreshold: 5 * time.Minute,
	}
}

// GetToken retrieves a token, using cache if available and valid
func (p *CachedTokenProvider) GetToken(ctx context.Context) (*Token, error) {
	// Check if context is already cancelled
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("cached token provider: context cancelled: %w", err)
	}

	// Try to get from cache first
	p.mutex.RLock()
	cached := p.cache
	needsRefresh := p.shouldRefresh(cached)
	isRefreshing := p.refreshing
	p.mutex.RUnlock()

	// If cache is valid and not being refreshed, return a copy
	if cached != nil && !needsRefresh {
		log.Debug().Msgf("cached token provider: using cached token for provider %s", p.provider.Name())
		// Return a copy to avoid concurrent modification issues
		return copyToken(cached), nil
	}

	// If another goroutine is already refreshing, wait briefly and retry
	if isRefreshing {
		time.Sleep(50 * time.Millisecond)
		p.mutex.RLock()
		cached = p.cache
		needsRefresh = p.shouldRefresh(cached)
		p.mutex.RUnlock()

		if cached != nil && !needsRefresh {
			return copyToken(cached), nil
		}
	}

	// Need to refresh - acquire write lock
	p.mutex.Lock()

	// Double-check after acquiring write lock
	if p.cache != nil && !p.shouldRefresh(p.cache) {
		p.mutex.Unlock()
		return copyToken(p.cache), nil
	}

	// Mark as refreshing to prevent other goroutines from also refreshing
	p.refreshing = true
	p.mutex.Unlock()

	// Fetch new token WITHOUT holding the lock
	log.Debug().Msgf("cached token provider: fetching new token from provider %s", p.provider.Name())
	token, err := p.provider.GetToken(ctx)

	// Update cache with result
	p.mutex.Lock()
	p.refreshing = false
	if err != nil {
		p.mutex.Unlock()
		return nil, fmt.Errorf("cached token provider: failed to get token: %w", err)
	}

	p.cache = token
	p.mutex.Unlock()

	return copyToken(token), nil
}

// copyToken creates a copy of a token to avoid concurrent modification issues
func copyToken(t *Token) *Token {
	if t == nil {
		return nil
	}

	scopesCopy := make([]string, len(t.Scopes))
	copy(scopesCopy, t.Scopes)

	return &Token{
		AccessToken: t.AccessToken,
		TokenType:   t.TokenType,
		ExpiresAt:   t.ExpiresAt,
		Scopes:      scopesCopy,
	}
}

// shouldRefresh determines if a token should be refreshed based on expiry time.
// Returns true if:
//   - token is nil
//   - token has expired
//   - token will expire within RefreshThreshold (default 5 minutes)
//
// Returns false if:
//   - token has no expiry time (never expires)
//   - token is still valid and not close to expiry
func (p *CachedTokenProvider) shouldRefresh(token *Token) bool {
	if token == nil {
		return true
	}

	// If no expiry time, assume token doesn't expire
	if token.ExpiresAt.IsZero() {
		return false
	}

	// Refresh if within threshold of expiry
	refreshAt := token.ExpiresAt.Add(-p.RefreshThreshold)
	return time.Now().After(refreshAt)
}

// Name returns the provider name
func (p *CachedTokenProvider) Name() string {
	return fmt.Sprintf("cached[%s]", p.provider.Name())
}

// ClearCache clears the cached token
func (p *CachedTokenProvider) ClearCache() {
	p.mutex.Lock()
	p.cache = nil
	p.mutex.Unlock()
}
