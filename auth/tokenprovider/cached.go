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
	provider TokenProvider
	cache    *Token
	mutex    sync.RWMutex
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
	// Try to get from cache first
	p.mutex.RLock()
	cached := p.cache
	p.mutex.RUnlock()

	if cached != nil && !p.shouldRefresh(cached) {
		log.Debug().Msgf("cached token provider: using cached token for provider %s", p.provider.Name())
		return cached, nil
	}

	// Need to refresh
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Double-check after acquiring write lock
	if p.cache != nil && !p.shouldRefresh(p.cache) {
		return p.cache, nil
	}

	log.Debug().Msgf("cached token provider: fetching new token from provider %s", p.provider.Name())
	token, err := p.provider.GetToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("cached token provider: failed to get token: %w", err)
	}

	p.cache = token
	return token, nil
}

// shouldRefresh determines if a token should be refreshed
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