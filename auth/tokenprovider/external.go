package tokenprovider

import (
	"context"
	"fmt"
	"time"
)

// ExternalTokenProvider provides tokens from an external source (passthrough).
// This provider calls a user-supplied function to retrieve tokens on-demand.
type ExternalTokenProvider struct {
	tokenSource func() (string, error)
	tokenType   string
}

// NewExternalTokenProvider creates a provider that gets tokens from an external function
func NewExternalTokenProvider(tokenSource func() (string, error)) *ExternalTokenProvider {
	return &ExternalTokenProvider{
		tokenSource: tokenSource,
		tokenType:   "Bearer",
	}
}

// NewExternalTokenProviderWithType creates a provider with a custom token type
func NewExternalTokenProviderWithType(tokenSource func() (string, error), tokenType string) *ExternalTokenProvider {
	return &ExternalTokenProvider{
		tokenSource: tokenSource,
		tokenType:   tokenType,
	}
}

// GetToken retrieves the token from the external source
func (p *ExternalTokenProvider) GetToken(ctx context.Context) (*Token, error) {
	// Check for cancellation first
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("external token provider: context cancelled: %w", err)
	}

	if p.tokenSource == nil {
		return nil, fmt.Errorf("external token provider: token source is nil")
	}

	accessToken, err := p.tokenSource()
	if err != nil {
		return nil, fmt.Errorf("external token provider: failed to get token: %w", err)
	}

	return &Token{
		AccessToken: accessToken,
		TokenType:   p.tokenType,
		ExpiresAt:   time.Time{}, // External tokens don't provide expiry info
	}, nil
}

// Name returns the provider name
func (p *ExternalTokenProvider) Name() string {
	return "external"
}
