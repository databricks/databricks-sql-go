package tokenprovider

import (
	"context"
	"fmt"
	"time"
)

// ExternalTokenProvider provides tokens from an external source (passthrough)
type ExternalTokenProvider struct {
	tokenFunc func() (string, error)
	tokenType string
}

// NewExternalTokenProvider creates a provider that gets tokens from an external function
func NewExternalTokenProvider(tokenFunc func() (string, error)) *ExternalTokenProvider {
	return &ExternalTokenProvider{
		tokenFunc: tokenFunc,
		tokenType: "Bearer",
	}
}

// NewExternalTokenProviderWithType creates a provider with a custom token type
func NewExternalTokenProviderWithType(tokenFunc func() (string, error), tokenType string) *ExternalTokenProvider {
	return &ExternalTokenProvider{
		tokenFunc: tokenFunc,
		tokenType: tokenType,
	}
}

// GetToken retrieves the token from the external source
func (p *ExternalTokenProvider) GetToken(ctx context.Context) (*Token, error) {
	if p.tokenFunc == nil {
		return nil, fmt.Errorf("external token provider: token function is nil")
	}

	accessToken, err := p.tokenFunc()
	if err != nil {
		return nil, fmt.Errorf("external token provider: failed to get token: %w", err)
	}

	if accessToken == "" {
		return nil, fmt.Errorf("external token provider: empty token returned")
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
