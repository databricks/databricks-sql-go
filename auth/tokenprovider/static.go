package tokenprovider

import (
	"context"
	"fmt"
	"time"
)

// StaticTokenProvider provides a static token that never changes
type StaticTokenProvider struct {
	token     string
	tokenType string
}

// NewStaticTokenProvider creates a provider with a static token
func NewStaticTokenProvider(token string) *StaticTokenProvider {
	return &StaticTokenProvider{
		token:     token,
		tokenType: "Bearer",
	}
}

// NewStaticTokenProviderWithType creates a provider with a static token and custom type
func NewStaticTokenProviderWithType(token string, tokenType string) *StaticTokenProvider {
	return &StaticTokenProvider{
		token:     token,
		tokenType: tokenType,
	}
}

// GetToken returns the static token
func (p *StaticTokenProvider) GetToken(ctx context.Context) (*Token, error) {
	if p.token == "" {
		return nil, fmt.Errorf("static token provider: token is empty")
	}

	return &Token{
		AccessToken: p.token,
		TokenType:   p.tokenType,
		ExpiresAt:   time.Time{}, // Static tokens don't expire
	}, nil
}

// Name returns the provider name
func (p *StaticTokenProvider) Name() string {
	return "static"
}
