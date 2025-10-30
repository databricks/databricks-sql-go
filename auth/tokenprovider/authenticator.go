package tokenprovider

import (
	"context"
	"fmt"
	"net/http"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/rs/zerolog/log"
)

// TokenProviderAuthenticator implements auth.Authenticator using a TokenProvider
type TokenProviderAuthenticator struct {
	provider TokenProvider
}

// NewAuthenticator creates an authenticator from a token provider
func NewAuthenticator(provider TokenProvider) auth.Authenticator {
	return &TokenProviderAuthenticator{
		provider: provider,
	}
}

// Authenticate implements auth.Authenticator
func (a *TokenProviderAuthenticator) Authenticate(r *http.Request) error {
	ctx := r.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	token, err := a.provider.GetToken(ctx)
	if err != nil {
		return fmt.Errorf("token provider authenticator: failed to get token: %w", err)
	}

	if token.AccessToken == "" {
		return fmt.Errorf("token provider authenticator: empty access token")
	}

	token.SetAuthHeader(r)
	log.Debug().Msgf("token provider authenticator: authenticated using provider %s", a.provider.Name())

	return nil
}
