package tokenprovider

import (
	"context"
	"fmt"
	"net/http"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/rs/zerolog/log"
)

// TokenProviderAuthenticator implements auth.Authenticator using a TokenProvider.
//
// Authentication Flow:
// 1. On each Authenticate() call, retrieves a token from the configured TokenProvider
// 2. The provider may implement its own caching and refresh logic
// 3. Validates the returned token is non-empty
// 4. Sets the Authorization header with the token type and value
//
// The authenticator delegates all token management (caching, refresh, expiry)
// to the underlying TokenProvider implementation.
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
