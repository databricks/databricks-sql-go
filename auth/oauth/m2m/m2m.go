package m2m

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth"
	"github.com/databricks/databricks-sql-go/logger"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func NewAuthenticator(clientID, clientSecret, hostName string) auth.Authenticator {
	return NewAuthenticatorWithScopes(clientID, clientSecret, hostName, []string{})
}

// NewAuthenticatorWithScopes requests exactly the given scopes, or "all-apis" when
// scopes is empty.
func NewAuthenticatorWithScopes(clientID, clientSecret, hostName string, scopes []string) auth.Authenticator {
	scopes = GetScopes(hostName, scopes)
	return &authClient{
		clientID:     clientID,
		clientSecret: clientSecret,
		hostName:     hostName,
		scopes:       scopes,
	}
}

type authClient struct {
	clientID     string
	clientSecret string
	hostName     string
	scopes       []string
	tokenSource  oauth2.TokenSource
	mx           sync.Mutex
}

// M2MCredentials exposes the raw client-credentials so the SEA-via-kernel backend
// can drive the kernel's own M2M flow, keeping cfg.Authenticator the single source
// of truth for auth mode. It structurally satisfies the M2MCredentialsProvider
// interface the kernel backend asserts (defined in internal/backend/kernel, so the
// secret-reading capability is not part of the driver's public API).
func (c *authClient) M2MCredentials() (clientID, clientSecret string) {
	return c.clientID, c.clientSecret
}

// M2MScopes exposes the configured scopes so the kernel backend can forward them.
func (c *authClient) M2MScopes() []string {
	return c.scopes
}

// Auth will start the OAuth Authorization Flow to authenticate the cli client
// using the users credentials in the browser. Compatible with SSO.
func (c *authClient) Authenticate(r *http.Request) error {
	c.mx.Lock()
	if ts := c.tokenSource; ts != nil {
		// The token source (an oauth2.ReuseTokenSource) is safe for concurrent
		// use, so don't hold c.mx across a potentially slow token exchange.
		c.mx.Unlock()
		token, err := ts.Token()
		if err != nil {
			return err
		}
		token.SetAuthHeader(r)
		return nil
	}
	defer c.mx.Unlock()

	config, err := GetConfig(context.Background(), c.hostName, c.clientID, c.clientSecret, c.scopes)
	if err != nil {
		return fmt.Errorf("unable to generate clientCredentials.Config: %w", err)
	}

	c.tokenSource = GetTokenSource(config)
	token, err := c.tokenSource.Token()
	if err != nil {
		logger.Err(err).Msg("failed to get token")

		return err
	}
	// Log via the driver's configurable logger (defaults to Warn) rather than the
	// global zerolog logger, so this line honors SetLogLevel like every other.
	logger.Debug().Msgf("databricks OAuth token fetched successfully")
	token.SetAuthHeader(r)

	return nil

}

// tokenExchangeTimeout bounds each client-credentials token request so a hung
// token endpoint surfaces as an error instead of blocking every request on the
// connector (endpoint discovery is similarly capped by oauth.hostConfigTimeout).
var tokenExchangeTimeout = 30 * time.Second

func GetTokenSource(config clientcredentials.Config) oauth2.TokenSource {
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Timeout: tokenExchangeTimeout})
	tokenSource := config.TokenSource(ctx)
	return tokenSource
}

func GetConfig(ctx context.Context, issuerURL, clientID, clientSecret string, scopes []string) (clientcredentials.Config, error) {
	// Get the endpoint based on the host name
	endpoint, err := oauth.GetEndpoint(ctx, issuerURL)
	if err != nil {
		return clientcredentials.Config{}, fmt.Errorf("could not lookup provider details: %w", err)
	}

	config := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     endpoint.TokenURL,
		Scopes:       scopes,
	}

	return config, nil
}

// GetScopes returns scopes unchanged, defaulting to "all-apis" only when empty so a
// least-privilege secret (e.g. scoped to "sql") can authenticate.
func GetScopes(hostName string, scopes []string) []string {
	if len(scopes) == 0 {
		return []string{"all-apis"}
	}

	return scopes
}
