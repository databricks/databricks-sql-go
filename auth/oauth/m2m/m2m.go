package m2m

import (
	"context"
	"fmt"
	"net/http"
	"sync"

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
	httpClient   *http.Client
	tokenSource  oauth2.TokenSource
	mx           sync.Mutex
}

// SetHTTPClient routes OIDC discovery and token requests through the given client
// instead of http.DefaultClient, so they carry the connector's transport and
// User-Agent (WithTransport, WithUserAgentEntry) like the rest of the driver's
// traffic. The driver calls it structurally once the connector config is final;
// it is a no-op on token sources already created.
func (c *authClient) SetHTTPClient(client *http.Client) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.httpClient = client
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
	defer c.mx.Unlock()
	if c.tokenSource != nil {
		token, err := c.tokenSource.Token()
		if err != nil {
			return err
		}
		token.SetAuthHeader(r)
		return nil
	}

	ctx := context.Background()
	if c.httpClient != nil {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, c.httpClient)
	}
	config, err := GetConfig(ctx, c.hostName, c.clientID, c.clientSecret, c.scopes)
	if err != nil {
		return fmt.Errorf("unable to generate clientCredentials.Config: %w", err)
	}

	c.tokenSource = config.TokenSource(ctx)
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

func GetTokenSource(config clientcredentials.Config) oauth2.TokenSource {
	tokenSource := config.TokenSource(context.Background())
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
