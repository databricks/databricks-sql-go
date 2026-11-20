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
	defer c.mx.Unlock()
	if c.tokenSource != nil {
		token, err := c.tokenSource.Token()
		if err != nil {
			return err
		}
		token.SetAuthHeader(r)
		return nil
	}

	ctx := withTokenRequestTimeout(context.Background())
	config, err := getConfig(ctx, c.hostName, c.clientID, c.clientSecret, c.scopes)
	if err != nil {
		return fmt.Errorf("unable to generate clientCredentials.Config: %w", err)
	}

	// The token source keeps ctx, so refreshes are bounded too.
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

var (
	// tokenRequestTimeout bounds each token request, matching the SQL kernel's
	// default HTTP timeout. Until a fetch succeeds, oauth2's AuthStyle auto-detect
	// retries each failed request once, so such a fetch can take up to twice this.
	tokenRequestTimeout = 60 * time.Second

	// getConfig is replaced in tests to skip endpoint discovery.
	getConfig = GetConfig
)

// withTokenRequestTimeout returns ctx carrying a copy of its oauth2.HTTPClient
// (http.DefaultClient when unset) whose Timeout is at most tokenRequestTimeout.
func withTokenRequestTimeout(ctx context.Context) context.Context {
	base := http.DefaultClient
	if hc, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); ok && hc != nil {
		base = hc
	}
	client := *base
	if client.Timeout <= 0 || client.Timeout > tokenRequestTimeout {
		client.Timeout = tokenRequestTimeout
	}
	return context.WithValue(ctx, oauth2.HTTPClient, &client)
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
