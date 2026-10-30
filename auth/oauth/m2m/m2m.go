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
	ts, first, err := c.getOrInitTokenSource()
	if err != nil {
		return err
	}

	// c.mx guards only the lazy init above. Token refreshes are still serialized
	// inside the oauth2.ReuseTokenSource (it holds its own mutex for the whole
	// exchange), so concurrent callers wait on an in-flight refresh; that wait is
	// bounded by tokenExchangeTimeout.
	token, err := ts.Token()
	if err != nil {
		if first {
			logger.Err(err).Msg("failed to get token")
		}
		return err
	}
	if first {
		// Log via the driver's configurable logger (defaults to Warn) rather than the
		// global zerolog logger, so this line honors SetLogLevel like every other.
		logger.Debug().Msgf("databricks OAuth token fetched successfully")
	}
	token.SetAuthHeader(r)

	return nil
}

// getOrInitTokenSource returns the cached token source, building it on first use.
// c.mx guards only this lazy initialization; first reports whether this call
// created the token source.
func (c *authClient) getOrInitTokenSource() (ts oauth2.TokenSource, first bool, err error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	if c.tokenSource != nil {
		return c.tokenSource, false, nil
	}

	config, err := getConfig(context.Background(), c.hostName, c.clientID, c.clientSecret, c.scopes)
	if err != nil {
		return nil, false, fmt.Errorf("unable to generate clientCredentials.Config: %w", err)
	}

	c.tokenSource = GetTokenSource(config)
	return c.tokenSource, true, nil
}

// tokenExchangeTimeout bounds each client-credentials token request so a hung
// token endpoint surfaces as an error instead of blocking every request on the
// connector (endpoint discovery is similarly capped by oauth.hostConfigTimeout).
//
// Known limitations: the value is fixed (it is a package variable only so unit
// tests can shorten it) and is not exposed as a ConnOption, so callers behind a
// slow IdP cannot tune it. The exchange uses its own http.Client, so connector
// transport/TLS/proxy settings (WithTransport, WithSkipTLSHostVerify, …) do not
// apply, matching the previous http.DefaultClient behaviour, and the request
// context (r.Context()) cancellation is not propagated to the exchange.
var tokenExchangeTimeout = 30 * time.Second

func GetTokenSource(config clientcredentials.Config) oauth2.TokenSource {
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Timeout: tokenExchangeTimeout})
	tokenSource := config.TokenSource(ctx)
	return tokenSource
}

// getConfig is the endpoint-resolution step Authenticate uses on its first call.
// It is a variable only so unit tests can point the token URL at a local server:
// oauth.GetEndpoint requires a Databricks hostname and a publicly trusted TLS
// certificate, which an httptest server cannot provide.
var getConfig = GetConfig

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
