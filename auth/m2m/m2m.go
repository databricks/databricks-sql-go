package m2m

// clientid e92aa085-4875-42fe-ad75-ba38fb3c9706
// secretid vUdzecmn4aUi2jRDamaBOy3qThu9LSgeV_BW4UnQ

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	// "golang.org/x/oauth2/authhandler"
)

func NewClient(clientID, clientSecret, issuerURL string, scopes []string) *authClient {
	return &authClient{
		clientID:     clientID,
		clientSecret: clientSecret,
		issuerURL:    issuerURL,
		scopes:       append([]string{}, scopes...),
	}
}

type authClient struct {
	clientID     string
	clientSecret string
	issuerURL    string
	scopes       []string
	tokenSource  oauth2.TokenSource
	mx           sync.Mutex
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
	ctx = oidc.InsecureIssuerURLContext(ctx, c.issuerURL)
	provider, err := oidc.NewProvider(ctx, c.issuerURL)
	if err != nil {
		return fmt.Errorf("could not lookup provider details: %w", err)
	}
	// ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	// defer cancel()

	config := clientcredentials.Config{
		ClientID:     c.clientID,
		ClientSecret: c.clientSecret,
		TokenURL:     provider.Endpoint().TokenURL,
		Scopes:       c.scopes,
	}

	c.tokenSource = config.TokenSource(ctx)
	token, err := c.tokenSource.Token()
	log.Info().Msgf("token fetched successfully")
	if err != nil {
		log.Err(err).Msg("failed to get token")

		return err
	}
	token.SetAuthHeader(r)

	return nil

}
