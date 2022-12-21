package u2m

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/databricks/databricks-sql-go/auth/pkce"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
	// "golang.org/x/oauth2/authhandler"
)

const (
	LISTEN_ADDR = "localhost:8020"
)

func NewClient(clientID, issuerURL string, scopes []string) *AuthClient {
	return &AuthClient{
		clientID:  clientID,
		issuerURL: issuerURL,
		scope:     append([]string{oidc.ScopeOpenID}, scopes...),
	}
}

type AuthClient struct {
	clientID    string
	issuerURL   string
	scope       []string
	tokenSource oauth2.TokenSource
	mx          sync.Mutex
}

type authResponse struct {
	err     string
	details string
	state   string
	code    string
}

// Auth will start the OAuth Authorization Flow to authenticate the cli client
// using the users credentials in the browser. Compatible with SSO.
func (c *AuthClient) Authenticate(r *http.Request) error {
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

	// handle ctrl-c while waiting for the callback
	sigintCh := make(chan os.Signal, 1)
	signal.Notify(sigintCh, os.Interrupt)
	// receive auth callback response
	authDoneCh := make(chan authResponse)
	ctx = oidc.InsecureIssuerURLContext(ctx, c.issuerURL)
	provider, err := oidc.NewProvider(ctx, c.issuerURL)
	if err != nil {
		return fmt.Errorf("could not lookup provider details: %w", err)
	}
	config := oauth2.Config{
		ClientID:    c.clientID,
		Endpoint:    provider.Endpoint(),
		RedirectURL: fmt.Sprintf("http://%s", LISTEN_ADDR),
		Scopes:      c.scope,
	}

	state, err := randString(16)
	if err != nil {
		return fmt.Errorf("unable to generate random number: %w", err)
	}
	// nonce, err := randString(16)
	if err != nil {
		return fmt.Errorf("unable to generate random number: %w", err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		resp := authResponse{
			err:     r.URL.Query().Get("error"),
			details: r.URL.Query().Get("error_description"),
			state:   r.URL.Query().Get("state"),
			code:    r.URL.Query().Get("code"),
		}

		// Send the response back to the to cli
		authDoneCh <- resp

		// Do some checking of the response here to show more relevant content
		if resp.err != "" {
			w.WriteHeader(http.StatusBadRequest)
			_, err = w.Write([]byte(errorHTML("Identity Provider returned an error: " + resp.err)))
			if err != nil {
				log.Error().Err(err).Msg("unable to write error response")
			}
			return
		}
		if resp.state != state {
			w.WriteHeader(http.StatusBadRequest)
			_, err = w.Write([]byte(errorHTML("Authentication state received did not match original request. Please try to login again.")))
			if err != nil {
				log.Error().Err(err).Msg("unable to write error response")
			}
			return
		}

		_, err = w.Write([]byte(infoHTML("CLI Login Success", "You may close this window anytime now and go back to terminal")))
		if err != nil {
			log.Error().Err(err).Msg("unable to write success response")
		}
	})

	log.Info().Msgf("listening on http://%s/", LISTEN_ADDR)
	listener, err := net.Listen("tcp", LISTEN_ADDR)
	if err != nil {
		return err
	}
	defer listener.Close()

	// Start local server to wait for callback
	go func() {
		err := http.Serve(listener, nil)
		// in case port is in use
		if err != nil && err != http.ErrServerClosed {
			authDoneCh <- authResponse{err.Error(), "", "", ""}
		}
	}()
	p, err := pkce.Generate()
	if err != nil {
		return err
	}
	// Open url in users browser
	loginURL := config.AuthCodeURL(
		state,
		p.Challenge(),
		p.Method(),
	)

	fmt.Printf("\nOpen URL in Browser to Continue: %s\n\n", loginURL)
	err = openbrowser(loginURL)
	if err != nil {
		fmt.Println("Unable to open browser automatically. Please open manually: ", loginURL)
	}

	// Wait for callback to be received, Wait for either the callback to finish, SIGINT to be received or up to 2 minutes
	select {
	case s := <-authDoneCh:
		if s.err != "" {
			return fmt.Errorf("identity provider error: %s: %s", s.err, s.details)
		}
		oauth2Token, err := config.Exchange(ctx, s.code, p.Verifier())
		if err != nil {
			return fmt.Errorf("failed to exchange token: %w", err)
		}

		c.tokenSource = config.TokenSource(ctx, oauth2Token)
		oauth2Token.SetAuthHeader(r)
		return nil
	case <-sigintCh:
		return errors.New("interrupted while waiting for auth callback")
	case <-time.After(2 * time.Minute):
		return errors.New("timed out waiting for response from provider")
	}
}
