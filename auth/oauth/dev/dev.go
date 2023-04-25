package dev

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth/u2m"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

const (
	LISTEN_ADDR = "localhost:8020"
)

func NewDevAuthenticator(clientID, hostName string, scopes ...string) auth.Authenticator {
	return &devAuthenticator{
		clientID: clientID,
		hostName: hostName,
		scopes:   scopes,
	}
}

type devAuthenticator struct {
	clientID    string
	hostName    string
	scopes      []string
	tokenSource oauth2.TokenSource
	mx          sync.Mutex
}

// Auth will start the OAuth Authorization Flow to authenticate the cli client
// using the users credentials in the browser. Compatible with SSO.
func (c *devAuthenticator) Authenticate(r *http.Request) error {
	c.mx.Lock()
	defer c.mx.Unlock()
	if c.tokenSource != nil {
		token, err := c.tokenSource.Token()
		if err == nil {
			token.SetAuthHeader(r)
			return nil
		} else if !strings.Contains(err.Error(), "invalid_grant") {
			return err
		}

		token.SetAuthHeader(r)
		return nil
	}

	config, err := u2m.GetConfig(context.Background(), c.hostName, c.clientID, "", LISTEN_ADDR, c.scopes)
	if err != nil {
		return fmt.Errorf("unable to generate oauth2.Config: %w", err)
	}

	tokenSource, err := GetTokenSource(context.Background(), config, 0)
	if err != nil {
		return fmt.Errorf("unable to get token source: %w", err)
	}

	c.tokenSource = tokenSource

	token, err := tokenSource.Token()
	if err != nil {
		return fmt.Errorf("unable to get token source: %w", err)
	}

	token.SetAuthHeader(r)

	return nil
}

type authResponse struct {
	err     string
	details string
	state   string
	code    string
}

func GetTokenSource(ctx context.Context, config oauth2.Config, timeout time.Duration) (oauth2.TokenSource, error) {
	if timeout == 0 {
		timeout = 2 * time.Minute
	}

	state, err := randString(16)
	if err != nil {
		err = fmt.Errorf("unable to generate random number: %w", err)
		return nil, err
	}

	challenge, challengeMethod, verifier, err := u2m.GetAuthCodeOptions()
	if err != nil {
		return nil, err
	}

	loginURL := u2m.GetLoginURL(config, state, challenge, challengeMethod)

	// handle ctrl-c while waiting for the callback
	sigintCh := make(chan os.Signal, 1)
	signal.Notify(sigintCh, os.Interrupt)
	// receive auth callback response
	authDoneCh := make(chan authResponse)

	u, _ := url.Parse(config.RedirectURL)
	if u.Path == "" {
		u.Path = "/"
	}

	http.HandleFunc(u.Path, handlerFunc(authDoneCh, state))

	log.Info().Msgf("listening on %s://%s/", u.Scheme, u.Host)
	listener, err := net.Listen("tcp", u.Host)
	if err != nil {
		return nil, err
	}
	defer listener.Close()

	srv := &http.Server{
		ReadHeaderTimeout: 3 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	defer srv.Close()

	// Start local server to wait for callback
	go func() {
		err := srv.Serve(listener)

		// in case port is in use
		if err != nil && err != http.ErrServerClosed {
			authDoneCh <- authResponse{err: err.Error()}
		}
	}()

	fmt.Printf("\nOpen URL in Browser to Continue: %s\n\n", loginURL)
	err = openbrowser(loginURL)
	if err != nil {
		fmt.Println("Unable to open browser automatically. Please open manually: ", loginURL)
	}

	// Wait for callback to be received, Wait for either the callback to finish, SIGINT to be received or up to 2 minutes
	select {
	case authResponse := <-authDoneCh:
		if authResponse.err != "" {
			return nil, fmt.Errorf("identity provider error: %s: %s", authResponse.err, authResponse.details)
		}
		token, err := config.Exchange(ctx, authResponse.code, verifier)
		if err != nil {
			return nil, fmt.Errorf("failed to exchange token: %w", err)
		}

		return config.TokenSource(ctx, token), nil

	case <-sigintCh:
		return nil, errors.New("interrupted while waiting for auth callback")

	case <-time.After(timeout):
		return nil, errors.New("timed out waiting for response from provider")
	}
}

func handlerFunc(authDoneCh chan authResponse, state string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := authResponse{
			err:     r.URL.Query().Get("error"),
			details: r.URL.Query().Get("error_description"),
			state:   r.URL.Query().Get("state"),
			code:    r.URL.Query().Get("code"),
		}

		// Send the response back to the to cli
		defer func() { authDoneCh <- resp }()

		// Do some checking of the response here to show more relevant content
		if resp.err != "" {
			w.WriteHeader(http.StatusBadRequest)
			_, err := w.Write([]byte(errorHTML("Identity Provider returned an error: " + resp.err)))
			if err != nil {
				log.Error().Err(err).Msg("unable to write error response")
			}
			return
		}
		if resp.state != state {
			w.WriteHeader(http.StatusBadRequest)
			_, err := w.Write([]byte(errorHTML("Authentication state received did not match original request. Please try to login again.")))
			if err != nil {
				log.Error().Err(err).Msg("unable to write error response")
			}
			return
		}

		_, err := w.Write([]byte(infoHTML("CLI Login Success", "You may close this window anytime now and go back to terminal")))
		if err != nil {
			log.Error().Err(err).Msg("unable to write success response")
		}
	}
}

func randString(nByte int) (string, error) {
	b := make([]byte, nByte)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func openbrowser(url string) error {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	return err
}
