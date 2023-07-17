package defauth

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
	"github.com/databricks/databricks-sql-go/auth/oauth"
	"github.com/databricks/databricks-sql-go/auth/oauth/u2m"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

const (
	azureEnterpriseAppId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
	azureRedirctURL      = "localhost:8020"
	awsAppId             = "databricks-sql-connector"
	awsRedirctURL        = "localhost:8030"
)

func NewDefaultAuthenticator(hostName string, timeout time.Duration) (auth.Authenticator, error) {

	cloud := oauth.InferCloudFromHost(hostName)

	var clientID, redirectURL string
	if cloud == oauth.AWS {
		clientID = awsAppId
		redirectURL = awsRedirctURL
	} else if cloud == oauth.Azure {
		clientID = azureEnterpriseAppId
		redirectURL = azureRedirctURL
	} else {
		return nil, errors.New("unhandled cloud type: " + cloud.String())
	}

	// Get an oauth2 config
	config, err := u2m.GetConfig(context.Background(), hostName, clientID, "", redirectURL, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to generate oauth2.Config: %w", err)
	}

	tsp, err := GetTokenSourceProvider(context.Background(), config, timeout)

	return &defAuthenticator{
		clientID: clientID,
		hostName: hostName,
		tsp:      tsp,
	}, err
}

type defAuthenticator struct {
	clientID string
	hostName string
	// scopes      []string
	tokenSource oauth2.TokenSource
	tsp         *tsp
	mx          sync.Mutex
}

// Auth will start the OAuth Authorization Flow to authenticate the cli client
// using the users credentials in the browser. Compatible with SSO.
func (c *defAuthenticator) Authenticate(r *http.Request) error {
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

	// Get an oauth2 config
	// config, err := u2m.GetConfig(context.Background(), c.hostName, c.clientID, "", LISTEN_ADDR, c.scopes)
	// if err != nil {
	// 	return fmt.Errorf("unable to generate oauth2.Config: %w", err)
	// }

	// tokenSource, err := GetTokenSource(context.Background(), config, 0)
	// if err != nil {
	// 	return fmt.Errorf("unable to get token source: %w", err)
	// }

	tokenSource, err := c.tsp.GetTokenSource()
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

type tsp struct {
	timeout     time.Duration
	state       string
	sigintCh    chan os.Signal
	authDoneCh  chan authResponse
	redirectURL *url.URL
	config      oauth2.Config
}

func (tsp *tsp) GetTokenSource() (oauth2.TokenSource, error) {
	state, err := randString(16)
	if err != nil {
		err = fmt.Errorf("unable to generate random number: %w", err)
		return nil, err
	}

	challenge, challengeMethod, verifier, err := u2m.GetAuthCodeOptions()
	if err != nil {
		return nil, err
	}

	loginURL := tsp.config.AuthCodeURL(state, challenge, challengeMethod)
	tsp.state = state

	log.Info().Msgf("listening on %s://%s/", tsp.redirectURL.Scheme, tsp.redirectURL.Host)
	listener, err := net.Listen("tcp", tsp.redirectURL.Host)
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
			tsp.authDoneCh <- authResponse{err: err.Error()}
		}
	}()

	fmt.Printf("\nOpen URL in Browser to Continue: %s\n\n", loginURL)
	err = openbrowser(loginURL)
	if err != nil {
		fmt.Println("Unable to open browser automatically. Please open manually: ", loginURL)
	}

	ctx := context.Background()
	// Wait for callback to be received, Wait for either the callback to finish, SIGINT to be received or up to 2 minutes
	select {
	case authResponse := <-tsp.authDoneCh:
		if authResponse.err != "" {
			return nil, fmt.Errorf("identity provider error: %s: %s", authResponse.err, authResponse.details)
		}
		token, err := tsp.config.Exchange(ctx, authResponse.code, verifier)
		if err != nil {
			return nil, fmt.Errorf("failed to exchange token: %w", err)
		}

		return tsp.config.TokenSource(ctx, token), nil

	case <-tsp.sigintCh:
		return nil, errors.New("interrupted while waiting for auth callback")

	case <-time.After(tsp.timeout):
		return nil, errors.New("timed out waiting for response from provider")
	}
}

func (tsp *tsp) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := authResponse{
		err:     r.URL.Query().Get("error"),
		details: r.URL.Query().Get("error_description"),
		state:   r.URL.Query().Get("state"),
		code:    r.URL.Query().Get("code"),
	}

	// Send the response back to the to cli
	defer func() { tsp.authDoneCh <- resp }()

	// Do some checking of the response here to show more relevant content
	if resp.err != "" {
		log.Error().Msg(resp.err)
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(errorHTML("Identity Provider returned an error: " + resp.err)))
		if err != nil {
			log.Error().Err(err).Msg("unable to write error response")
		}
		return
	}
	if resp.state != tsp.state {
		msg := "Authentication state received did not match original request. Please try to login again."
		log.Error().Msg(msg)
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(errorHTML(msg)))
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

func GetTokenSourceProvider(ctx context.Context, config oauth2.Config, timeout time.Duration) (*tsp, error) {
	if timeout == 0 {
		timeout = 2 * time.Minute
	}

	// handle ctrl-c while waiting for the callback
	sigintCh := make(chan os.Signal, 1)
	signal.Notify(sigintCh, os.Interrupt)

	// receive auth callback response
	authDoneCh := make(chan authResponse)

	u, _ := url.Parse(config.RedirectURL)
	if u.Path == "" {
		u.Path = "/"
	}

	tsp := &tsp{
		timeout:     timeout,
		sigintCh:    sigintCh,
		authDoneCh:  authDoneCh,
		redirectURL: u,
		config:      config,
	}

	http.Handle(u.Path, tsp)

	return tsp, nil
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
