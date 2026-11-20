package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type oauthTestRoundTripper func(*http.Request) (*http.Response, error)

func (transport oauthTestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

type oauthClientRecorder struct {
	auth.Authenticator
	client *http.Client
}

func (recorder *oauthClientRecorder) WithHTTPClient(client *http.Client) auth.Authenticator {
	recorder.client = client
	return recorder.Authenticator.(interface {
		WithHTTPClient(*http.Client) auth.Authenticator
	}).WithHTTPClient(client)
}

func newM2MClient(t *testing.T, cfg *config.Config) (*http.Client, *http.Client) {
	t.Helper()
	recorder := &oauthClientRecorder{Authenticator: cfg.Authenticator}
	copyConfig := *cfg
	copyConfig.Authenticator = recorder
	pooled := PooledClient(&copyConfig)
	if transport, ok := pooled.Transport.(*Transport).Base.(*http.Transport); ok {
		t.Cleanup(transport.CloseIdleConnections)
	}
	return pooled, recorder.client
}

type m2mRequest struct{ host, path, userAgent, authorization string }

type m2mTestEndpoint struct {
	transport           http.RoundTripper
	requests            chan m2mRequest
	discoveries, grants atomic.Int32
}

func newM2MTestEndpoint(t *testing.T, host string, expiresIn int) *m2mTestEndpoint {
	t.Helper()
	const tokenHost = "accounts.staging.cloud.databricks.com"
	endpoint := &m2mTestEndpoint{requests: make(chan m2mRequest, 64)}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		endpoint.requests <- m2mRequest{request.Host, request.URL.Path, request.UserAgent(), request.Header.Get("Authorization")}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/.well-known/databricks-config":
			_, _ = fmt.Fprintf(writer, `{"oidc_endpoint":"https://%s/oidc/accounts/account-1"}`, host)
		case "/oidc/accounts/account-1/.well-known/openid-configuration":
			endpoint.discoveries.Add(1)
			_, _ = fmt.Fprintf(writer, `{"issuer":"https://%s/oidc/accounts/account-1","token_endpoint":"https://%s/token"}`, host, tokenHost)
		case "/token", "/oidc/oauth2/v2.0/token":
			_, _ = fmt.Fprintf(writer, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, endpoint.grants.Add(1), expiresIn)
		default:
			_, _ = writer.Write([]byte(`{}`))
		}
	}))
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{host, tokenHost},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	encoded, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	require.NoError(t, err)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{encoded}, PrivateKey: key}}}
	server.StartTLS()
	t.Cleanup(server.Close)
	base := oauthTransport()
	originalTLS, originalDial := base.TLSClientConfig, base.DialContext
	base.CloseIdleConnections()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	base.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	base.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	t.Cleanup(func() {
		base.CloseIdleConnections()
		base.TLSClientConfig, base.DialContext = originalTLS, originalDial
	})
	endpoint.transport = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/query" {
			return nil, fmt.Errorf("caller SQL transport received OAuth request: %s", request.URL.Path)
		}
		return base.RoundTrip(request)
	})
	return endpoint
}

func requestWithM2M(client *http.Client, host string) error {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://"+host+"/query", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	if err := response.Body.Close(); err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK || request.Header.Get("Authorization") != "" {
		return fmt.Errorf("unexpected response status or mutated request")
	}
	return nil
}

func TestM2MRequests(t *testing.T) {
	for _, host := range []string{"test.cloud.databricks.com", "test.gcp.databricks.com", "adb-test.azuredatabricks.net", "test.staging.databricks.com"} {
		t.Run(host, func(t *testing.T) {
			endpoint := newM2MTestEndpoint(t, host, 1)
			cfg := config.WithDefaults()
			cfg.Authenticator = m2m.NewAuthenticator("id", "secret", host)
			cfg.Transport = endpoint.transport
			for index := range 2 {
				cfg.UserAgentEntry = fmt.Sprintf("Issue413/partner-%d", index)
				pooled, oauthClient := newM2MClient(t, cfg)
				assert.Equal(t, cfg.ClientTimeout, oauthClient.Timeout)
				require.NoError(t, requestWithM2M(pooled, host))
				for len(endpoint.requests) > 0 {
					request := <-endpoint.requests
					if request.path != "/query" {
						assert.Equal(t, BuildUserAgent(cfg), request.userAgent)
					} else {
						assert.Equal(t, fmt.Sprintf("Bearer token-%d", index+1), request.authorization)
					}
					if request.path == "/token" {
						assert.Equal(t, "accounts.staging.cloud.databricks.com", request.host)
					}
				}
			}
			assert.Equal(t, int32(2), endpoint.grants.Load())
			if !strings.Contains(host, "azuredatabricks") {
				assert.Equal(t, int32(1), endpoint.discoveries.Load())
			}
		})
	}
}

func TestM2MSharedAuthenticatorSingleFlight(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrent=%t", concurrent), func(t *testing.T) {
			const host = "workspace.cloud.databricks.com"
			endpoint := newM2MTestEndpoint(t, host, 3600)
			cfg := config.WithDefaults()
			cfg.Authenticator = m2m.NewAuthenticator("id", "secret", host)
			cfg.Transport = endpoint.transport
			start, results := make(chan struct{}), make(chan error, 20)
			for range 20 {
				pooled, _ := newM2MClient(t, cfg)
				if concurrent {
					go func() { <-start; results <- requestWithM2M(pooled, host) }()
				} else {
					results <- requestWithM2M(pooled, host)
				}
			}
			close(start)
			for range 20 {
				assert.NoError(t, <-results)
			}
			assert.Equal(t, int32(1), endpoint.grants.Load())
			assert.Equal(t, int32(1), endpoint.discoveries.Load())
		})
	}
}

func TestM2MTransportCanReauthenticateSharedAuthenticator(t *testing.T) {
	const host = "workspace.cloud.databricks.com"
	endpoint := newM2MTestEndpoint(t, host, 3600)
	shared := m2m.NewAuthenticator("id", "secret", host)
	cfg := config.WithDefaults()
	cfg.Authenticator = shared
	cfg.Transport = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		if err := shared.Authenticate(request); err != nil {
			return nil, err
		}
		return endpoint.transport.RoundTrip(request)
	})
	pooled, _ := newM2MClient(t, cfg)
	done := make(chan error, 1)
	go func() { done <- requestWithM2M(pooled, host) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("caller transport reauthentication deadlocked")
	}
}
