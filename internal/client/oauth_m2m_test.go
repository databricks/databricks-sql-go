package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type m2mRequest struct {
	host, path, userAgent, authorization string
}

type m2mTestEndpoint struct {
	transport http.RoundTripper
	tokenHits atomic.Int32
	mutex     sync.Mutex
	requests  []m2mRequest
}

func newM2MTestEndpoint(t *testing.T, host, tokenHost string, expiresIn int) *m2mTestEndpoint {
	t.Helper()
	endpoint := &m2mTestEndpoint{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/.well-known/databricks-config":
			_, _ = fmt.Fprintf(writer, `{"oidc_endpoint":"https://%s/oidc/accounts/account-1"}`, host)
		case "/oidc/accounts/account-1/.well-known/openid-configuration":
			_, _ = fmt.Fprintf(writer, `{"issuer":"https://%s/oidc/accounts/account-1","token_endpoint":"https://%s/token"}`, host, tokenHost)
		case "/token", "/oidc/oauth2/v2.0/token":
			clientID, secret, ok := request.BasicAuth()
			if !ok || clientID != "test-client" || secret != "test-secret" {
				writer.WriteHeader(http.StatusUnauthorized)
				_, _ = writer.Write([]byte(`{"error":"invalid_client"}`))
				return
			}
			if err := request.ParseForm(); err != nil || request.Form.Get("grant_type") != "client_credentials" {
				http.Error(writer, "invalid grant", http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprintf(writer, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, endpoint.tokenHits.Add(1), expiresIn)
		case "/query":
			_, _ = writer.Write([]byte(`{}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	base := server.Client().Transport
	t.Cleanup(base.(*http.Transport).CloseIdleConnections)
	endpoint.transport = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		endpoint.mutex.Lock()
		endpoint.requests = append(endpoint.requests, m2mRequest{request.URL.Host, request.URL.Path, request.UserAgent(), request.Header.Get("Authorization")})
		endpoint.mutex.Unlock()
		forwarded := request.Clone(request.Context())
		forwarded.URL.Scheme, forwarded.URL.Host = target.Scheme, target.Host
		return base.RoundTrip(forwarded)
	})
	return endpoint
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

func m2mTestClient(t *testing.T, cfg *config.Config, transport http.RoundTripper) *http.Client {
	t.Helper()
	recorder := &oauthClientRecorder{Authenticator: cfg.Authenticator}
	copyConfig := *cfg
	copyConfig.Authenticator = recorder
	if cfg.Transport != nil {
		copyConfig.Transport = &struct{ http.RoundTripper }{cfg.Transport}
	}
	pooled := PooledClient(&copyConfig)
	decorator := recorder.client.Transport.(*userAgentTransport)
	driverTransport := decorator.base.(*http.Transport)
	t.Cleanup(driverTransport.CloseIdleConnections)
	if cfg.Transport != nil {
		assert.NotSame(t, copyConfig.Transport, driverTransport)
		assert.Same(t, copyConfig.Transport, pooled.Transport.(*Transport).Base)
	}
	assert.Nil(t, driverTransport.TLSClientConfig)
	assert.NotNil(t, driverTransport.Proxy)
	decorator.base = transport
	return pooled
}

func requestWithM2M(t *testing.T, client *http.Client, host string) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://"+host+"/query", nil)
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Empty(t, request.Header.Get("Authorization"))
}

func TestM2MDriverTransportDiscoveryGrantAndRefresh(t *testing.T) {
	for _, host := range []string{"test.cloud.databricks.com", "test.gcp.databricks.com", "adb-test.azuredatabricks.net", "test.staging.databricks.com"} {
		t.Run(host, func(t *testing.T) {
			const tokenHost = "accounts.staging.cloud.databricks.com"
			endpoint := newM2MTestEndpoint(t, host, tokenHost, 1)
			cfg := config.WithDefaults()
			cfg.UserAgentEntry = "Issue413/partner"
			cfg.Authenticator = m2m.NewAuthenticator("test-client", "test-secret", host)
			var sqlCalls atomic.Int32
			cfg.Transport = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
				assert.Equal(t, "/query", request.URL.Path, "caller transport must never see OAuth credentials")
				sqlCalls.Add(1)
				return endpoint.transport.RoundTrip(request)
			})
			pooled := m2mTestClient(t, cfg, endpoint.transport)
			requestWithM2M(t, pooled, host)
			requestWithM2M(t, pooled, host)
			assert.Equal(t, int32(2), endpoint.tokenHits.Load())
			assert.Equal(t, int32(2), sqlCalls.Load())
			for _, request := range endpoint.requests {
				if request.path != "/query" {
					assert.Equal(t, BuildUserAgent(cfg), request.userAgent)
				}
				if strings.Contains(request.path, ".well-known") {
					assert.Empty(t, request.authorization)
				}
				if request.path == "/token" {
					assert.Equal(t, tokenHost, request.host)
				}
			}
		})
	}
}

func TestM2MSharedAuthenticatorSingleFlight(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrent=%t", concurrent), func(t *testing.T) {
			const host = "workspace.cloud.databricks.com"
			endpoint := newM2MTestEndpoint(t, host, host, 3600)
			shared := m2m.NewAuthenticator("test-client", "test-secret", host)
			clients := make([]*http.Client, 20)
			for index := range clients {
				cfg := config.WithDefaults()
				cfg.Authenticator, cfg.Transport = shared, endpoint.transport
				cfg.UserAgentEntry = fmt.Sprintf("partner/%d", index)
				clients[index] = m2mTestClient(t, cfg, endpoint.transport)
			}
			var workers sync.WaitGroup
			start := make(chan struct{})
			for _, client := range clients {
				if concurrent {
					workers.Go(func() { <-start; requestWithM2M(t, client, host) })
				} else {
					requestWithM2M(t, client, host)
				}
			}
			close(start)
			workers.Wait()
			assert.Equal(t, int32(1), endpoint.tokenHits.Load())
			discoveries := 0
			for _, request := range endpoint.requests {
				if strings.HasSuffix(request.path, "/.well-known/openid-configuration") {
					discoveries++
				}
			}
			assert.Equal(t, 1, discoveries)
		})
	}
}

func TestM2MRootedDiscoveryEndpoints(t *testing.T) {
	const host = "test.cloud.databricks.com"
	for _, tokenHost := range []string{host + ".", host + ".:443", "TEST.CLOUD.DATABRICKS.COM.:443"} {
		t.Run(tokenHost, func(t *testing.T) {
			endpoint := newM2MTestEndpoint(t, host, tokenHost, 3600)
			cfg := config.WithDefaults()
			cfg.Authenticator = m2m.NewAuthenticator("test-client", "test-secret", host)
			cfg.Transport = endpoint.transport
			pooled := m2mTestClient(t, cfg, endpoint.transport)
			requestWithM2M(t, pooled, host)
			requestWithM2M(t, pooled, host)
			assert.Equal(t, int32(1), endpoint.tokenHits.Load())
			for _, request := range endpoint.requests {
				if request.path == "/token" {
					assert.Equal(t, tokenHost, request.host)
				}
			}
		})
	}
}

func TestM2MTransportCanReauthenticateSharedAuthenticator(t *testing.T) {
	const host = "workspace.cloud.databricks.com"
	endpoint := newM2MTestEndpoint(t, host, host, 3600)
	shared := m2m.NewAuthenticator("test-client", "test-secret", host)
	cfg := config.WithDefaults()
	cfg.Authenticator = shared
	cfg.Transport = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		cloned := request.Clone(request.Context())
		if err := shared.Authenticate(cloned); err != nil {
			return nil, err
		}
		return endpoint.transport.RoundTrip(cloned)
	})
	pooled := m2mTestClient(t, cfg, endpoint.transport)
	done := make(chan struct{})
	go func() { defer close(done); requestWithM2M(t, pooled, host) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("caller transport reauthentication deadlocked")
	}
	assert.Equal(t, int32(1), endpoint.tokenHits.Load())
}

func TestM2MConnectorTimeoutBoundsDiscoveryAndRefresh(t *testing.T) {
	for _, timeout := range []time.Duration{0, 15 * time.Minute, 2 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			const host = "workspace.cloud.databricks.com"
			endpoint := newM2MTestEndpoint(t, host, host, 1)
			shared := m2m.NewAuthenticator("test-client", "test-secret", host)
			for range 2 {
				cfg := config.WithDefaults()
				cfg.Authenticator, cfg.Transport, cfg.ClientTimeout = shared, endpoint.transport, timeout
				cfg.UserAgentEntry = "Issue413/composed"
				pooled := m2mTestClient(t, cfg, oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
					wantTimeout := time.Minute
					if strings.Contains(request.URL.Path, ".well-known") {
						wantTimeout = 10 * time.Second
					}
					if timeout > 0 && timeout < wantTimeout {
						wantTimeout = timeout
					}
					deadline, ok := request.Context().Deadline()
					assert.True(t, ok)
					assert.InDelta(t, wantTimeout.Seconds(), time.Until(deadline).Seconds(), 0.5)
					assert.Contains(t, request.UserAgent(), "Issue413/composed")
					return endpoint.transport.RoundTrip(request)
				}))
				requestWithM2M(t, pooled, host)
			}
			assert.Equal(t, int32(2), endpoint.tokenHits.Load())
		})
	}
}

func TestM2MDriverTransportPreservesTokenError(t *testing.T) {
	const host = "adb-test.azuredatabricks.net"
	endpoint := newM2MTestEndpoint(t, host, host, 3600)
	cfg := config.WithDefaults()
	cfg.Authenticator = m2m.NewAuthenticator("invalid", "invalid", host)
	cfg.Transport = oauthTestRoundTripper(func(*http.Request) (*http.Response, error) {
		t.Error("SQL must not run after failed OAuth")
		return nil, nil
	})
	pooled := m2mTestClient(t, cfg, endpoint.transport)
	response, err := pooled.Get("https://" + host + "/query")
	if response != nil {
		require.NoError(t, response.Body.Close())
	}
	var retrieveError *oauth2.RetrieveError
	require.ErrorAs(t, err, &retrieveError)
	assert.Equal(t, http.StatusUnauthorized, retrieveError.Response.StatusCode)
	assert.Equal(t, "invalid_client", retrieveError.ErrorCode)
}
