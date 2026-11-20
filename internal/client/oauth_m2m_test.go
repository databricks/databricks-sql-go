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
	t.Cleanup(recorder.client.Transport.(*userAgentTransport).base.(*http.Transport).CloseIdleConnections)
	if transport, ok := pooled.Transport.(*Transport).Base.(*http.Transport); ok {
		t.Cleanup(transport.CloseIdleConnections)
	}
	return pooled, recorder.client
}

type m2mTestEndpoint struct {
	transport           http.RoundTripper
	tokenURL            atomic.Value
	discoveries, grants atomic.Int32
}

func newM2MTestEndpoint(t *testing.T, host string, expiresIn int) *m2mTestEndpoint {
	t.Helper()
	endpoint := &m2mTestEndpoint{}
	endpoint.tokenURL.Store("https://accounts.staging.cloud.databricks.com/token")
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/.well-known/databricks-config":
			_, _ = fmt.Fprintf(writer, `{"oidc_endpoint":"https://%s/oidc/accounts/account-1"}`, host)
		case "/oidc/accounts/account-1/.well-known/openid-configuration", "/oidc/.well-known/openid-configuration":
			endpoint.discoveries.Add(1)
			_, _ = fmt.Fprintf(writer, `{"issuer":"https://%s/oidc/accounts/account-1","token_endpoint":%q}`, host, endpoint.tokenURL.Load())
		case "/token", "/oidc/oauth2/v2.0/token":
			_, _ = fmt.Fprintf(writer, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, endpoint.grants.Add(1), expiresIn)
		default:
			_, _ = writer.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(server.Close)
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	base := server.Client().Transport.(*http.Transport)
	t.Cleanup(base.CloseIdleConnections)
	endpoint.transport = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
		forwarded := request.Clone(request.Context())
		forwarded.URL.Scheme, forwarded.URL.Host = target.Scheme, target.Host
		return base.RoundTrip(forwarded)
	})
	return endpoint
}

func requestWithM2M(t *testing.T, client *http.Client, host string) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://"+host+"/query", nil)
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	assert.Empty(t, request.Header.Get("Authorization"))
}

func TestM2MRequests(t *testing.T) {
	for _, test := range []struct {
		host    string
		timeout time.Duration
	}{
		{"test.cloud.databricks.com", 0},
		{"test.gcp.databricks.com", 15 * time.Minute},
		{"adb-test.azuredatabricks.net", time.Second},
		{"test.staging.databricks.com", time.Second},
	} {
		t.Run(test.host, func(t *testing.T) {
			endpoint := newM2MTestEndpoint(t, test.host, 1)
			if strings.Contains(test.host, ".gcp.") {
				endpoint.tokenURL.Store("https://ACCOUNTS.STAGING.CLOUD.DATABRICKS.COM.:443/token")
			}
			cfg := config.WithDefaults()
			cfg.ClientTimeout = test.timeout
			cfg.Authenticator = m2m.NewAuthenticator("id", "secret", test.host)
			var sqlCalls int
			cfg.Transport = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
				assert.Equal(t, "/query", request.URL.Path)
				assert.Equal(t, fmt.Sprintf("Bearer token-%d", sqlCalls+1), request.Header.Get("Authorization"))
				sqlCalls++
				return endpoint.transport.RoundTrip(request)
			})
			for index := range 2 {
				cfg.UserAgentEntry = fmt.Sprintf("Issue413/partner-%d", index)
				pooled, oauthClient := newM2MClient(t, cfg)
				oauthClient.Transport.(*userAgentTransport).base = oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
					assert.Equal(t, BuildUserAgent(cfg), request.UserAgent())
					bound := time.Minute
					if strings.Contains(request.URL.Path, ".well-known") {
						bound = 10 * time.Second
						assert.Empty(t, request.Header.Get("Authorization"))
					} else if request.URL.Path == "/token" {
						assert.Equal(t, endpoint.tokenURL.Load(), request.URL.String())
					}
					if test.timeout > 0 && test.timeout < bound {
						bound = test.timeout
					}
					deadline, ok := request.Context().Deadline()
					assert.True(t, ok)
					assert.InDelta(t, bound.Seconds(), time.Until(deadline).Seconds(), 0.5)
					return endpoint.transport.RoundTrip(request)
				})
				requestWithM2M(t, pooled, test.host)
			}
			assert.Equal(t, 2, sqlCalls)
			assert.Equal(t, int32(2), endpoint.grants.Load())
			if !strings.Contains(test.host, "azuredatabricks") {
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
			var workers sync.WaitGroup
			start := make(chan struct{})
			for range 20 {
				pooled, oauthClient := newM2MClient(t, cfg)
				oauthClient.Transport.(*userAgentTransport).base = endpoint.transport
				if concurrent {
					workers.Go(func() { <-start; requestWithM2M(t, pooled, host) })
				} else {
					requestWithM2M(t, pooled, host)
				}
			}
			close(start)
			workers.Wait()
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
	pooled, oauthClient := newM2MClient(t, cfg)
	oauthClient.Transport.(*userAgentTransport).base = endpoint.transport
	done := make(chan struct{})
	go func() { defer close(done); requestWithM2M(t, pooled, host) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("caller transport reauthentication deadlocked")
	}
}

func TestM2MRejectsDiscoveredEndpointWithoutCaching(t *testing.T) {
	const host = "workspace.cloud.databricks.com.corp.example"
	endpoint := newM2MTestEndpoint(t, host, 3600)
	endpoint.tokenURL.Store("https://" + host + "/token")
	cfg := config.WithDefaults()
	cfg.Authenticator = m2m.NewAuthenticator("id", "secret", host)
	cfg.Transport = endpoint.transport
	pooled, oauthClient := newM2MClient(t, cfg)
	oauthClient.Transport.(*userAgentTransport).base = endpoint.transport
	response, err := pooled.Get("https://" + host + "/query")
	if response != nil {
		require.NoError(t, response.Body.Close())
	}
	require.ErrorContains(t, err, "invalid token_endpoint")
	assert.Zero(t, endpoint.grants.Load())
	endpoint.tokenURL.Store("https://accounts.cloud.databricks.com/token")
	requestWithM2M(t, pooled, host)
	assert.Equal(t, int32(2), endpoint.discoveries.Load())
	assert.Equal(t, int32(1), endpoint.grants.Load())
}
