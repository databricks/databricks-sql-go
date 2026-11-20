package dbsql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type m2mRequest struct {
	path          string
	userAgent     string
	authorization string
}

type m2mRoundTripper func(*http.Request) (*http.Response, error)

type m2mIdentityTransport struct {
	http.RoundTripper
}

func (transport m2mRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

type m2mTestEndpoint struct {
	transport http.RoundTripper
	tokenHits atomic.Int32
	mutex     sync.Mutex
	requests  []m2mRequest
}

func newM2MTestEndpoint(t *testing.T, host string, expiresIn int) *m2mTestEndpoint {
	t.Helper()
	endpoint := &m2mTestEndpoint{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/.well-known/databricks-config":
			_, _ = fmt.Fprintf(writer, `{"oidc_endpoint":"https://%s/oidc"}`, host)
		case "/oidc/.well-known/openid-configuration":
			_, _ = fmt.Fprintf(writer, `{"issuer":"https://%s/oidc","token_endpoint":"https://%s/token"}`, host, host)
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
			_, _ = fmt.Fprintf(writer, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`,
				endpoint.tokenHits.Add(1), expiresIn)
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
	endpoint.transport = &m2mIdentityTransport{m2mRoundTripper(func(request *http.Request) (*http.Response, error) {
		endpoint.mutex.Lock()
		endpoint.requests = append(endpoint.requests, m2mRequest{
			path: request.URL.Path, userAgent: request.Header.Get("User-Agent"),
			authorization: request.Header.Get("Authorization"),
		})
		endpoint.mutex.Unlock()
		forwarded := request.Clone(request.Context())
		forwarded.URL.Scheme = target.Scheme
		forwarded.URL.Host = target.Host
		return base.RoundTrip(forwarded)
	})}
	return endpoint
}

func blockM2MDefaultTransport(t *testing.T) {
	t.Helper()
	previousTransport, previousClient := http.DefaultTransport, http.DefaultClient
	http.DefaultTransport = m2mRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("OAuth bypassed connector transport")
	})
	http.DefaultClient = &http.Client{Transport: http.DefaultTransport}
	t.Cleanup(func() { http.DefaultTransport, http.DefaultClient = previousTransport, previousClient })
}

func requestWithM2M(t *testing.T, connector *connector) {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"https://"+connector.cfg.Host+"/query", nil)
	require.NoError(t, err)
	response, err := connector.client.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Empty(t, request.Header.Get("Authorization"))
}

//nolint:gosec // The subprocess only reexecutes this test binary.
func TestM2MTransportCanReauthenticateSharedAuthenticator(t *testing.T) {
	if os.Getenv("DATABRICKS_M2M_REENTRY_TEST") != "1" {
		executable, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, executable,
			"-test.run=^TestM2MTransportCanReauthenticateSharedAuthenticator$", "-test.timeout=5s")
		command.Env = append(os.Environ(), "DATABRICKS_M2M_REENTRY_TEST=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	blockM2MDefaultTransport(t)
	for _, host := range []string{"test.cloud.databricks.com", "test.gcp.databricks.com", "adb-test.azuredatabricks.net"} {
		t.Run(host, func(t *testing.T) {
			endpoint := newM2MTestEndpoint(t, host, 3600)
			shared := m2m.NewAuthenticator("test-client", "test-secret", host)
			active := shared
			transport := &m2mIdentityTransport{m2mRoundTripper(func(request *http.Request) (*http.Response, error) {
				cloned := request.Clone(request.Context())
				authenticator := shared
				if request.URL.Path == "/query" {
					authenticator = active
				}
				if err := authenticator.Authenticate(cloned); err != nil {
					return nil, err
				}
				return endpoint.transport.RoundTrip(cloned)
			})}
			created, err := NewConnector(WithServerHostname(host), WithHTTPPath("/query"),
				WithAuthenticator(shared), WithTransport(transport), WithRetries(0, 0, 0))
			require.NoError(t, err)
			retryable := created.(*connector).client.Transport.(*retryablehttp.RoundTripper).Client
			active = retryable.HTTPClient.Transport.(*client.Transport).Authr
			requestWithM2M(t, created.(*connector))
			requestWithM2M(t, created.(*connector))
			require.Equal(t, int32(1), endpoint.tokenHits.Load())
			for _, request := range endpoint.requests {
				if strings.Contains(request.path, ".well-known") {
					assert.Empty(t, request.authorization)
				}
				if request.path == "/query" {
					assert.True(t, strings.HasPrefix(request.authorization, "Bearer token-"))
				}
			}
		})
	}
}

func TestM2MConnectorTimeoutBoundsDiscoveryAndRefresh(t *testing.T) {
	blockM2MDefaultTransport(t)
	for _, host := range []string{"test.cloud.databricks.com", "test.gcp.databricks.com", "adb-test.azuredatabricks.net"} {
		for _, timeout := range []time.Duration{0, 15 * time.Minute, 2 * time.Second} {
			t.Run(host+"/"+timeout.String(), func(t *testing.T) {
				shared := m2m.NewAuthenticator("test-client", "test-secret", host)
				endpoint := newM2MTestEndpoint(t, host, 1)
				var discoveries, grants atomic.Int32
				transport := &m2mIdentityTransport{m2mRoundTripper(func(request *http.Request) (*http.Response, error) {
					if request.URL.Path != "/query" {
						wantTimeout := time.Minute
						if strings.Contains(request.URL.Path, ".well-known") {
							discoveries.Add(1)
							wantTimeout = 10 * time.Second
						} else {
							grants.Add(1)
						}
						if timeout > 0 && timeout < wantTimeout {
							wantTimeout = timeout
						}
						deadline, ok := request.Context().Deadline()
						assert.True(t, ok)
						assert.InDelta(t, wantTimeout.Seconds(), time.Until(deadline).Seconds(), 0.5)
						assert.Contains(t, request.UserAgent(), "Issue413/composed")
						authorization := request.Header.Get("Authorization")
						require.NoError(t, shared.Authenticate(request))
						assert.Equal(t, authorization, request.Header.Get("Authorization"))
					}
					return endpoint.transport.RoundTrip(request)
				})}
				for range 2 {
					created, err := NewConnector(WithServerHostname(host), WithAuthenticator(shared),
						WithUserAgentEntry("Issue413/composed"), WithTransport(transport), WithRetries(0, 0, 0),
						func(cfg *config.Config) { cfg.ClientTimeout = timeout })
					require.NoError(t, err)
					connector := created.(*connector)
					requestWithM2M(t, connector)
					assert.Equal(t, timeout, connector.cfg.ClientTimeout)
				}
				assert.Equal(t, int32(2), grants.Load())
				assert.Equal(t, int32(2), endpoint.tokenHits.Load())
				wantDiscoveries := int32(4)
				if strings.Contains(host, "azuredatabricks.net") {
					wantDiscoveries = 0
				}
				assert.Equal(t, wantDiscoveries, discoveries.Load())
			})
		}
	}
}

func TestM2MConnectorHTTPConfiguration(t *testing.T) {
	blockM2MDefaultTransport(t)
	for _, host := range []string{"test.cloud.databricks.com", "test.gcp.databricks.com", "adb-test.azuredatabricks.net"} {
		for _, order := range []string{"auth-first", "auth-last", "dsn", "dsn-inferred-auth"} {
			for _, expiresIn := range []int{3600, 1} {
				t.Run(fmt.Sprintf("%s/%s/expires-%d", host, order, expiresIn), func(t *testing.T) {
					endpoint := newM2MTestEndpoint(t, host, expiresIn)
					options := []ConnOption{WithServerHostname(host), WithPort(443), WithHTTPPath("/query")}
					httpOptions := []ConnOption{WithUserAgentEntry("Issue413/partner"), WithTransport(endpoint.transport)}
					switch order {
					case "auth-first":
						options = append(options, WithClientCredentials("test-client", "test-secret"))
						options = append(options, httpOptions...)
					case "auth-last":
						options = append(options, httpOptions...)
						options = append(options, WithClientCredentials("test-client", "test-secret"))
					default:
						dsn := host + ":443/query?clientID=test-client&clientSecret=test-secret&userAgentEntry=Issue413%2Fpartner"
						if order == "dsn" {
							dsn = "https://" + dsn + "&authType=OauthM2M"
						}
						userConfig, err := config.ParseDSN(dsn)
						require.NoError(t, err)
						options = []ConnOption{withUserConfig(userConfig), WithTransport(endpoint.transport)}
					}
					options = append(options, WithRetries(0, 0, 0))
					created, err := NewConnector(options...)
					require.NoError(t, err)
					connector := created.(*connector)
					requestWithM2M(t, connector)
					requestWithM2M(t, connector)
					wantTokens := int32(1)
					if expiresIn == 1 {
						wantTokens = 2
					}
					assert.Equal(t, wantTokens, endpoint.tokenHits.Load())
					wantAgent := client.BuildUserAgent(connector.cfg)
					require.Contains(t, wantAgent, "Issue413/partner")
					endpoint.mutex.Lock()
					defer endpoint.mutex.Unlock()
					var paths []string
					for _, request := range endpoint.requests {
						paths = append(paths, request.path)
						if request.path != "/query" {
							assert.Equal(t, wantAgent, request.userAgent, request.path)
						}
						if strings.Contains(request.path, ".well-known") {
							assert.Empty(t, request.authorization)
						}
						if request.path == "/query" {
							assert.True(t, strings.HasPrefix(request.authorization, "Bearer token-"))
						}
					}
					if !strings.Contains(host, "azuredatabricks.net") {
						assert.Contains(t, paths, "/.well-known/databricks-config")
						assert.Contains(t, paths, "/oidc/.well-known/openid-configuration")
					}
				})
			}
		}
	}
}

func TestM2MSharedAuthenticatorKeepsConnectorConfiguration(t *testing.T) {
	blockM2MDefaultTransport(t)
	const host = "adb-test.azuredatabricks.net"
	shared := m2m.NewAuthenticator("test-client", "test-secret", host)
	var connectors []*connector
	var endpoints []*m2mTestEndpoint
	for _, partner := range []string{"first/partner", "second/partner"} {
		endpoint := newM2MTestEndpoint(t, host, 1)
		created, err := NewConnector(WithServerHostname(host), WithAuthenticator(shared), WithRetries(0, 0, 0),
			WithUserAgentEntry(partner), WithTransport(endpoint.transport))
		require.NoError(t, err)
		connectors = append(connectors, created.(*connector))
		endpoints = append(endpoints, endpoint)
		requestWithM2M(t, created.(*connector))
	}
	for index, connector := range connectors {
		requestWithM2M(t, connector)
		endpoint := endpoints[index]
		assert.Equal(t, int32(2), endpoint.tokenHits.Load())
		endpoint.mutex.Lock()
		for _, request := range endpoint.requests {
			if request.path != "/query" {
				assert.Equal(t, client.BuildUserAgent(connector.cfg), request.userAgent)
			}
		}
		endpoint.mutex.Unlock()
	}
}

func TestM2MPreviouslyUsedAuthenticatorGetsConnectorClient(t *testing.T) {
	for _, expiresIn := range []int{3600, 1} {
		t.Run(fmt.Sprintf("expires-%d", expiresIn), func(t *testing.T) {
			testM2MPreviouslyUsedAuthenticator(t, expiresIn)
		})
	}
}

func testM2MPreviouslyUsedAuthenticator(t *testing.T, expiresIn int) {
	t.Helper()
	const host = "adb-test.azuredatabricks.net"
	original := newM2MTestEndpoint(t, host, expiresIn)
	previousClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: original.transport}
	t.Cleanup(func() { http.DefaultClient = previousClient })
	shared := m2m.NewAuthenticator("test-client", "test-secret", host)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+host+"/query", nil)
	require.NoError(t, err)
	require.NoError(t, shared.Authenticate(request))
	require.Equal(t, int32(1), original.tokenHits.Load())
	blockM2MDefaultTransport(t)

	configured := newM2MTestEndpoint(t, host, 3600)
	created, err := NewConnector(WithServerHostname(host), WithAuthenticator(shared),
		WithTransport(configured.transport), WithRetries(0, 0, 0))
	require.NoError(t, err)
	connector := created.(*connector)
	requestWithM2M(t, connector)
	requestWithM2M(t, connector)
	require.Equal(t, int32(1), configured.tokenHits.Load())
	configured.mutex.Lock()
	defer configured.mutex.Unlock()
	for _, observed := range configured.requests {
		if observed.path != "/query" {
			assert.Equal(t, client.BuildUserAgent(connector.cfg), observed.userAgent)
		}
	}
	if expiresIn == 1 {
		require.ErrorContains(t, shared.Authenticate(request), "OAuth bypassed connector transport")
	} else {
		require.NoError(t, shared.Authenticate(request))
	}
	require.Equal(t, int32(1), original.tokenHits.Load())
}

func TestM2MCustomConnectorCachesAreLocal(t *testing.T) {
	blockM2MDefaultTransport(t)
	for _, host := range []string{"test.cloud.databricks.com", "adb-test.azuredatabricks.net"} {
		t.Run(host, func(t *testing.T) {
			shared := m2m.NewAuthenticator("test-client", "test-secret", host)
			endpoint := newM2MTestEndpoint(t, host, 3600)
			for index := range 5 {
				created, err := NewConnector(WithServerHostname(host), WithAuthenticator(shared),
					WithUserAgentEntry(fmt.Sprintf("partner/%d", index)), WithTransport(endpoint.transport),
					WithRetries(0, 0, 0))
				require.NoError(t, err)
				for range 3 {
					requestWithM2M(t, created.(*connector))
				}
			}
			assert.Equal(t, int32(5), endpoint.tokenHits.Load())
			discoveries := 0
			for _, request := range endpoint.requests {
				if strings.Contains(request.path, ".well-known") {
					discoveries++
				}
			}
			wantDiscoveries := 0
			if strings.Contains(host, "cloud.databricks.com") {
				wantDiscoveries = 10
			}
			assert.Equal(t, wantDiscoveries, discoveries)
		})
	}
}

func TestM2MRootedDiscoveryEndpoints(t *testing.T) {
	const host = "test.cloud.databricks.com"
	for _, endpointHost := range []string{host + ".", host + ".:443", "TEST.CLOUD.DATABRICKS.COM.:443"} {
		t.Run(endpointHost, func(t *testing.T) {
			endpoint := newM2MTestEndpoint(t, host, 3600)
			transport := m2mRoundTripper(func(request *http.Request) (*http.Response, error) {
				if strings.HasSuffix(request.URL.Path, "/.well-known/openid-configuration") {
					body := fmt.Sprintf(`{"issuer":"https://%s/oidc","token_endpoint":"https://%s/token","authorization_endpoint":"https://%s/authorize","device_authorization_endpoint":"https://%s/device"}`, host, endpointHost, endpointHost, endpointHost)
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
				}
				if request.URL.Path == "/token" {
					assert.Equal(t, endpointHost, request.URL.Host)
				}
				return endpoint.transport.RoundTrip(request)
			})
			created, err := NewConnector(WithServerHostname(host), WithClientCredentials("test-client", "test-secret"), WithTransport(transport), WithRetries(0, 0, 0))
			require.NoError(t, err)
			requestWithM2M(t, created.(*connector))
			requestWithM2M(t, created.(*connector))
			assert.Equal(t, int32(1), endpoint.tokenHits.Load())
		})
	}
}

func TestM2MCustomConnectorsDoNotGrowSharedState(t *testing.T) {
	blockM2MDefaultTransport(t)
	const host = "adb-test.azuredatabricks.net"
	const connectorCount = 256
	shared := m2m.NewAuthenticator("test-client", "test-secret", host)
	expected := m2m.NewAuthenticator("test-client", "test-secret", host)
	endpoint := newM2MTestEndpoint(t, host, 3600)
	for range connectorCount {
		created, err := NewConnector(WithServerHostname(host), WithAuthenticator(shared),
			WithTransport(&m2mIdentityTransport{endpoint.transport}), WithRetries(0, 0, 0))
		require.NoError(t, err)
		requestWithM2M(t, created.(*connector))
	}
	assert.Equal(t, int32(connectorCount), endpoint.tokenHits.Load())
	require.True(t, assert.ObjectsAreEqual(expected, shared), "custom connectors must not retain any state on the original authenticator")
}

func TestM2MConfiguredTransportPreservesTokenError(t *testing.T) {
	blockM2MDefaultTransport(t)
	const host = "adb-test.azuredatabricks.net"
	endpoint := newM2MTestEndpoint(t, host, 3600)
	created, err := NewConnector(WithServerHostname(host), WithClientCredentials("invalid", "invalid"),
		WithTransport(endpoint.transport), WithRetries(0, 0, 0))
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+host+"/query", nil)
	require.NoError(t, err)
	response, err := created.(*connector).client.Do(request)
	if response != nil {
		require.NoError(t, response.Body.Close())
	}
	var retrieveError *oauth2.RetrieveError
	require.ErrorAs(t, err, &retrieveError)
	assert.Equal(t, http.StatusUnauthorized, retrieveError.Response.StatusCode)
	assert.Equal(t, "invalid_client", retrieveError.ErrorCode)
}
