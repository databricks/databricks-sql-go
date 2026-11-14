package client

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type oauthHTTPClientAuthenticator struct {
	client    *http.Client
	returnNil bool
}

func (authenticator *oauthHTTPClientAuthenticator) Authenticate(*http.Request) error { return nil }

func (authenticator *oauthHTTPClientAuthenticator) WithHTTPClient(client *http.Client) auth.Authenticator {
	authenticator.client = client
	if authenticator.returnNil {
		return nil
	}
	return authenticator
}

func TestOAuthClientPreservesAuthenticatorWhenHookReturnsNil(t *testing.T) {
	authenticator := &oauthHTTPClientAuthenticator{returnNil: true}
	cfg := config.WithDefaults()
	cfg.Authenticator = authenticator
	cfg.Transport = oauthTestRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	pooled := PooledClient(cfg)
	assert.Same(t, authenticator, pooled.Transport.(*Transport).Authr)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	response, err := pooled.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
}

func TestOAuthClientUsesConnectorBaseTransport(t *testing.T) {
	for _, customTransport := range []bool{false, true} {
		t.Run(map[bool]string{false: "pooled-tls", true: "custom"}[customTransport], func(t *testing.T) {
			authenticator := &oauthHTTPClientAuthenticator{}
			cfg := config.WithDefaults()
			cfg.Authenticator = authenticator
			cfg.TLSConfig.InsecureSkipVerify = true
			cfg.ClientTimeout = 7 * time.Second
			if customTransport {
				cfg.Transport = &http.Transport{}
			}
			pooled := PooledClient(cfg)
			require.NotNil(t, authenticator.client)
			tokenTransport := authenticator.client.Transport.(*userAgentTransport)
			queryTransport := pooled.Transport.(*Transport)
			assert.Same(t, queryTransport.Base, tokenTransport.base)
			assert.Equal(t, BuildUserAgent(cfg), tokenTransport.userAgent)
			assert.Equal(t, cfg.ClientTimeout, authenticator.client.Timeout)
			if !customTransport {
				assert.Same(t, cfg.TLSConfig, tokenTransport.base.(*http.Transport).TLSClientConfig)
			}
		})
	}
}

type oauthTestRoundTripper func(*http.Request) (*http.Response, error)

func (transport oauthTestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestOAuthUserAgentTransportPreservesRequest(t *testing.T) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	request.Header.Set("User-Agent", "caller-agent")
	transport := &userAgentTransport{
		userAgent: "driver-agent",
		base: oauthTestRoundTripper(func(received *http.Request) (*http.Response, error) {
			assert.NotSame(t, request, received)
			assert.Equal(t, "driver-agent", received.Header.Get("User-Agent"))
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
	}
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	assert.Equal(t, "caller-agent", request.Header.Get("User-Agent"))
}

func TestM2MUsesPooledTLSVerification(t *testing.T) {
	var tokenHits atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		tokenHits.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"access_token":"tls-token","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(server.Close)
	for _, skipVerify := range []bool{false, true} {
		t.Run(map[bool]string{false: "verify", true: "skip-verify"}[skipVerify], func(t *testing.T) {
			cfg := config.WithDefaults()
			cfg.Authenticator = m2m.NewAuthenticator("id", "secret", "adb-test.azuredatabricks.net")
			cfg.TLSConfig.InsecureSkipVerify = skipVerify
			pooled := PooledClient(cfg)
			transport := pooled.Transport.(*Transport)
			base := transport.Base.(*http.Transport)
			t.Cleanup(base.CloseIdleConnections)
			base.Proxy = nil
			base.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
			require.NoError(t, err)
			err = transport.Authr.Authenticate(request)
			if skipVerify {
				require.NoError(t, err)
				assert.Equal(t, "Bearer tls-token", request.Header.Get("Authorization"))
				assert.Equal(t, int32(1), tokenHits.Load())
			} else {
				require.ErrorContains(t, err, "certificate")
				assert.Zero(t, tokenHits.Load())
			}
		})
	}
}
