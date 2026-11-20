package client

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
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
			if customTransport {
				assert.Same(t, queryTransport.Base, tokenTransport.base)
			} else {
				assert.NotSame(t, queryTransport.Base, tokenTransport.base)
			}
			assert.Equal(t, BuildUserAgent(cfg), tokenTransport.userAgent)
			assert.Equal(t, cfg.ClientTimeout, authenticator.client.Timeout)
			if !customTransport {
				assert.NotSame(t, cfg.TLSConfig, tokenTransport.base.(*http.Transport).TLSClientConfig)
				assert.False(t, tokenTransport.base.(*http.Transport).TLSClientConfig.InsecureSkipVerify)
				assert.Nil(t, tokenTransport.base.(*http.Transport).TLSClientConfig.VerifyConnection)
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
