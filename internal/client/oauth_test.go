package client

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthTransportIsolation(t *testing.T) {
	var shared http.RoundTripper
	for _, custom := range []bool{false, true} {
		for _, skip := range []bool{false, true} {
			cfg := config.WithDefaults()
			cfg.TLSConfig.InsecureSkipVerify = skip
			caller := &http.Transport{}
			if custom {
				cfg.Transport = caller
			}
			cfg.Authenticator = m2m.NewAuthenticator("id", "secret", "test.cloud.databricks.com")
			pooled, oauthClient := newM2MClient(t, cfg)
			transport := oauthClient.Transport.(*userAgentTransport).base.(*http.Transport)
			assert.Nil(t, transport.TLSClientConfig)
			require.NotNil(t, transport.Proxy)
			assert.Equal(t, reflect.ValueOf(http.ProxyFromEnvironment).Pointer(), reflect.ValueOf(transport.Proxy).Pointer())
			if shared == nil {
				shared = transport
			}
			assert.Same(t, shared, transport)
			assert.NotSame(t, pooled.Transport.(*Transport).Base, transport)
			if custom {
				assert.Nil(t, caller.Proxy)
				assert.Same(t, caller, pooled.Transport.(*Transport).Base)
			}
			assert.Equal(t, skip, cfg.TLSConfig.InsecureSkipVerify)
		}
	}
}

func TestOAuthUserAgentPreservesRequest(t *testing.T) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	request.Header.Set("User-Agent", "original")
	transport := userAgentTransport{userAgent: "partner", base: oauthTestRoundTripper(func(received *http.Request) (*http.Response, error) {
		assert.Equal(t, "partner", received.UserAgent())
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})}
	response, err := transport.RoundTrip(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	assert.Equal(t, "original", request.UserAgent())
}
