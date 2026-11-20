package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthTransportIsolation(t *testing.T) {
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
			if custom {
				assert.Nil(t, caller.Proxy)
				assert.Same(t, caller, pooled.Transport.(*Transport).Base)
			}
			assert.Equal(t, !custom && !skip, transport == pooled.Transport.(*Transport).Base)
			assert.Equal(t, skip, cfg.TLSConfig.InsecureSkipVerify)
		}
	}
}

func TestOAuthTLSVerification(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Error("OAuth must reject the certificate")
		}))
		server.Config.ErrorLog = log.New(io.Discard, "", 0)
		server.StartTLS()
		t.Cleanup(server.Close)
		cfg := config.WithDefaults()
		cfg.TLSConfig.InsecureSkipVerify = true
		cfg.Authenticator = m2m.NewAuthenticator("id", "secret", "test.cloud.databricks.com")
		_, oauthClient := newM2MClient(t, cfg)
		transport := oauthClient.Transport.(*userAgentTransport).base.(*http.Transport)
		target := server.URL
		if trusted {
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			transport.TLSClientConfig = &tls.Config{RootCAs: roots}
			target = "https://test.cloud.databricks.com/token"
		}
		transport.Proxy = nil
		transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		}
		response, err := oauthClient.Get(target)
		if response != nil {
			require.NoError(t, response.Body.Close())
		}
		if trusted {
			var hostnameError x509.HostnameError
			require.ErrorAs(t, err, &hostnameError)
		} else {
			var authorityError x509.UnknownAuthorityError
			require.ErrorAs(t, err, &authorityError)
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
