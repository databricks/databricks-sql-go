package client

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type oauthClientRecorder struct {
	auth.Authenticator
	client         *http.Client
	cacheTransport http.RoundTripper
}

func (recorder *oauthClientRecorder) WithHTTPClient(client *http.Client) auth.Authenticator {
	recorder.client = client
	return recorder.Authenticator.(interface {
		WithHTTPClient(*http.Client) auth.Authenticator
	}).WithHTTPClient(client)
}

func (recorder *oauthClientRecorder) WithHTTPClientForTransport(client *http.Client, transport http.RoundTripper) auth.Authenticator {
	recorder.client = client
	recorder.cacheTransport = transport
	if configurable, ok := recorder.Authenticator.(interface {
		WithHTTPClientForTransport(*http.Client, http.RoundTripper) auth.Authenticator
	}); ok {
		return configurable.WithHTTPClientForTransport(client, transport)
	}
	return recorder.WithHTTPClient(client)
}

func TestBuiltInOAuthCacheTrustProfiles(t *testing.T) {
	for _, customRoots := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom-roots=%t", customRoots), func(t *testing.T) {
			shared := m2m.NewAuthenticator("id", "secret", "workspace.cloud.databricks.com")
			var identities []http.RoundTripper
			for range 2 {
				recorder := &oauthClientRecorder{Authenticator: shared}
				cfg := config.WithDefaults()
				cfg.Authenticator = recorder
				if customRoots {
					cfg.TLSConfig.RootCAs = x509.NewCertPool()
				}
				pooled := PooledClient(cfg)
				t.Cleanup(pooled.Transport.(*Transport).Base.(*http.Transport).CloseIdleConnections)
				t.Cleanup(recorder.client.Transport.(*userAgentTransport).base.(*http.Transport).CloseIdleConnections)
				identities = append(identities, recorder.cacheTransport)
			}
			if customRoots {
				assert.NotNil(t, identities[0])
				assert.NotSame(t, identities[0], identities[1])
			} else {
				assert.Nil(t, identities[0])
				assert.Nil(t, identities[1])
			}
		})
	}
}

func TestM2MCustomTokenCannotPoisonBuiltInConnector(t *testing.T) {
	const host = "workspace.cloud.databricks.com"
	for _, skipSQLVerify := range []bool{false, true} {
		t.Run(fmt.Sprintf("skip-sql=%t", skipSQLVerify), func(t *testing.T) {
			var grants, strictDials atomic.Int32
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				switch request.URL.Path {
				case "/.well-known/databricks-config":
					_, _ = fmt.Fprintf(writer, `{"oidc_endpoint":"https://%s/oidc"}`, host)
				case "/oidc/.well-known/openid-configuration":
					_, _ = fmt.Fprintf(writer, `{"issuer":"https://%s/oidc","token_endpoint":"https://%s/token"}`, host, host)
				case "/token":
					grants.Add(1)
					_, _ = io.WriteString(writer, `{"access_token":"untrusted-token","token_type":"Bearer","expires_in":315360000}`)
				}
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.StartTLS()
			t.Cleanup(server.Close)
			shared := m2m.NewAuthenticator("id", "secret", host)
			custom := server.Client().Transport.(*http.Transport).Clone()
			custom.TLSClientConfig.InsecureSkipVerify = true
			t.Cleanup(custom.CloseIdleConnections)
			custom.Proxy = nil
			custom.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			firstConfig := config.WithDefaults()
			firstConfig.Host, firstConfig.Authenticator, firstConfig.Transport = host, shared, custom
			first := PooledClient(firstConfig).Transport.(*Transport).Authr
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+host+"/query", nil)
			require.NoError(t, err)
			require.NoError(t, first.Authenticate(request))
			require.Equal(t, "Bearer untrusted-token", request.Header.Get("Authorization"))
			secondConfig := config.WithDefaults()
			secondConfig.Host = host
			recorder := &oauthClientRecorder{Authenticator: shared}
			secondConfig.Authenticator = recorder
			secondConfig.TLSConfig.InsecureSkipVerify = skipSQLVerify
			secondConfig.TLSConfig.RootCAs = x509.NewCertPool()
			secondConfig.TLSConfig.RootCAs.AddCert(server.Certificate())
			second := PooledClient(secondConfig)
			t.Cleanup(second.Transport.(*Transport).Base.(*http.Transport).CloseIdleConnections)
			strict := recorder.client.Transport.(*userAgentTransport).base.(*http.Transport)
			t.Cleanup(strict.CloseIdleConnections)
			strict.Proxy = nil
			strict.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				strictDials.Add(1)
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			request.Header.Del("Authorization")
			err = second.Transport.(*Transport).Authr.Authenticate(request)
			var hostnameError x509.HostnameError
			require.ErrorAs(t, err, &hostnameError)
			assert.Positive(t, strictDials.Load())
			assert.Empty(t, request.Header.Get("Authorization"))
			assert.Equal(t, int32(1), grants.Load())
		})
	}
}
