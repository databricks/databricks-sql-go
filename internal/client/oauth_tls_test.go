package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/databricks/databricks-sql-go/auth/oauth"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func TestOAuthTLSHostVerification(t *testing.T) {
	const workspace = "workspace.cloud.databricks.com"
	for _, operation := range []string{"discovery", "grant"} {
		for _, skipVerify := range []bool{false, true} {
			for _, scenario := range []struct {
				name       string
				configured string
				target     string
				trusted    bool
			}{
				{"untrusted-chain", workspace, workspace, false},
				{"workspace-name-mismatch", workspace, workspace, true},
				{"case-and-port", "WORKSPACE.CLOUD.DATABRICKS.COM:443", workspace, true},
				{"other-name-mismatch", workspace, "accounts.cloud.databricks.com", true},
			} {
				t.Run(fmt.Sprintf("%s/skip=%t/%s", operation, skipVerify, scenario.name), func(t *testing.T) {
					server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						writer.Header().Set("Content-Type", "application/json")
						if request.URL.Path == "/token" {
							_, _ = io.WriteString(writer, `{"access_token":"tls-token","token_type":"Bearer","expires_in":3600}`)
							return
						}
						_, _ = fmt.Fprintf(writer, `{"issuer":"https://%s/oidc","token_endpoint":"https://%s/token"}`, scenario.target, scenario.target)
					}))
					server.Config.ErrorLog = log.New(io.Discard, "", 0)
					server.StartTLS()
					t.Cleanup(server.Close)
					authenticator := &oauthHTTPClientAuthenticator{}
					cfg := config.WithDefaults()
					cfg.Host = scenario.configured
					cfg.Authenticator = authenticator
					cfg.TLSConfig.InsecureSkipVerify = skipVerify
					if scenario.trusted {
						cfg.TLSConfig.RootCAs = x509.NewCertPool()
						cfg.TLSConfig.RootCAs.AddCert(server.Certificate())
					}
					pooled := PooledClient(cfg)
					queryTransport := pooled.Transport.(*Transport).Base.(*http.Transport)
					t.Cleanup(queryTransport.CloseIdleConnections)
					tokenTransport := authenticator.client.Transport.(*userAgentTransport).base.(*http.Transport)
					t.Cleanup(tokenTransport.CloseIdleConnections)
					tokenTransport.Proxy = nil
					tokenTransport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
						return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
					}
					ctx := context.WithValue(context.Background(), oauth2.HTTPClient, authenticator.client)
					var err error
					if operation == "discovery" {
						_, err = oauth.GetEndpoint(ctx, scenario.target)
					} else {
						grant := clientcredentials.Config{ClientID: "id", ClientSecret: "secret", TokenURL: "https://" + scenario.target + "/token"}
						_, err = grant.Token(ctx)
					}
					require.ErrorContains(t, err, "certificate")
					assert.False(t, tokenTransport.TLSClientConfig.InsecureSkipVerify)
					assert.Equal(t, skipVerify, cfg.TLSConfig.InsecureSkipVerify)
					assert.Nil(t, cfg.TLSConfig.VerifyConnection)
					if skipVerify {
						assert.Same(t, cfg.TLSConfig, queryTransport.TLSClientConfig)
						assert.Nil(t, queryTransport.TLSClientConfig.VerifyConnection)
					}
				})
			}
		}
	}
}

func TestOAuthVerifiesHTTPSProxyByIP(t *testing.T) {
	var connects, verified atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, http.MethodConnect, request.Method)
		connects.Add(1)
		http.Error(writer, "proxy refused target", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)
	cfg := config.WithDefaults()
	cfg.Host = "workspace.cloud.databricks.com"
	cfg.TLSConfig.InsecureSkipVerify = true
	cfg.TLSConfig.RootCAs = x509.NewCertPool()
	cfg.TLSConfig.RootCAs.AddCert(server.Certificate())
	cfg.TLSConfig.VerifyConnection = func(tls.ConnectionState) error { verified.Add(1); return nil }
	authenticator := &oauthHTTPClientAuthenticator{}
	cfg.Authenticator = authenticator
	pooled := PooledClient(cfg)
	t.Cleanup(pooled.Transport.(*Transport).Base.(*http.Transport).CloseIdleConnections)
	transport := authenticator.client.Transport.(*userAgentTransport).base.(*http.Transport)
	t.Cleanup(transport.CloseIdleConnections)
	proxyURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	transport.Proxy = http.ProxyURL(proxyURL)
	response, err := authenticator.client.Get("https://" + cfg.Host + "/token")
	if response != nil {
		require.NoError(t, response.Body.Close())
	}
	require.ErrorContains(t, err, "Bad Gateway")
	assert.Equal(t, int32(1), connects.Load())
	assert.Equal(t, int32(1), verified.Load())
}
