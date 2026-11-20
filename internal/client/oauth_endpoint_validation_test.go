package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestM2MRejectsDiscoveredEndpointsWithoutCaching(t *testing.T) {
	for _, host := range []string{"workspace.cloud.databricks.com", "workspace.gcp.databricks.com"} {
		for _, accountRooted := range []bool{false, true} {
			for _, field := range []string{"token_endpoint", "authorization_endpoint", "device_authorization_endpoint"} {
				for _, invalid := range []string{"https://untrusted.example/secret", "http://accounts.cloud.databricks.com/secret", "", "https://databricks.com.untrusted.example/secret"} {
					if field != "token_endpoint" && invalid == "" {
						continue
					}
					name := host + "/" + field + "/" + invalid
					if accountRooted {
						name += "/account-rooted"
					}
					t.Run(name, func(t *testing.T) {
						issuer := "https://" + host + "/oidc"
						metadata := `{"oidc_endpoint":"` + issuer + `"}`
						if accountRooted {
							issuer = "https://accounts.cloud.databricks.com/oidc/accounts/account-1"
							metadata = `{"oidc_endpoint":"https://accounts.cloud.databricks.com/oidc/accounts/{account_id}","account_id":"account-1"}`
						}
						var discoveries, grants atomic.Int32
						var bad atomic.Bool
						bad.Store(true)
						transport := func() http.RoundTripper {
							return oauthTestRoundTripper(func(request *http.Request) (*http.Response, error) {
								body := metadata
								switch {
								case strings.HasSuffix(request.URL.Path, "/.well-known/openid-configuration"):
									discoveries.Add(1)
									document := map[string]string{"issuer": issuer, "token_endpoint": issuer + "/token", "authorization_endpoint": issuer + "/authorize", "device_authorization_endpoint": issuer + "/device"}
									if bad.Load() {
										document[field] = invalid
									}
									encoded, err := json.Marshal(document)
									require.NoError(t, err)
									body = string(encoded)
								case request.Method == http.MethodPost:
									grants.Add(1)
									assert.Equal(t, issuer+"/token", request.URL.String())
									body = `{"access_token":"validated-token","token_type":"Bearer","expires_in":3600}`
								case request.URL.Path == "/query":
									assert.Equal(t, "Bearer validated-token", request.Header.Get("Authorization"))
								}
								return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
							})
						}
						sharedTransport := transport()
						shared := m2m.NewAuthenticator("id", "secret", host)
						cfg := config.WithDefaults()
						cfg.Authenticator, cfg.Transport = shared, sharedTransport
						cfg.TLSConfig.InsecureSkipVerify = true
						first := m2mTestClient(t, cfg, transport())
						cfg.TLSConfig.InsecureSkipVerify = false
						second := m2mTestClient(t, cfg, transport())
						request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+host+"/query", nil)
						require.NoError(t, err)
						response, err := first.Do(request)
						if response != nil {
							require.NoError(t, response.Body.Close())
						}
						require.ErrorContains(t, err, field)
						assert.NotContains(t, err.Error(), "/secret")
						assert.Zero(t, grants.Load())
						bad.Store(false)
						response, err = first.Do(request)
						require.NoError(t, err)
						require.NoError(t, response.Body.Close())
						assert.Equal(t, int32(2), discoveries.Load())
						assert.Equal(t, int32(1), grants.Load())
						response, err = second.Do(request)
						require.NoError(t, err)
						require.NoError(t, response.Body.Close())
						assert.Equal(t, int32(2), discoveries.Load())
						assert.Equal(t, int32(1), grants.Load())
					})
				}
			}
		}
	}
}
