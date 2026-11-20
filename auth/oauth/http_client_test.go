package oauth

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type discoveryRoundTripper func(*http.Request) (*http.Response, error)

func TestDiscoveredEndpointRootDotValidation(t *testing.T) {
	for _, test := range []struct {
		value string
		valid bool
	}{
		{"https://test.cloud.databricks.com/token", true},
		{"https://test.cloud.databricks.com./token", true},
		{"https://TEST.CLOUD.DATABRICKS.COM.:443/token", true},
		{"https://test.cloud.databricks.com../token", false},
		{"https://test.cloud.databricks.com.evil./token", false},
		{"http://test.cloud.databricks.com./token", false},
		{"https:///token", false},
		{"https://test.cloud.databricks.com./{account_id}", false},
	} {
		t.Run(test.value, func(t *testing.T) {
			assert.Equal(t, test.valid, isValidDatabricksURL(test.value, true))
		})
	}
	assert.False(t, isValidDatabricksIssuer("https://test.cloud.databricks.com./oidc"))
}

func TestGetEndpointPreservesConfiguredAzureHostForms(t *testing.T) {
	for _, host := range []string{"adb-1.1.azuredatabricks.net.", "adb-1.1.azuredatabricks.net.corp.example"} {
		t.Run(host, func(t *testing.T) {
			endpoint, err := GetEndpoint(context.Background(), host)
			require.NoError(t, err)
			assert.Equal(t, "https://"+host+"/oidc/oauth2/v2.0/token", endpoint.TokenURL)
			assert.Equal(t, "https://"+host+"/oidc/oauth2/v2.0/authorize", endpoint.AuthURL)
		})
	}
}

func (transport discoveryRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestGetEndpointPreservesConfiguredClient(t *testing.T) {
	const host = "test.cloud.databricks.com"
	for _, timeout := range []time.Duration{0, time.Minute, time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			wantTimeout := hostConfigTimeout
			if timeout > 0 && timeout < wantTimeout {
				wantTimeout = timeout
			}
			var paths []string
			redirects := 0
			supplied := &http.Client{
				Timeout: timeout,
				CheckRedirect: func(*http.Request, []*http.Request) error {
					redirects++
					return nil
				},
				Transport: discoveryRoundTripper(func(request *http.Request) (*http.Response, error) {
					paths = append(paths, request.URL.Path)
					deadline, ok := request.Context().Deadline()
					assert.True(t, ok)
					assert.InDelta(t, wantTimeout.Seconds(), time.Until(deadline).Seconds(), 0.5)
					response := &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": {"application/json"}},
						Request:    request,
					}
					body := fmt.Sprintf(`{"issuer":"https://%s/oidc","token_endpoint":"https://%s/token"}`, host, host)
					switch request.URL.Path {
					case "/.well-known/databricks-config":
						response.StatusCode = http.StatusFound
						response.Header.Set("Location", "https://"+host+"/metadata")
						body = ""
					case "/metadata":
						body = fmt.Sprintf(`{"oidc_endpoint":"https://%s/oidc"}`, host)
					}
					response.Body = io.NopCloser(strings.NewReader(body))
					return response, nil
				}),
			}
			ctx := context.WithValue(context.Background(), oauth2.HTTPClient, supplied)
			endpoint, err := GetEndpoint(ctx, host)
			require.NoError(t, err)
			assert.Equal(t, "https://"+host+"/token", endpoint.TokenURL)
			assert.Equal(t, timeout, supplied.Timeout)
			assert.Equal(t, 1, redirects)
			assert.Equal(t, []string{"/.well-known/databricks-config", "/metadata", "/oidc/.well-known/openid-configuration"}, paths)
		})
	}
}
