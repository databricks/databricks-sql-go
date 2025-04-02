package dbsql

import (
	"net/http"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnector(t *testing.T) {
	t.Run("Connector initialized with functional options should have all options set", func(t *testing.T) {
		host := "databricks-host"
		port := 1
		accessToken := "token"
		httpPath := "http-path"
		maxRows := 100
		timeout := 100 * time.Second
		catalog := "catalog-name"
		schema := "schema-string"
		userAgentEntry := "user-agent"
		sessionParams := map[string]string{"key": "value"}
		roundTripper := mockRoundTripper{}
		con, err := NewConnector(
			WithServerHostname(host),
			WithPort(port),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
			WithMaxRows(maxRows),
			WithTimeout(timeout),
			WithInitialNamespace(catalog, schema),
			WithUserAgentEntry(userAgentEntry),
			WithSessionParams(sessionParams),
			WithRetries(10, 3*time.Second, 60*time.Second),
			WithTransport(roundTripper),
			WithCloudFetch(true),
			WithMaxDownloadThreads(15),
			WithSkipTLSHostVerify(),
		)
		expectedCloudFetchConfig := config.CloudFetchConfig{
			UseCloudFetch:      true,
			MaxDownloadThreads: 15,
			MaxFilesInMemory:   10,
			MinTimeToExpiry:    0 * time.Second,
		}
		expectedUserConfig := config.UserConfig{
			Host:             host,
			Port:             port,
			Protocol:         "https",
			AccessToken:      accessToken,
			Authenticator:    &pat.PATAuth{AccessToken: accessToken},
			HTTPPath:         "/" + httpPath,
			MaxRows:          maxRows,
			QueryTimeout:     timeout,
			Catalog:          catalog,
			Schema:           schema,
			UserAgentEntry:   userAgentEntry,
			SessionParams:    sessionParams,
			RetryMax:         10,
			RetryWaitMin:     3 * time.Second,
			RetryWaitMax:     60 * time.Second,
			Transport:        roundTripper,
			CloudFetchConfig: expectedCloudFetchConfig,
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.DriverVersion = DriverVersion
		expectedCfg.UserConfig = expectedUserConfig
		expectedCfg.TLSConfig.InsecureSkipVerify = true
		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.Nil(t, err)
		assert.Equal(t, expectedCfg, coni.cfg)
	})
	t.Run("Connector initialized minimal settings", func(t *testing.T) {
		host := "databricks-host"
		port := 443
		accessToken := "token"
		httpPath := "http-path"
		maxRows := 100000
		sessionParams := map[string]string{}
		con, err := NewConnector(
			WithServerHostname(host),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
		)
		expectedCloudFetchConfig := config.CloudFetchConfig{
			UseCloudFetch:      true,
			MaxDownloadThreads: 10,
			MaxFilesInMemory:   10,
			MinTimeToExpiry:    0 * time.Second,
		}
		expectedUserConfig := config.UserConfig{
			Host:             host,
			Port:             port,
			Protocol:         "https",
			AccessToken:      accessToken,
			Authenticator:    &pat.PATAuth{AccessToken: accessToken},
			HTTPPath:         "/" + httpPath,
			MaxRows:          maxRows,
			SessionParams:    sessionParams,
			RetryMax:         4,
			RetryWaitMin:     1 * time.Second,
			RetryWaitMax:     30 * time.Second,
			CloudFetchConfig: expectedCloudFetchConfig,
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig
		expectedCfg.DriverVersion = DriverVersion
		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.Nil(t, err)
		assert.Equal(t, expectedCfg, coni.cfg)
	})
	t.Run("Connector initialized with retries turned off", func(t *testing.T) {
		host := "databricks-host"
		port := 443
		accessToken := "token"
		httpPath := "http-path"
		maxRows := 100000
		sessionParams := map[string]string{}
		con, err := NewConnector(
			WithServerHostname(host),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
			WithRetries(-1, 0, 0),
		)
		expectedCloudFetchConfig := config.CloudFetchConfig{
			UseCloudFetch:      true,
			MaxDownloadThreads: 10,
			MaxFilesInMemory:   10,
			MinTimeToExpiry:    0 * time.Second,
		}
		expectedUserConfig := config.UserConfig{
			Host:             host,
			Port:             port,
			Protocol:         "https",
			AccessToken:      accessToken,
			Authenticator:    &pat.PATAuth{AccessToken: accessToken},
			HTTPPath:         "/" + httpPath,
			MaxRows:          maxRows,
			SessionParams:    sessionParams,
			RetryMax:         -1,
			RetryWaitMin:     0,
			RetryWaitMax:     0,
			CloudFetchConfig: expectedCloudFetchConfig,
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.DriverVersion = DriverVersion
		expectedCfg.UserConfig = expectedUserConfig
		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.Nil(t, err)
		assert.Equal(t, expectedCfg, coni.cfg)
	})

	t.Run("Connector test WithServerHostname", func(t *testing.T) {
		cases := []struct {
			hostname, host, protocol string
		}{
			{"databricks-host", "databricks-host", "https"},
			{"http://databricks-host", "databricks-host", "http"},
			{"https://databricks-host", "databricks-host", "https"},
			{"http:databricks-host", "databricks-host", "http"},
			{"https:databricks-host", "databricks-host", "https"},
			{"htt://databricks-host", "htt://databricks-host", "https"},
			{"localhost", "localhost", "http"},
			{"http:localhost", "localhost", "http"},
			{"https:localhost", "localhost", "https"},
		}

		for i := range cases {
			c := cases[i]
			con, err := NewConnector(
				WithServerHostname(c.hostname),
			)
			assert.Nil(t, err)

			coni, ok := con.(*connector)
			require.True(t, ok)
			userConfig := coni.cfg.UserConfig
			require.Equal(t, c.protocol, userConfig.Protocol)
			require.Equal(t, c.host, userConfig.Host)
		}

	})

	t.Run("Connector test WithSkipTLSHostVerify with PoolClient", func(t *testing.T) {
		hostname := "databricks-host"
		con, err := NewConnector(
			WithServerHostname(hostname),
			WithSkipTLSHostVerify(),
		)
		assert.Nil(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		userConfig := coni.cfg.UserConfig
		require.Equal(t, hostname, userConfig.Host)

		httpClient, ok := coni.client.Transport.(*retryablehttp.RoundTripper)
		require.True(t, ok)
		poolClient, ok := httpClient.Client.HTTPClient.Transport.(*client.Transport)
		require.True(t, ok)
		internalClient, ok := poolClient.Base.(*http.Transport)
		require.True(t, ok)
		require.True(t, internalClient.TLSClientConfig.InsecureSkipVerify)
	})
}

type mockRoundTripper struct{}

var _ http.RoundTripper = mockRoundTripper{}

func (m mockRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200}, nil
}
