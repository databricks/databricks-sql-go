package dbsql

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/auth/tokenprovider"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFederatedTokenAuthenticatorPreservesThriftTokenExchange(t *testing.T) {
	type exchangeRequest struct {
		path         string
		subjectToken string
	}
	exchangeRequests := make(chan exchangeRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		exchangeRequests <- exchangeRequest{
			path:         r.URL.Path,
			subjectToken: r.FormValue("subject_token"),
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"exchanged-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer server.Close()

	subjectToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": "https://external.example.com/",
	}).SignedString([]byte("test-key"))
	require.NoError(t, err)

	cfg := config.WithDefaults()
	cfg.Host = server.URL
	WithFederatedTokenProvider(tokenprovider.NewStaticTokenProvider(subjectToken))(cfg)

	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	require.NoError(t, err)
	require.NoError(t, cfg.Authenticator.Authenticate(req))
	require.Equal(t, "Bearer exchanged-token", req.Header.Get("Authorization"))

	exchange := <-exchangeRequests
	assert.Equal(t, "/oidc/v1/token", exchange.path)
	assert.Equal(t, subjectToken, exchange.subjectToken)
}

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
			UseCloudFetch:                true,
			MaxDownloadThreads:           15,
			MaxFilesInMemory:             10,
			MinTimeToExpiry:              0 * time.Second,
			CloudFetchSpeedThresholdMbps: 0.1,
			HTTPClient:                   &http.Client{Transport: roundTripper},
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
			UseCloudFetch:                true,
			MaxDownloadThreads:           10,
			MaxFilesInMemory:             10,
			MinTimeToExpiry:              0 * time.Second,
			CloudFetchSpeedThresholdMbps: 0.1,
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
			UseCloudFetch:                true,
			MaxDownloadThreads:           10,
			MaxFilesInMemory:             10,
			MinTimeToExpiry:              0 * time.Second,
			CloudFetchSpeedThresholdMbps: 0.1,
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

	t.Run("Connector test WithEnableMetricViewMetadata enabled", func(t *testing.T) {
		host := "databricks-host"
		accessToken := "token"
		httpPath := "http-path"
		con, err := NewConnector(
			WithServerHostname(host),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
			WithEnableMetricViewMetadata(true),
		)
		assert.Nil(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.True(t, coni.cfg.EnableMetricViewMetadata)
	})

	t.Run("Connector test WithEnableMetricViewMetadata disabled by default", func(t *testing.T) {
		host := "databricks-host"
		accessToken := "token"
		httpPath := "http-path"
		con, err := NewConnector(
			WithServerHostname(host),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
		)
		assert.Nil(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.False(t, coni.cfg.EnableMetricViewMetadata)
	})

	t.Run("Connector test WithArrowNativeDecimal enabled", func(t *testing.T) {
		host := "databricks-host"
		accessToken := "token"
		httpPath := "http-path"
		con, err := NewConnector(
			WithServerHostname(host),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
			WithArrowNativeDecimal(true),
		)
		assert.Nil(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.True(t, coni.cfg.ArrowConfig.UseArrowNativeDecimal)
	})

	t.Run("Connector test WithArrowNativeDecimal disabled by default", func(t *testing.T) {
		host := "databricks-host"
		accessToken := "token"
		httpPath := "http-path"
		con, err := NewConnector(
			WithServerHostname(host),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
		)
		assert.Nil(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.False(t, coni.cfg.ArrowConfig.UseArrowNativeDecimal)
	})

	t.Run("Connector test useArrowNativeDecimal DSN param propagates to ArrowConfig", func(t *testing.T) {
		// Covers the full DSN path: ParseDSN -> withUserConfig -> ArrowConfig,
		// which is what connection.go reads. This is the bridge that makes the
		// DSN parameter actually take effect (databricks/databricks-sql-go#274).
		ucfg, err := config.ParseDSN("token:supersecret@databricks-host:443/sql/1.0/endpoints/abc?useArrowNativeDecimal=true")
		require.NoError(t, err)
		con, err := NewConnector(withUserConfig(ucfg))
		require.NoError(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.True(t, coni.cfg.ArrowConfig.UseArrowNativeDecimal)
	})

	t.Run("Connector test tokenCache DSN param propagates to KernelExperimental", func(t *testing.T) {
		// Covers the full DSN path: ParseDSN -> withUserConfig -> KernelExperimental,
		// which is what buildKernelConfig reads. This is the bridge that makes the
		// DSN parameter actually take effect (mirrors the useArrowNativeDecimal test).
		ucfg, err := config.ParseDSN("token:supersecret@databricks-host:443/sql/1.0/endpoints/abc?tokenCache=true")
		require.NoError(t, err)
		con, err := NewConnector(withUserConfig(ucfg))
		require.NoError(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		require.NotNil(t, coni.cfg.KernelExperimental)
		assert.True(t, coni.cfg.KernelExperimental.TokenCacheEnabled)
	})

	t.Run("Connector test tokenCache=false DSN param is a no-op", func(t *testing.T) {
		// tokenCache=false must not allocate KernelExperimental (in-memory is the
		// default); allocating it would opt the connection into the kernel-only
		// backend just to disable a feature.
		ucfg, err := config.ParseDSN("token:supersecret@databricks-host:443/sql/1.0/endpoints/abc?tokenCache=false")
		require.NoError(t, err)
		con, err := NewConnector(withUserConfig(ucfg))
		require.NoError(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.Nil(t, coni.cfg.KernelExperimental)
	})

	t.Run("Connector test WithTransport sets HTTPClient in CloudFetchConfig", func(t *testing.T) {
		host := "databricks-host"
		accessToken := "token"
		httpPath := "http-path"
		customTransport := &http.Transport{MaxIdleConns: 10}
		con, err := NewConnector(
			WithServerHostname(host),
			WithAccessToken(accessToken),
			WithHTTPPath(httpPath),
			WithTransport(customTransport),
		)
		assert.Nil(t, err)

		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.NotNil(t, coni.cfg.HTTPClient)
		assert.Equal(t, customTransport, coni.cfg.HTTPClient.Transport)
	})
}

func TestWithQueryTags(t *testing.T) {
	t.Run("WithQueryTags serializes map into SessionParams QUERY_TAGS", func(t *testing.T) {
		con, err := NewConnector(
			WithQueryTags(map[string]string{
				"team": "data-eng",
			}),
		)
		require.NoError(t, err)
		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.Equal(t, "team:data-eng", coni.cfg.SessionParams["QUERY_TAGS"])
	})

	t.Run("WithQueryTags with multiple tags", func(t *testing.T) {
		con, err := NewConnector(
			WithQueryTags(map[string]string{
				"team": "eng",
				"app":  "etl",
			}),
		)
		require.NoError(t, err)
		coni, ok := con.(*connector)
		require.True(t, ok)
		// Map iteration is non-deterministic
		qt := coni.cfg.SessionParams["QUERY_TAGS"]
		assert.True(t, qt == "team:eng,app:etl" || qt == "app:etl,team:eng", "got: %s", qt)
	})

	t.Run("WithQueryTags with empty map does not set QUERY_TAGS", func(t *testing.T) {
		con, err := NewConnector(
			WithQueryTags(map[string]string{}),
		)
		require.NoError(t, err)
		coni, ok := con.(*connector)
		require.True(t, ok)
		_, exists := coni.cfg.SessionParams["QUERY_TAGS"]
		assert.False(t, exists)
	})

	t.Run("WithQueryTags overrides WithSessionParams QUERY_TAGS", func(t *testing.T) {
		con, err := NewConnector(
			WithSessionParams(map[string]string{
				"QUERY_TAGS": "old:value",
				"ansi_mode":  "false",
			}),
			WithQueryTags(map[string]string{
				"team": "new-team",
			}),
		)
		require.NoError(t, err)
		coni, ok := con.(*connector)
		require.True(t, ok)
		// WithQueryTags should override the QUERY_TAGS from WithSessionParams
		assert.Equal(t, "team:new-team", coni.cfg.SessionParams["QUERY_TAGS"])
		// Other session params should be preserved
		assert.Equal(t, "false", coni.cfg.SessionParams["ansi_mode"])
	})
}

type mockRoundTripper struct{}

var _ http.RoundTripper = mockRoundTripper{}

func (m mockRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200}, nil
}
