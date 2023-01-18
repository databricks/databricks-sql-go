package dbsql

import (
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/internal/config"
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
		)
		expectedUserConfig := config.UserConfig{
			Host:           host,
			Port:           port,
			Protocol:       "https",
			AccessToken:    accessToken,
			Authenticator:  &pat.PATAuth{AccessToken: accessToken},
			HTTPPath:       "/" + httpPath,
			MaxRows:        maxRows,
			QueryTimeout:   timeout,
			Catalog:        catalog,
			Schema:         schema,
			UserAgentEntry: userAgentEntry,
			SessionParams:  sessionParams,
			RetryMax:       10,
			RetryWaitMin:   3 * time.Second,
			RetryWaitMax:   60 * time.Second,
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig
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
		expectedUserConfig := config.UserConfig{
			Host:          host,
			Port:          port,
			Protocol:      "https",
			AccessToken:   accessToken,
			Authenticator: &pat.PATAuth{AccessToken: accessToken},
			HTTPPath:      "/" + httpPath,
			MaxRows:       maxRows,
			SessionParams: sessionParams,
			RetryMax:      4,
			RetryWaitMin:  1 * time.Second,
			RetryWaitMax:  30 * time.Second,
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig
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
		expectedUserConfig := config.UserConfig{
			Host:          host,
			Port:          port,
			Protocol:      "https",
			AccessToken:   accessToken,
			Authenticator: &pat.PATAuth{AccessToken: accessToken},
			HTTPPath:      "/" + httpPath,
			MaxRows:       maxRows,
			SessionParams: sessionParams,
			RetryMax:      -1,
			RetryWaitMin:  0,
			RetryWaitMax:  0,
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig
		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.Nil(t, err)
		assert.Equal(t, expectedCfg, coni.cfg)
	})
}
