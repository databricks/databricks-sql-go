package dbsql

import (
	"testing"
	"time"

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
		)
		expectedUserConfig := config.UserConfig{
			Host:           host,
			Port:           port,
			Protocol:       "https",
			AccessToken:    accessToken,
			HTTPPath:       "/" + httpPath,
			MaxRows:        maxRows,
			QueryTimeout:   timeout,
			Catalog:        catalog,
			Schema:         schema,
			UserAgentEntry: userAgentEntry,
			SessionParams:  sessionParams,
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig
		coni, ok := con.(*connector)
		require.True(t, ok)
		assert.Nil(t, err)
		assert.Equal(t, expectedCfg, coni.cfg)
	})
}
