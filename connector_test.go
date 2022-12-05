package dbsql

import (
	"context"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnector_Connect(t *testing.T) {
	t.Run("Connect returns err when thrift client initialization fails", func(t *testing.T) {
		cfg := config.WithDefaults()
		var openSessionCount int
		openSession := func(ctx context.Context, req *cli_service.TOpenSessionReq) (r *cli_service.TOpenSessionResp, err error) {
			openSessionCount++
			return getTestSession(), nil
		}
		testClient := &client.TestClient{
			FnOpenSession: openSession,
		}
		testConnector := connector{
			cfg:    cfg,
			client: testClient,
		}
		conn, err := testConnector.Connect(context.Background())
		assert.NotNil(t, conn)
		assert.NoError(t, err)
	})
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
