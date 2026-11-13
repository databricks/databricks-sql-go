package dbsql

import (
	"fmt"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/pat"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOOpenConnector(t *testing.T) {
	t.Run("Should work with valid dsn", func(t *testing.T) {
		host := "databricks-host"
		port := 443
		accessToken := "token"
		httpPath := "http-path"
		expectedUserConfig := config.UserConfig{
			Host:          host,
			Port:          port,
			Protocol:      "https",
			AccessToken:   accessToken,
			HTTPPath:      "/" + httpPath,
			Authenticator: &pat.PATAuth{AccessToken: accessToken},
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig.WithDefaults()
		expectedCfg.DriverVersion = DriverVersion
		d := &databricksDriver{}
		c, err := d.OpenConnector(fmt.Sprintf("token:%s@%s:%d/%s", accessToken, host, port, httpPath))
		require.NoError(t, err)
		coni, ok := c.(*connector)
		require.True(t, ok)
		assert.Equal(t, expectedCfg, coni.cfg)
		assert.NotNil(t, coni.client)
	})

	t.Run("Should reject a timeout that overflows time.Duration", func(t *testing.T) {
		d := &databricksDriver{}
		_, err := d.OpenConnector("token:token@databricks-host:443/http-path?timeout=9223372037")
		require.Error(t, err)
		assert.ErrorIs(t, err, dbsqlerr.RequestError)
		assert.ErrorContains(t, err, "param timeout with value 9223372037 is out of range")

		c, err := d.OpenConnector("token:token@databricks-host:443/http-path?timeout=-1")
		require.NoError(t, err)
		assert.Equal(t, -time.Second, c.(*connector).cfg.QueryTimeout)
	})
}
