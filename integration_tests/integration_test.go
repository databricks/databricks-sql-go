//go:build integration

package integration_tests

import (
	"context"
	"database/sql"
	"testing"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/require"
)

func TestIntegrationPing(t *testing.T) {

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname("localhost"),
		dbsql.WithPort(8087),
	)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close()

	t.Run("it can ping", func(t *testing.T) {
		if err := db.PingContext(context.Background()); err != nil {
			require.NoError(t, err)
		}
	})
}
