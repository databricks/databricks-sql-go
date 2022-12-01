package dbsql

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/databricks/databricks-sdk-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetEnvOrSkipTest proceeds with test only with that env variable
func GetEnvOrSkipTest(t *testing.T, name string) string {
	value := os.Getenv(name)
	if value == "" {
		t.Skipf("Environment variable %s is missing", name)
	}
	return value
}

func TestAccDefaultAuthAndWarehouseName(t *testing.T) {
	ctx := context.Background()
	w := databricks.Must(databricks.NewWorkspaceClient())
	wh, err := w.Warehouses.GetWarehouseById(ctx, GetEnvOrSkipTest(t, "TEST_DEFAULT_WAREHOUSE_ID"))
	require.NoError(t, err)

	conn, err := NewConnector(WithWarehouseName(wh.Name))
	require.NoError(t, err)

	db := sql.OpenDB(conn)
	defer db.Close()

	var res int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) AS trips FROM samples.nyctaxi.trips`).Scan(&res)
	require.NoError(t, err)

	assert.Equal(t, 21932, res)
}

func TestAccPatAuthAndOtherParams(t *testing.T) {
	ctx := context.Background()
	w := databricks.Must(databricks.NewWorkspaceClient())
	wh, err := w.Warehouses.GetWarehouseById(ctx, GetEnvOrSkipTest(t, "TEST_DEFAULT_WAREHOUSE_ID"))
	require.NoError(t, err)

	conn, err := NewConnector(
		WithAccessToken(GetEnvOrSkipTest(t, "DATABRICKS_TOKEN")),
		WithServerHostname(wh.OdbcParams.Hostname),
		WithHTTPPath(wh.OdbcParams.Path),
		WithPort(wh.OdbcParams.Port),
	)
	require.NoError(t, err)

	db := sql.OpenDB(conn)
	defer db.Close()

	var res int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) AS trips FROM samples.nyctaxi.trips`).Scan(&res)
	require.NoError(t, err)

	assert.Equal(t, 21932, res)
}
