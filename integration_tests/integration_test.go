//go:build integration

package integration_tests

import (
	"context"
	"database/sql"
	"testing"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationPing(t *testing.T) {

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname("localhost"),
		dbsql.WithPort(8087),
		dbsql.WithHTTPPath("session"), //test case name
		dbsql.WithMaxRows(100),
	)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close()

	t.Run("it can query multiple result pages", func(t *testing.T) {
		rows, err1 := db.QueryContext(context.Background(), `select * from default.diamonds limit 250`)
		require.Nil(t, err1)
		require.NotNil(t, rows)
		defer rows.Close()
		type row struct {
			_c0     int
			carat   float64
			cut     string
			color   string
			clarity string
			depth   sql.NullFloat64
			table   sql.NullFloat64
			price   int
			x       float64
			y       float64
			z       float64
		}
		expectedColumnNames := []string{"_c0", "carat", "cut", "color", "clarity", "depth", "table", "price", "x", "y", "z"}
		expectedDatabaseType := []string{"INT", "DOUBLE", "STRING", "STRING", "STRING", "DOUBLE", "DOUBLE", "INT", "DOUBLE", "DOUBLE", "DOUBLE"}
		expectedNullable := []bool{false, false, false, false, false, false, false, false, false, false, false}

		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, expectedColumnNames, cols)

		types, err := rows.ColumnTypes()
		require.NoError(t, err)

		for i, v := range types {
			assert.Equal(t, expectedColumnNames[i], v.Name())
			assert.Equal(t, expectedDatabaseType[i], v.DatabaseTypeName())
			nullable, ok := v.Nullable()
			assert.False(t, ok)
			assert.Equal(t, expectedNullable[i], nullable)
		}
		var allrows []row
		for rows.Next() {
			// After row 10 this will cause one fetch call, as 10 rows (maxRows config) will come from the first execute statement call.
			r := row{}
			err := rows.Scan(&r._c0, &r.carat, &r.cut, &r.color, &r.clarity, &r.depth, &r.table, &r.price, &r.x, &r.y, &r.z)
			assert.Nil(t, err)
			allrows = append(allrows, r)
		}
		assert.Equal(t, 250, len(allrows))
		assert.Nil(t, rows.Err())

	})
}
