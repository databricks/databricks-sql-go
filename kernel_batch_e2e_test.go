//go:build cgo && databricks_kernel && e2e

package dbsql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKernelE2EResultLifecycleAndReuse(test *testing.T) {
	database := kernelTestDB(test)
	defer database.Close() //nolint:errcheck
	database.SetMaxOpenConns(1)
	// Abort after roughly 8K rows; this is a lifecycle sample, not a batch-boundary assertion.
	const rowsBeforeAbort = 8193
	const query = `SELECT id,
		IF(id % 3 = 0, NULL, id % 2 = 0) AS boolean_value,
		IF(id % 3 = 0, NULL, CAST(id / 4 AS FLOAT)) AS float_value,
		IF(id % 3 = 0, NULL, CAST(id / 8 AS DOUBLE)) AS double_value
		FROM range(1000000) ORDER BY id`
	for _, mode := range []string{"limit", "early close", "cancel"} {
		test.Run(mode, func(test *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			statement := query
			if mode == "limit" {
				statement += " LIMIT 10000"
			}
			result, err := database.QueryContext(ctx, statement)
			require.NoError(test, err)
			defer result.Close() //nolint:errcheck
			var count int64
			for result.Next() {
				var index int64
				var boolean, single, double any
				require.NoError(test, result.Scan(&index, &boolean, &single, &double))
				require.Equal(test, count, index)
				if index%3 == 0 {
					require.Nil(test, boolean)
					require.Nil(test, single)
					require.Nil(test, double)
				} else {
					require.Equal(test, index%2 == 0, boolean)
					require.EqualValues(test, float32(index)/4, single)
					require.EqualValues(test, float64(index)/8, double)
				}
				count++
				if count == rowsBeforeAbort && mode != "limit" {
					if mode == "cancel" {
						cancel()
						for result.Next() {
						}
						require.True(test, errors.Is(result.Err(), context.Canceled), "rows error: %v", result.Err())
					}
					break
				}
			}
			if mode == "limit" {
				require.NoError(test, result.Err())
				require.EqualValues(test, 10000, count)
			} else {
				require.EqualValues(test, rowsBeforeAbort, count)
			}
			require.NoError(test, result.Close())
			var value int
			require.NoError(test, database.QueryRowContext(context.Background(), "SELECT 1").Scan(&value))
			require.Equal(test, 1, value)
		})
	}
}
