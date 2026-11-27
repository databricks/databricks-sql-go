//go:build cgo && databricks_kernel && e2e

package dbsql

import (
	"context"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/databricks/databricks-sql-go/v2/internal/decimalfmt"
	"github.com/stretchr/testify/require"
)

func TestKernelE2EDecimalArenaLifetime(test *testing.T) {
	for _, mode := range []string{"drain", "limit", "early_close", "cancel"} {
		test.Run(mode, func(test *testing.T) {
			database := kernelTestDBWith(test, WithMaxRows(127))
			defer func() { require.NoError(test, database.Close()) }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			query := "SELECT CAST(CASE WHEN id % 31 = 0 THEN NULL ELSE id - 35000 END AS DECIMAL(10,2)) FROM range(70000) ORDER BY id"
			rowCount := 70000
			if mode == "limit" {
				query += " LIMIT 17"
				rowCount = 17
			} else if mode == "early_close" || mode == "cancel" {
				rowCount = 37
			}
			result, err := database.QueryContext(ctx, query)
			require.NoError(test, err)
			defer func() { require.NoError(test, result.Close()) }()
			var kept []any
			for result.Next() {
				var value any
				require.NoError(test, result.Scan(&value))
				kept = append(kept, value)
				if len(kept) == rowCount && (mode == "early_close" || mode == "cancel") {
					break
				}
			}
			require.NoError(test, result.Err())
			if mode == "cancel" {
				cancel()
			}
			require.NoError(test, result.Close())
			require.Len(test, kept, rowCount)
			runtime.GC()
			for index, value := range kept {
				if index%31 == 0 {
					require.Nil(test, value)
				} else {
					require.Equal(test, decimalfmt.ExactString(decimal128.FromI64(int64(index-35000)*100), 2), value)
				}
			}
		})
	}
}
