//go:build cgo && databricks_kernel && e2e

package dbsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/databricks/databricks-sql-go/v2/internal/decimalfmt"
	dbsqlrows "github.com/databricks/databricks-sql-go/v2/rows"
	"github.com/stretchr/testify/require"
)

func TestKernelE2EDecimalArenaLifetime(test *testing.T) {
	for _, mode := range []string{"drain", "limit", "early_close", "cancel"} {
		test.Run(mode, func(test *testing.T) {
			database := kernelTestDBWith(test)
			defer func() { require.NoError(test, database.Close()) }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			query := "SELECT CAST(CASE WHEN id % 31 = 0 THEN NULL ELSE id - 35000 END AS DECIMAL(10,2)) FROM range(70000) ORDER BY id"
			rowCount := 70000
			switch mode {
			case "limit":
				query += " LIMIT 17"
				rowCount = 17
			case "early_close", "cancel":
				rowCount = 37
			}
			if mode == "drain" {
				connection, err := database.Conn(ctx)
				require.NoError(test, err)
				defer func() { require.NoError(test, connection.Close()) }()
				require.NoError(test, connection.Raw(func(raw any) error {
					result, err := raw.(driver.QueryerContext).QueryContext(ctx, query, nil)
					if err != nil {
						return err
					}
					defer func() { require.NoError(test, result.Close()) }()
					batches, err := result.(dbsqlrows.Rows).GetArrowBatches(ctx)
					if err != nil {
						return err
					}
					defer batches.Close()
					batchCount, rowsSeen := 0, int64(0)
					for batches.HasNext() {
						record, err := batches.Next()
						if err != nil {
							return err
						}
						batchCount++
						rowsSeen += record.NumRows()
						record.Release()
					}
					require.Equal(test, int64(rowCount), rowsSeen)
					require.Greater(test, batchCount, 1)
					return nil
				}))
			}
			result, err := database.QueryContext(ctx, query)
			require.NoError(test, err)
			defer func() { require.NoError(test, result.Close()) }()
			var kept []any
			for result.Next() {
				var value any
				require.NoError(test, result.Scan(&value))
				kept = append(kept, value)
				if len(kept) == rowCount && mode == "early_close" {
					break
				}
				if len(kept) == rowCount && mode == "cancel" {
					cancel()
					require.Eventually(test, func() bool { return errors.Is(result.Err(), context.Canceled) }, time.Second, time.Millisecond)
				}
			}
			if mode == "cancel" {
				require.ErrorIs(test, result.Err(), context.Canceled)
			} else {
				require.NoError(test, result.Err())
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
