//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"database/sql/driver"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/v2/internal/decimalfmt"
	"github.com/stretchr/testify/require"
)

func decimalArenaTestRows(test *testing.T, count int) *kernelRows {
	test.Helper()
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	test.Cleanup(func() { pool.AssertSize(test, 0) })
	decimalType := &arrow.Decimal128Type{Precision: 38, Scale: 38}
	builder := array.NewDecimal128Builder(pool, decimalType)
	for index := 0; index < count; index++ {
		if index%31 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(decimal128.FromI64(int64(index) - 5000))
		}
	}
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "decimal", Type: decimalType, Nullable: true}}, nil), []arrow.Array{column}, int64(count))
	column.Release()
	resultRows := &kernelRows{ctx: context.Background(), op: &kernelOp{location: time.UTC}, cols: []string{"decimal"}, geoBinary: []bool{false}, ntz: []bool{false}, cur: record}
	test.Cleanup(func() { require.NoError(test, resultRows.Close()) })
	return resultRows
}

func TestKernelRowsDecimalArenaLifetime(test *testing.T) {
	resultRows := decimalArenaTestRows(test, 10000)
	kept := make([]driver.Value, 10000)
	dest := make([]driver.Value, 1)
	for index := range kept {
		require.NoError(test, resultRows.Next(dest))
		kept[index] = dest[0]
	}
	require.NoError(test, resultRows.Close())
	require.Equal(test, decimalfmt.Arena{}, resultRows.decimalStrings)
	require.ErrorIs(test, resultRows.Next(dest), io.EOF)
	runtime.GC()
	for index, value := range kept {
		if index%31 == 0 {
			require.Nil(test, value)
		} else {
			require.Equal(test, decimalfmt.ExactString(decimal128.FromI64(int64(index)-5000), 38), value)
		}
	}
}

func TestKernelRowsUsesDecimalArena(test *testing.T) {
	resultRows := decimalArenaTestRows(test, 2)
	dest := make([]driver.Value, 1)
	allocations := testing.AllocsPerRun(10000, func() {
		resultRows.rowInCur = 1
		if err := resultRows.Next(dest); err != nil {
			panic(err)
		}
	})
	require.Less(test, allocations, 1.01)
}

func TestKernelRowsDecimalArenaCanceledBatch(test *testing.T) {
	resultRows := decimalArenaTestRows(test, 2)
	ctx, cancel := context.WithCancel(context.Background())
	resultRows.ctx = ctx
	resultRows.cancel = cancel
	dest := make([]driver.Value, 1)
	require.NoError(test, resultRows.Next(dest))
	require.NoError(test, resultRows.Next(dest))
	kept := dest[0]
	cancel()
	require.ErrorIs(test, resultRows.Next(dest), context.Canceled)
	require.Equal(test, decimalfmt.Arena{}, resultRows.decimalStrings)
	require.NoError(test, resultRows.Close())
	require.Equal(test, decimalfmt.ExactString(decimal128.FromI64(-4999), 38), kept)
}
