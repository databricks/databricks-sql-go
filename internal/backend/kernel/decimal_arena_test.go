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
	require.Nil(test, resultRows.decimalStrings)
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
	require.Equal(test, float64(1), allocations)
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
	arena := resultRows.decimalStrings[0]
	cancel()
	require.ErrorIs(test, resultRows.Next(dest), context.Canceled)
	require.Same(test, arena, resultRows.decimalStrings[0])
	batchAllocations := testing.AllocsPerRun(10000, func() {
		if err := resultRows.nextBatch(); err != context.Canceled {
			panic(err)
		}
	})
	allocations := testing.AllocsPerRun(10000, func() {
		resultRows.rowInCur = 1
		if err := resultRows.Next(dest); err != nil {
			panic(err)
		}
		if err := resultRows.Next(dest); err != context.Canceled {
			panic(err)
		}
	})
	require.Equal(test, batchAllocations+1, allocations)
	require.NoError(test, resultRows.Close())
	require.Nil(test, resultRows.decimalStrings)
	require.Equal(test, decimalfmt.ExactString(decimal128.FromI64(-4999), 38), kept)
}

func TestKernelRowsDecimalArenaColumns(test *testing.T) {
	fields := []arrow.Field{
		{Name: "integer", Type: arrow.PrimitiveTypes.Int64},
		{Name: "first", Type: &arrow.Decimal128Type{Precision: 7, Scale: 2}},
		{Name: "nested", Type: arrow.ListOf(&arrow.Decimal128Type{Precision: 38, Scale: 38})},
		{Name: "second", Type: &arrow.Decimal128Type{Precision: 38, Scale: 38}},
	}
	arenas := decimalStringArenas(fields, false)
	require.Len(test, arenas, len(fields))
	require.Nil(test, arenas[0])
	require.Nil(test, arenas[2])
	require.NotNil(test, arenas[1])
	require.NotNil(test, arenas[3])
	require.NotSame(test, arenas[1], arenas[3])
	kept := arenas[1].ExactString(decimal128.FromI64(-12345), 2)
	for index := 0; index < 10000; index++ {
		_ = arenas[3].ExactString(decimal128.FromI64(int64(index)), 38)
	}
	require.Equal(test, "-123.45", kept)
	for _, options := range []struct {
		fields  []arrow.Field
		asFloat bool
	}{{fields, true}, {fields[:1], false}, {fields[2:3], false}, {nil, false}} {
		var unused []*decimalfmt.Arena
		allocations := testing.AllocsPerRun(100, func() {
			unused = decimalStringArenas(options.fields, options.asFloat)
		})
		require.Empty(test, unused)
		require.Zero(test, allocations)
	}
}

func TestKernelRowsDecimalFloatBypassesArena(test *testing.T) {
	resultRows := decimalArenaTestRows(test, 2)
	resultRows.op.decimalAsFloat = true
	dest := make([]driver.Value, 1)
	require.Nil(test, resultRows.decimalStrings)
	require.NoError(test, resultRows.Next(dest))
	require.Nil(test, dest[0])
	require.NoError(test, resultRows.Next(dest))
	require.Equal(test, decimal128.FromI64(-4999).ToFloat64(38), dest[0])
	require.NotNil(test, resultRows.decimalStrings)
	require.Empty(test, resultRows.decimalStrings)
}
