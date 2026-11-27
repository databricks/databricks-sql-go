//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/v2/internal/arrowscan"
	"github.com/stretchr/testify/require"
)

func TestBoundRowsBatchLifecycle(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := array.NewInt32Builder(pool)
	builder.AppendValues([]int32{300, 400}, nil)
	first := builder.NewArray()
	builder.AppendValues([]int32{0, -700}, []bool{false, true})
	second := builder.NewArray()
	builder.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int32}}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	rows := &kernelRows{ctx: ctx, cancel: cancel, op: &kernelOp{location: time.UTC},
		cols: []string{"value"}, ntz: []bool{false}, geoBinary: []bool{false},
		cur: array.NewRecordBatch(schema, []arrow.Array{first}, 2), keyCache: arrowscan.NewStructKeyCache()}
	first.Release()
	require.Nil(t, rows.cells)
	destination := make([]driver.Value, 1)
	require.NoError(t, rows.Next(destination))
	require.Equal(t, int32(300), destination[0])
	require.True(t, rows.cellsBound)
	reader := rows.cells[0]
	readerCalls := 0
	rows.cells[0] = func(row int) (driver.Value, error) {
		readerCalls++
		return reader(row)
	}
	require.NoError(t, rows.Next(destination))
	require.Equal(t, int32(400), destination[0])
	require.Equal(t, 1, readerCalls)
	cancel()
	require.ErrorIs(t, rows.nextBatch(), context.Canceled)
	require.False(t, rows.cellsBound)
	require.Nil(t, rows.cells[0])
	rows.cur.Release()
	rows.cur = array.NewRecordBatch(schema, []arrow.Array{second}, 2)
	second.Release()
	rows.rowInCur = 0
	require.NoError(t, rows.Next(destination))
	require.Nil(t, destination[0])
	require.NoError(t, rows.Next(destination))
	require.Equal(t, int32(-700), destination[0])
	rows.op = nil
	require.NoError(t, rows.Close())
	require.Nil(t, rows.cells)
	require.False(t, rows.cellsBound)
	require.NoError(t, rows.Close())
	require.ErrorIs(t, rows.Next(destination), io.EOF)
}

func TestBoundRowsLazyClose(t *testing.T) {
	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	builder.Append(true)
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.FixedWidthTypes.Boolean}}, nil)
	rows := &kernelRows{cur: array.NewRecordBatch(schema, []arrow.Array{column}, 1)}
	require.Nil(t, rows.cells)
	require.NoError(t, rows.Close())
	require.Nil(t, rows.cells)
}

func TestBoundRowsDecimalOption(t *testing.T) {
	decimalType := &arrow.Decimal128Type{Precision: 5, Scale: 2}
	decimal, _, err := array.FromJSON(memory.DefaultAllocator, decimalType, strings.NewReader(`["12.50",null]`))
	require.NoError(t, err)
	defer decimal.Release()
	integer, _, err := array.FromJSON(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32, strings.NewReader(`[12345,null]`))
	require.NoError(t, err)
	defer integer.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "decimal", Type: decimalType}, {Name: "integer", Type: arrow.PrimitiveTypes.Int32}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{decimal, integer}, 2)
	defer record.Release()
	for _, asFloat := range []bool{false, true} {
		rows := &kernelRows{cur: record, op: &kernelOp{decimalAsFloat: asFloat},
			cols: []string{"decimal", "integer"}, ntz: []bool{false, false}, geoBinary: []bool{false, false}}
		destination := make([]driver.Value, 2)
		require.NoError(t, rows.Next(destination))
		var want driver.Value = "12.50"
		if asFloat {
			want = float64(12.5)
		}
		require.Equal(t, []driver.Value{want, int32(12345)}, destination)
		require.NoError(t, rows.Next(destination))
		require.Equal(t, []driver.Value{nil, nil}, destination)
	}
}

func TestBoundRowsFallbackError(t *testing.T) {
	builder := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 1})
	builder.AppendNull()
	builder.Append([]byte{42})
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "unsupported", Type: column.DataType()}}, nil)
	record := array.NewRecordBatch(schema, []arrow.Array{column}, 2)
	defer record.Release()
	rows := &kernelRows{cur: record, op: &kernelOp{}, cols: []string{"unsupported"}, ntz: []bool{false}, geoBinary: []bool{false}}
	destination := make([]driver.Value, 1)
	require.NoError(t, rows.Next(destination))
	require.Nil(t, destination[0])
	_, scanError := arrowscan.ScanCell(column, 1, nil)
	err := rows.Next(destination)
	require.EqualError(t, err, "kernel: scan col 0 (unsupported): "+scanError.Error())
	require.Same(t, err, rows.iterationErr)
	require.Equal(t, 1, rows.rowInCur)
}
