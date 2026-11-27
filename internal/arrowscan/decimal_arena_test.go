package arrowscan

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/v2/internal/decimalfmt"
	"github.com/stretchr/testify/require"
)

func TestDecimalArenaScanOptions(test *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(test, 0)
	decimalType := &arrow.Decimal128Type{Precision: 38, Scale: 2}
	builder := array.NewDecimal128Builder(pool, decimalType)
	builder.Append(decimal128.FromI64(-12345))
	builder.AppendNull()
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	var arena decimalfmt.Arena
	for _, options := range []ScanOptions{{}, {DecimalStrings: &arena}, {DecimalStrings: &arena, DecimalAsFloat: true}} {
		value, err := ScanCellCachedWithOptions(column, 0, time.UTC, nil, options)
		require.NoError(test, err)
		if options.DecimalAsFloat {
			require.Equal(test, decimal128.FromI64(-12345).ToFloat64(2), value)
		} else {
			require.Equal(test, "-123.45", value)
		}
		value, err = ScanCellCachedWithOptions(column, 1, time.UTC, nil, options)
		require.NoError(test, err)
		require.Nil(test, value)
	}
	listBuilder := array.NewListBuilder(pool, decimalType)
	listBuilder.Append(true)
	values := listBuilder.ValueBuilder().(*array.Decimal128Builder)
	values.Append(decimal128.FromI64(-12345))
	values.AppendNull()
	listBuilder.AppendNull()
	list := listBuilder.NewArray()
	listBuilder.Release()
	defer list.Release()
	value, err := ScanCellCachedWithOptions(list, 0, time.UTC, nil, ScanOptions{DecimalStrings: &arena, DecimalAsFloat: true})
	require.NoError(test, err)
	require.Equal(test, "[-123.45,null]", value)
	value, err = ScanCellCachedWithOptions(list, 1, time.UTC, nil, ScanOptions{DecimalStrings: &arena})
	require.NoError(test, err)
	require.Nil(test, value)
}
