//go:build cgo && databricks_kernel

package kernel

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	"github.com/stretchr/testify/require"
)

func TestKernelRowsRetainedStringsAfterClose(t *testing.T) {
	poison := &poisoningAllocator{Allocator: memory.NewGoAllocator()}
	mem := memory.NewCheckedAllocator(poison)
	defer mem.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "s", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"first", "last"}, nil)
	r := &kernelRows{
		op: &kernelOp{location: time.UTC}, cols: []string{"s"},
		ntz: []bool{false}, geoBinary: []bool{false},
		cur: b.NewRecord(), keyCache: arrowscan.NewStructKeyCache(),
	}
	b.Release()
	defer r.Close()
	require.Nil(t, r.prepared)
	var retained []driver.Value
	dest := make([]driver.Value, 1)
	for i := 0; i < 2; i++ {
		require.NoError(t, r.Next(dest))
		retained = append(retained, dest[0])
	}
	require.NoError(t, r.Close())
	require.Nil(t, r.prepared)
	require.Positive(t, poison.frees)
	require.Equal(t, []driver.Value{"first", "last"}, retained)
}
