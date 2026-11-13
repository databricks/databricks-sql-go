//go:build cgo && databricks_kernel

package kernel

import (
	"bytes"
	"database/sql/driver"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/cdata"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	"github.com/stretchr/testify/require"
)

// TestRowsNestedTimestampNTZFromKernelBatch replays a live kernel batch through
// the C Data Interface and the row scanner: nested TIMESTAMP_NTZ leaves keep
// their wall clock under a non-UTC session zone, exactly as on the Thrift path
// (databricks/databricks-sql-go#520), while zoned siblings are still shifted.
func TestRowsNestedTimestampNTZFromKernelBatch(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}
	data, err := os.ReadFile("../../arrowscan/testdata/timestamp_ntz_nested_kernel.arrow")
	require.NoError(t, err)
	reader, err := ipc.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	require.True(t, reader.Next())
	var carr cdata.CArrowArray
	var csch cdata.CArrowSchema
	cdata.ExportArrowRecordBatch(reader.Record(), &carr, &csch)
	reader.Release()

	rec, err := importBatch(&carr, &csch)
	require.NoError(t, err)
	fields := rec.Schema().Fields()
	r := &kernelRows{
		op: &kernelOp{location: loc}, cur: rec, cols: make([]string, len(fields)),
		geoBinary: make([]bool, len(fields)), ntz: timestampNTZColumns(fields),
		keyCache: arrowscan.NewStructKeyCache(),
	}
	defer r.Close() //nolint:errcheck

	const ntz = `"2024-06-15 09:15:00.123456 +0000 UTC"`
	const zoned = `"2024-06-15 09:15:00.123456 -0400 EDT"`
	got := make([]driver.Value, len(fields))
	require.NoError(t, r.Next(got))
	require.Equal(t, []driver.Value{
		`[` + ntz + `]`,
		`{"a":` + ntz + `}`,
		`{"label":"pt","ntz":` + ntz + `,"zoned":` + zoned + `,"inner":{"ntz":` + ntz + `}}`,
		`{` + ntz + `:1}`,
		`[{"ntz":` + ntz + `,"zoned":` + zoned + `}]`,
	}, got)
}

// TestImportBatchRejectsNonStructSchema keeps ImportCRecordBatch's error (not a
// panic) for a non-struct export and releases the exported array. (Success-path
// imports free C memory via a GC finalizer, so only this path can assert it.)
func TestImportBatchRejectsNonStructSchema(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	b := array.NewInt64Builder(mem)
	b.Append(1)
	arr := b.NewArray()
	b.Release()
	var carr cdata.CArrowArray
	var csch cdata.CArrowSchema
	cdata.ExportArrowArray(arr, &carr, &csch)
	arr.Release()

	rec, err := importBatch(&carr, &csch)
	require.Nil(t, rec)
	require.ErrorContains(t, err, "not a struct")
}

// TestImportBatchKeepsUnlabelledSchema passes a batch with no nested
// timestamps through unchanged.
func TestImportBatchKeepsUnlabelledSchema(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	lb := b.Field(0).(*array.ListBuilder)
	lb.Append(true)
	lb.ValueBuilder().(*array.Int64Builder).Append(7)
	src := b.NewRecord()
	b.Release()
	var carr cdata.CArrowArray
	var csch cdata.CArrowSchema
	cdata.ExportArrowRecordBatch(src, &carr, &csch)
	src.Release()

	rec, err := importBatch(&carr, &csch)
	require.NoError(t, err)
	defer rec.Release()
	require.True(t, rec.Schema().Equal(schema), "schema = %s", rec.Schema())
	v, err := arrowscan.ScanCell(rec.Column(0), 0, time.UTC)
	require.NoError(t, err)
	require.Equal(t, "[7]", v)
}
