package kernel

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// TestColumnTypeInfosVariantTypeName pins the per-column mapping newKernelRows uses. It
// runs in the default build, so a dropped VARIANT branch fails here rather than only in
// the cgo-tagged kernel tests. With the option off every column keeps its
// ColumnTypeInfoForField name; with it on only the VARIANT column changes, and its scan
// type and length do not move.
func TestColumnTypeInfosVariantTypeName(t *testing.T) {
	fields := []arrow.Field{
		{Name: "v", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "VARIANT",
		})},
		{Name: "ntz", Type: arrow.FixedWidthTypes.Timestamp_us, Metadata: arrow.MetadataFrom(map[string]string{
			"databricks.type_name": "TIMESTAMP_NTZ",
		})},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
	}
	wantOff := []string{"STRING", "TIMESTAMP_NTZ", "STRING", "BIGINT"}
	wantOn := []string{"VARIANT", "TIMESTAMP_NTZ", "STRING", "BIGINT"}

	off := columnTypeInfos(fields, false)
	on := columnTypeInfos(fields, true)
	if len(off) != len(fields) || len(on) != len(fields) {
		t.Fatalf("columnTypeInfos returned %d/%d infos, want %d", len(off), len(on), len(fields))
	}
	for i := range fields {
		if got := off[i].DatabaseTypeName; got != wantOff[i] {
			t.Errorf("option off: column %q DatabaseTypeName = %q, want %q", fields[i].Name, got, wantOff[i])
		}
		if got := on[i].DatabaseTypeName; got != wantOn[i] {
			t.Errorf("option on: column %q DatabaseTypeName = %q, want %q", fields[i].Name, got, wantOn[i])
		}
		if off[i].ScanType != on[i].ScanType || off[i].Length != on[i].Length || off[i].HasLength != on[i].HasLength {
			t.Errorf("column %q: ScanType/Length changed with the option: off %+v, on %+v", fields[i].Name, off[i], on[i])
		}
	}
}
