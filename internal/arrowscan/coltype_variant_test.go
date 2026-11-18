package arrowscan

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// TestIsVariantField pins the single VARIANT check shared by the Thrift and kernel
// backends. The server marks a VARIANT column as a string field whose logical type
// name lives in field metadata; the physical value stays JSON text.
func TestIsVariantField(t *testing.T) {
	for _, tc := range []struct {
		name string
		dt   arrow.DataType
		meta map[string]string
		want bool
	}{
		{"kernel type_name VARIANT on string", arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "VARIANT"}, true},
		{"thrift SqlName VARIANT on string", arrow.BinaryTypes.String, map[string]string{sparkSQLNameMetadataKey: "VARIANT"}, true},
		{"case and surrounding whitespace", arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "  variant "}, true},
		{"large string physical type", arrow.BinaryTypes.LargeString, map[string]string{sparkSQLNameMetadataKey: "VARIANT"}, true},
		{"type_name wins over SqlName", arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "STRING", sparkSQLNameMetadataKey: "VARIANT"}, false},
		{"blank type_name falls through to SqlName", arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: " ", sparkSQLNameMetadataKey: "VARIANT"}, true},
		{"SqlName VARIANT on binary is not a VARIANT column", arrow.BinaryTypes.Binary, map[string]string{sparkSQLNameMetadataKey: "VARIANT"}, false},
		{"type_name VARIANT on int64 is not a VARIANT column", arrow.PrimitiveTypes.Int64, map[string]string{databricksTypeNameMetadataKey: "VARIANT"}, false},
		{"no metadata", arrow.BinaryTypes.String, nil, false},
		{"plain STRING", arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "STRING"}, false},
		{"prefix of a longer name", arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "VARIANTX"}, false},
		{"GEOGRAPHY on string", arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "GEOGRAPHY"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := arrow.Field{Name: "v", Type: tc.dt, Metadata: arrow.MetadataFrom(tc.meta)}
			if got := IsVariantField(field); got != tc.want {
				t.Errorf("IsVariantField = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestApplyVariantTypeName checks the kernel wiring helper: a VARIANT field reports
// "VARIANT" while the scan type and length stay exactly what the STRING mapping gives,
// so the reported type still matches the scanned value.
func TestApplyVariantTypeName(t *testing.T) {
	variant := arrow.Field{Name: "v", Type: arrow.BinaryTypes.String,
		Metadata: arrow.MetadataFrom(map[string]string{sparkSQLNameMetadataKey: "VARIANT"})}
	plain := arrow.Field{Name: "s", Type: arrow.BinaryTypes.String}

	base := ColumnTypeInfoForField(variant)
	got := ApplyVariantTypeName(base, variant)
	if got.DatabaseTypeName != "VARIANT" {
		t.Errorf("DatabaseTypeName = %q, want %q", got.DatabaseTypeName, "VARIANT")
	}
	if got.ScanType != base.ScanType || got.Length != base.Length || got.HasLength != base.HasLength {
		t.Errorf("ApplyVariantTypeName changed ScanType/Length: got %+v, base %+v", got, base)
	}

	if got := ApplyVariantTypeName(ColumnTypeInfoForField(plain), plain); got.DatabaseTypeName != "STRING" {
		t.Errorf("plain string DatabaseTypeName = %q, want %q", got.DatabaseTypeName, "STRING")
	}
}
