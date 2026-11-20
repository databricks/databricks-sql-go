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

// TestColumnTypeInfoForFieldVariant pins the kernel column mapping. A VARIANT field
// reports "VARIANT" while its scan type and length stay exactly what the physical
// string mapping gives, so the reported type still matches the scanned value. Other
// string-backed logical types keep their existing names.
func TestColumnTypeInfoForFieldVariant(t *testing.T) {
	field := func(dt arrow.DataType, meta map[string]string) arrow.Field {
		return arrow.Field{Name: "c", Type: dt, Metadata: arrow.MetadataFrom(meta)}
	}
	for _, tc := range []struct {
		name  string
		field arrow.Field
		want  string
	}{
		{"kernel VARIANT on string", field(arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "VARIANT"}), "VARIANT"},
		{"thrift VARIANT on string", field(arrow.BinaryTypes.String, map[string]string{sparkSQLNameMetadataKey: "VARIANT"}), "VARIANT"},
		{"VARIANT on large string", field(arrow.BinaryTypes.LargeString, map[string]string{databricksTypeNameMetadataKey: "VARIANT"}), "VARIANT"},
		{"plain string", field(arrow.BinaryTypes.String, nil), "STRING"},
		{"STRING type name", field(arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "STRING"}), "STRING"},
		{"VARCHAR type name", field(arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "VARCHAR(10)"}), "STRING"},
		{"GEOMETRY text", field(arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "GEOMETRY"}), "STRING"},
		{"TIME text", field(arrow.BinaryTypes.String, map[string]string{databricksTypeNameMetadataKey: "TIME"}), "TIME"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ColumnTypeInfoForField(tc.field)
			if got.DatabaseTypeName != tc.want {
				t.Errorf("DatabaseTypeName = %q, want %q", got.DatabaseTypeName, tc.want)
			}
			if tc.want != "VARIANT" {
				return
			}
			physical := ColumnTypeInfoFor(tc.field.Type)
			if got.ScanType != physical.ScanType || got.Length != physical.Length || got.HasLength != physical.HasLength {
				t.Errorf("VARIANT changed ScanType/Length: got %+v, string mapping %+v", got, physical)
			}
		})
	}
}
