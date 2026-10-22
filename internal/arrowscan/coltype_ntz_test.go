package arrowscan

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
)

// TestColumnTypeInfoForTimestampNTZ pins the TIMESTAMP vs TIMESTAMP_NTZ type-name
// distinction. Over Arrow both arrive as timestamp[us], differing only in the
// schema's TimestampType.TimeZone field: a zoned TIMESTAMP carries a zone, a
// TIMESTAMP_NTZ carries an empty TimeZone. database/sql's
// ColumnTypeDatabaseTypeName must report them distinctly ("TIMESTAMP" vs
// "TIMESTAMP_NTZ") so a zone-less wall clock is not mistaken for an instant.
//
// External authority: the Databricks TIMESTAMP_NTZ type is a distinct SQL type
// (https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type)
// and issue #506 requires it be surfaced as such.
func TestColumnTypeInfoForTimestampNTZ(t *testing.T) {
	t.Run("zoned timestamp reports TIMESTAMP", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		if got := ColumnTypeInfoFor(dt).DatabaseTypeName; got != "TIMESTAMP" {
			t.Errorf("DatabaseTypeName = %q, want %q", got, "TIMESTAMP")
		}
	})

	t.Run("zone-less timestamp reports TIMESTAMP_NTZ", func(t *testing.T) {
		dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
		if got := ColumnTypeInfoFor(dt).DatabaseTypeName; got != "TIMESTAMP_NTZ" {
			t.Errorf("DatabaseTypeName = %q, want %q", got, "TIMESTAMP_NTZ")
		}
	})
}

func TestColumnTypeInfoForFieldTimestampNTZ(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typeName string
		dataType arrow.DataType
		want     string
	}{
		{"kernel NTZ with UTC zone", "TIMESTAMP_NTZ", arrow.FixedWidthTypes.Timestamp_us, "TIMESTAMP_NTZ"},
		{"zoned timestamp", "TIMESTAMP", arrow.FixedWidthTypes.Timestamp_us, "TIMESTAMP"},
		{"no logical metadata", "", arrow.FixedWidthTypes.Timestamp_us, "TIMESTAMP"},
		{"string representation", "TIMESTAMP_NTZ", arrow.BinaryTypes.String, "STRING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := arrow.Field{Type: tc.dataType, Metadata: arrow.MetadataFrom(map[string]string{
				databricksTypeNameMetadataKey: tc.typeName,
			})}
			if got := ColumnTypeInfoForField(field).DatabaseTypeName; got != tc.want {
				t.Errorf("DatabaseTypeName = %q, want %q", got, tc.want)
			}
		})
	}
}
