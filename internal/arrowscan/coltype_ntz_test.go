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

// TestIsTimestampNTZField pins the single NTZ check shared by type reporting and
// value handling on both backends: explicit metadata (databricks.type_name, then
// Spark:DataType:SqlName) wins over the physical TimeZone, which is only the
// fallback when no logical type name is present.
func TestIsTimestampNTZField(t *testing.T) {
	zoned := arrow.FixedWidthTypes.Timestamp_us // tz=UTC
	zoneless := &arrow.TimestampType{Unit: arrow.Microsecond}
	for _, tc := range []struct {
		name string
		dt   arrow.DataType
		meta map[string]string
		want bool
	}{
		{"zone-less, no metadata", zoneless, nil, true},
		{"zoned, no metadata", zoned, nil, false},
		{"kernel type_name NTZ on UTC zone", zoned, map[string]string{databricksTypeNameMetadataKey: "TIMESTAMP_NTZ"}, true},
		{"thrift SqlName NTZ on zone-less", zoneless, map[string]string{sparkSQLNameMetadataKey: "TIMESTAMP_NTZ"}, true},
		{"SqlName NTZ on UTC zone", zoned, map[string]string{sparkSQLNameMetadataKey: "timestamp_ntz"}, true},
		{"SqlName TIMESTAMP overrides zone-less fallback", zoneless, map[string]string{sparkSQLNameMetadataKey: "TIMESTAMP"}, false},
		{"type_name wins over SqlName", zoned, map[string]string{databricksTypeNameMetadataKey: "TIMESTAMP", sparkSQLNameMetadataKey: "TIMESTAMP_NTZ"}, false},
		{"blank metadata falls back to zone", zoneless, map[string]string{databricksTypeNameMetadataKey: " "}, true},
		{"non-timestamp labeled NTZ", arrow.BinaryTypes.String, map[string]string{sparkSQLNameMetadataKey: "TIMESTAMP_NTZ"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := arrow.Field{Type: tc.dt, Metadata: arrow.MetadataFrom(tc.meta)}
			if got := IsTimestampNTZField(field); got != tc.want {
				t.Errorf("IsTimestampNTZField = %v, want %v", got, tc.want)
			}
			if tc.dt.ID() == arrow.TIMESTAMP {
				want := "TIMESTAMP"
				if tc.want {
					want = "TIMESTAMP_NTZ"
				}
				if got := ColumnTypeInfoForField(field).DatabaseTypeName; got != want {
					t.Errorf("DatabaseTypeName = %q, want %q", got, want)
				}
			}
		})
	}
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
