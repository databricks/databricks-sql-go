package rows

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"

	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
)

// TestColumnTypeInfoMatchesThriftMapping cross-checks arrowscan.ColumnTypeInfoFor
// against the Thrift mapping (getScanType / GetDBTypeName / ColumnTypeLength)
// directly, so a change to one that isn't mirrored in the other fails a pure-Go PR
// run — not just the nightly warehouse-gated parity test. It enumerates each shared
// type as an {Arrow type, equivalent Thrift TColumnDesc} pair and asserts they agree.
// White-box (package rows) to reach the unexported getScanType.
func TestColumnTypeInfoMatchesThriftMapping(t *testing.T) {
	// thriftCol builds the minimal TColumnDesc the Thrift mapping functions read.
	thriftCol := func(id cli_service.TTypeId) *cli_service.TColumnDesc {
		return &cli_service.TColumnDesc{
			TypeDesc: &cli_service.TTypeDesc{
				Types: []*cli_service.TTypeEntry{
					{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: id}},
				},
			},
		}
	}

	// Each shared Databricks type as it arrives on each backend: the Arrow type the
	// kernel receives, and the Thrift TTypeId the server declares for the SAME logical
	// column. ColumnTypeInfoFor(arrow) must match the Thrift functions on thriftID.
	cases := []struct {
		name     string
		arrow    arrow.DataType
		thriftID cli_service.TTypeId
	}{
		{"boolean", arrow.FixedWidthTypes.Boolean, cli_service.TTypeId_BOOLEAN_TYPE},
		{"tinyint", arrow.PrimitiveTypes.Int8, cli_service.TTypeId_TINYINT_TYPE},
		{"smallint", arrow.PrimitiveTypes.Int16, cli_service.TTypeId_SMALLINT_TYPE},
		{"int", arrow.PrimitiveTypes.Int32, cli_service.TTypeId_INT_TYPE},
		{"bigint", arrow.PrimitiveTypes.Int64, cli_service.TTypeId_BIGINT_TYPE},
		{"float", arrow.PrimitiveTypes.Float32, cli_service.TTypeId_FLOAT_TYPE},
		{"double", arrow.PrimitiveTypes.Float64, cli_service.TTypeId_DOUBLE_TYPE},
		{"string", arrow.BinaryTypes.String, cli_service.TTypeId_STRING_TYPE},
		{"binary", arrow.BinaryTypes.Binary, cli_service.TTypeId_BINARY_TYPE},
		{"date", arrow.FixedWidthTypes.Date32, cli_service.TTypeId_DATE_TYPE},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us, cli_service.TTypeId_TIMESTAMP_TYPE},
		{"decimal", &arrow.Decimal128Type{Precision: 10, Scale: 2}, cli_service.TTypeId_DECIMAL_TYPE},
		{"array", arrow.ListOf(arrow.PrimitiveTypes.Int64), cli_service.TTypeId_ARRAY_TYPE},
		{"map", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), cli_service.TTypeId_MAP_TYPE},
		{"struct", arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}), cli_service.TTypeId_STRUCT_TYPE},
		// Intervals: the server pre-formats them to text and declares STRING_TYPE over
		// Thrift, so pair with STRING_TYPE (the kernel formats the native arrow value to
		// the same string). See the arrowscan DURATION / INTERVAL_MONTHS arms.
		{"interval_day_time", arrow.FixedWidthTypes.Duration_us, cli_service.TTypeId_STRING_TYPE},
		{"interval_year_month", arrow.FixedWidthTypes.MonthInterval, cli_service.TTypeId_STRING_TYPE},
		// VOID/NULL: the server stringifies it over Thrift as STRING_TYPE (verified
		// live — even bare SELECT NULL), same as the interval types above.
		{"null", arrow.Null, cli_service.TTypeId_STRING_TYPE},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			col := thriftCol(c.thriftID)
			info := arrowscan.ColumnTypeInfoFor(c.arrow)

			// ScanType: ColumnTypeInfoFor must recommend the same Go type as the Thrift
			// getScanType. This is the DECIMAL-drift guard the finding named.
			if want := getScanType(col); info.ScanType != want {
				t.Errorf("ScanType = %v, Thrift getScanType = %v", info.ScanType, want)
			}

			// DatabaseTypeName: the Thrift name is the enum name minus the _TYPE suffix
			// (e.g. BIGINT, DECIMAL, ARRAY), which ColumnTypeInfoFor reports verbatim.
			if want := rowscanner.GetDBTypeName(col); info.DatabaseTypeName != want {
				t.Errorf("DatabaseTypeName = %q, Thrift GetDBTypeName = %q", info.DatabaseTypeName, want)
			}

			// Length: only variable-length types report an (unbounded) length. Drive the
			// expectation from the SAME classifier ColumnTypeLength uses
			// (columnTypeLengthForID), not a hand-copied switch, so a change to the
			// Thrift-side length rule fails this test instead of the two copies drifting
			// together and staying green.
			wantLen, wantOk := columnTypeLengthForID(c.thriftID)
			if info.Length != wantLen || info.HasLength != wantOk {
				t.Errorf("Length = (%d,%v), Thrift = (%d,%v)", info.Length, info.HasLength, wantLen, wantOk)
			}
		})
	}
}
