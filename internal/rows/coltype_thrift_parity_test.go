package rows

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"

	"github.com/databricks/databricks-sql-go/internal/arrowscan"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
)

// TestColumnTypeInfoMatchesThriftMapping is the pure-Go drift guard the kernel
// backend's column-metadata mapping (internal/arrowscan.ColumnTypeInfoFor) was
// missing. That mapping restates the Thrift path's per-type decisions — getScanType
// (this package), rowscanner.GetDBTypeName, and ColumnTypeLength — but pinned its own
// hardcoded expectations, so a change to the Thrift mapping (e.g. DECIMAL's scan
// type) would leave arrowscan silently divergent while CI stayed green; parity was
// then enforced only by the warehouse-gated, build-tagged TestKernelThriftColumnTypeParity
// (nightly, not PR CI).
//
// This test cross-checks the two mappings DIRECTLY, with no cgo and no warehouse, so
// drift fails a default CGO_ENABLED=0 PR run. The two switch on different type systems
// (Arrow IDs vs Thrift TTypeIds), so it can't share a map — instead it enumerates the
// shared Databricks types as {Arrow type, equivalent Thrift TColumnDesc} pairs and
// asserts ColumnTypeInfoFor(arrow) reports exactly what the Thrift functions produce
// for the paired column.
//
// It lives in package rows (white-box) to reach the unexported getScanType;
// arrowscan is a pure leaf import (no cycle).
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

	// thriftLength mirrors rows.ColumnTypeLength's variable-length classification
	// (the method needs a *rows with a live client, so replicate its switch here on
	// the same TTypeId set — the exact list the method checks).
	thriftLength := func(id cli_service.TTypeId) (int64, bool) {
		switch id {
		case cli_service.TTypeId_STRING_TYPE,
			cli_service.TTypeId_VARCHAR_TYPE,
			cli_service.TTypeId_BINARY_TYPE,
			cli_service.TTypeId_ARRAY_TYPE,
			cli_service.TTypeId_MAP_TYPE,
			cli_service.TTypeId_STRUCT_TYPE:
			return math.MaxInt64, true
		default:
			return 0, false
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
		{"null", arrow.Null, cli_service.TTypeId_NULL_TYPE},
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

			// Length: only variable-length types report an (unbounded) length.
			wantLen, wantOk := thriftLength(c.thriftID)
			if info.Length != wantLen || info.HasLength != wantOk {
				t.Errorf("Length = (%d,%v), Thrift = (%d,%v)", info.Length, info.HasLength, wantLen, wantOk)
			}
		})
	}
}
