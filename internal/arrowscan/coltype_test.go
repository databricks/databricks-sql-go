package arrowscan

import (
	"database/sql"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
)

// TestColumnTypeInfoFor pins the Arrow → column-metadata mapping so it stays
// byte-identical to the Thrift path — a pure-Go guard the value-parity suites miss
// (they compare scanned values, not ColumnType metadata). Expected values are
// ground truth captured from Thrift on a live warehouse.
func TestColumnTypeInfoFor(t *testing.T) {
	str := reflect.TypeOf("")
	raw := reflect.TypeOf(sql.RawBytes{})
	tm := reflect.TypeOf(time.Time{})

	cases := []struct {
		name    string
		dt      arrow.DataType
		wantDB  string
		wantST  reflect.Type
		wantLen int64
		wantOk  bool
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, "BOOLEAN", reflect.TypeOf(true), 0, false},
		{"int8", arrow.PrimitiveTypes.Int8, "TINYINT", reflect.TypeOf(int8(0)), 0, false},
		{"int16", arrow.PrimitiveTypes.Int16, "SMALLINT", reflect.TypeOf(int16(0)), 0, false},
		{"int32", arrow.PrimitiveTypes.Int32, "INT", reflect.TypeOf(int32(0)), 0, false},
		{"int64", arrow.PrimitiveTypes.Int64, "BIGINT", reflect.TypeOf(int64(0)), 0, false},
		// Unsigned types don't occur in Databricks SQL, but ScanCellCached widens them
		// to int64, so the metadata reports BIGINT/int64 to match (not the fallthrough).
		{"uint8", arrow.PrimitiveTypes.Uint8, "BIGINT", reflect.TypeOf(int64(0)), 0, false},
		{"uint16", arrow.PrimitiveTypes.Uint16, "BIGINT", reflect.TypeOf(int64(0)), 0, false},
		{"uint32", arrow.PrimitiveTypes.Uint32, "BIGINT", reflect.TypeOf(int64(0)), 0, false},
		{"uint64", arrow.PrimitiveTypes.Uint64, "BIGINT", reflect.TypeOf(int64(0)), 0, false},
		{"float32", arrow.PrimitiveTypes.Float32, "FLOAT", reflect.TypeOf(float32(0)), 0, false},
		{"float64", arrow.PrimitiveTypes.Float64, "DOUBLE", reflect.TypeOf(float64(0)), 0, false},
		{"string", arrow.BinaryTypes.String, "STRING", str, math.MaxInt64, true},
		{"binary", arrow.BinaryTypes.Binary, "BINARY", raw, math.MaxInt64, true},
		{"date32", arrow.FixedWidthTypes.Date32, "DATE", tm, 0, false},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us, "TIMESTAMP", tm, 0, false},
		{"decimal", &arrow.Decimal128Type{Precision: 10, Scale: 2}, "DECIMAL", raw, 0, false},
		{"list", arrow.ListOf(arrow.PrimitiveTypes.Int64), "ARRAY", raw, math.MaxInt64, true},
		{"map", arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64), "MAP", raw, math.MaxInt64, true},
		{"struct", arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}), "STRUCT", raw, math.MaxInt64, true},
		{"duration", &arrow.DurationType{Unit: arrow.Microsecond}, "STRING", str, math.MaxInt64, true},
		{"month_interval", arrow.FixedWidthTypes.MonthInterval, "STRING", str, math.MaxInt64, true},
		// VOID/NULL: Thrift stringifies to STRING on the wire (verified live), like intervals.
		{"null", arrow.Null, "STRING", str, math.MaxInt64, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ColumnTypeInfoFor(c.dt)
			if got.DatabaseTypeName != c.wantDB {
				t.Errorf("DatabaseTypeName = %q, want %q", got.DatabaseTypeName, c.wantDB)
			}
			if got.ScanType != c.wantST {
				t.Errorf("ScanType = %v, want %v", got.ScanType, c.wantST)
			}
			if got.Length != c.wantLen || got.HasLength != c.wantOk {
				t.Errorf("Length = (%d,%v), want (%d,%v)", got.Length, got.HasLength, c.wantLen, c.wantOk)
			}
		})
	}
}

// TestColumnTypeInfoScanTypeCoversScanner is the lockstep guard: every Arrow type
// ScanCellCached handles must have a non-fallback ColumnTypeInfoFor entry, so a new
// scanner type without matching metadata is caught here, not at runtime.
func TestColumnTypeInfoScanTypeCoversScanner(t *testing.T) {
	unknown := reflect.TypeOf(new(any))
	scannerTypes := []arrow.DataType{
		arrow.FixedWidthTypes.Boolean,
		arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16,
		arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64,
		arrow.PrimitiveTypes.Uint8, arrow.PrimitiveTypes.Uint16,
		arrow.PrimitiveTypes.Uint32, arrow.PrimitiveTypes.Uint64,
		arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64,
		arrow.BinaryTypes.String, arrow.BinaryTypes.Binary,
		arrow.FixedWidthTypes.Date32, arrow.FixedWidthTypes.Timestamp_us,
		&arrow.Decimal128Type{Precision: 10, Scale: 2},
		arrow.ListOf(arrow.PrimitiveTypes.Int64),
		arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
		arrow.StructOf(arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}),
		&arrow.DurationType{Unit: arrow.Microsecond},
		arrow.FixedWidthTypes.MonthInterval,
	}
	for _, dt := range scannerTypes {
		info := ColumnTypeInfoFor(dt)
		if info.ScanType == unknown || info.DatabaseTypeName == "" {
			t.Errorf("%s: scanner-handled type fell through to the default mapping (db=%q scan=%v)",
				dt, info.DatabaseTypeName, info.ScanType)
		}
	}
}
