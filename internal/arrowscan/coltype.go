package arrowscan

import (
	"database/sql"
	"math"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
)

// ColumnTypeInfo is the per-column metadata database/sql surfaces through
// sql.ColumnType. The kernel derives it from the result's Arrow schema via
// ColumnTypeInfoFor — the mapping the value scanner and type reporter share, kept
// byte-identical to the Thrift backend (guarded by the coltype parity tests).
type ColumnTypeInfo struct {
	// DatabaseTypeName is the Databricks type name (e.g. "BIGINT", "DECIMAL"),
	// matching the Thrift path; "" for a type with no Databricks name.
	DatabaseTypeName string
	// ScanType is the Go type database/sql recommends scanning the column into,
	// matching the Thrift path.
	ScanType reflect.Type
	// Length / HasLength report a variable-length column's unbounded length
	// (math.MaxInt64), matching Thrift; fixed-width types report (0, false).
	Length    int64
	HasLength bool
}

// Scan types, matching the Thrift path's vars in internal/rows/rows.go so both
// backends recommend the identical Go destination type per column.
var (
	scanTypeBool     = reflect.TypeOf(true)
	scanTypeInt8     = reflect.TypeOf(int8(0))
	scanTypeInt16    = reflect.TypeOf(int16(0))
	scanTypeInt32    = reflect.TypeOf(int32(0))
	scanTypeInt64    = reflect.TypeOf(int64(0))
	scanTypeFloat32  = reflect.TypeOf(float32(0))
	scanTypeFloat64  = reflect.TypeOf(float64(0))
	scanTypeString   = reflect.TypeOf("")
	scanTypeDateTime = reflect.TypeOf(time.Time{})
	scanTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown  = reflect.TypeOf(new(any)) // *interface{}, as on the Thrift path
)

// ColumnTypeInfoFor maps an Arrow column type to the metadata database/sql
// exposes, matching the Thrift backend for every Databricks type. The Arrow types
// here are exactly those ScanCellCached scans, so a column's reported type and its
// scanned value stay in lockstep.
func ColumnTypeInfoFor(dt arrow.DataType) ColumnTypeInfo {
	switch dt.ID() {
	case arrow.BOOL:
		return ColumnTypeInfo{DatabaseTypeName: "BOOLEAN", ScanType: scanTypeBool}
	case arrow.INT8:
		return ColumnTypeInfo{DatabaseTypeName: "TINYINT", ScanType: scanTypeInt8}
	case arrow.INT16:
		return ColumnTypeInfo{DatabaseTypeName: "SMALLINT", ScanType: scanTypeInt16}
	case arrow.INT32:
		return ColumnTypeInfo{DatabaseTypeName: "INT", ScanType: scanTypeInt32}
	case arrow.INT64:
		return ColumnTypeInfo{DatabaseTypeName: "BIGINT", ScanType: scanTypeInt64}
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		// Databricks SQL has no unsigned types; defensive arm matching ScanCellCached,
		// which widens unsigned ints to int64 (driver.Value has no uint64).
		return ColumnTypeInfo{DatabaseTypeName: "BIGINT", ScanType: scanTypeInt64}
	case arrow.FLOAT32:
		return ColumnTypeInfo{DatabaseTypeName: "FLOAT", ScanType: scanTypeFloat32}
	case arrow.FLOAT64:
		return ColumnTypeInfo{DatabaseTypeName: "DOUBLE", ScanType: scanTypeFloat64}
	case arrow.STRING, arrow.LARGE_STRING:
		// STRING covers VARCHAR/CHAR/VARIANT/GEOMETRY/GEOGRAPHY too — all arrive as
		// Arrow Utf8, and the Thrift path collapses them to STRING as well.
		return varLen("STRING", scanTypeString)
	case arrow.BINARY:
		return varLen("BINARY", scanTypeRawBytes)
	case arrow.DATE32, arrow.DATE64:
		return ColumnTypeInfo{DatabaseTypeName: "DATE", ScanType: scanTypeDateTime}
	case arrow.TIMESTAMP:
		return ColumnTypeInfo{DatabaseTypeName: "TIMESTAMP", ScanType: scanTypeDateTime}
	case arrow.DECIMAL128:
		// Match Thrift's sql.RawBytes scan type even though the kernel renders the
		// value as an exact string — both convert cleanly to a caller's *string/*[]byte.
		return ColumnTypeInfo{DatabaseTypeName: "DECIMAL", ScanType: scanTypeRawBytes}
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return varLen("ARRAY", scanTypeRawBytes)
	case arrow.MAP:
		return varLen("MAP", scanTypeRawBytes)
	case arrow.STRUCT:
		return varLen("STRUCT", scanTypeRawBytes)
	case arrow.DURATION:
		// INTERVAL DAY TO SECOND: Thrift's prod default pre-formats intervals to text
		// and declares STRING_TYPE (verified live), so match STRING; the kernel formats
		// the native arrow.DURATION Go-side to the identical string.
		return varLen("STRING", scanTypeString)
	case arrow.INTERVAL_MONTHS:
		// INTERVAL YEAR TO MONTH — same server-config reasoning as arrow.DURATION.
		return varLen("STRING", scanTypeString)
	case arrow.NULL:
		// VOID/NULL columns: the server stringifies them over Thrift (verified live:
		// even bare SELECT NULL reports STRING), same as intervals — so match STRING.
		return varLen("STRING", scanTypeString)
	default:
		// A type ScanCellCached does not handle: report the Thrift default scan type
		// (*interface{}) and no database name, rather than inventing one.
		return ColumnTypeInfo{DatabaseTypeName: "", ScanType: scanTypeUnknown}
	}
}

// varLen builds a ColumnTypeInfo for a variable-length type, reporting the
// unbounded length (math.MaxInt64) the Thrift path uses for such columns.
func varLen(name string, scan reflect.Type) ColumnTypeInfo {
	return ColumnTypeInfo{DatabaseTypeName: name, ScanType: scan, Length: math.MaxInt64, HasLength: true}
}
