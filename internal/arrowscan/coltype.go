package arrowscan

import (
	"database/sql"
	"math"
	"reflect"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
)

// ColumnTypeInfo is the per-column metadata database/sql surfaces through
// sql.ColumnType — the DatabaseTypeName, ScanType, and Length the optional
// driver.RowsColumnType* interfaces return. The kernel backend derives it from
// the result's Arrow schema; ColumnTypeInfoFor is the single mapping both the
// value scanner (ScanCellCached) and the type-metadata reporter agree on, so a
// column's reported type can never drift from what a row actually scans into.
//
// The mapping mirrors the Thrift backend (internal/rows/rows.go getScanType /
// ColumnTypeDatabaseTypeName / ColumnTypeLength) so a query reports byte-identical
// column metadata on either backend — the same "identical results across backends"
// contract the value renderers hold. TestColumnTypeInfoMatchesThrift and the live
// TestKernelThriftColumnTypeParity are the guards.
type ColumnTypeInfo struct {
	// DatabaseTypeName is the Databricks type name (e.g. "BIGINT", "DECIMAL",
	// "ARRAY"), matching what the Thrift path reports; "" for a type with no
	// Databricks name.
	DatabaseTypeName string
	// ScanType is the Go type database/sql recommends scanning the column into,
	// matching the Thrift path. nil (only for the NULL type) makes database/sql
	// fall back to interface{}, exactly as the Thrift path's nil scan type does.
	ScanType reflect.Type
	// Length / HasLength report a variable-length column's length. As on the
	// Thrift path, only variable-length types (string/binary/nested, and the
	// server-stringified interval/geo types) report a length, and the reported
	// value is math.MaxInt64 (unbounded); fixed-width types report (0, false).
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
// listed here are exactly those ScanCellCached scans, so the type reported for a
// column and the value produced for its cells stay in lockstep.
//
// Notably: DECIMAL reports sql.RawBytes (the Thrift scan type for DECIMAL) even
// though the value is rendered as an exact string — matching Thrift, which also
// scans DECIMAL into RawBytes; and the interval / geo types report STRING because
// the Thrift server pre-formats them to strings and the kernel formats them
// Go-side to the same string, so both are indistinguishable to a caller.
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
		// Thrift scans DECIMAL into sql.RawBytes; match it even though the kernel
		// renders the value as an exact fixed-point string (both convert cleanly to
		// a caller's *string/*[]byte, and reporting the same scan type keeps the
		// backends indistinguishable).
		return ColumnTypeInfo{DatabaseTypeName: "DECIMAL", ScanType: scanTypeRawBytes}
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return varLen("ARRAY", scanTypeRawBytes)
	case arrow.MAP:
		return varLen("MAP", scanTypeRawBytes)
	case arrow.STRUCT:
		return varLen("STRUCT", scanTypeRawBytes)
	case arrow.DURATION:
		// INTERVAL DAY TO SECOND. Parity target is what the Thrift backend REPORTS,
		// which is config-dependent: in the prod default (native-interval Arrow off)
		// the server pre-formats intervals to text and declares the Thrift column
		// STRING_TYPE, so Thrift's GetDBTypeName yields "STRING" and MaxInt64 length
		// — verified live against both backends. We therefore report STRING here even
		// though the kernel receives a native arrow.DURATION (which it formats Go-side
		// to the identical string). If a warehouse ever enables native-interval Thrift
		// the server would instead declare INTERVAL_DAY_TIME and Thrift would report
		// that; matching STRING is correct for the default path the parity test pins,
		// and the scanned VALUE is identical either way — only this label would differ.
		return varLen("STRING", scanTypeString)
	case arrow.INTERVAL_MONTHS:
		// INTERVAL YEAR TO MONTH — same server-config reasoning as arrow.DURATION.
		return varLen("STRING", scanTypeString)
	case arrow.NULL:
		// The NULL type has no scan type on the Thrift path (nil → database/sql
		// falls back to interface{}); mirror that.
		return ColumnTypeInfo{DatabaseTypeName: "NULL", ScanType: nil}
	default:
		// A type ScanCellCached does not handle: report the Thrift default scan type
		// (*interface{}) and no database name, rather than inventing one.
		return ColumnTypeInfo{DatabaseTypeName: "", ScanType: scanTypeUnknown}
	}
}

// varLen builds a ColumnTypeInfo for a variable-length type, which reports an
// unbounded length (math.MaxInt64) just as the Thrift path does for
// string/binary/nested columns.
func varLen(name string, scan reflect.Type) ColumnTypeInfo {
	return ColumnTypeInfo{DatabaseTypeName: name, ScanType: scan, Length: math.MaxInt64, HasLength: true}
}
