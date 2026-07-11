// Package arrowscan converts Arrow array cells to database/sql driver.Values,
// with nested types (List/Map/Struct, and VARIANT which arrives nested) rendered
// to a JSON string byte-identical to the Thrift arrow path
// (internal/rows/arrowbased). It is pure Go (no cgo), so it is shared by the
// kernel backend and testable in the default CGO_ENABLED=0 build — the tests here
// are the regression guard for the exact rendering rules (native float32, exact
// decimals, time.Time formatting, JSON grammar) both backends must agree on.
//
// Rendering to JSON (not a Go map/slice) is deliberate: it is what the Thrift
// path returns, so a query's result is identical across backends.
//   - list        → [v0,v1,...]
//   - map         → {"k0":v0,"k1":v1,...}   (keys stringified)
//   - struct      → {"field0":v0,...}
//   - nested NULL → null
//   - time.Time   → quoted .String()  (matches the Thrift marshal() special-case)
//   - nested decimal → exact scale-applied JSON number literal (never a lossy
//     float64), matching Thrift's marshalScalar → ValueString (#253/#274)
//   - float32     → native float32 (not widened to float64), so JSON renders
//     3.14, not 3.140000104904175
package arrowscan

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/databricks/databricks-sql-go/internal/decimalfmt"
)

// ScanCell extracts one cell as a driver.Value. Scalars map to their Go value:
// bool, all int/uint widths, float (native float32/float64), string, binary,
// date, timestamp, and top-level decimal (as an exact fixed-point string,
// matching the Thrift path — a float64 would lose precision beyond ~17 digits;
// see databricks-sql-go#274). Nested types (List/Map/Struct, and VARIANT which
// arrives nested) render to a JSON string byte-identical to the Thrift path;
// GEOMETRY arrives as a WKB/WKT string and is handled by the string arm. NULLs
// map to nil. A genuinely unhandled type (e.g. interval/duration) returns an
// error rather than a silently wrong value. loc renders DATE / TIMESTAMP in the
// session time zone (nil = UTC, arrow's ToTime default).
func ScanCell(col arrow.Array, row int, loc *time.Location) (driver.Value, error) {
	return ScanCellCached(col, row, loc, nil)
}

// StructKeyCache memoizes the JSON-escaped `"name":` prefixes for a struct type,
// so writeStructJSON doesn't re-marshal constant field names on every row. It is
// caller-owned and must be scoped to a single result set (e.g. one driver.Rows)
// and discarded with it — NOT a process-global, which would leak because the Arrow
// C Data import allocates a fresh *StructType per batch (see databricks-sql-go
// round-2 N1). A nil cache is valid: rendering just recomputes the keys inline.
type StructKeyCache struct {
	m map[*arrow.StructType][]string
}

// NewStructKeyCache returns a cache ready to pass to ScanCellCached.
func NewStructKeyCache() *StructKeyCache {
	return &StructKeyCache{m: make(map[*arrow.StructType][]string)}
}

// keyPrefixes returns the escaped `"name":` prefix for each field of st,
// memoized. Safe on a nil receiver (computes without caching).
func (c *StructKeyCache) keyPrefixes(st *arrow.StructType) []string {
	if c != nil {
		if p, ok := c.m[st]; ok {
			return p
		}
	}
	fields := st.Fields()
	prefixes := make([]string, len(fields))
	for i := range fields {
		name, _ := json.Marshal(fields[i].Name) // JSON-escapes the field name
		prefixes[i] = string(name) + ":"
	}
	if c != nil {
		c.m[st] = prefixes
	}
	return prefixes
}

// ScanCellCached is ScanCell with a caller-owned StructKeyCache (see
// StructKeyCache) so struct field-name keys are escaped once per result set
// rather than once per row. Pass nil for the un-memoized one-shot behavior.
func ScanCellCached(col arrow.Array, row int, loc *time.Location, keys *StructKeyCache) (driver.Value, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch c := col.(type) {
	case *array.Null:
		return nil, nil
	case *array.Boolean:
		return c.Value(row), nil
	case *array.Int8:
		return int64(c.Value(row)), nil
	case *array.Int16:
		return int64(c.Value(row)), nil
	case *array.Int32:
		return int64(c.Value(row)), nil
	case *array.Int64:
		return c.Value(row), nil
	case *array.Uint8:
		return int64(c.Value(row)), nil
	case *array.Uint16:
		return int64(c.Value(row)), nil
	case *array.Uint32:
		return int64(c.Value(row)), nil
	case *array.Uint64:
		// Databricks SQL has no unsigned types, so a Uint64 column does not occur
		// in practice; this arm is defensive. driver.Value has no uint64 and the
		// driver convention is int64 for integers, so a value above MaxInt64 would
		// wrap — acceptable for an unreachable path.
		return int64(c.Value(row)), nil // #nosec G115 -- see above; unreachable for Databricks types
	case *array.Float32:
		// Return the native float32, NOT a widened float64: the Thrift path returns
		// a float32 driver.Value for a bare FLOAT column, and database/sql's
		// asString formats it at bit-size 32 — so widening here would render
		// CAST(0.1 AS FLOAT) as "0.10000000149011612" vs Thrift's "0.1".
		return c.Value(row), nil
	case *array.Float64:
		return c.Value(row), nil
	case *array.String:
		return c.Value(row), nil
	case *array.LargeString:
		return c.Value(row), nil
	case *array.Binary:
		return c.Value(row), nil
	case *array.Date32:
		return inLocation(c.Value(row).ToTime(), loc), nil
	case *array.Date64:
		return inLocation(c.Value(row).ToTime(), loc), nil
	case *array.Timestamp:
		dt, ok := col.DataType().(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("timestamp column has unexpected datatype %s", col.DataType())
		}
		return inLocation(c.Value(row).ToTime(dt.Unit), loc), nil
	case *array.Decimal128:
		dt := col.DataType().(*arrow.Decimal128Type)
		return decimalfmt.ExactString(c.Value(row), dt.Scale), nil
	case *array.List, *array.LargeList, *array.FixedSizeList, *array.Map, *array.Struct:
		// Nested types (and VARIANT, which arrives as a nested value) render to a
		// JSON string matching the Thrift path.
		return renderJSONString(col, row, loc, keys)
	default:
		return nil, fmt.Errorf("scanning arrow type %s is not supported "+
			"(intervals are not yet handled)", col.DataType())
	}
}

// inLocation renders t in loc, matching the Thrift path's .In(location); a nil
// loc leaves the value in UTC (arrow's ToTime default).
func inLocation(t time.Time, loc *time.Location) time.Time {
	if loc == nil {
		return t
	}
	return t.In(loc)
}

// renderJSONString renders one nested cell as a JSON string (nil for a NULL
// cell). loc is applied to any timestamp/date leaves, matching the top-level
// scan and the Thrift path.
func renderJSONString(col arrow.Array, row int, loc *time.Location, keys *StructKeyCache) (driver.Value, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	var b strings.Builder
	if err := writeJSON(&b, col, row, loc, keys); err != nil {
		return nil, err
	}
	return b.String(), nil
}

// writeJSON writes the JSON form of col[row] into b, recursing for nested types.
func writeJSON(b *strings.Builder, col arrow.Array, row int, loc *time.Location, keys *StructKeyCache) error {
	if col.IsNull(row) {
		b.WriteString("null")
		return nil
	}
	switch c := col.(type) {
	case *array.List:
		return writeListJSON(b, c.ListValues(), int(c.Offsets()[row]), int(c.Offsets()[row+1]), loc, keys)
	case *array.LargeList:
		return writeListJSON(b, c.ListValues(), int(c.Offsets()[row]), int(c.Offsets()[row+1]), loc, keys)
	case *array.FixedSizeList:
		n := int(c.DataType().(*arrow.FixedSizeListType).Len())
		return writeListJSON(b, c.ListValues(), row*n, row*n+n, loc, keys)
	case *array.Map:
		return writeMapJSON(b, c, row, loc, keys)
	case *array.Struct:
		return writeStructJSON(b, c, row, loc, keys)
	case *array.Decimal128:
		// Emit the exact scale-applied decimal as a raw JSON number literal, not a
		// float64 — a float64 would render DECIMAL(5,2) 19.99 as 19.990000000000002
		// and corrupt high-precision values. Matches the Thrift path's marshalScalar
		// → ValueString (databricks-sql-go#253/#274).
		b.WriteString(decimalfmt.ExactString(c.Value(row), col.DataType().(*arrow.Decimal128Type).Scale))
		return nil
	case *array.Float32:
		// Marshal the native float32, NOT a widened float64: json.Marshal(float32
		// (3.14)) is "3.14" but json.Marshal(float64(float32(3.14))) is
		// "3.140000104904175". The Thrift nested path marshals the native float32,
		// so widening here would break byte-parity for ARRAY/MAP/STRUCT<…FLOAT…>.
		return writeScalarJSON(b, c.Value(row))
	default:
		v, err := scalarForJSON(col, row, loc)
		if err != nil {
			return err
		}
		return writeScalarJSON(b, v)
	}
}

func writeListJSON(b *strings.Builder, values arrow.Array, start, end int, loc *time.Location, keys *StructKeyCache) error {
	b.WriteByte('[')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		if err := writeJSON(b, values, i, loc, keys); err != nil {
			return err
		}
	}
	b.WriteByte(']')
	return nil
}

func writeMapJSON(b *strings.Builder, m *array.Map, row int, loc *time.Location, keys *StructKeyCache) error {
	start, end := int(m.Offsets()[row]), int(m.Offsets()[row+1])
	mapKeys := m.Keys()
	items := m.Items()
	b.WriteByte('{')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		// JSON object keys must be strings; stringify the key value.
		kv, err := scalarForJSON(mapKeys, i, loc)
		if err != nil {
			return err
		}
		writeJSONKey(b, kv)
		b.WriteByte(':')
		if err := writeJSON(b, items, i, loc, keys); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

func writeStructJSON(b *strings.Builder, s *array.Struct, row int, loc *time.Location, keys *StructKeyCache) error {
	// Field-name keys are constant across rows; keys.keyPrefixes memoizes them per
	// result set so this per-row/per-cell path doesn't re-marshal them (a nil cache
	// recomputes inline — correct, just not memoized).
	prefixes := keys.keyPrefixes(s.DataType().(*arrow.StructType))
	b.WriteByte('{')
	for f := 0; f < s.NumField(); f++ {
		if f > 0 {
			b.WriteByte(',')
		}
		b.WriteString(prefixes[f]) // pre-escaped `"name":`
		if err := writeJSON(b, s.Field(f), row, loc, keys); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

// writeJSONKey writes a value as a JSON object key (always a quoted string),
// matching the Thrift path's mapValueContainer: marshal the value the same way a
// leaf is marshaled (marshalScalar → json.Marshal, so a []byte key renders as
// base64 "YWJj", NOT fmt "%v" [97 98 99]), then quote-wrap the result if it is
// not already a JSON string (numbers/bools become "7"/"true" keys).
func writeJSONKey(b *strings.Builder, v any) {
	// A time.Time key renders as its quoted .String() (same special-case as
	// writeScalarJSON / the Thrift marshal()), not RFC3339.
	if t, ok := v.(time.Time); ok {
		kb, _ := json.Marshal(t.String())
		b.Write(kb)
		return
	}
	kb, err := json.Marshal(v)
	if err != nil {
		// Unmarshalable key value — fall back to its stringified form, quoted.
		kb, _ = json.Marshal(fmt.Sprintf("%v", v))
	}
	if len(kb) > 0 && kb[0] == '"' {
		b.Write(kb) // already a JSON string (string / []byte via marshal)
		return
	}
	b.WriteByte('"')
	b.Write(kb)
	b.WriteByte('"')
}

// writeScalarJSON writes a scalar leaf, mirroring the Thrift marshal(): a
// time.Time becomes a quoted .String(); everything else uses json.Marshal.
func writeScalarJSON(b *strings.Builder, v any) error {
	if v == nil {
		b.WriteString("null")
		return nil
	}
	if t, ok := v.(time.Time); ok {
		b.WriteByte('"')
		b.WriteString(t.String())
		b.WriteByte('"')
		return nil
	}
	vb, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b.Write(vb)
	return nil
}

// scalarForJSON returns the Go value used for a nested leaf that is not itself a
// container — today only a map key (values are written directly by writeJSON).
// It reuses ScanCell's scalar arm, so a decimal key renders via the exact-string
// path (writeJSONKey then quotes it), never a lossy float64.
func scalarForJSON(col arrow.Array, row int, loc *time.Location) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	return ScanCell(col, row, loc)
}
