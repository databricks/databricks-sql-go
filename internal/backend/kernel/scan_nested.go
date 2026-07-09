//go:build cgo && databricks_kernel

package kernel

// Recursive rendering of nested Arrow values (List/Map/Struct) to a JSON string,
// byte-compatible with the Thrift arrow path
// (internal/rows/arrowbased/columnValues.go). scanCell delegates here for nested
// columns; database/sql consumers then get the same JSON shape from either
// backend:
//   - list        → [v0,v1,...]
//   - map         → {"k0":v0,"k1":v1,...}   (keys stringified)
//   - struct      → {"field0":v0,...}
//   - nested NULL → null
//   - time.Time   → quoted .String()  (matches the Thrift marshal() special-case)
//   - nested decimal → float64 (lossy, matching Thrift's in-JSON rendering; the
//     exact-string path applies only to top-level decimal columns, #274)
//
// VARIANT arrives as a nested value and renders through this path; GEOMETRY
// arrives as a WKB/WKT string and is handled by scanCell's scalar arm.
//
// Rendering to JSON (not a Go map/slice) is deliberate: it is what the Thrift
// path returns, so a query's result is identical across backends — the property
// the Thrift-parity test asserts.

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

// renderJSONString renders one nested cell as a JSON string (nil for a NULL
// cell). loc is applied to any timestamp/date leaves, matching the top-level
// scan and the Thrift path.
func renderJSONString(col arrow.Array, row int, loc *time.Location) (driver.Value, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	var b strings.Builder
	if err := writeJSON(&b, col, row, loc); err != nil {
		return nil, err
	}
	return b.String(), nil
}

// writeJSON writes the JSON form of col[row] into b, recursing for nested types.
func writeJSON(b *strings.Builder, col arrow.Array, row int, loc *time.Location) error {
	if col.IsNull(row) {
		b.WriteString("null")
		return nil
	}
	switch c := col.(type) {
	case *array.List:
		return writeListJSON(b, c.ListValues(), int(c.Offsets()[row]), int(c.Offsets()[row+1]), loc)
	case *array.LargeList:
		return writeListJSON(b, c.ListValues(), int(c.Offsets()[row]), int(c.Offsets()[row+1]), loc)
	case *array.FixedSizeList:
		n := int(c.DataType().(*arrow.FixedSizeListType).Len())
		return writeListJSON(b, c.ListValues(), row*n, row*n+n, loc)
	case *array.Map:
		return writeMapJSON(b, c, row, loc)
	case *array.Struct:
		return writeStructJSON(b, c, row, loc)
	case *array.Decimal128:
		// Emit the exact scale-applied decimal as a raw JSON number literal, not a
		// float64 — a float64 would render DECIMAL(5,2) 19.99 as 19.990000000000002
		// and corrupt high-precision values. Matches the Thrift path's marshalScalar
		// → ValueString (databricks-sql-go#253/#274).
		b.WriteString(decimal128ToExactString(c.Value(row), col.DataType().(*arrow.Decimal128Type).Scale))
		return nil
	default:
		v, err := scalarForJSON(col, row, loc)
		if err != nil {
			return err
		}
		return writeScalarJSON(b, v)
	}
}

func writeListJSON(b *strings.Builder, values arrow.Array, start, end int, loc *time.Location) error {
	b.WriteByte('[')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		if err := writeJSON(b, values, i, loc); err != nil {
			return err
		}
	}
	b.WriteByte(']')
	return nil
}

func writeMapJSON(b *strings.Builder, m *array.Map, row int, loc *time.Location) error {
	start, end := int(m.Offsets()[row]), int(m.Offsets()[row+1])
	keys := m.Keys()
	items := m.Items()
	b.WriteByte('{')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		// JSON object keys must be strings; stringify the key value.
		kv, err := scalarForJSON(keys, i, loc)
		if err != nil {
			return err
		}
		writeJSONKey(b, kv)
		b.WriteByte(':')
		if err := writeJSON(b, items, i, loc); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

func writeStructJSON(b *strings.Builder, s *array.Struct, row int, loc *time.Location) error {
	st := s.DataType().(*arrow.StructType)
	b.WriteByte('{')
	for f := 0; f < s.NumField(); f++ {
		if f > 0 {
			b.WriteByte(',')
		}
		keyBytes, _ := json.Marshal(st.Field(f).Name)
		b.Write(keyBytes)
		b.WriteByte(':')
		if err := writeJSON(b, s.Field(f), row, loc); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

// writeJSONKey writes a value as a JSON object key (always a quoted string).
func writeJSONKey(b *strings.Builder, v any) {
	switch k := v.(type) {
	case string:
		kb, _ := json.Marshal(k)
		b.Write(kb)
	default:
		kb, _ := json.Marshal(fmt.Sprintf("%v", k))
		b.Write(kb)
	}
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
// It reuses scanCell's scalar arm, so a decimal key renders via the exact-string
// path (writeJSONKey then quotes it), never a lossy float64.
func scalarForJSON(col arrow.Array, row int, loc *time.Location) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	return scanCell(col, row, loc)
}
