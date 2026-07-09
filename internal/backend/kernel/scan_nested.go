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

// renderJSONString renders one nested cell as a JSON string (nil for a NULL cell).
func renderJSONString(col arrow.Array, row int) (driver.Value, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	var b strings.Builder
	if err := writeJSON(&b, col, row); err != nil {
		return nil, err
	}
	return b.String(), nil
}

// writeJSON writes the JSON form of col[row] into b, recursing for nested types.
func writeJSON(b *strings.Builder, col arrow.Array, row int) error {
	if col.IsNull(row) {
		b.WriteString("null")
		return nil
	}
	switch c := col.(type) {
	case *array.List:
		return writeListJSON(b, c.ListValues(), int(c.Offsets()[row]), int(c.Offsets()[row+1]))
	case *array.LargeList:
		return writeListJSON(b, c.ListValues(), int(c.Offsets()[row]), int(c.Offsets()[row+1]))
	case *array.FixedSizeList:
		n := int(c.DataType().(*arrow.FixedSizeListType).Len())
		return writeListJSON(b, c.ListValues(), row*n, row*n+n)
	case *array.Map:
		return writeMapJSON(b, c, row)
	case *array.Struct:
		return writeStructJSON(b, c, row)
	default:
		v, err := scalarForJSON(col, row)
		if err != nil {
			return err
		}
		return writeScalarJSON(b, v)
	}
}

func writeListJSON(b *strings.Builder, values arrow.Array, start, end int) error {
	b.WriteByte('[')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		if err := writeJSON(b, values, i); err != nil {
			return err
		}
	}
	b.WriteByte(']')
	return nil
}

func writeMapJSON(b *strings.Builder, m *array.Map, row int) error {
	start, end := int(m.Offsets()[row]), int(m.Offsets()[row+1])
	keys := m.Keys()
	items := m.Items()
	b.WriteByte('{')
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(',')
		}
		// JSON object keys must be strings; stringify the key value.
		kv, err := scalarForJSON(keys, i)
		if err != nil {
			return err
		}
		writeJSONKey(b, kv)
		b.WriteByte(':')
		if err := writeJSON(b, items, i); err != nil {
			return err
		}
	}
	b.WriteByte('}')
	return nil
}

func writeStructJSON(b *strings.Builder, s *array.Struct, row int) error {
	st := s.DataType().(*arrow.StructType)
	b.WriteByte('{')
	for f := 0; f < s.NumField(); f++ {
		if f > 0 {
			b.WriteByte(',')
		}
		keyBytes, _ := json.Marshal(st.Field(f).Name)
		b.Write(keyBytes)
		b.WriteByte(':')
		if err := writeJSON(b, s.Field(f), row); err != nil {
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

// scalarForJSON returns the Go value used inside JSON for a leaf cell. It differs
// from scanCell in one way: a nested decimal renders as float64 (lossy), matching
// the Thrift path's in-JSON decimal rendering (#274); the exact-string decimal
// applies only to a top-level decimal column.
func scalarForJSON(col arrow.Array, row int) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch c := col.(type) {
	case *array.Decimal128:
		dt := col.DataType().(*arrow.Decimal128Type)
		return c.Value(row).ToFloat64(dt.Scale), nil
	case *array.Decimal256:
		dt := col.DataType().(*arrow.Decimal256Type)
		return c.Value(row).ToFloat64(dt.Scale), nil
	default:
		// Reuse scanCell's scalar arm for every non-nested, non-decimal leaf.
		return scanCell(col, row)
	}
}
