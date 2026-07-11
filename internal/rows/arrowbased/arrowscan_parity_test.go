package arrowbased

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/memory"

	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

// The kernel backend (internal/arrowscan) and the Thrift backend (this package)
// each render Arrow cells to driver.Values — nested types to a JSON string — and
// the "identical results across backends" contract requires byte-for-byte
// agreement. That contract used to be guarded only by TestKernelThriftParity,
// which is build-tagged AND needs a live warehouse, so it never runs in CI. This
// test feeds the same arrow.Array through both renderers with no cgo and no
// warehouse, so a divergence (e.g. struct-key JSON escaping) fails a default
// CGO_ENABLED=0 run.
//
// It lives in package arrowbased because the container factory
// (makeColumnValueContainer) is package-private; arrowscan is a pure leaf import
// (no cycle).
func renderViaArrowbased(t *testing.T, arr arrow.Array, row int) any {
	t.Helper()
	maker := &arrowValueContainerMaker{}
	holder, err := maker.makeColumnValueContainer(arr.DataType(), time.UTC, func(ts arrow.Timestamp) time.Time {
		return ts.ToTime(arrow.Microsecond)
	}, nil)
	if err != nil {
		t.Fatalf("makeColumnValueContainer(%s): %v", arr.DataType(), err)
	}
	if err := holder.SetValueArray(arr.Data()); err != nil {
		t.Fatalf("SetValueArray(%s): %v", arr.DataType(), err)
	}
	if holder.IsNull(row) {
		return nil
	}
	v, err := holder.Value(row)
	if err != nil {
		t.Fatalf("Value(%s): %v", arr.DataType(), err)
	}
	return v
}

func TestArrowbasedKernelRenderParity(t *testing.T) {
	pool := memory.NewGoAllocator()

	// structWithKey builds a single-row STRUCT<name:INT> whose field is `name`.
	structWithKey := func(name string) arrow.Array {
		dt := arrow.StructOf(arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64})
		b := array.NewStructBuilder(pool, dt)
		b.Append(true)
		b.FieldBuilder(0).(*array.Int64Builder).Append(1)
		return b.NewArray()
	}

	cases := []struct {
		name  string
		build func() arrow.Array
	}{
		{"list_int", func() arrow.Array {
			b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
			vb := b.ValueBuilder().(*array.Int64Builder)
			b.Append(true)
			vb.Append(1)
			vb.Append(2)
			return b.NewArray()
		}},
		{"struct_simple", func() arrow.Array { return structWithKey("a") }},
		// The escaping divergence: a field name with a quote must render as valid,
		// identically-escaped JSON on both backends.
		{"struct_key_with_quote", func() arrow.Array { return structWithKey(`a"b`) }},
		{"struct_key_with_backslash", func() arrow.Array { return structWithKey(`a\b`) }},
		{"struct_key_with_newline", func() arrow.Array { return structWithKey("a\nb") }},
		{"map_string_key", func() arrow.Array {
			b := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
			b.Append(true) // open row 0's map
			b.KeyBuilder().(*array.StringBuilder).Append("k")
			b.ItemBuilder().(*array.Int64Builder).Append(9)
			return b.NewArray()
		}},
		{"map_special_string_key", func() arrow.Array {
			b := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
			b.Append(true)
			b.KeyBuilder().(*array.StringBuilder).Append(`k"x`)
			b.ItemBuilder().(*array.Int64Builder).Append(9)
			return b.NewArray()
		}},
		{"map_int_key", func() arrow.Array {
			b := array.NewMapBuilder(pool, arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64, false)
			b.Append(true)
			b.KeyBuilder().(*array.Int64Builder).Append(7)
			b.ItemBuilder().(*array.Int64Builder).Append(9)
			return b.NewArray()
		}},
		{"map_binary_key", func() arrow.Array {
			// []byte key: json.Marshal → base64 "YWJj", NOT fmt "%v" [97 98 99].
			b := array.NewMapBuilder(pool, arrow.BinaryTypes.Binary, arrow.PrimitiveTypes.Int64, false)
			b.Append(true)
			b.KeyBuilder().(*array.BinaryBuilder).Append([]byte("abc"))
			b.ItemBuilder().(*array.Int64Builder).Append(9)
			return b.NewArray()
		}},
		{"map_date_key", func() arrow.Array {
			b := array.NewMapBuilder(pool, arrow.FixedWidthTypes.Date32, arrow.PrimitiveTypes.Int64, false)
			b.Append(true)
			b.KeyBuilder().(*array.Date32Builder).Append(arrow.Date32FromTime(time.Date(2026, time.July, 9, 0, 0, 0, 0, time.UTC)))
			b.ItemBuilder().(*array.Int64Builder).Append(9)
			return b.NewArray()
		}},
		{"nested_float32", func() arrow.Array {
			dt := arrow.StructOf(arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Float32})
			b := array.NewStructBuilder(pool, dt)
			b.Append(true)
			b.FieldBuilder(0).(*array.Float32Builder).Append(0.1)
			return b.NewArray()
		}},
		{"nested_decimal", func() arrow.Array {
			dt := arrow.StructOf(arrow.Field{Name: "d", Type: &arrow.Decimal128Type{Precision: 5, Scale: 2}})
			b := array.NewStructBuilder(pool, dt)
			b.Append(true)
			b.FieldBuilder(0).(*array.Decimal128Builder).Append(decimal128.FromU64(1999))
			return b.NewArray()
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.build()
			defer arr.Release()

			kernel, err := arrowscan.ScanCell(arr, 0, time.UTC)
			if err != nil {
				t.Fatalf("arrowscan.ScanCell: %v", err)
			}
			thrift := renderViaArrowbased(t, arr, 0)

			if kernel != thrift {
				t.Errorf("backend divergence for %s:\n  kernel  = %#v\n  thrift  = %#v", tc.name, kernel, thrift)
			}
		})
	}
}
