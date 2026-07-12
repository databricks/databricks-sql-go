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
func renderViaArrowbased(t *testing.T, arr arrow.Array, row int, loc *time.Location) any {
	t.Helper()
	maker := &arrowValueContainerMaker{}
	holder, err := maker.makeColumnValueContainer(arr.DataType(), loc, func(ts arrow.Timestamp) time.Time {
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
		// Highest-drift shapes: recursive nesting, a nested timestamp leaf (the
		// time.Time → quoted .String() special-case), and a null leaf inside a
		// container (vs a null container, which is already covered).
		{"array_of_struct", func() arrow.Array {
			elem := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64})
			b := array.NewListBuilder(pool, elem)
			sb := b.ValueBuilder().(*array.StructBuilder)
			b.Append(true)
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Int64Builder).Append(1)
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Int64Builder).Append(2)
			return b.NewArray()
		}},
		{"struct_of_list", func() arrow.Array {
			dt := arrow.StructOf(arrow.Field{Name: "xs", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)})
			b := array.NewStructBuilder(pool, dt)
			b.Append(true)
			lb := b.FieldBuilder(0).(*array.ListBuilder)
			lb.Append(true)
			vb := lb.ValueBuilder().(*array.Int64Builder)
			vb.Append(1)
			vb.Append(2)
			return b.NewArray()
		}},
		{"map_of_struct_value", func() arrow.Array {
			valT := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64})
			b := array.NewMapBuilder(pool, arrow.BinaryTypes.String, valT, false)
			b.Append(true)
			b.KeyBuilder().(*array.StringBuilder).Append("k")
			sb := b.ItemBuilder().(*array.StructBuilder)
			sb.Append(true)
			sb.FieldBuilder(0).(*array.Int64Builder).Append(5)
			return b.NewArray()
		}},
		{"nested_timestamp", func() arrow.Array {
			dt := arrow.StructOf(arrow.Field{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}})
			b := array.NewStructBuilder(pool, dt)
			b.Append(true)
			ts := time.Date(2026, time.July, 9, 12, 34, 56, 0, time.UTC)
			b.FieldBuilder(0).(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UnixMicro()))
			return b.NewArray()
		}},
		{"null_leaf_in_struct", func() arrow.Array {
			dt := arrow.StructOf(
				arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
				arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64},
			)
			b := array.NewStructBuilder(pool, dt)
			b.Append(true)
			b.FieldBuilder(0).(*array.Int64Builder).Append(1)
			b.FieldBuilder(1).(*array.Int64Builder).AppendNull()
			return b.NewArray()
		}},
	}

	// A non-UTC location, so the timestamp/date rendering path (both backends
	// apply .In(loc)) is actually exercised rather than a UTC no-op.
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.build()
			defer arr.Release()

			kernel, err := arrowscan.ScanCell(arr, 0, loc)
			if err != nil {
				t.Fatalf("arrowscan.ScanCell: %v", err)
			}
			thrift := renderViaArrowbased(t, arr, 0, loc)

			if kernel != thrift {
				t.Errorf("backend divergence for %s:\n  kernel  = %#v\n  thrift  = %#v", tc.name, kernel, thrift)
			}
		})
	}
}

// Top-level (non-nested) scalars: assert the kernel ScanCell and the Thrift
// container render the same Go value. DECIMAL is deliberately excluded here — see
// TestTopLevelDecimalRendering for why the two *container-level* paths differ by
// arrow type while the actual driver results agree.
func TestArrowbasedKernelTopLevelScalarParity(t *testing.T) {
	pool := memory.NewGoAllocator()
	loc := time.UTC

	cases := []struct {
		name  string
		build func() arrow.Array
	}{
		{"tinyint", func() arrow.Array {
			b := array.NewInt8Builder(pool)
			b.Append(1)
			return b.NewArray()
		}},
		{"smallint", func() arrow.Array {
			b := array.NewInt16Builder(pool)
			b.Append(2)
			return b.NewArray()
		}},
		{"int", func() arrow.Array {
			b := array.NewInt32Builder(pool)
			b.Append(3)
			return b.NewArray()
		}},
		{"int64", func() arrow.Array {
			b := array.NewInt64Builder(pool)
			b.Append(42)
			return b.NewArray()
		}},
		{"float32", func() arrow.Array {
			b := array.NewFloat32Builder(pool)
			b.Append(0.1)
			return b.NewArray()
		}},
		{"float64", func() arrow.Array {
			b := array.NewFloat64Builder(pool)
			b.Append(3.5)
			return b.NewArray()
		}},
		{"string", func() arrow.Array {
			b := array.NewStringBuilder(pool)
			b.Append("hi")
			return b.NewArray()
		}},
		{"timestamp", func() arrow.Array {
			b := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
			b.Append(arrow.Timestamp(time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC).UnixMicro()))
			return b.NewArray()
		}},
		{"date32", func() arrow.Array {
			b := array.NewDate32Builder(pool)
			b.Append(arrow.Date32FromTime(time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)))
			return b.NewArray()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr := tc.build()
			defer arr.Release()
			kernel, err := arrowscan.ScanCell(arr, 0, loc)
			if err != nil {
				t.Fatalf("arrowscan.ScanCell: %v", err)
			}
			thrift := renderViaArrowbased(t, arr, 0, loc)
			if kernel != thrift {
				t.Errorf("top-level %s divergence:\n  kernel = %#v (%T)\n  thrift = %#v (%T)", tc.name, kernel, kernel, thrift, thrift)
			}
		})
	}
}

// TestArrowbasedKernelTimestampTZParity pins the TIMESTAMP vs TIMESTAMP_NTZ
// rendering contract: over Arrow, a TIMESTAMP arrives with TimeZone "UTC" and a
// TIMESTAMP_NTZ with an empty TimeZone (kernel json.rs:171 — "TIMESTAMP carries a
// tz on the Arrow side; TIMESTAMP_NTZ does not"). Both backends deliberately IGNORE
// that tz field: each renders via ToTime + .In(loc), so the LTZ-vs-NTZ difference is
// carried entirely by the instant value the server sends, NOT by the client
// inspecting the tz. Verified live on both backends (NY + Kolkata, incl. a DST-gap
// literal): kernel == Thrift byte-for-byte for both types.
//
// This test locks that in so a future "semantically-correct" change that skips
// .In(loc) for TimeZone=="" (which would look right in isolation) fails CI — it
// would make the kernel diverge from the Thrift path, which shifts NTZ too.
func TestArrowbasedKernelTimestampTZParity(t *testing.T) {
	pool := memory.NewGoAllocator()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}
	// Same instant for both; only the arrow TimeZone field differs (UTC vs "").
	instant := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	build := func(tz string) arrow.Array {
		b := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tz})
		b.Append(arrow.Timestamp(instant.UnixMicro()))
		return b.NewArray()
	}
	for _, tc := range []struct {
		name string
		tz   string
	}{
		{"timestamp_ltz_utc_zone", "UTC"},
		{"timestamp_ntz_empty_zone", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arr := build(tc.tz)
			defer arr.Release()
			kernel, err := arrowscan.ScanCell(arr, 0, loc)
			if err != nil {
				t.Fatalf("arrowscan.ScanCell: %v", err)
			}
			thrift := renderViaArrowbased(t, arr, 0, loc)
			if kernel != thrift {
				t.Errorf("TimeZone=%q divergence:\n  kernel = %#v\n  thrift = %#v", tc.tz, kernel, thrift)
			}
		})
	}
}

// TestTopLevelDecimalRendering documents the top-level DECIMAL story, which is
// subtler than "kernel string vs Thrift float64":
//
//   - The kernel path always delivers DECIMAL as a native arrow decimal128 and
//     renders it as an exact scale-applied string (arrowscan.ScanCell).
//   - The Thrift *container* (decimal128Container.Value) converts a decimal128 to
//     a lossy float64 — but that path is only reached when UseArrowNativeDecimal
//     is on AND the value is read as a nested leaf. For a top-level column the
//     scan uses DecimalStringValue (exact string) when the flag is on.
//   - In the DEFAULT config the flag is off, so the server sends DECIMAL as a
//     string column (DecimalAsArrow=false) — no decimal128 arrives at all, and the
//     user gets the exact string.
//
// So across every real driver configuration a top-level DECIMAL comes back as the
// exact string on both backends (verified live). This test pins the kernel side
// (exact string) and the two Thrift container behaviors so the distinction can't
// silently drift into an actual result divergence.
func TestTopLevelDecimalRendering(t *testing.T) {
	pool := memory.NewGoAllocator()
	dt := &arrow.Decimal128Type{Precision: 38, Scale: 4}
	b := array.NewDecimal128Builder(pool, dt)
	defer b.Release()
	n, _ := decimal128.FromString("1234567890123456.7890", dt.Precision, dt.Scale)
	b.Append(n)
	arr := b.NewArray()
	defer arr.Release()

	// Kernel: exact string.
	got, err := arrowscan.ScanCell(arr, 0, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if got != "1234567890123456.7890" {
		t.Errorf("kernel top-level decimal = %#v, want exact string", got)
	}

	// Thrift container Value() is the lossy-float64 path (reached only with native
	// decimal + nested read); DecimalStringValue is the exact top-level scan path.
	// Assert both so a change to either is caught.
	holder := &decimal128Container{scale: dt.Scale}
	if err := holder.SetValueArray(arr.Data()); err != nil {
		t.Fatal(err)
	}
	if s := holder.ValueString(0); s != "1234567890123456.7890" {
		t.Errorf("Thrift DecimalStringValue = %q, want exact string", s)
	}
	// Value() is the lossy path: it returns a float64, and a 20-digit decimal
	// cannot survive it — assert the actual rounded value, not just the type.
	v, err := holder.Value(0)
	if err != nil {
		t.Fatal(err)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("Thrift container Value() = %T, want the documented lossy float64", v)
	}
	if f != 1234567890123456.7890 {
		t.Errorf("Thrift container Value() = %v, want the float64 rounding of the decimal", f)
	}
}
