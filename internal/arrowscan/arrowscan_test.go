package arrowscan

import (
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// ScanCell renders the supported scalar types and rejects an unsupported type
// (rather than returning a silently wrong value).
func TestScanCellScalars(t *testing.T) {
	pool := memory.NewGoAllocator()

	t.Run("int64", func(t *testing.T) {
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.Append(42)
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(int64) != 42 {
			t.Errorf("got %v", v)
		}
	})

	t.Run("string", func(t *testing.T) {
		b := array.NewStringBuilder(pool)
		defer b.Release()
		b.Append("hi")
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != "hi" {
			t.Errorf("got %v", v)
		}
	})

	t.Run("float32_native", func(t *testing.T) {
		// A top-level FLOAT column must scan to a native float32, not a widened
		// float64 — matching Thrift, so database/sql's asString renders
		// CAST(0.1 AS FLOAT) as "0.1", not "0.10000000149011612".
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		b.Append(0.1)
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := v.(float32)
		if !ok {
			t.Fatalf("want float32, got %T", v)
		}
		if got != float32(0.1) {
			t.Errorf("got %v, want 0.1", got)
		}
	})

	t.Run("null", func(t *testing.T) {
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("null should scan to nil, got %v", v)
		}
	})

	t.Run("decimal_exact_string", func(t *testing.T) {
		// 12345 at scale 2 = "123.45", exact (not a float64).
		dt := &arrow.Decimal128Type{Precision: 10, Scale: 2}
		b := array.NewDecimal128Builder(pool, dt)
		defer b.Release()
		b.Append(decimal128.FromU64(12345))
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != "123.45" {
			t.Errorf("got %v, want 123.45", v)
		}
	})

	t.Run("unsupported_type_errors", func(t *testing.T) {
		// An unhandled arrow type must error, not return a silently wrong value.
		// Duration/MonthInterval are now handled (see TestScanCellInterval); use a
		// type with no scan arm.
		b := array.NewTime32Builder(pool, &arrow.Time32Type{Unit: arrow.Second})
		defer b.Release()
		b.Append(1000)
		arr := b.NewArray()
		defer arr.Release()
		if _, err := ScanCell(arr, 0, nil); err == nil {
			t.Error("scanning an unhandled arrow type should return an error")
		}
	})
}

// INTERVAL day-time (arrow duration) and year-month (arrow month-interval) arrive
// as native arrow values on the kernel path and must format to the exact string the
// Thrift path receives pre-formatted from the server: "D HH:MM:SS.nnnnnnnnn" and
// "years-months", with negatives signed. (These formatters were validated live
// kernel==Thrift in the PuPr POC; this is the regression guard.)
func TestScanCellInterval(t *testing.T) {
	pool := memory.NewGoAllocator()

	dayTime := []struct {
		name string
		unit arrow.TimeUnit
		v    int64
		want string
	}{
		{"one_day_us", arrow.Microsecond, 86400 * 1_000_000, "1 00:00:00.000000000"},
		{"day_to_sec_us", arrow.Microsecond, 90061_500000, "1 01:01:01.500000000"},
		{"seconds_unit", arrow.Second, 3661, "0 01:01:01.000000000"},
		{"negative_us", arrow.Microsecond, -90061_500000, "-1 01:01:01.500000000"},
		// A large microsecond magnitude (~106.75M days, near Long.MaxValue μs) must
		// NOT overflow int64 while scaling to nanoseconds — regression guard for the
		// prior multiply-first bug that produced a wrong/negative string here.
		{"large_us_no_overflow", arrow.Microsecond, 9223372036854775807, "106751991 04:00:54.775807000"},
		// math.MinInt64 μs is a representable negative bound. Negating the full
		// magnitude (`v = -v`) wraps it back negative, doubly-negating into garbage;
		// deriving components from the signed value renders it correctly. Its
		// magnitude is exactly one μs past MaxInt64, so the last fractional digit is
		// 8 where the MaxInt64 case above is 7.
		{"min_int64_us", arrow.Microsecond, -9223372036854775808, "-106751991 04:00:54.775808000"},
		// Same MinInt64 wrap-on-negate hazard at the nanosecond unit (no scaling
		// involved — this isolates the negation bug from the multiply-overflow one).
		{"min_int64_ns", arrow.Nanosecond, -9223372036854775808, "-106751 23:47:16.854775808"},
	}
	for _, tc := range dayTime {
		t.Run("daytime_"+tc.name, func(t *testing.T) {
			b := array.NewDurationBuilder(pool, &arrow.DurationType{Unit: tc.unit})
			defer b.Release()
			b.Append(arrow.Duration(tc.v))
			arr := b.NewArray()
			defer arr.Release()
			v, err := ScanCell(arr, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			if v.(string) != tc.want {
				t.Errorf("got %q, want %q", v, tc.want)
			}
		})
	}

	yearMonth := []struct {
		name   string
		months int32
		want   string
	}{
		{"two_years", 24, "2-0"},
		{"year_and_month", 13, "1-1"},
		{"months_only", 5, "0-5"},
		{"negative", -13, "-1-1"},
		// math.MinInt32 months is a representable negative bound. Negating it as an
		// int32 (`months = -months`) overflows and wraps back negative, doubly-
		// negating into garbage; widening to int64 before negating renders it right.
		{"min_int32", -2147483648, "-178956970-8"},
	}
	for _, tc := range yearMonth {
		t.Run("yearmonth_"+tc.name, func(t *testing.T) {
			b := array.NewMonthIntervalBuilder(pool)
			defer b.Release()
			b.Append(arrow.MonthInterval(tc.months))
			arr := b.NewArray()
			defer arr.Release()
			v, err := ScanCell(arr, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			if v.(string) != tc.want {
				t.Errorf("got %q, want %q", v, tc.want)
			}
		})
	}
}

// ScanCell renders DATE / TIMESTAMP in the requested location, matching the
// Thrift path's .In(location); a nil location leaves the value in UTC.
func TestScanCellTimestampLocation(t *testing.T) {
	pool := memory.NewGoAllocator()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}

	// 2026-07-09T12:00:00Z as microseconds since epoch.
	utcTS := time.Date(2026, time.July, 9, 12, 0, 0, 0, time.UTC)
	b := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer b.Release()
	b.Append(arrow.Timestamp(utcTS.UnixMicro()))
	arr := b.NewArray()
	defer arr.Release()

	t.Run("location applied", func(t *testing.T) {
		v, err := ScanCell(arr, 0, loc)
		if err != nil {
			t.Fatal(err)
		}
		got := v.(time.Time)
		if got.Location() != loc {
			t.Errorf("location = %v, want %v", got.Location(), loc)
		}
		if !got.Equal(utcTS) {
			t.Errorf("instant changed: got %v, want %v", got, utcTS)
		}
	})

	t.Run("nil location is UTC", func(t *testing.T) {
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(time.Time).Location() != time.UTC {
			t.Errorf("nil location should render UTC, got %v", v.(time.Time).Location())
		}
	})
}

// ScanCell honors the TIMESTAMP column's declared arrow unit (not a hardcoded
// microsecond assumption), so the same wall-clock instant is rendered whether the
// unit is s / ms / µs / ns. The cross-backend parity test can't cover this (it
// pins both sides to µs, matching the Databricks wire reality), so the unit
// arithmetic is verified here directly.
func TestScanCellTimestampUnits(t *testing.T) {
	pool := memory.NewGoAllocator()
	// One fixed instant, expressed in each unit's ticks-since-epoch.
	inst := time.Date(2026, time.July, 9, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		unit  arrow.TimeUnit
		ticks int64
	}{
		{arrow.Second, inst.Unix()},
		{arrow.Millisecond, inst.UnixMilli()},
		{arrow.Microsecond, inst.UnixMicro()},
		{arrow.Nanosecond, inst.UnixNano()},
	}
	for _, tc := range cases {
		t.Run(tc.unit.String(), func(t *testing.T) {
			b := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: tc.unit})
			defer b.Release()
			b.Append(arrow.Timestamp(tc.ticks))
			arr := b.NewArray()
			defer arr.Release()

			v, err := ScanCell(arr, 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := v.(time.Time); !got.Equal(inst) {
				t.Errorf("unit %s: got %v, want %v", tc.unit, got.UTC(), inst)
			}
		})
	}
}

// ScanCell renders nested types (list/struct/map) to a JSON string matching the
// Thrift path.
func TestScanCellNested(t *testing.T) {
	pool := memory.NewGoAllocator()

	t.Run("list", func(t *testing.T) {
		b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int64Builder)
		b.Append(true)
		vb.Append(1)
		vb.Append(2)
		vb.Append(3)
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != "[1,2,3]" {
			t.Errorf("got %q, want [1,2,3]", v)
		}
	})

	t.Run("struct", func(t *testing.T) {
		dt := arrow.StructOf(
			arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
		)
		b := array.NewStructBuilder(pool, dt)
		defer b.Release()
		b.Append(true)
		b.FieldBuilder(0).(*array.Int64Builder).Append(1)
		b.FieldBuilder(1).(*array.StringBuilder).Append("x")
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != `{"a":1,"b":"x"}` {
			t.Errorf("got %q, want {\"a\":1,\"b\":\"x\"}", v)
		}
	})

	t.Run("map", func(t *testing.T) {
		b := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
		defer b.Release()
		kb := b.KeyBuilder().(*array.StringBuilder)
		ib := b.ItemBuilder().(*array.Int64Builder)
		b.Append(true)
		kb.Append("k")
		ib.Append(9)
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != `{"k":9}` {
			t.Errorf("got %q, want {\"k\":9}", v)
		}
	})

	t.Run("nested_decimal_exact", func(t *testing.T) {
		// A decimal inside a struct must render as an exact JSON number, not a
		// lossy float64 (19.99, not 19.990000000000002) — matching Thrift's
		// marshalScalar.
		dt := arrow.StructOf(arrow.Field{Name: "d", Type: &arrow.Decimal128Type{Precision: 5, Scale: 2}})
		b := array.NewStructBuilder(pool, dt)
		defer b.Release()
		b.Append(true)
		b.FieldBuilder(0).(*array.Decimal128Builder).Append(decimal128.FromU64(1999))
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != `{"d":19.99}` {
			t.Errorf("got %q, want {\"d\":19.99}", v)
		}
	})

	t.Run("nested_float32_exact", func(t *testing.T) {
		// A float32 inside a struct must marshal as the native float32 (3.14), not
		// a widened float64 (3.140000104904175) — matching Thrift's nested path,
		// which marshals the native float32.
		dt := arrow.StructOf(arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Float32})
		b := array.NewStructBuilder(pool, dt)
		defer b.Release()
		b.Append(true)
		b.FieldBuilder(0).(*array.Float32Builder).Append(3.14)
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != `{"f":3.14}` {
			t.Errorf("got %q, want {\"f\":3.14}", v)
		}
	})

	t.Run("nested_null", func(t *testing.T) {
		b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("null list should scan to nil, got %v", v)
		}
	})
}

// A StructKeyCache must produce byte-identical output to the nil (recompute-inline)
// path, and reused across rows of the same struct type — so the round-trip
// through the cache can never silently diverge from the one-shot rendering.
func TestScanCellCachedMatchesUncached(t *testing.T) {
	pool := memory.NewGoAllocator()
	dt := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: `q"x`, Type: arrow.BinaryTypes.String}, // needs escaping
	)
	b := array.NewStructBuilder(pool, dt)
	defer b.Release()
	for i := 0; i < 3; i++ { // multiple rows: exercises the memoized second+ hit
		b.Append(true)
		b.FieldBuilder(0).(*array.Int64Builder).Append(int64(i))
		b.FieldBuilder(1).(*array.StringBuilder).Append("v")
	}
	arr := b.NewArray()
	defer arr.Release()

	cache := NewStructKeyCache()
	for row := 0; row < 3; row++ {
		uncached, err := ScanCell(arr, row, nil)
		if err != nil {
			t.Fatal(err)
		}
		cached, err := ScanCellCached(arr, row, nil, cache)
		if err != nil {
			t.Fatal(err)
		}
		if cached != uncached {
			t.Errorf("row %d: cached %q != uncached %q", row, cached, uncached)
		}
		// Pin the escaped output to an independent expected value, not just
		// cached==uncached: a wrong-but-consistent escaping rule would satisfy the
		// equality check but fail here. The `q"x` field name must be JSON-escaped
		// to q\"x in the key.
		want := fmt.Sprintf(`{"a":%d,"q\"x":"v"}`, row)
		if cached != want {
			t.Errorf("row %d: got %q, want %q", row, cached, want)
		}
	}
}

// StructKeyCache.Reset drops memoized entries (callers scope the cache to one
// batch this way) and stays correct afterward; Reset on a nil cache is a no-op.
func TestStructKeyCacheReset(t *testing.T) {
	pool := memory.NewGoAllocator()
	dt := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64})
	b := array.NewStructBuilder(pool, dt)
	defer b.Release()
	b.Append(true)
	b.FieldBuilder(0).(*array.Int64Builder).Append(7)
	arr := b.NewArray()
	defer arr.Release()

	cache := NewStructKeyCache()
	first, err := ScanCellCached(arr, 0, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	if len(cache.m) == 0 {
		t.Fatal("expected the cache to memoize the struct type")
	}
	cache.Reset()
	if len(cache.m) != 0 {
		t.Errorf("Reset should empty the cache, got %d entries", len(cache.m))
	}
	// Rendering after Reset must still be correct (re-memoizes on demand).
	second, err := ScanCellCached(arr, 0, nil, cache)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Errorf("render after Reset diverged: %q != %q", first, second)
	}

	var nilCache *StructKeyCache
	nilCache.Reset() // must not panic
}

// LargeList and FixedSizeList are handled by arrowscan but NOT by the Thrift
// arrowbased renderer, so the cross-backend parity test can't reach them; their
// offset arithmetic (hand-rolled, distinct from List's ValueOffsets — arrow-go v12
// LargeList.ValueOffsets does not add the logical offset, and FixedSizeList has no
// ValueOffsets at all) is verified here directly, including on a sliced array where
// a non-zero logical offset would expose an off-by-offset bug.
func TestScanCellLargeAndFixedSizeList(t *testing.T) {
	pool := memory.NewGoAllocator()

	t.Run("large list", func(t *testing.T) {
		b := array.NewLargeListBuilder(pool, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int64Builder)
		for r := 0; r < 3; r++ { // 3 rows so a slice can drop the head
			b.Append(true)
			vb.Append(int64(r * 10))
			vb.Append(int64(r*10 + 1))
		}
		full := b.NewArray()
		defer full.Release()
		sliced := array.NewSlice(full, 1, 3) // logical offset != 0
		defer sliced.Release()

		// row 0 of the slice is row 1 of full → [10,11]
		got, err := ScanCell(sliced, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != "[10,11]" {
			t.Errorf("sliced large-list row 0 = %q, want [10,11]", got)
		}
	})

	t.Run("fixed-size list", func(t *testing.T) {
		b := array.NewFixedSizeListBuilder(pool, 2, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		vb := b.ValueBuilder().(*array.Int64Builder)
		for r := 0; r < 3; r++ {
			b.Append(true)
			vb.Append(int64(r * 10))
			vb.Append(int64(r*10 + 1))
		}
		full := b.NewArray()
		defer full.Release()
		sliced := array.NewSlice(full, 1, 3) // offset != 0 → base = (row+offset)*n
		defer sliced.Release()

		got, err := ScanCell(sliced, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != "[10,11]" {
			t.Errorf("sliced fixed-size-list row 0 = %q, want [10,11]", got)
		}
	})
}

// Empty-but-non-null collections must render as [] / {} (not null): the row is
// present, the collection just has zero elements. A nil/absent parent is a
// separate case (covered by the null-leaf tests).
func TestScanCellEmptyCollections(t *testing.T) {
	pool := memory.NewGoAllocator()

	t.Run("empty list", func(t *testing.T) {
		b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		b.Append(true) // present row, no ValueBuilder appends → empty
		arr := b.NewArray()
		defer arr.Release()
		got, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != "[]" {
			t.Errorf("empty list = %q, want []", got)
		}
	})

	t.Run("empty map", func(t *testing.T) {
		b := array.NewMapBuilder(pool, arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, false)
		defer b.Release()
		b.Append(true) // present row, no entries → empty
		arr := b.NewArray()
		defer arr.Release()
		got, err := ScanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got != "{}" {
			t.Errorf("empty map = %q, want {}", got)
		}
	})
}

// DECIMAL rendering at the len(digits) == scale boundary: a value whose digit
// count equals the scale must render as "0.<digits>" (leading zero + full
// fractional part), not drop the integer-part zero or misplace the point.
func TestScanCellDecimalScaleBoundary(t *testing.T) {
	pool := memory.NewGoAllocator()
	// scale 2, value 0.05 → unscaled 5 (1 digit, scale 2): integer part is 0.
	dt := &arrow.Decimal128Type{Precision: 5, Scale: 2}
	b := array.NewDecimal128Builder(pool, dt)
	defer b.Release()
	b.Append(decimal128.FromU64(5))
	arr := b.NewArray()
	defer arr.Release()
	got, err := ScanCell(arr, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "0.05" {
		t.Errorf("decimal 5 @ scale 2 = %q, want 0.05", got)
	}
}
