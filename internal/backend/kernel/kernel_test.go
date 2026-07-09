//go:build cgo && databricks_kernel

package kernel

import (
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// isBadConnection maps the session-unusable status codes so the pool evicts the
// conn; every other code stays a plain kernel error.
func TestIsBadConnection(t *testing.T) {
	bad := []int{statusUnauthenticated, statusUnavailable, statusNetworkError}
	for _, code := range bad {
		if !isBadConnection(code) {
			t.Errorf("code %d should be a bad connection", code)
		}
	}
	notBad := []int{statusInvalidArgument, statusSqlError, statusTimeout}
	for _, code := range notBad {
		if isBadConnection(code) {
			t.Errorf("code %d should not be a bad connection", code)
		}
	}
}

// toDriverError wraps a session-unusable KernelError as driver.ErrBadConn (so
// database/sql evicts the conn) and leaves other errors, and their sqlstate,
// intact.
func TestToDriverError(t *testing.T) {
	if toDriverError(nil) != nil {
		t.Fatal("nil should map to nil")
	}

	badConn := &KernelError{Code: statusUnavailable, Message: "gone"}
	if !errors.Is(toDriverError(badConn), driver.ErrBadConn) {
		t.Errorf("unavailable kernel error should identify as driver.ErrBadConn")
	}

	sqlErr := &KernelError{Code: statusSqlError, Message: "boom", SQLState: "42703"}
	got := toDriverError(sqlErr)
	ke, ok := got.(*KernelError)
	if !ok {
		t.Fatalf("sql error should remain a *KernelError, got %T", got)
	}
	if ke.SQLState != "42703" {
		t.Errorf("sqlstate lost: got %q", ke.SQLState)
	}
}

// scanCell renders the supported scalar types and rejects an unsupported type
// (rather than returning a silently wrong value).
func TestScanCellScalars(t *testing.T) {
	pool := memory.NewGoAllocator()

	t.Run("int64", func(t *testing.T) {
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.Append(42)
		arr := b.NewArray()
		defer arr.Release()
		v, err := scanCell(arr, 0, nil)
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
		v, err := scanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != "hi" {
			t.Errorf("got %v", v)
		}
	})

	t.Run("null", func(t *testing.T) {
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()
		v, err := scanCell(arr, 0, nil)
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
		v, err := scanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != "123.45" {
			t.Errorf("got %v, want 123.45", v)
		}
	})

	t.Run("unsupported_type_errors", func(t *testing.T) {
		// A duration (INTERVAL) is not yet handled: must error, not return a
		// wrong value.
		b := array.NewDurationBuilder(pool, &arrow.DurationType{Unit: arrow.Microsecond})
		defer b.Release()
		b.Append(1000)
		arr := b.NewArray()
		defer arr.Release()
		if _, err := scanCell(arr, 0, nil); err == nil {
			t.Error("scanning a Duration should return an unsupported-type error")
		}
	})
}

// scanCell renders DATE / TIMESTAMP in the requested location, matching the
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
		v, err := scanCell(arr, 0, loc)
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
		v, err := scanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(time.Time).Location() != time.UTC {
			t.Errorf("nil location should render UTC, got %v", v.(time.Time).Location())
		}
	})
}

// scanCell renders nested types (list/struct/map) to a JSON string matching the
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
		v, err := scanCell(arr, 0, nil)
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
		v, err := scanCell(arr, 0, nil)
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
		v, err := scanCell(arr, 0, nil)
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
		// marshalScalar. Regression guard for a bug the POC missed by only testing
		// float64-exact values.
		dt := arrow.StructOf(arrow.Field{Name: "d", Type: &arrow.Decimal128Type{Precision: 5, Scale: 2}})
		b := array.NewStructBuilder(pool, dt)
		defer b.Release()
		b.Append(true)
		b.FieldBuilder(0).(*array.Decimal128Builder).Append(decimal128.FromU64(1999))
		arr := b.NewArray()
		defer arr.Release()
		v, err := scanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != `{"d":19.99}` {
			t.Errorf("got %q, want {\"d\":19.99}", v)
		}
	})

	t.Run("nested_null", func(t *testing.T) {
		b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		b.AppendNull()
		arr := b.NewArray()
		defer arr.Release()
		v, err := scanCell(arr, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if v != nil {
			t.Errorf("null list should scan to nil, got %v", v)
		}
	})
}

// decimal128ToExactString applies scale by string placement, preserving digits a
// float64 would lose.
func TestDecimal128ToExactString(t *testing.T) {
	cases := []struct {
		unscaled uint64
		scale    int32
		want     string
	}{
		{12345, 2, "123.45"},
		{5, 3, "0.005"},
		{100, 0, "100"},
	}
	for _, c := range cases {
		got := decimal128ToExactString(decimal128.FromU64(c.unscaled), c.scale)
		if got != c.want {
			t.Errorf("unscaled=%d scale=%d: got %q want %q", c.unscaled, c.scale, got, c.want)
		}
	}
}

var _ driver.Value // keep database/sql/driver imported for the scanCell signature
