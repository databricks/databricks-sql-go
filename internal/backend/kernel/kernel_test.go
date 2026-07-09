//go:build cgo && databricks_kernel

package kernel

import (
	"database/sql/driver"
	"errors"
	"testing"

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
		v, err := scanCell(arr, 0)
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
		v, err := scanCell(arr, 0)
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
		v, err := scanCell(arr, 0)
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
		v, err := scanCell(arr, 0)
		if err != nil {
			t.Fatal(err)
		}
		if v.(string) != "123.45" {
			t.Errorf("got %v, want 123.45", v)
		}
	})

	t.Run("unsupported_type_errors", func(t *testing.T) {
		// A List is unsupported: must error, not return a wrong value.
		b := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int64)
		defer b.Release()
		b.Append(true)
		b.ValueBuilder().(*array.Int64Builder).Append(1)
		arr := b.NewArray()
		defer arr.Release()
		if _, err := scanCell(arr, 0); err == nil {
			t.Error("scanning a List should return an unsupported-type error")
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
