package arrowbased

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// TestThriftTimestampNTZPreservesWallClock pins the Thrift (arrowbased) backend's
// value contract for TIMESTAMP_NTZ: a zone-less timestamp (empty
// TimestampType.TimeZone) is a wall clock and must be returned with the exact
// wall-clock fields it was given, WITHOUT the session-tz .In(location) shift that
// is correct only for a zoned TIMESTAMP instant.
//
// External authority: Databricks TIMESTAMP_NTZ
// (https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type)
// and issue #506.
func TestThriftTimestampNTZPreservesWallClock(t *testing.T) {
	pool := memory.NewGoAllocator()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}

	wall := time.Date(2024, time.June, 15, 9, 15, 0, 123456000, time.UTC)
	build := func(tz string) arrow.Array {
		b := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tz})
		b.Append(arrow.Timestamp(wall.UnixMicro()))
		return b.NewArray()
	}

	t.Run("ntz empty zone keeps wall clock", func(t *testing.T) {
		arr := build("")
		defer arr.Release()
		got := renderViaArrowbased(t, arr, 0, loc).(time.Time)
		if got.Year() != 2024 || got.Month() != time.June || got.Day() != 15 ||
			got.Hour() != 9 || got.Minute() != 15 || got.Second() != 0 || got.Nanosecond() != 123456000 {
			t.Errorf("NTZ wall clock not preserved: got %v, want wall 2024-06-15 09:15:00.123456", got)
		}
	})

	t.Run("zoned keeps instant via location", func(t *testing.T) {
		arr := build("UTC")
		defer arr.Release()
		got := renderViaArrowbased(t, arr, 0, loc).(time.Time)
		if !got.Equal(wall) {
			t.Errorf("zoned instant changed: got %v, want %v", got.UTC(), wall)
		}
	})
}

// TestArrowRowScannerDatabaseTypeNameOverride pins the Thrift-path type-name
// refinement (#506): only a zone-less Arrow timestamp column is reported as
// TIMESTAMP_NTZ; zoned timestamps, non-timestamp columns, out-of-range indexes
// and a missing schema all fall through (ok=false) to the Thrift-derived name.
func TestArrowRowScannerDatabaseTypeNameOverride(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "ts_ntz", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "i", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	ars := &arrowRowScanner{arrowSchema: schema}

	cases := []struct {
		index    int
		wantName string
		wantOK   bool
	}{
		{0, "", false},
		{1, "TIMESTAMP_NTZ", true},
		{2, "", false},
		{-1, "", false},
		{3, "", false},
	}
	for _, c := range cases {
		name, ok := ars.DatabaseTypeNameOverride(c.index)
		if name != c.wantName || ok != c.wantOK {
			t.Errorf("index %d: got (%q, %v), want (%q, %v)", c.index, name, ok, c.wantName, c.wantOK)
		}
	}

	if name, ok := (&arrowRowScanner{}).DatabaseTypeNameOverride(0); name != "" || ok {
		t.Errorf("nil schema: got (%q, %v), want (\"\", false)", name, ok)
	}
	var nilScanner *arrowRowScanner
	if name, ok := nilScanner.DatabaseTypeNameOverride(0); name != "" || ok {
		t.Errorf("nil scanner: got (%q, %v), want (\"\", false)", name, ok)
	}
}
