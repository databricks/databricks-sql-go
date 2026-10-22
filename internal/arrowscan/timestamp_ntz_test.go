package arrowscan

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// TestScanCellTimestampNTZPreservesWallClock pins the value contract for
// TIMESTAMP_NTZ: a zone-less timestamp (empty TimestampType.TimeZone) is a wall
// clock with no zone, so it must be returned with the exact wall-clock fields it
// was given, WITHOUT the session-timezone .In(loc) conversion that is correct for
// a zoned TIMESTAMP instant. Applying the location shift to an NTZ value (the
// pre-#506 behavior) corrupts the wall clock.
//
// External authority: Databricks TIMESTAMP_NTZ
// (https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type)
// "does not ... apply a time zone"; issue #506 requires the wall clock be kept.
func TestScanCellTimestampNTZPreservesWallClock(t *testing.T) {
	pool := memory.NewGoAllocator()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}

	// The wall clock the TIMESTAMP_NTZ literal carries. On the wire the server
	// encodes it as microseconds-since-epoch interpreted as if UTC.
	wall := time.Date(2024, time.June, 15, 9, 15, 0, 123456000, time.UTC)

	build := func(tz string) arrow.Array {
		b := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: tz})
		b.Append(arrow.Timestamp(wall.UnixMicro()))
		return b.NewArray()
	}

	t.Run("ntz empty zone keeps wall clock", func(t *testing.T) {
		arr := build("")
		defer arr.Release()
		v, err := ScanCell(arr, 0, loc)
		if err != nil {
			t.Fatal(err)
		}
		got := v.(time.Time)
		if got.Year() != 2024 || got.Month() != time.June || got.Day() != 15 ||
			got.Hour() != 9 || got.Minute() != 15 || got.Second() != 0 || got.Nanosecond() != 123456000 {
			t.Errorf("NTZ wall clock not preserved: got %v, want wall 2024-06-15 09:15:00.123456", got)
		}
		if got.Location() != time.UTC {
			t.Errorf("NTZ location = %q, want UTC", got.Location())
		}
		if !got.Equal(wall) {
			t.Errorf("NTZ instant changed: got %v, want %v", got.UTC(), wall)
		}
	})

	t.Run("zoned keeps instant via location", func(t *testing.T) {
		arr := build("UTC")
		defer arr.Release()
		v, err := ScanCell(arr, 0, loc)
		if err != nil {
			t.Fatal(err)
		}
		got := v.(time.Time)
		// A zoned TIMESTAMP is an instant: rendered in loc, it is the same
		// instant as the UTC wall value.
		if !got.Equal(wall) {
			t.Errorf("zoned instant changed: got %v, want %v", got.UTC(), wall)
		}
	})
}
