package arrowbased

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTime64Array builds a single-element arrow Time64 array in the given unit
// holding the provided raw value (value since midnight in that unit).
func buildTime64Array(t *testing.T, unit arrow.TimeUnit, v arrow.Time64) arrow.ArrayData {
	t.Helper()
	dt := &arrow.Time64Type{Unit: unit}
	b := array.NewTime64Builder(memory.DefaultAllocator, dt)
	defer b.Release()
	b.Append(v)
	arr := b.NewArray()
	defer arr.Release()
	arr.Data().Retain()
	return arr.Data()
}

// buildTime32Array builds a single-element arrow Time32 array in the given unit.
func buildTime32Array(t *testing.T, unit arrow.TimeUnit, v arrow.Time32) arrow.ArrayData {
	t.Helper()
	dt := &arrow.Time32Type{Unit: unit}
	b := array.NewTime32Builder(memory.DefaultAllocator, dt)
	defer b.Release()
	b.Append(v)
	arr := b.NewArray()
	defer arr.Release()
	arr.Data().Retain()
	return arr.Data()
}

// TestTime64ValueContainer guards the time64 -> time-of-day conversion for both
// nanosecond and microsecond Arrow units. A TIME '09:15:00.123456' arrives as a
// time64 value counting units since midnight; the scanned value must be a
// wall-clock time-of-day (Hour/Minute/Second/Nanosecond) in UTC, anchored on the
// epoch date (no meaningful calendar date, no location shift).
func TestTime64ValueContainer(t *testing.T) {
	vcm := &arrowValueContainerMaker{}

	cases := []struct {
		name   string
		unit   arrow.TimeUnit
		raw    arrow.Time64
		wantNs int
	}{
		{
			name: "nanosecond",
			unit: arrow.Nanosecond,
			// 09:15:00.123456 -> (9*3600+15*60)*1e9 + 123456000 ns since midnight
			raw:    arrow.Time64((9*3600+15*60)*int64(time.Second) + 123456000),
			wantNs: 123456000,
		},
		{
			name: "microsecond",
			unit: arrow.Microsecond,
			// 09:15:00.123456 -> (9*3600+15*60)*1e6 + 123456 us since midnight
			raw:    arrow.Time64((9*3600+15*60)*int64(time.Second/time.Microsecond) + 123456),
			wantNs: 123456000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := &arrow.Time64Type{Unit: tc.unit}
			holder, err := vcm.makeColumnValueContainer(dt, time.UTC, nil, nil)
			require.NoError(t, err)

			data := buildTime64Array(t, tc.unit, tc.raw)
			defer data.Release()
			require.NoError(t, holder.SetValueArray(data))

			v, err := holder.Value(0)
			require.NoError(t, err)

			tv, ok := v.(time.Time)
			require.True(t, ok, "expected time.Time, got %T", v)

			assert.Equal(t, 9, tv.Hour())
			assert.Equal(t, 15, tv.Minute())
			assert.Equal(t, 0, tv.Second())
			assert.Equal(t, tc.wantNs, tv.Nanosecond())
			assert.Equal(t, time.UTC, tv.Location())
		})
	}
}

// TestTime32ValueContainer is the defensive edge: a TIME carried as Arrow time32
// (second / millisecond units) must also scan as a wall-clock time-of-day rather
// than hitting the unhandled-type error.
func TestTime32ValueContainer(t *testing.T) {
	vcm := &arrowValueContainerMaker{}

	cases := []struct {
		name   string
		unit   arrow.TimeUnit
		raw    arrow.Time32
		wantNs int
	}{
		{
			name:   "second",
			unit:   arrow.Second,
			raw:    arrow.Time32(9*3600 + 15*60),
			wantNs: 0,
		},
		{
			name:   "millisecond",
			unit:   arrow.Millisecond,
			raw:    arrow.Time32((9*3600+15*60)*1000 + 123),
			wantNs: 123000000,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := &arrow.Time32Type{Unit: tc.unit}
			holder, err := vcm.makeColumnValueContainer(dt, time.UTC, nil, nil)
			require.NoError(t, err)

			data := buildTime32Array(t, tc.unit, tc.raw)
			defer data.Release()
			require.NoError(t, holder.SetValueArray(data))

			v, err := holder.Value(0)
			require.NoError(t, err)

			tv, ok := v.(time.Time)
			require.True(t, ok, "expected time.Time, got %T", v)

			assert.Equal(t, 9, tv.Hour())
			assert.Equal(t, 15, tv.Minute())
			assert.Equal(t, 0, tv.Second())
			assert.Equal(t, tc.wantNs, tv.Nanosecond())
			assert.Equal(t, time.UTC, tv.Location())
		})
	}
}
