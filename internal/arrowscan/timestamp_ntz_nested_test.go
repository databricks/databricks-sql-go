package arrowscan

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// TestScanCellNestedKernelNTZKeepsWallClock pins that a nested child the kernel
// labels TIMESTAMP_NTZ via field metadata — while giving it a physical
// TimeZone "UTC" — renders its wall clock unshifted, exactly like a top-level
// NTZ column, whereas a zoned TIMESTAMP sibling is still shifted into loc.
func TestScanCellNestedKernelNTZKeepsWallClock(t *testing.T) {
	pool := memory.NewGoAllocator()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}
	wall := time.Date(2024, time.June, 15, 9, 15, 0, 123456000, time.UTC)
	tsUTC := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	ntzMeta := arrow.NewMetadata([]string{databricksTypeNameMetadataKey}, []string{"TIMESTAMP_NTZ"})
	ntzField := arrow.Field{Name: "n", Type: tsUTC, Nullable: true, Metadata: ntzMeta}
	zonedField := arrow.Field{Name: "z", Type: tsUTC, Nullable: true}

	const ntzStr = `"2024-06-15 09:15:00.123456 +0000 UTC"`
	const zonedStr = `"2024-06-15 05:15:00.123456 -0400 EDT"`

	t.Run("struct", func(t *testing.T) {
		b := array.NewStructBuilder(pool, arrow.StructOf(ntzField, zonedField))
		defer b.Release()
		b.Append(true)
		b.FieldBuilder(0).(*array.TimestampBuilder).Append(arrow.Timestamp(wall.UnixMicro()))
		b.FieldBuilder(1).(*array.TimestampBuilder).Append(arrow.Timestamp(wall.UnixMicro()))
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCellCached(arr, 0, loc, NewStructKeyCache())
		if err != nil {
			t.Fatal(err)
		}
		want := `{"n":` + ntzStr + `,"z":` + zonedStr + `}`
		if v != want {
			t.Errorf("got %v, want %v", v, want)
		}
	})

	t.Run("list", func(t *testing.T) {
		b := array.NewBuilder(pool, arrow.ListOfField(arrow.Field{Name: "item", Type: tsUTC, Nullable: true, Metadata: ntzMeta})).(*array.ListBuilder)
		defer b.Release()
		b.Append(true)
		b.ValueBuilder().(*array.TimestampBuilder).Append(arrow.Timestamp(wall.UnixMicro()))
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCellCached(arr, 0, loc, NewStructKeyCache())
		if err != nil {
			t.Fatal(err)
		}
		if want := `[` + ntzStr + `]`; v != want {
			t.Errorf("got %v, want %v", v, want)
		}
	})

	t.Run("list without NTZ metadata still shifts", func(t *testing.T) {
		b := array.NewListBuilder(pool, tsUTC)
		defer b.Release()
		b.Append(true)
		b.ValueBuilder().(*array.TimestampBuilder).Append(arrow.Timestamp(wall.UnixMicro()))
		arr := b.NewArray()
		defer arr.Release()
		v, err := ScanCellCached(arr, 0, loc, NewStructKeyCache())
		if err != nil {
			t.Fatal(err)
		}
		if want := `[` + zonedStr + `]`; v != want {
			t.Errorf("got %v, want %v", v, want)
		}
	})
}
