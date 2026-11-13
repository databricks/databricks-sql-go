package arrowscan

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type overwriteOnFreeAllocator struct{ memory.Allocator }

func (a overwriteOnFreeAllocator) Free(b []byte) {
	clear(b)
	a.Allocator.Free(b)
}

func TestScannedStringOutlivesArrowArray(t *testing.T) {
	const want = "SRID=4326;POINT(1 2)"
	for _, dt := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString} {
		t.Run(dt.Name(), func(t *testing.T) {
			pool := overwriteOnFreeAllocator{memory.NewGoAllocator()}
			builder := array.NewBuilder(pool, dt)
			if err := builder.AppendValueFromString(want); err != nil {
				builder.Release()
				t.Fatal(err)
			}
			values := builder.NewArray()
			builder.Release()

			got, err := ScanCell(values, 0, nil)
			values.Release()
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("scanned string changed after releasing its Arrow array: got %q, want %q", got, want)
			}
		})
	}
}
