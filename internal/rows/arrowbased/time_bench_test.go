package arrowbased

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

func BenchmarkScanExistingColumns(b *testing.B) {
	for _, test := range []struct {
		name     string
		dataType arrow.DataType
		value    string
	}{
		{"int64", arrow.PrimitiveTypes.Int64, "123456789"},
		{"string", arrow.BinaryTypes.String, "existing column"},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us, "2026-11-14 09:15:00.123456Z"},
	} {
		builder := array.NewBuilder(memory.DefaultAllocator, test.dataType)
		if err := builder.AppendValueFromString(test.value); err != nil {
			b.Fatal(err)
		}
		values := builder.NewArray()
		builder.Release()
		b.Run(test.name+"/thrift", func(b *testing.B) {
			holder, err := (&arrowValueContainerMaker{}).makeColumnValueContainer(test.dataType, time.UTC, func(timestamp arrow.Timestamp) time.Time {
				return timestamp.ToTime(arrow.Microsecond)
			}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if err := holder.SetValueArray(values.Data()); err != nil {
				b.Fatal(err)
			}
			defer holder.Release()
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := holder.Value(0); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(test.name+"/kernel", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := arrowscan.ScanCell(values, 0, time.UTC); err != nil {
					b.Fatal(err)
				}
			}
		})
		values.Release()
	}
}
