package arrowscan

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func BenchmarkColumnTypeInfoForField(benchmark *testing.B) {
	for _, field := range []arrow.Field{
		{Name: "string", Type: arrow.BinaryTypes.String},
		{Name: "int64", Type: arrow.PrimitiveTypes.Int64},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "TIME manifest", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			databricksTypeNameMetadataKey: "TIME", sparkSQLNameMetadataKey: "TIME(6)",
		})},
	} {
		benchmark.Run(field.Name, func(benchmark *testing.B) {
			benchmark.ReportAllocs()
			var info ColumnTypeInfo
			for iteration := 0; iteration < benchmark.N; iteration++ {
				info = ColumnTypeInfoForField(field)
			}
			if info.ScanType == nil {
				benchmark.Fatal("missing scan type")
			}
		})
	}
}
