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
		{Name: "geometry Thrift", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			sparkSQLNameMetadataKey: "GEOMETRY(3857)",
		})},
		{Name: "geography kernel", Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			databricksTypeNameMetadataKey: "GEOGRAPHY", "databricks.type_text": "GEOGRAPHY(4326)",
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

func BenchmarkColumnTypeInfoForGeoFreeSchema(benchmark *testing.B) {
	templates := []arrow.Field{
		{Type: arrow.BinaryTypes.String, Metadata: arrow.MetadataFrom(map[string]string{
			databricksTypeNameMetadataKey: "STRING", "databricks.type_text": "STRING", sparkSQLNameMetadataKey: "STRING",
		})},
		{Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.MetadataFrom(map[string]string{
			databricksTypeNameMetadataKey: "BIGINT", "databricks.type_text": "BIGINT", sparkSQLNameMetadataKey: "BIGINT",
		})},
		{Type: arrow.StructOf(arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int32}), Metadata: arrow.MetadataFrom(map[string]string{
			databricksTypeNameMetadataKey: "STRUCT", "databricks.type_text": "STRUCT<value: INT>", sparkSQLNameMetadataKey: "STRUCT<value: INT>",
		})},
		{Type: arrow.ListOf(arrow.BinaryTypes.String), Metadata: arrow.MetadataFrom(map[string]string{
			databricksTypeNameMetadataKey: "ARRAY", "databricks.type_text": "ARRAY<STRING>", sparkSQLNameMetadataKey: "ARRAY<STRING>",
		})},
	}
	fields := make([]arrow.Field, 0, 400)
	for range 100 {
		fields = append(fields, templates...)
	}
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for range benchmark.N {
		for _, field := range fields {
			info := ColumnTypeInfoForField(field)
			if info.ScanType == nil || IsBinaryGeospatialField(field) {
				benchmark.Fatal("invalid non-geospatial field")
			}
		}
	}
}
