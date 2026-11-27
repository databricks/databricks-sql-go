package arrowbased

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/v2/logger"
)

func BenchmarkFactoryCells(benchmark *testing.B) {
	for _, shape := range []string{"catalog", "int32", "string", "bool", "float32", "float64"} {
		for _, nullPercent := range []int{0, 5} {
			benchmark.Run(fmt.Sprintf("%s/null%d", shape, nullPercent), func(benchmark *testing.B) {
				const rowCount = 2900
				columnCount := 1
				if shape == "catalog" {
					columnCount = 34
				}
				fields := make([]arrow.Field, columnCount)
				columns := make([]arrow.Array, columnCount)
				infos := make([]colInfo, columnCount)
				for column := range columns {
					dataType := arrow.DataType(arrow.PrimitiveTypes.Int32)
					dbType := cli_service.TTypeId_INT_TYPE
					value := "1234567"
					switch {
					case shape == "string" || shape == "catalog" && column >= 20:
						dataType, dbType, value = arrow.BinaryTypes.String, cli_service.TTypeId_DECIMAL_TYPE, "12345.67"
					case shape == "catalog" && column >= 18:
						dataType, dbType = arrow.PrimitiveTypes.Int64, cli_service.TTypeId_BIGINT_TYPE
					case shape == "bool":
						dataType, dbType, value = arrow.FixedWidthTypes.Boolean, cli_service.TTypeId_BOOLEAN_TYPE, "true"
					case shape == "float32":
						dataType, dbType, value = arrow.PrimitiveTypes.Float32, cli_service.TTypeId_FLOAT_TYPE, "123.25"
					case shape == "float64":
						dataType, dbType, value = arrow.PrimitiveTypes.Float64, cli_service.TTypeId_DOUBLE_TYPE, "123.25"
					}
					builder := array.NewBuilder(memory.DefaultAllocator, dataType)
					for row := 0; row < rowCount; row++ {
						if nullPercent > 0 && (row*17+column*31)%100 < nullPercent {
							builder.AppendNull()
						} else if err := builder.AppendValueFromString(value); err != nil {
							benchmark.Fatal(err)
						}
					}
					columns[column] = builder.NewArray()
					builder.Release()
					defer columns[column].Release()
					fields[column] = arrow.Field{Name: fmt.Sprint(column), Type: dataType, Nullable: true}
					infos[column] = colInfo{name: fields[column].Name, arrowType: dataType, dbType: dbType}
				}
				schema := arrow.NewSchema(fields, nil)
				delimiter := rowscanner.NewDelimiter(0, rowCount)
				record := &sparkArrowRecord{Delimiter: delimiter, RecordBatch: array.NewRecordBatch(schema, columns, rowCount)}
				scanner := &arrowRowScanner{
					Delimiter: delimiter, valueContainerMaker: &arrowValueContainerMaker{},
					arrowSchema: schema, colInfo: infos, location: time.UTC,
					DBSQLLogger: dbsqllog.Logger, ctx: context.Background(),
					batchIterator: &fakeBatchIterator{index: -1, batches: []SparkArrowBatch{
						&sparkArrowBatch{Delimiter: delimiter, arrowRecords: []SparkArrowRecord{record}},
					}},
				}
				defer scanner.Close()
				destination := make([]driver.Value, columnCount)
				if err := scanner.ScanRow(destination, 0); err != nil {
					benchmark.Fatal(err)
				}
				benchmark.ReportAllocs()
				benchmark.ResetTimer()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					for row := int64(0); row < rowCount; row++ {
						if err := scanner.ScanRow(destination, row); err != nil {
							benchmark.Fatal(err)
						}
					}
				}
				benchmark.ReportMetric(float64(benchmark.Elapsed().Nanoseconds())/float64(benchmark.N*rowCount*columnCount), "ns/cell")
			})
		}
	}
}
