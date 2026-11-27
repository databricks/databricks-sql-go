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
	"github.com/stretchr/testify/require"

	"github.com/databricks/databricks-sql-go/v2/internal/arrowscan"
	"github.com/databricks/databricks-sql-go/v2/internal/cli_service"
	"github.com/databricks/databricks-sql-go/v2/internal/config"
	"github.com/databricks/databricks-sql-go/v2/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/v2/logger"
)

func readerTestScanner(fields []arrow.Field, types []cli_service.TTypeId, cfg config.ArrowConfig, location *time.Location, batches ...[]arrow.Array) *arrowRowScanner {
	schema := arrow.NewSchema(fields, nil)
	iterator := &fakeBatchIterator{index: -1}
	start := int64(41)
	for _, columns := range batches {
		count := int64(columns[0].Len())
		delimiter := rowscanner.NewDelimiter(start, count)
		iterator.batches = append(iterator.batches, &sparkArrowBatch{
			Delimiter: delimiter,
			arrowRecords: []SparkArrowRecord{&sparkArrowRecord{
				Delimiter: delimiter, RecordBatch: array.NewRecordBatch(schema, columns, count),
			}},
		})
		start += count
	}
	infos := make([]colInfo, len(fields))
	for column := range fields {
		infos[column] = colInfo{name: fields[column].Name, arrowType: fields[column].Type, dbType: types[column]}
	}
	return &arrowRowScanner{
		Delimiter: rowscanner.NewDelimiter(41, start-41), valueContainerMaker: &arrowValueContainerMaker{},
		ArrowConfig: cfg, arrowSchema: schema, colInfo: infos, location: location,
		DBSQLLogger: dbsqllog.Logger, ctx: context.Background(), batchIterator: iterator,
		toTimestampFn: func(value arrow.Timestamp) time.Time { return value.ToTime(arrow.Microsecond) },
	}
}

func TestBoundColumnReaders(t *testing.T) {
	for _, test := range []struct {
		name     string
		dataType arrow.DataType
		dbType   cli_service.TTypeId
		input    string
		expected any
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, cli_service.TTypeId_BOOLEAN_TYPE, "true", true},
		{"int8", arrow.PrimitiveTypes.Int8, cli_service.TTypeId_TINYINT_TYPE, "-97", int8(-97)},
		{"int16", arrow.PrimitiveTypes.Int16, cli_service.TTypeId_SMALLINT_TYPE, "-30258", int16(-30258)},
		{"int32", arrow.PrimitiveTypes.Int32, cli_service.TTypeId_INT_TYPE, "1234567890", int32(1234567890)},
		{"int64", arrow.PrimitiveTypes.Int64, cli_service.TTypeId_BIGINT_TYPE, "9223372036854775807", int64(9223372036854775807)},
		{"float32", arrow.PrimitiveTypes.Float32, cli_service.TTypeId_FLOAT_TYPE, "0.1", float32(0.1)},
		{"float64", arrow.PrimitiveTypes.Float64, cli_service.TTypeId_DOUBLE_TYPE, "0.1", float64(0.1)},
		{"string", arrow.BinaryTypes.String, cli_service.TTypeId_STRING_TYPE, "a\x00é🌎", "a\x00é🌎"},
		{"binary", arrow.BinaryTypes.Binary, cli_service.TTypeId_BINARY_TYPE, "AP+A", []byte{0, 255, 128}},
		{"decimal-string", arrow.BinaryTypes.String, cli_service.TTypeId_DECIMAL_TYPE, "1234567890123456789.9900", "1234567890123456789.9900"},
		{"date", arrow.FixedWidthTypes.Date32, cli_service.TTypeId_DATE_TYPE, "2026-11-27", time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)},
		{"time32s", arrow.FixedWidthTypes.Time32s, cli_service.TTypeId_STRING_TYPE, "09:15:00", time.Date(1, 1, 1, 9, 15, 0, 0, time.UTC)},
		{"time32ms", arrow.FixedWidthTypes.Time32ms, cli_service.TTypeId_STRING_TYPE, "09:15:00.123", time.Date(1, 1, 1, 9, 15, 0, 123000000, time.UTC)},
		{"time64us", arrow.FixedWidthTypes.Time64us, cli_service.TTypeId_STRING_TYPE, "09:15:00.123456", time.Date(1, 1, 1, 9, 15, 0, 123456000, time.UTC)},
		{"time64ns", arrow.FixedWidthTypes.Time64ns, cli_service.TTypeId_STRING_TYPE, "09:15:00.123456789", time.Date(1, 1, 1, 9, 15, 0, 123456789, time.UTC)},
		{"timestamp", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, cli_service.TTypeId_TIMESTAMP_TYPE, "2026-11-27 09:15:00.123456Z", time.Date(2026, 11, 27, 9, 15, 0, 123456000, time.UTC)},
		{"timestamp-string", arrow.BinaryTypes.String, cli_service.TTypeId_TIMESTAMP_TYPE, "2026-11-27 09:15:00.123456", time.Date(2026, 11, 27, 9, 15, 0, 123456000, time.UTC)},
		{"interval-string", arrow.BinaryTypes.String, cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE, "-8 00:00:00.000000000", "-8 00:00:00.000000000"},
	} {
		for _, pattern := range []string{"no-nulls", "mixed", "all-null"} {
			t.Run(test.name+"/"+pattern, func(t *testing.T) {
				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer pool.AssertSize(t, 0)
				builder := array.NewBuilder(pool, test.dataType)
				defer builder.Release()
				builder.AppendNull()
				for row := 0; row < 3; row++ {
					if pattern == "all-null" || pattern == "mixed" && row == 1 {
						builder.AppendNull()
					} else {
						require.NoError(t, builder.AppendValueFromString(test.input))
					}
				}
				full := builder.NewArray()
				defer full.Release()
				values := array.NewSlice(full, 1, 4)
				defer values.Release()
				scanner := readerTestScanner([]arrow.Field{{Name: test.name, Type: test.dataType, Nullable: true}}, []cli_service.TTypeId{test.dbType}, config.ArrowConfig{UseArrowNativeDecimal: true}, time.UTC, []arrow.Array{values})
				defer scanner.Close()
				destination := []driver.Value{"stale", "extra"}
				for row := int64(0); row < 3; row++ {
					require.NoError(t, scanner.ScanRow(destination, 41+row))
					require.Nil(t, destination[1])
					if values.IsNull(int(row)) {
						require.Nil(t, destination[0])
					} else {
						require.IsType(t, test.expected, destination[0])
						require.Equal(t, test.expected, destination[0])
						if test.dbType != cli_service.TTypeId_TIMESTAMP_TYPE {
							parity, err := arrowscan.ScanCell(values, int(row), time.UTC)
							require.NoError(t, err)
							require.Equal(t, parity, destination[0])
						}
					}
				}
			})
		}
	}
}

func TestBoundReadersRebindAndRelease(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := array.NewInt32Builder(pool)
	defer builder.Release()
	var batches [][]arrow.Array
	for batch := 0; batch < 3; batch++ {
		builder.Append(int32(300 + batch))
		if batch == 1 {
			builder.AppendNull()
		} else {
			builder.Append(int32(400 + batch))
		}
		values := builder.NewArray()
		defer values.Release()
		batches = append(batches, []arrow.Array{values})
	}
	scanner := readerTestScanner([]arrow.Field{{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, []cli_service.TTypeId{cli_service.TTypeId_INT_TYPE}, config.ArrowConfig{}, time.UTC, batches...)
	destination := make([]driver.Value, 1)
	for row, expected := range []any{int32(300), int32(400), int32(301), nil, int32(302), int32(402)} {
		require.NoError(t, scanner.ScanRow(destination, 41+int64(row)))
		require.Equal(t, expected, destination[0])
	}
	scanner.Close()
	require.Nil(t, scanner.rowValues.readers)
	require.Equal(t, int32(402), destination[0])
}

func TestBoundNativeIntervalNullAndError(t *testing.T) {
	for _, dbType := range []cli_service.TTypeId{cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE, cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE} {
		t.Run(dbType.String(), func(t *testing.T) {
			builder := array.NewStringBuilder(memory.DefaultAllocator)
			defer builder.Release()
			builder.AppendNull()
			builder.Append("1")
			values := builder.NewArray()
			defer values.Release()
			scanner := readerTestScanner([]arrow.Field{{Name: "interval", Type: arrow.BinaryTypes.String, Nullable: true}}, []cli_service.TTypeId{dbType}, config.ArrowConfig{UseArrowNativeIntervalTypes: true}, time.UTC, []arrow.Array{values})
			defer scanner.Close()
			destination := []driver.Value{"stale"}
			require.NoError(t, scanner.ScanRow(destination, 41))
			require.Nil(t, destination[0])
			err := scanner.ScanRow(destination, 42)
			require.EqualError(t, err, "databricks: driver error: "+errArrowRowsUnsupportedNativeType(dbType.String()))
			require.Nil(t, destination[0])
		})
	}
}

func TestBoundDecimalConfig(t *testing.T) {
	dataType := &arrow.Decimal128Type{Precision: 38, Scale: 2}
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			builder := array.NewDecimal128Builder(memory.DefaultAllocator, dataType)
			defer builder.Release()
			require.NoError(t, builder.AppendValueFromString("1234567890123456789.99"))
			builder.AppendNull()
			values := builder.NewArray()
			defer values.Release()
			scanner := readerTestScanner([]arrow.Field{{Name: "decimal", Type: dataType, Nullable: true}}, []cli_service.TTypeId{cli_service.TTypeId_DECIMAL_TYPE}, config.ArrowConfig{UseArrowNativeDecimal: native}, time.UTC, []arrow.Array{values})
			defer scanner.Close()
			destination := make([]driver.Value, 1)
			require.NoError(t, scanner.ScanRow(destination, 41))
			if native {
				require.Equal(t, "1234567890123456789.99", destination[0])
			} else {
				require.Equal(t, values.(*array.Decimal128).Value(0).ToFloat64(2), destination[0])
			}
			require.NoError(t, scanner.ScanRow(destination, 42))
			require.Nil(t, destination[0])
		})
	}
}

func TestBoundTimestampLocation(t *testing.T) {
	location := time.FixedZone("east", 9*3600)
	wall := time.Date(2026, 11, 27, 9, 15, 0, 123456000, time.UTC)
	for _, test := range []struct {
		name     string
		zone     string
		metadata arrow.Metadata
		expected time.Time
	}{
		{"zoned", "UTC", arrow.Metadata{}, wall.In(location)},
		{"ntz", "", arrow.Metadata{}, wall},
		{"metadata-ntz", "UTC", arrow.MetadataFrom(map[string]string{"databricks.type_name": "TIMESTAMP_NTZ"}), wall},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: test.zone}
			builder := array.NewTimestampBuilder(memory.DefaultAllocator, dataType)
			defer builder.Release()
			builder.Append(arrow.Timestamp(wall.UnixMicro()))
			builder.AppendNull()
			values := builder.NewArray()
			defer values.Release()
			scanner := readerTestScanner([]arrow.Field{{Name: "timestamp", Type: dataType, Nullable: true, Metadata: test.metadata}}, []cli_service.TTypeId{cli_service.TTypeId_TIMESTAMP_TYPE}, config.ArrowConfig{}, location, []arrow.Array{values})
			defer scanner.Close()
			destination := make([]driver.Value, 1)
			require.NoError(t, scanner.ScanRow(destination, 41))
			require.Equal(t, test.expected, destination[0])
			require.NoError(t, scanner.ScanRow(destination, 42))
			require.Nil(t, destination[0])
		})
	}
}
