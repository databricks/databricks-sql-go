package arrowbased

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowRowScanner(t *testing.T) {

	t.Run("Convert thrift db types to non-native arrow types", func(t *testing.T) {
		arrowConfig := &config.ArrowConfig{}

		schema := getAllTypesSchema()
		var arrowType arrow.DataType
		var err error

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[0], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Boolean, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[1], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Int8, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[2], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Int16, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[3], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Int32, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[4], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Int64, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[5], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Float32, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[6], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.PrimitiveTypes.Float64, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[7], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[8], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		arrowConfig.UseArrowNativeTimestamp = true
		arrowType, err = tColumnDescToArrowDataType(schema.Columns[8], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Timestamp_us, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[9], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.Binary, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[10], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[11], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[12], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		// native complext types are not supported with a thrift schema
		arrowConfig.UseArrowNativeComplexTypes = true
		_, err = tColumnDescToArrowDataType(schema.Columns[10], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("ARRAY"))

		_, err = tColumnDescToArrowDataType(schema.Columns[11], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("MAP"))

		_, err = tColumnDescToArrowDataType(schema.Columns[12], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("STRUCT"))

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[13], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		arrowConfig.UseArrowNativeDecimal = true
		arrowType, err = tColumnDescToArrowDataType(schema.Columns[13], arrowConfig)
		assert.Nil(t, err)
		dt, _ := arrow.NewDecimalType(arrow.DECIMAL128, 2, 10)
		assert.Equal(t, dt, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[14], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Date32, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[15], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		arrowType, err = tColumnDescToArrowDataType(schema.Columns[16], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.BinaryTypes.String, arrowType)

		// native complext types are not supported with a thrift schema
		arrowConfig.UseArrowNativeIntervalTypes = true
		_, err = tColumnDescToArrowDataType(schema.Columns[15], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("INTERVAL_YEAR_MONTH"))

		_, err = tColumnDescToArrowDataType(schema.Columns[16], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("INTERVAL_DAY_TIME"))

		// unknown type
		_, err = tColumnDescToArrowDataType(&cli_service.TColumnDesc{TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId(999)}}}}}, arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnknownDBType)

	})

	t.Run("Convert thrift db types to native arrow types", func(t *testing.T) {
		arrowConfig := &config.ArrowConfig{}

		schema := getAllTypesSchema()
		var arrowType arrow.DataType
		var err error

		arrowConfig.UseArrowNativeTimestamp = true
		arrowType, err = tColumnDescToArrowDataType(schema.Columns[8], arrowConfig)
		assert.Nil(t, err)
		assert.Equal(t, arrow.FixedWidthTypes.Timestamp_us, arrowType)

		arrowConfig.UseArrowNativeComplexTypes = true
		_, err = tColumnDescToArrowDataType(schema.Columns[10], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("ARRAY"))

		_, err = tColumnDescToArrowDataType(schema.Columns[11], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("MAP"))

		_, err = tColumnDescToArrowDataType(schema.Columns[12], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("STRUCT"))

		arrowConfig.UseArrowNativeDecimal = true
		arrowType, err = tColumnDescToArrowDataType(schema.Columns[13], arrowConfig)
		assert.Nil(t, err)
		dt, _ := arrow.NewDecimalType(arrow.DECIMAL128, 2, 10)
		assert.Equal(t, dt, arrowType)

		arrowConfig.UseArrowNativeIntervalTypes = true
		_, err = tColumnDescToArrowDataType(schema.Columns[15], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("INTERVAL_YEAR_MONTH"))

		_, err = tColumnDescToArrowDataType(schema.Columns[16], arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnsupportedWithHiveSchema("INTERVAL_DAY_TIME"))
	})

	t.Run("Convert unknown thrift db type to arrow type", func(t *testing.T) {
		arrowConfig := &config.ArrowConfig{}

		columnDesc := &cli_service.TColumnDesc{
			TypeDesc: &cli_service.TTypeDesc{
				Types: []*cli_service.TTypeEntry{
					{
						PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
							Type: cli_service.TTypeId(999),
						},
					},
				},
			},
		}

		_, err := tColumnDescToArrowDataType(columnDesc, arrowConfig)
		assert.NotNil(t, err)
		assert.EqualError(t, err, errArrowRowsUnknownDBType)
	})

	t.Run("NRows", func(t *testing.T) {
		// test counting the number of rows by summing individual batches
		var dummy *arrowRowScanner
		assert.Equal(t, int64(0), dummy.NRows())

		rowSet := &cli_service.TRowSet{}
		schema := &cli_service.TTableSchema{}
		metadataResp := getMetadataResp(schema)

		ars, err := NewArrowRowScanner(metadataResp, rowSet, nil, nil, context.Background())
		assert.NotNil(t, ars)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), ars.NRows())

		rowSet.ArrowBatches = []*cli_service.TSparkArrowBatch{}
		ars, err = NewArrowRowScanner(metadataResp, rowSet, nil, nil, context.Background())
		assert.NotNil(t, ars)
		assert.Nil(t, err)
		assert.Equal(t, int64(0), ars.NRows())

		rowSet.ArrowBatches = []*cli_service.TSparkArrowBatch{{RowCount: 2}, {RowCount: 3}}
		ars, _ = NewArrowRowScanner(metadataResp, rowSet, nil, nil, context.Background())
		assert.NotNil(t, ars)
		assert.Equal(t, int64(5), ars.NRows())
	})

	t.Run("Create default column value holders", func(t *testing.T) {
		// Check that correct typing is happening when creating column value
		// holders
		rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{RowCount: 2}, {RowCount: 3}}}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		d, _ := NewArrowRowScanner(metadataResp, rowSet, nil, nil, context.Background())

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		err := ars.makeColumnValuesContainers(ars, rowscanner.NewDelimiter(0, 1))
		require.Nil(t, err)

		var ok bool

		rowValues, ok := ars.rowValues.(*rowValues)
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[0].(*columnValuesTyped[*array.Boolean, bool])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[1].(*columnValuesTyped[*array.Int8, int8])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[2].(*columnValuesTyped[*array.Int16, int16])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[3].(*columnValuesTyped[*array.Int32, int32])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[4].(*columnValuesTyped[*array.Int64, int64])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[5].(*columnValuesTyped[*array.Float32, float32])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[6].(*columnValuesTyped[*array.Float64, float64])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[7].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[8].(*timestampStringValueContainer)
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[9].(*columnValuesTyped[*array.Binary, []byte])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[10].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[11].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[12].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[13].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[14].(*dateValueContainer)
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[15].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)

		_, ok = rowValues.columnValueHolders[16].(*columnValuesTyped[*array.String, string])
		assert.True(t, ok)
	})

	t.Run("Create native column value holders", func(t *testing.T) {
		// The types of the column  value holders will be different
		// from the default if when using native types is specified
		rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{RowCount: 2}, {RowCount: 3}}}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseArrowBatches = true
		cfg.UseArrowNativeTimestamp = true
		cfg.UseArrowNativeDecimal = true

		d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		err := ars.makeColumnValuesContainers(ars, rowscanner.NewDelimiter(0, 1))
		require.Nil(t, err)

		var ok bool

		rowValues, ok := ars.rowValues.(*rowValues)
		assert.True(t, ok)

		// timestamp
		_, ok = rowValues.columnValueHolders[8].(*timestampValueContainer)
		assert.True(t, ok)

		// decimal
		_, ok = rowValues.columnValueHolders[13].(*decimal128Container)
		assert.True(t, ok)
	})

	t.Run("Fail creating arrow row scanner on invalid native decimal type", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{RowCount: 2}, {RowCount: 3}}}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseArrowBatches = true
		cfg.UseArrowNativeTimestamp = true
		cfg.UseArrowNativeDecimal = true

		_, err := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.Nil(t, err)

		// missing type qualifiers
		schema = getAllTypesSchema()
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers = nil
		metadataResp.Schema = schema
		_, err = NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "databricks: driver error: "+errArrowRowsConvertSchema+": "+errArrowRowsInvalidDecimalType))

		// type qualifiers missing qualifiers
		schema = getAllTypesSchema()
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers.Qualifiers = nil
		metadataResp.Schema = schema
		_, err = NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "databricks: driver error: "+errArrowRowsConvertSchema+": "+errArrowRowsInvalidDecimalType))

		// empty qualifiers
		schema = getAllTypesSchema()
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers.Qualifiers = map[string]*cli_service.TTypeQualifierValue{}
		metadataResp.Schema = schema
		_, err = NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "databricks: driver error: "+errArrowRowsConvertSchema+": "+errArrowRowsInvalidDecimalType))

		// nil precision
		schema = getAllTypesSchema()
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers.Qualifiers["precision"] = nil
		metadataResp.Schema = schema
		_, err = NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "databricks: driver error: "+errArrowRowsConvertSchema+": "+errArrowRowsInvalidDecimalType))

		// precision missing value
		schema = getAllTypesSchema()
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers.Qualifiers["precision"].I32Value = nil
		metadataResp.Schema = schema
		_, err = NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "databricks: driver error: "+errArrowRowsConvertSchema+": "+errArrowRowsInvalidDecimalType))

		// nil scale
		schema = getAllTypesSchema()
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers.Qualifiers["scale"] = nil
		metadataResp.Schema = schema
		_, err = NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), "databricks: driver error: "+errArrowRowsConvertSchema+": "+errArrowRowsInvalidDecimalType))

		// scale missing value
		schema = getAllTypesSchema()
		schema.Columns[13].TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers.Qualifiers["scale"].I32Value = nil
		metadataResp.Schema = schema
		_, err = NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.NotNil(t, err)
		msg := err.Error()
		pre := "databricks: driver error: " + errArrowRowsConvertSchema + ": " + errArrowRowsInvalidDecimalType
		assert.True(t, strings.HasPrefix(msg, pre))

	})

	t.Run("Fail to scan row when no batches are present", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseArrowBatches = true
		d, err1 := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.Nil(t, err1)

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		err := ars.makeColumnValuesContainers(ars, rowscanner.NewDelimiter(0, 0))
		require.Nil(t, err)

		dest := make([]driver.Value, 1)
		err = ars.ScanRow(dest, 0)
		require.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "databricks: driver error: "+errArrowRowsInvalidRowNumber(0)))
	})

	t.Run("Close releases column values", func(t *testing.T) {
		// Making sure the test fails gracefully if there is a panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("arrow row scanner Close panic")
			}
		}()

		// The following is the code under test
		rowSet := &cli_service.TRowSet{ArrowBatches: []*cli_service.TSparkArrowBatch{{RowCount: 2}, {RowCount: 3}}}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseArrowBatches = true

		d, err := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())
		require.Nil(t, err)
		d.Close()

		ars := d.(*arrowRowScanner)
		var releaseCount int
		fc := &fakeColumnValues{fnRelease: func() { releaseCount++ }}
		ars.rowValues = NewRowValues(rowscanner.NewDelimiter(0, 1), []columnValues{fc, fc, fc})
		d.Close()
		assert.Equal(t, 3, releaseCount)
	})

	t.Run("loadBatch invalid row scanner", func(t *testing.T) {
		var ars *arrowRowScanner
		err := ars.loadBatchFor(0)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, errArrowRowsNoArrowBatches)

		ars = &arrowRowScanner{}
		ars.DBSQLLogger = dbsqllog.Logger
		err = ars.loadBatchFor(0)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, errArrowRowsNoArrowBatches)
	})

	t.Run("Create column value holders on first batch load", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{
			ArrowBatches: []*cli_service.TSparkArrowBatch{
				{RowCount: 5},
				{RowCount: 3},
				{RowCount: 7},
			},
		}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseLz4Compression = false

		d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		assert.Nil(t, ars.rowValues)

		b1 := &sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(0, 5), arrowRecords: []SparkArrowRecord{
			&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 2), Record: &fakeRecord{}},
			&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(2, 3), Record: &fakeRecord{}}}}
		b2 := &sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(5, 3), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(5, 3), Record: &fakeRecord{}}}}
		b3 := &sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(8, 7), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(8, 7), Record: &fakeRecord{}}}}

		fbi := &fakeBatchIterator{
			batches:   []SparkArrowBatch{b1, b2, b3},
			index:     -1,
			callCount: 0,
		}
		ars.batchIterator = fbi

		var callCount int
		ars.valueContainerMaker = &fakeValueContainerMaker{fnMakeColumnValuesContainers: func(ars *arrowRowScanner, d rowscanner.Delimiter) dbsqlerr.DBError {
			callCount += 1
			columnValueHolders := make([]columnValues, len(ars.arrowSchema.Fields()))
			for i := range ars.arrowSchema.Fields() {
				columnValueHolders[i] = &fakeColumnValues{}
			}
			ars.rowValues = NewRowValues(rowscanner.NewDelimiter(0, 0), make([]columnValues, len(ars.arrowSchema.Fields())))
			return nil
		}}

		err := ars.loadBatchFor(0)
		assert.Nil(t, err)
		assert.Equal(t, len(metadataResp.Schema.Columns), ars.rowValues.NColumns())
		assert.Equal(t, 1, callCount)
		assert.Equal(t, 1, fbi.callCount)

		err = ars.loadBatchFor(1)
		assert.Nil(t, err)
		assert.Equal(t, len(metadataResp.Schema.Columns), ars.rowValues.NColumns())
		assert.Equal(t, 1, callCount)
		assert.Equal(t, 1, fbi.callCount)

		err = ars.loadBatchFor(2)
		assert.Nil(t, err)
		assert.Equal(t, len(metadataResp.Schema.Columns), ars.rowValues.NColumns())
		assert.Equal(t, 1, callCount)
		assert.Equal(t, 1, fbi.callCount)

		err = ars.loadBatchFor(5)
		assert.Nil(t, err)
		assert.Equal(t, len(metadataResp.Schema.Columns), ars.rowValues.NColumns())
		assert.Equal(t, 1, callCount)
		assert.Equal(t, 2, fbi.callCount)

	})

	t.Run("don't reload current batch", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{
			ArrowBatches: []*cli_service.TSparkArrowBatch{
				{RowCount: 5},
				{RowCount: 3},
				{RowCount: 7},
			},
		}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseLz4Compression = false

		d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, nil)

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		fbi := &fakeBatchIterator{
			batches: []SparkArrowBatch{
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(0, 5), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 5), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(5, 3), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(5, 3), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(8, 7), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(8, 7), Record: &fakeRecord{}}}},
			},
			index:     -1,
			callCount: 0,
		}
		ars.batchIterator = fbi

		err := ars.loadBatchFor(0)
		assert.Nil(t, err)
		assert.Equal(t, 1, fbi.callCount)

		err = ars.loadBatchFor(0)
		assert.Nil(t, err)
		assert.Equal(t, 1, fbi.callCount)
	})

	t.Run("loadBatch index out of bounds", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{
			ArrowBatches: []*cli_service.TSparkArrowBatch{
				{RowCount: 5},
				{RowCount: 3},
				{RowCount: 7},
			},
		}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseLz4Compression = false

		d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, nil)

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		fbi := &fakeBatchIterator{
			batches: []SparkArrowBatch{
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(0, 5), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 5), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(5, 3), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(5, 3), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(8, 7), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(8, 7), Record: &fakeRecord{}}}},
			},
			index:     -1,
			callCount: 0,
		}
		ars.batchIterator = fbi

		err := ars.loadBatchFor(-1)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, errArrowRowsInvalidRowNumber(-1))

		err = ars.loadBatchFor(17)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, errArrowRowsInvalidRowNumber(17))
	})

	t.Run("loadBatch container failure", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{
			ArrowBatches: []*cli_service.TSparkArrowBatch{
				{RowCount: 5},
				{RowCount: 3},
				{RowCount: 7},
			},
		}

		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseLz4Compression = false

		d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, nil)

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		fbi := &fakeBatchIterator{
			batches: []SparkArrowBatch{
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(0, 5), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 5), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(5, 3), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(5, 3), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(8, 7), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(8, 7), Record: &fakeRecord{}}}},
			},
			index:     -1,
			callCount: 0,
		}
		ars.batchIterator = fbi

		ars.valueContainerMaker = &fakeValueContainerMaker{
			fnMakeColumnValuesContainers: func(ars *arrowRowScanner, d rowscanner.Delimiter) dbsqlerr.DBError {
				return dbsqlerrint.NewDriverError(context.TODO(), "error making containers", nil)
			},
		}

		err := ars.loadBatchFor(0)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "error making containers")
	})

	t.Run("loadBatch record read failure", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{
			ArrowBatches: []*cli_service.TSparkArrowBatch{
				{RowCount: 5},
				{RowCount: 3},
				{RowCount: 7},
			},
		}

		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseLz4Compression = false

		d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, nil)

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		fbi := &fakeBatchIterator{
			batches: []SparkArrowBatch{
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(0, 5), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 5), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(5, 3), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(5, 3), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(8, 7), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(8, 7), Record: &fakeRecord{}}}},
			},
			index:     -1,
			callCount: 0,
			err:       dbsqlerrint.NewDriverError(context.TODO(), "error reading record", nil),
		}
		ars.batchIterator = fbi

		err := ars.loadBatchFor(0)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "error reading record")

	})

	t.Run("Paginate through batches", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{
			ArrowBatches: []*cli_service.TSparkArrowBatch{
				{RowCount: 5},
				{RowCount: 3},
				{RowCount: 7},
			},
		}
		schema := getAllTypesSchema()
		metadataResp := getMetadataResp(schema)

		cfg := config.Config{}
		cfg.UseLz4Compression = false

		d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())

		var ars *arrowRowScanner = d.(*arrowRowScanner)

		fbi := &fakeBatchIterator{
			batches: []SparkArrowBatch{
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(0, 5), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 5), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(5, 3), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(5, 3), Record: &fakeRecord{}}}},
				&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(8, 7), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(8, 7), Record: &fakeRecord{}}}},
			},
			index:     -1,
			callCount: 0,
		}
		ars.batchIterator = fbi

		for _, i := range []int64{0, 1, 2, 3, 4} {
			err := ars.loadBatchFor(i)
			assert.Nil(t, err)
			assert.NotNil(t, fbi.lastReadBatch)
			assert.Equal(t, 1, fbi.callCount)
			assert.Equal(t, int64(0), fbi.lastReadBatch.Start())
		}

		for _, i := range []int64{5, 6, 7} {
			err := ars.loadBatchFor(i)
			assert.Nil(t, err)
			assert.NotNil(t, fbi.lastReadBatch)
			assert.Equal(t, 2, fbi.callCount)
			assert.Equal(t, int64(5), fbi.lastReadBatch.Start())
		}

		for _, i := range []int64{8, 9, 10, 11, 12, 13, 14} {
			err := ars.loadBatchFor(i)
			assert.Nil(t, err)
			assert.NotNil(t, fbi.lastReadBatch)
			assert.Equal(t, 3, fbi.callCount)
			assert.Equal(t, int64(8), fbi.lastReadBatch.Start())
		}

		err := ars.loadBatchFor(-1)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "databricks: driver error: "+errArrowRowsInvalidRowNumber(-1))

		err = ars.loadBatchFor(15)
		assert.NotNil(t, err)
		assert.EqualError(t, err, "databricks: driver error: "+errArrowRowsInvalidRowNumber(15))
	})

	t.Run("Error on retrieving not implemented native arrow types", func(t *testing.T) {
		rowSet := &cli_service.TRowSet{
			ArrowBatches: []*cli_service.TSparkArrowBatch{
				{RowCount: 5},
				{RowCount: 3},
				{RowCount: 7},
			},
		}

		var scale int32 = 10
		var precision int32 = 2
		var columns []*cli_service.TColumnDesc = []*cli_service.TColumnDesc{
			{
				ColumnName: "array_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_ARRAY_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "map_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_MAP_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "struct_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_STRUCT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "decimal_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_DECIMAL_TYPE,
								TypeQualifiers: &cli_service.TTypeQualifiers{
									Qualifiers: map[string]*cli_service.TTypeQualifierValue{
										"scale":     {I32Value: &scale},
										"precision": {I32Value: &precision},
									},
								},
							},
						},
					},
				},
			},
			{
				ColumnName: "interval_ym_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "interval_dt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE,
							},
						},
					},
				},
			},
		}

		for i := range columns {
			schema := &cli_service.TTableSchema{
				Columns: []*cli_service.TColumnDesc{columns[i]},
			}
			metadataResp := getMetadataResp(schema)

			cfg := config.Config{}
			cfg.UseLz4Compression = false

			d, _ := NewArrowRowScanner(metadataResp, rowSet, &cfg, nil, context.Background())

			var ars *arrowRowScanner = d.(*arrowRowScanner)
			ars.UseArrowNativeComplexTypes = true
			ars.UseArrowNativeDecimal = true
			ars.UseArrowNativeIntervalTypes = true

			fbi := &fakeBatchIterator{
				batches: []SparkArrowBatch{
					&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(0, 5), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(0, 5), Record: &fakeRecord{}}}},
					&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(5, 3), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(5, 3), Record: &fakeRecord{}}}},
					&sparkArrowBatch{Delimiter: rowscanner.NewDelimiter(8, 7), arrowRecords: []SparkArrowRecord{&sparkArrowRecord{Delimiter: rowscanner.NewDelimiter(8, 7), Record: &fakeRecord{}}}},
				},
				index:     -1,
				callCount: 0,
			}
			ars.batchIterator = fbi

			ars.valueContainerMaker = &fakeValueContainerMaker{fnMakeColumnValuesContainers: func(ars *arrowRowScanner, d rowscanner.Delimiter) dbsqlerr.DBError {
				columnValueHolders := make([]columnValues, len(ars.arrowSchema.Fields()))
				for i := range ars.arrowSchema.Fields() {
					columnValueHolders[i] = &fakeColumnValues{}
				}
				ars.rowValues = NewRowValues(rowscanner.NewDelimiter(0, 0), columnValueHolders)
				return nil
			}}

			dest := make([]driver.Value, len(schema.Columns))

			err := ars.ScanRow(dest, 0)

			if i < 3 {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
			}
		}
	})

	t.Run("Retrieve values", func(t *testing.T) {
		// 	bool_col
		// 	int_col,
		// 	bigint_col,
		// 	float_col,
		// 	double_col,
		// 	string_col,
		// 	timestamp_col,
		// 	binary_col,
		// 	array_col,
		// 	map_col,
		// struct_col,
		// 	decimal_col,
		// 	date_col,
		// interval_ym_col,
		// interval_dt_col
		expected := []driver.Value{
			true, int8(4), int16(3), int32(2), int64(1), float32(3.3), float64(2.2), "stringval",
			time.Date(2021, 7, 1, 5, 43, 28, 0, time.UTC),
			[]uint8{26, 191},
			"[1,2,3]",
			"{\"key1\":1}",
			"{\"Field1\":77,\"Field2\":\"Field 2 value\"}",
			"1",
			time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC),
			"100-0",
			"-8 00:00:00.000000000",
		}

		readValues := func(fileName string, nativeDates bool) {
			executeStatementResp := cli_service.TExecuteStatementResp{}
			loadTestData(t, fileName, &executeStatementResp)

			config := config.WithDefaults()
			config.UseArrowNativeTimestamp = nativeDates
			config.UseArrowNativeComplexTypes = false
			config.UseArrowNativeDecimal = false
			config.UseArrowNativeIntervalTypes = false
			d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
			assert.Nil(t, err)

			ars := d.(*arrowRowScanner)

			dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
			err = ars.ScanRow(dest, 0)
			assert.Nil(t, err)

			for i := range expected {
				assert.Equal(t, expected[i], dest[i])
			}
		}

		readValues("all_types.json", true)
		readValues("all_types_time_strings.json", false)
	})

	t.Run("Retrieve negative date/time values", func(t *testing.T) {
		expectedTime := time.Date(-2021, 7, 1, 5, 43, 28, 0, time.UTC)
		expectedDate := time.Date(-2020, 12, 31, 0, 0, 0, 0, time.UTC)

		readValues := func(fileName string, useNativeTimestamp bool) {
			executeStatementResp := cli_service.TExecuteStatementResp{}
			loadTestData(t, "all_types.json", &executeStatementResp)

			config := config.WithDefaults()
			config.UseArrowNativeComplexTypes = false
			d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
			assert.Nil(t, err)

			ars := d.(*arrowRowScanner)

			dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
			err = ars.ScanRow(dest, 1)
			assert.Nil(t, err)

			assert.Equal(t, expectedTime, dest[8])
			assert.Equal(t, expectedDate, dest[14])
		}

		readValues("all_types.json", true)
		readValues("all_types_time_strings.json", true)
	})

	t.Run("Retrieve null values", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "all_types.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeComplexTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := []driver.Value{
			true, int8(4), int16(3), int32(2), int64(1), float32(3.3), float64(2.2), "stringval",
			time.Date(2021, 7, 1, 5, 43, 28, 0, time.UTC),
			[]uint8{26, 191},
			"[1,2,3]",
			"{\"key1\":1}",
			"{\"Field1\":77,\"Field2\":\"Field 2 value\"}",
			"1",
			time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC),
			"100-0",
			"-8 00:00:00.000000000",
		}

		err = ars.ScanRow(dest, 2)
		assert.Nil(t, err)

		for i := range dest {
			assert.Nil(t, dest[i])
		}
	})

	t.Run("Clear previous values when retrieving null values", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "all_types.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeComplexTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err = ars.ScanRow(dest, 2)
		assert.Nil(t, err)

		for i := range dest {
			assert.Nil(t, dest[i])
		}
	})

	t.Run("Retrieve and read multiple arrow batches", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "diamonds.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeComplexTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)
		assert.Equal(t, int64(53940), ars.NRows())

		fbi := &testBatchIteratorWrapper{
			bi: ars.batchIterator,
		}

		ars.batchIterator = fbi

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		for i := int64(0); i < ars.NRows(); i = i + 1 {
			err := ars.ScanRow(dest, i)
			assert.Nil(t, err)
			assert.Equal(t, int32(i+1), dest[0])

			if i%1000 == 0 {
				assert.NotNil(t, ars.currentBatch)
				assert.Equal(t, int64(i), ars.currentBatch.Start())
				if i < 53000 {
					assert.Equal(t, int64(1000), ars.currentBatch.Count())
				} else {
					assert.Equal(t, int64(940), ars.currentBatch.Count())
				}
				assert.Equal(t, ars.currentBatch.Start()+ars.currentBatch.Count()-1, ars.currentBatch.End())
			}
		}

		assert.Equal(t, 54, fbi.callCount)
	})

	t.Run("Retrieve values - native arrow schema", func(t *testing.T) {
		// 	bool_col
		// 	int_col,
		// 	bigint_col,
		// 	float_col,
		// 	double_col,
		// 	string_col,
		// 	timestamp_col,
		// 	binary_col,
		// 	array_col,
		// 	map_col,
		// struct_col,
		// 	decimal_col,
		// 	date_col,
		// interval_ym_col,
		// interval_dt_col
		expected := []driver.Value{
			true, int8(1), int16(2), int32(3), int64(4), float32(1.1), float64(2.2), "stringval",
			time.Date(2021, 7, 1, 5, 43, 28, 0, time.UTC),
			[]uint8{26, 191},
			"[1,2,3]",
			"{\"key1\":1,\"key2\":2}",
			"{\"Field1\":77,\"Field2\":2020-12-31}",
			"3.30",
			time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC),
			"100-0",
			"-8 00:00:00.000000000",
		}

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "all_types_arrow_schema.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = false
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		// err = ars.ScanRow(dest, 0)
		// assert.Nil(t, err)

		err = ars.ScanRow(dest, 1)
		assert.Nil(t, err)

		for i := range expected {
			assert.Equal(t, expected[i], dest[i])
		}

	})

	t.Run("Retrieve null values - native arrow schema", func(t *testing.T) {

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "all_types_arrow_schema.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = false
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err = ars.ScanRow(dest, 0)
		assert.Nil(t, err)

		for i := range dest {
			assert.Nil(t, dest[i])
		}

	})

	t.Run("Retrieve values - native complex types", func(t *testing.T) {
		// 	bool_col
		// 	int_col,
		// 	bigint_col,
		// 	float_col,
		// 	double_col,
		// 	string_col,
		// 	timestamp_col,
		// 	binary_col,
		// 	array_col,
		// 	map_col,
		// struct_col,
		// 	decimal_col,
		// 	date_col,
		// interval_ym_col,
		// interval_dt_col
		expected := []driver.Value{
			true, int8(1), int16(2), int32(3), int64(4), float32(1.1), float64(2.2), "stringval",
			time.Date(2021, 7, 1, 5, 43, 28, 0, time.UTC),
			[]uint8{26, 191},
			"[1,2,3]",
			"{\"key1\":1,\"key2\":2}",
			"{\"Field1\":77,\"Field2\":\"2020-12-31 00:00:00 +0000 UTC\"}",
			"3.30",
			time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC),
			"100-0",
			"-8 00:00:00.000000000",
		}

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "all_types_native_complex.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err1 := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err1)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err1 = ars.ScanRow(dest, 1)
		assert.Nil(t, err1)

		for i := range expected {
			assert.Equal(t, expected[i], dest[i])
		}

		// verify that the returned values for the complex type
		// columns are valid json strings
		var foo []any
		var s string = dest[10].(string)
		err := json.Unmarshal([]byte(s), &foo)
		assert.Nil(t, err)

		var foo2 map[string]any
		s = dest[11].(string)
		err = json.Unmarshal([]byte(s), &foo2)
		assert.Nil(t, err)

		var foo3 map[string]any
		s = dest[12].(string)
		err = json.Unmarshal([]byte(s), &foo3)
		assert.Nil(t, err)
	})

	t.Run("Retrieve null values - native complex types", func(t *testing.T) {

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "all_types_native_complex.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err = ars.ScanRow(dest, 0)
		assert.Nil(t, err)

		for i := range dest {
			assert.Nil(t, dest[i])
		}

	})

	t.Run("Retrieve values - arrays", func(t *testing.T) {
		// array_bool array<boolean>,
		// array_tinyint array<tinyint>,
		// array_smallint array<smallint>,
		// array_int array<int>,
		// array_bigint array<bigint>,
		// array_float array<float>,
		// array_double array<double>,
		// array_string array<string>,
		// array_timestamp array<timestamp>,
		// array_binary array<binary>,
		// array_array array<array<int>>,
		// array_map array<map<string, int>>,
		// array_struct array<struct<Field1:INT, Field2:DATE>>,
		// array_decimal array<decimal(10, 2)>,
		// array_date array<date>,
		// array_interval_ym array<interval year>,
		// array_interval_dt array<interval day>
		expected := []driver.Value{
			"[true,false,null]",
			"[1,2,null,3]",
			"[4,5,null,6]",
			"[7,8,null,9]",
			"[10,11,null,12]",
			"[null,1.1,2.2]",
			"[3.3,null,4.4]",
			"[\"s1\",\"s2\",null]",
			"[\"2021-07-01 05:43:28 +0000 UTC\",\"-2022-08-13 14:01:01 +0000 UTC\",null]",
			"[\"Gr8=\",\"D/8=\",null]",
			"[[1,2,3],[4,5,6],null]",
			"[{\"key1\":1,\"key2\":2},{\"key3\":3,\"key4\":4},null]",
			"[{\"Field1\":77,\"Field2\":\"2020-12-31 00:00:00 +0000 UTC\"},{\"Field1\":13,\"Field2\":\"-2020-12-31 00:00:00 +0000 UTC\"},{\"Field1\":null,\"Field2\":null}]",
			"[5.15,123.45,null]",
			"[\"2020-12-31 00:00:00 +0000 UTC\",\"-2020-12-31 00:00:00 +0000 UTC\",null]",
		}

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "arrays_native.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err = ars.ScanRow(dest, 0)
		assert.Nil(t, err)

		for i := range expected {
			s := dest[i].(string)
			var foo []any
			err := json.Unmarshal([]byte(s), &foo)
			assert.Nil(t, err)
			assert.Equal(t, expected[i], dest[i])
		}

	})

	t.Run("Retrieve values - maps", func(t *testing.T) {
		expected := []driver.Value{
			"{\"[1,2,3]\":{\"Field1\":77,\"Field2\":\"2020-12-31 00:00:00 +0000 UTC\"},\"[4,5,6]\":{\"Field1\":13,\"Field2\":\"2020-12-31 00:00:00 +0000 UTC\"}}",
			"{\"{\\\"Field1\\\":77,\\\"Field2\\\":\\\"2020-12-31 00:00:00 +0000 UTC\\\"}\":[1,2,3],\"{\\\"Field1\\\":13,\\\"Field2\\\":\\\"2020-12-31 00:00:00 +0000 UTC\\\"}\":[4,5,6]}",
		}

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "maps_native.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err = ars.ScanRow(dest, 0)
		assert.Nil(t, err)

		for i := range expected {
			var foo map[string]interface{}
			s := dest[i].(string)
			err := json.Unmarshal([]byte(s), &foo)
			assert.Nil(t, err)
			assert.Equal(t, expected[i], dest[i])
		}

	})

	t.Run("Retrieve values - maps issue 147", func(t *testing.T) {
		// This is a test for a bug reported as github issue 147
		// After copying a table with a column of type 'MAP<STRING, STRING>' querying the copy
		// would return the map value for the first row in all rows.
		// This was caused by an indexing bug when retrieving map values that showed up based on
		// how the result set was broken into arrow batches.
		expected := [][]driver.Value{
			{1, map[string]string{"name": "alice2"}},
			{2, map[string]string{"name": "bob2"}},
			{3, map[string]string{"name": "jon2"}},
		}

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "issue147.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))

		for i := range expected {
			err = ars.ScanRow(dest, int64(i))
			assert.Nil(t, err)

			var id int
			s := dest[0].(string)
			err := json.Unmarshal([]byte(s), &id)
			assert.Nil(t, err)
			assert.Equal(t, expected[i][0], id)

			var foo map[string]string
			s = dest[1].(string)
			err = json.Unmarshal([]byte(s), &foo)
			assert.Nil(t, err)
			assert.Equal(t, expected[i][1], foo)
		}

	})

	t.Run("Retrieve values - structs", func(t *testing.T) {
		expected := []driver.Value{
			"{\"f1\":1,\"f2\":\"-0450-11-13 00:00:00 +0000 UTC\",\"f3\":\"-2022-08-13 14:01:01 +0000 UTC\",\"f4\":{\"5\":5,\"6\":7},\"f5\":{\"ield1\":7,\"Field2\":\"-0450-11-13 00:00:00 +0000 UTC\"}}",
		}

		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "structs_native.json", &executeStatementResp)

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err = ars.ScanRow(dest, 0)
		assert.Nil(t, err)

		for i := range expected {
			var foo map[string]interface{}
			s := dest[i].(string)
			err := json.Unmarshal([]byte(s), &foo)
			assert.Nil(t, err)
			assert.Equal(t, expected[i], dest[i])
		}

	})

	t.Run("Retrieve null values in complex types", func(t *testing.T) {
		// results of executing query:
		// "select map('red', NULL, 'green', NULL) as sample_map, named_struct('Field1', NULL, 'Field2', NULL) as sample_struct, ARRAY(NULL, NULL, NULL) as sample_list"
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "nullsInComplexTypes.json", &executeStatementResp)

		expected := []driver.Value{
			"{\"red\":null,\"green\":null}",
			"{\"Field1\":null,\"Field2\":null}",
			"[null,null,null]",
		}

		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		d, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)

		ars := d.(*arrowRowScanner)

		dest := make([]driver.Value, len(executeStatementResp.DirectResults.ResultSetMetadata.Schema.Columns))
		err = ars.ScanRow(dest, 0)
		assert.Nil(t, err)

		for i := range expected {
			assert.Equal(t, expected[i], dest[i])
		}
	})

	t.Run("Mismatched schemas", func(t *testing.T) {
		// Test for
		var arrowSchema *arrow.Schema
		var schema *cli_service.TTableSchema
		colInfos := getColumnInfo(arrowSchema, schema)
		assert.NotNil(t, colInfos)
		assert.Zero(t, len(colInfos))

		arrowSchema = &arrow.Schema{}
		colInfos = getColumnInfo(arrowSchema, schema)
		assert.NotNil(t, colInfos)
		assert.Zero(t, len(colInfos))

		arrowSchema = nil
		schema = &cli_service.TTableSchema{}
		colInfos = getColumnInfo(arrowSchema, schema)
		assert.NotNil(t, colInfos)
		assert.Zero(t, len(colInfos))

		arrowSchema = &arrow.Schema{}
		schema.Columns = []*cli_service.TColumnDesc{{ColumnName: "Result"}}
		colInfos = getColumnInfo(arrowSchema, schema)
		assert.NotNil(t, colInfos)
		assert.Zero(t, len(colInfos))

		schema.Columns = nil
		arrowSchema = arrow.NewSchema([]arrow.Field{{Name: "Result", Type: arrow.PrimitiveTypes.Int16}}, nil)
		colInfos = getColumnInfo(arrowSchema, schema)
		assert.NotNil(t, colInfos)
		assert.Zero(t, len(colInfos))

		schema.Columns = []*cli_service.TColumnDesc{
			{
				ColumnName: "Result",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_BOOLEAN_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "Result2",
			},
		}
		colInfos = getColumnInfo(arrowSchema, schema)
		assert.NotNil(t, colInfos)
		assert.Equal(t, 1, len(colInfos))
		assert.Equal(t, "Result", colInfos[0].name)
		assert.Equal(t, cli_service.TTypeId_BOOLEAN_TYPE, colInfos[0].dbType)
		assert.Equal(t, arrow.PrimitiveTypes.Int16, colInfos[0].arrowType)

		// results of executing query:
		// "create or replace view hive_metastore.databricks_sql_go.test as select 1"
		// using DB.Query() instead of DB.Exec()
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "queryVExec.json", &executeStatementResp)
		config := config.WithDefaults()
		config.UseArrowNativeTimestamp = true
		config.UseArrowNativeComplexTypes = true
		config.UseArrowNativeDecimal = false
		config.UseArrowNativeIntervalTypes = false
		_, err := NewArrowRowScanner(executeStatementResp.DirectResults.ResultSetMetadata, executeStatementResp.DirectResults.ResultSet.Results, config, nil, context.Background())
		assert.Nil(t, err)
	})

	t.Run("GetArrowBatches", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData2(t, "directResultsMultipleFetch/ExecuteStatement.json", &executeStatementResp)

		fetchResp1 := cli_service.TFetchResultsResp{}
		loadTestData2(t, "directResultsMultipleFetch/FetchResults1.json", &fetchResp1)

		fetchResp2 := cli_service.TFetchResultsResp{}
		loadTestData2(t, "directResultsMultipleFetch/FetchResults2.json", &fetchResp2)

		ctx := driverctx.NewContextWithConnId(context.Background(), "connectionId")
		ctx = driverctx.NewContextWithCorrelationId(ctx, "correlationId")

		var fetchesInfo []fetchResultsInfo
		client := getSimpleClient(&fetchesInfo, []cli_service.TFetchResultsResp{fetchResp1, fetchResp2})
		logger := dbsqllog.WithContext("connectionId", "correlationId", "")

		rpi := rowscanner.NewResultPageIterator(
			ctx,
			rowscanner.NewDelimiter(0, 7311),
			5000,
			nil,
			false,
			client,
			logger)

		cfg := config.WithDefaults()

		ars, err := NewArrowRowScanner(
			executeStatementResp.DirectResults.ResultSetMetadata,
			executeStatementResp.DirectResults.ResultSet.Results,
			cfg,
			logger,
			context.Background(),
		)
		assert.Nil(t, err)

		rs, err2 := ars.GetArrowBatches(context.Background(), *cfg, rpi)
		assert.Nil(t, err2)

		hasNext := rs.HasNext()
		assert.True(t, hasNext)
		r, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches[0].RowCount, r.NumRows())
		r.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r2, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches[1].RowCount, r2.NumRows())
		r2.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r3, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[0].RowCount, r3.NumRows())
		r3.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r4, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[1].RowCount, r4.NumRows())
		r4.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r5, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[0].RowCount, r5.NumRows())
		r5.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r6, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[1].RowCount, r6.NumRows())
		r6.Release()

		hasNext = rs.HasNext()
		assert.False(t, hasNext)
		r7, err2 := rs.Next()
		assert.Nil(t, r7)
		assert.ErrorContains(t, err2, io.EOF.Error())
	})
}

type fakeColumnValues struct {
	fnValue   func(int) any
	fnIsNull  func(int) bool
	fnRelease func()
}

func (fc *fakeColumnValues) Value(i int) (any, error) {
	if fc.fnValue != nil {
		return fc.fnValue(i), nil
	}
	return nil, nil
}

func (fc *fakeColumnValues) IsNull(i int) bool {
	if fc.fnIsNull != nil {
		return fc.fnIsNull(i)
	}
	return false
}

func (fc *fakeColumnValues) Release() {
	if fc.fnRelease != nil {
		fc.fnRelease()
	}
}

func (cv *fakeColumnValues) SetValueArray(colData arrow.ArrayData) error {
	return nil
}

type fakeBatchIterator struct {
	batches       []SparkArrowBatch
	index         int
	callCount     int
	err           dbsqlerr.DBError
	lastReadBatch SparkArrowBatch
}

var _ BatchIterator = (*fakeBatchIterator)(nil)

func (fbi *fakeBatchIterator) Next() (SparkArrowBatch, error) {
	fbi.callCount += 1

	if fbi.err != nil {
		return nil, fbi.err
	}

	cnt := len(fbi.batches)
	fbi.index++
	if fbi.index < cnt {
		fbi.lastReadBatch = fbi.batches[fbi.index]
		return fbi.lastReadBatch, nil
	}

	fbi.lastReadBatch = nil
	return nil, io.EOF
}

func (fbi *fakeBatchIterator) HasNext() bool {
	// `Next()` will first increment an index, and only then return a batch
	// So `HasNext` should check if index can be incremented and still be within array
	return fbi.index+1 < len(fbi.batches)
}

func (fbi *fakeBatchIterator) Close() {
	fbi.index = len(fbi.batches)
	fbi.lastReadBatch = nil
}

type testBatchIteratorWrapper struct {
	bi              BatchIterator
	callCount       int
	lastLoadedBatch SparkArrowBatch
}

var _ BatchIterator = (*testBatchIteratorWrapper)(nil)

func (biw *testBatchIteratorWrapper) Next() (SparkArrowBatch, error) {
	biw.callCount += 1
	batch, err := biw.bi.Next()
	biw.lastLoadedBatch = batch
	return batch, err
}

func (biw *testBatchIteratorWrapper) HasNext() bool {
	return biw.bi.HasNext()
}

func (biw *testBatchIteratorWrapper) Close() {
	biw.bi.Close()
}

type fakeRecord struct {
	fnRelease    func()
	fnRetain     func()
	fnSchema     func() *arrow.Schema
	fnNumRows    func() int64
	fnNumCols    func() int64
	fnColumns    func() []arrow.Array
	fnColumn     func(i int) arrow.Array
	fnColumnName func(i int) string
	fnNewSlice   func(i, j int64) arrow.Record
	fnSetColumn  func(int, arrow.Array) (arrow.Record, error)
}

func (fr fakeRecord) Release() {
	if fr.fnRelease != nil {
		fr.fnRelease()
	}
}
func (fr fakeRecord) Retain() {
	if fr.fnRetain != nil {
		fr.fnRetain()
	}
}

func (fr fakeRecord) Schema() *arrow.Schema {
	if fr.fnSchema != nil {
		return fr.fnSchema()
	}
	return nil
}

func (fr fakeRecord) NumRows() int64 {
	if fr.fnNumRows != nil {
		return fr.fnNumRows()
	}
	return 0
}
func (fr fakeRecord) NumCols() int64 {
	if fr.fnNumCols != nil {
		return fr.fnNumCols()
	}
	return 0
}

func (fr fakeRecord) Columns() []arrow.Array {
	if fr.fnColumns != nil {
		return fr.fnColumns()
	}
	return nil
}
func (fr fakeRecord) Column(i int) arrow.Array {
	if fr.fnColumn != nil {
		return fr.fnColumn(i)
	}
	return nil
}
func (fr fakeRecord) ColumnName(i int) string {
	if fr.fnColumnName != nil {
		return fr.fnColumnName(i)
	}
	return ""
}

func (fr fakeRecord) NewSlice(i, j int64) arrow.Record {
	if fr.fnNewSlice != nil {
		return fr.fnNewSlice(i, j)
	}
	return nil
}

func (fr fakeRecord) MarshalJSON() ([]byte, error) { return nil, nil }

func (fr fakeRecord) SetColumn(i int, arr arrow.Array) (arrow.Record, error) {
	if fr.fnSetColumn != nil {
		return fr.fnSetColumn(i, arr)
	}
	return nil, nil
}

func getAllTypesSchema() *cli_service.TTableSchema {
	var scale int32 = 10
	var precision int32 = 2

	return &cli_service.TTableSchema{
		Columns: []*cli_service.TColumnDesc{
			{
				ColumnName: "bool_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_BOOLEAN_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "tinyInt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_TINYINT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "smallInt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_SMALLINT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "int_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_INT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "bigInt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_BIGINT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "float_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_FLOAT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "double_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_DOUBLE_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "string_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_STRING_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "timestamp_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_TIMESTAMP_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "binary_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_BINARY_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "array_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_ARRAY_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "map_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_MAP_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "struct_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_STRUCT_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "decimal_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_DECIMAL_TYPE,
								TypeQualifiers: &cli_service.TTypeQualifiers{
									Qualifiers: map[string]*cli_service.TTypeQualifierValue{
										"scale":     {I32Value: &scale},
										"precision": {I32Value: &precision},
									},
								},
							},
						},
					},
				},
			},
			{
				ColumnName: "date_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_DATE_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "interval_ym_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE,
							},
						},
					},
				},
			},
			{
				ColumnName: "interval_dt_col",
				TypeDesc: &cli_service.TTypeDesc{
					Types: []*cli_service.TTypeEntry{
						{
							PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
								Type: cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE,
							},
						},
					},
				},
			},
		},
	}
}

type fakeValueContainerMaker struct {
	fnMakeColumnValuesContainers func(ars *arrowRowScanner, d rowscanner.Delimiter) dbsqlerr.DBError
}

var _ valueContainerMaker = (*fakeValueContainerMaker)(nil)

func (vcm *fakeValueContainerMaker) makeColumnValuesContainers(ars *arrowRowScanner, d rowscanner.Delimiter) error {
	if vcm.fnMakeColumnValuesContainers != nil {
		return vcm.fnMakeColumnValuesContainers(ars, d)
	}

	return nil
}

func loadTestData(t *testing.T, name string, v any) {
	if f, err := os.ReadFile(fmt.Sprintf("testdata/%s", name)); err != nil {
		t.Errorf("could not read data from: %s", name)
	} else {
		if err := json.Unmarshal(f, v); err != nil {
			t.Errorf("could not load data from: %s", name)
		}
	}
}

func getMetadataResp(schema *cli_service.TTableSchema) *cli_service.TGetResultSetMetadataResp {
	rowSetType := cli_service.TSparkRowSetType_ARROW_BASED_SET
	return &cli_service.TGetResultSetMetadataResp{Schema: schema, ResultFormat: &rowSetType}
}
