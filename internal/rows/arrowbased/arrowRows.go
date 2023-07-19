package arrowbased

import (
	"bytes"
	"context"
	"database/sql/driver"

	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

type recordReader interface {
	NewRecordFromBytes(arrowSchemaBytes []byte, sparkArrowBatch sparkArrowBatch) (arrow.Record, error)
}

type valueContainerMaker interface {
	makeColumnValuesContainers(ars *arrowRowScanner) error
}

type sparkArrowBatch struct {
	rowCount, startRow, endRow int64
	arrowRecordBytes           []byte
	index                      int
}

func (sab *sparkArrowBatch) contains(rowIndex int64) bool {
	return sab != nil && sab.startRow <= rowIndex && sab.endRow >= rowIndex
}

type timeStampFn func(arrow.Timestamp) time.Time

type colInfo struct {
	name      string
	arrowType arrow.DataType
	dbType    cli_service.TTypeId
}

// arrowRowScanner handles extracting values from arrow records
type arrowRowScanner struct {
	recordReader
	valueContainerMaker

	// configuration of different arrow options for retrieving results
	config.ArrowConfig

	// arrow batches returned by the thrift server
	arrowBatches []sparkArrowBatch

	// arrow schema corresponding to the TTableSchema
	arrowSchema *arrow.Schema

	// serialized form of arrow format schema
	arrowSchemaBytes []byte

	// database types for the columns
	colInfo []colInfo

	// number of rows in the current TRowSet
	nRows int64

	// a TRowSet contains multiple arrow batches
	currentBatch *sparkArrowBatch

	// Values for each column
	columnValues []columnValues

	// function to convert arrow timestamp when using native arrow format
	toTimestampFn timeStampFn

	// hold on to a logger instance with context, rather than just use the global variable
	*dbsqllog.DBSQLLogger

	location *time.Location
	ctx      context.Context
}

// Make sure arrowRowScanner fulfills the RowScanner interface
var _ rowscanner.RowScanner = (*arrowRowScanner)(nil)

// NewArrowRowScanner returns an instance of RowScanner which handles arrow format results
func NewArrowRowScanner(resultSetMetadata *cli_service.TGetResultSetMetadataResp, rowSet *cli_service.TRowSet, cfg *config.Config, logger *dbsqllog.DBSQLLogger, ctx context.Context) (rowscanner.RowScanner, dbsqlerr.DBError) {

	// we take a passed in logger, rather than just using the global from dbsqllog, so that the containing rows
	// instance can pass in a logger with context such as correlation ID and operation ID
	if logger == nil {
		logger = dbsqllog.Logger
	}

	logger.Debug().Msgf("databricks: creating arrow row scanner, nArrowBatches: %d", len(rowSet.ArrowBatches))

	var arrowConfig config.ArrowConfig
	if cfg != nil {
		arrowConfig = cfg.ArrowConfig
	}

	var arrowSchema *arrow.Schema
	schemaBytes := resultSetMetadata.ArrowSchema
	if schemaBytes == nil {
		var err error
		// convert the TTableSchema to an arrow Schema
		arrowSchema, err = tTableSchemaToArrowSchema(resultSetMetadata.Schema, &arrowConfig)
		if err != nil {
			logger.Err(err).Msg(errArrowRowsConvertSchema)
			return nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsConvertSchema, err)
		}

		// serialize the arrow schema
		schemaBytes, err = getArrowSchemaBytes(arrowSchema, ctx)
		if err != nil {
			logger.Err(err).Msg(errArrowRowsSerializeSchema)
			return nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsSerializeSchema, err)
		}
	} else {
		br := &chunkedByteReader{chunks: [][]byte{schemaBytes}}
		rdr, err := ipc.NewReader(br)
		if err != nil {
			return nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsUnableToReadBatch, err)
		}
		defer rdr.Release()

		arrowSchema = rdr.Schema()
	}

	// get the database type names for each column
	colInfos := make([]colInfo, len(resultSetMetadata.Schema.Columns))
	for i := range resultSetMetadata.Schema.Columns {
		col := resultSetMetadata.Schema.Columns[i]
		field := arrowSchema.Field(i)
		colInfos[i] = colInfo{name: field.Name, arrowType: field.Type, dbType: rowscanner.GetDBType(col)}
	}

	// get the function for converting arrow timestamps to a time.Time
	// time values from the server are returned as UTC with microsecond precision
	ttsf, err := arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType).GetToTimeFunc()
	if err != nil {
		logger.Err(err).Msg(errArrowRowsToTimestampFn)
		return nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsToTimestampFn, err)
	}

	arrowBatches := make([]sparkArrowBatch, len(rowSet.ArrowBatches))
	var startRow int64 = 0
	for i := range rowSet.ArrowBatches {
		rsab := rowSet.ArrowBatches[i]
		arrowBatches[i] = sparkArrowBatch{
			rowCount:         rsab.RowCount,
			startRow:         startRow,
			endRow:           startRow + rsab.RowCount - 1,
			arrowRecordBytes: rsab.Batch,
			index:            i,
		}
		startRow = startRow + rsab.RowCount
	}

	var location *time.Location = time.UTC
	if cfg != nil {
		if cfg.Location != nil {
			location = cfg.Location
		}
	}

	rs := &arrowRowScanner{
		recordReader:        sparkRecordReader{},
		valueContainerMaker: &arrowValueContainerMaker{},
		ArrowConfig:         arrowConfig,
		arrowBatches:        arrowBatches,
		nRows:               countRows(rowSet),
		arrowSchemaBytes:    schemaBytes,
		arrowSchema:         arrowSchema,
		toTimestampFn:       ttsf,
		colInfo:             colInfos,
		DBSQLLogger:         logger,
		location:            location,
		ctx:                 ctx,
	}

	return rs, nil
}

// Close is called when the Rows instance is closed.
func (ars *arrowRowScanner) Close() {
	// release any retained arrow arrays
	for i := range ars.columnValues {
		if ars.columnValues[i] != nil {
			ars.columnValues[i].Release()
		}
	}
}

// NRows returns the number of rows in the current set of batches
func (ars *arrowRowScanner) NRows() int64 {
	if ars != nil {
		return ars.nRows
	}

	return 0
}

var complexTypes map[cli_service.TTypeId]struct{} = map[cli_service.TTypeId]struct{}{
	cli_service.TTypeId_ARRAY_TYPE:  {},
	cli_service.TTypeId_MAP_TYPE:    {},
	cli_service.TTypeId_STRUCT_TYPE: {}}

var intervalTypes map[cli_service.TTypeId]struct{} = map[cli_service.TTypeId]struct{}{
	cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE:   {},
	cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE: {}}

// ScanRow is called to populate the provided slice with the
// content of the current row. The provided slice will be the same
// size as the number of columns.
// The dest should not be written to outside of ScanRow. Care
// should be taken when closing a RowScanner not to modify
// a buffer held in dest.
func (ars *arrowRowScanner) ScanRow(
	destination []driver.Value,
	rowIndex int64) dbsqlerr.DBError {

	// load the error batch for the specified row, if necessary
	err := ars.loadBatchFor(rowIndex)
	if err != nil {
		return err
	}

	var rowInBatchIndex int = int(rowIndex - ars.currentBatch.startRow)

	// if no location is provided default to UTC
	if ars.location == nil {
		ars.location = time.UTC
	}

	nCols := len(ars.columnValues)

	// loop over the destination slice filling in values
	for i := range destination {
		// clear the destination
		destination[i] = nil

		// if there is a corresponding column and the value for the specified row
		// is not null we put the value in the destination
		if i < nCols && !ars.columnValues[i].IsNull(rowInBatchIndex) {

			col := ars.colInfo[i]
			dbType := col.dbType

			if (dbType == cli_service.TTypeId_DECIMAL_TYPE && ars.UseArrowNativeDecimal) ||
				(isIntervalType(dbType) && ars.UseArrowNativeIntervalTypes) {
				//	not yet fully supported
				ars.Error().Msgf(errArrowRowsUnsupportedNativeType(dbType.String()))
				return dbsqlerrint.NewDriverError(ars.ctx, errArrowRowsUnsupportedNativeType(dbType.String()), nil)
			}

			// get the value from the column values holder
			var err1 error
			destination[i], err1 = ars.columnValues[i].Value(rowInBatchIndex)
			if err1 != nil {
				err = dbsqlerrint.NewDriverError(ars.ctx, errArrowRowsColumnValue(col.name), err1)
			}
		}
	}

	return err
}

func isIntervalType(typeId cli_service.TTypeId) bool {
	_, ok := intervalTypes[typeId]
	return ok
}

// countRows returns the number of rows in the TRowSet
func countRows(rowSet *cli_service.TRowSet) int64 {
	if rowSet != nil && rowSet.ArrowBatches != nil {
		batches := rowSet.ArrowBatches
		var n int64
		for i := range batches {
			n += batches[i].RowCount
		}

		return n
	}

	return 0
}

// loadBatchFor loads the batch containing the specified row if necessary
func (ars *arrowRowScanner) loadBatchFor(rowIndex int64) dbsqlerr.DBError {

	// if we haven't loaded the initial batch or the row is not in the current batch
	// we need to load a different batch
	if ars.columnValues == nil || !ars.currentBatch.contains(rowIndex) {
		batchIndex, err := ars.rowIndexToBatchIndex(rowIndex)
		if err != nil {
			return err
		}

		ars.Debug().Msgf("databricks: loading arrow batch, rowIndex: %d, batchIndex: %d", rowIndex, batchIndex)

		err = ars.loadBatch(batchIndex)
		if err != nil {
			return err
		}
	}

	return nil
}

// loadBatch loads the arrow batch at the specified index
func (ars *arrowRowScanner) loadBatch(batchIndex int) dbsqlerr.DBError {
	if ars == nil || ars.arrowBatches == nil {
		if ars != nil {
			ars.Error().Msg(errArrowRowsNoArrowBatches)
			return dbsqlerrint.NewDriverError(ars.ctx, errArrowRowsNoArrowBatches, nil)
		}
		return dbsqlerrint.NewDriverError(context.Background(), errArrowRowsNoArrowBatches, nil)
	}

	// if the batch already loaded we can just return
	if ars.currentBatch != nil && ars.currentBatch.index == batchIndex && ars.columnValues != nil {
		return nil
	}

	if batchIndex < 0 || batchIndex >= len(ars.arrowBatches) {
		ars.Error().Msg(errArrowRowsInvalidBatchIndex(batchIndex, len(ars.arrowBatches)))
		return dbsqlerrint.NewDriverError(ars.ctx, errArrowRowsInvalidBatchIndex(batchIndex, len(ars.arrowBatches)), nil)
	}

	// set up the column values containers
	if ars.columnValues == nil {
		err := ars.makeColumnValuesContainers(ars)
		if err != nil {
			return dbsqlerrint.NewDriverError(ars.ctx, errArrowRowsMakeColumnValueContainers, err)
		}
	}

	r, err := ars.NewRecordFromBytes(ars.arrowSchemaBytes, ars.arrowBatches[batchIndex])
	if err != nil {
		ars.Err(err).Msg(errArrowRowsUnableToReadBatch)
		return dbsqlerrint.NewDriverError(ars.ctx, errArrowRowsUnableToReadBatch, err)
	}

	defer r.Release()

	// for each column we want to create an arrow array specific to the data type
	for i, col := range r.Columns() {
		col.Retain()
		defer col.Release()

		colData := col.Data()
		colData.Retain()
		defer colData.Release()

		colValsHolder := ars.columnValues[i]

		// release the arrow array already held
		colValsHolder.Release()

		err := colValsHolder.SetValueArray(colData)
		if err != nil {
			ars.Error().Msg(err.Error())
		}
	}

	ars.currentBatch = &ars.arrowBatches[batchIndex]

	return nil
}

// getArrowSchemaBytes returns the serialized schema in ipc format
func getArrowSchemaBytes(schema *arrow.Schema, ctx context.Context) ([]byte, dbsqlerr.DBError) {
	if schema == nil {
		return nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsNilArrowSchema, nil)
	}

	var output bytes.Buffer
	w := ipc.NewWriter(&output, ipc.WithSchema(schema))
	err := w.Close()
	if err != nil {
		return nil, dbsqlerrint.NewDriverError(ctx, errArrowRowsUnableToWriteArrowSchema, err)
	}

	arrowSchemaBytes := output.Bytes()

	// the writer serializes to an arrow batch but we just want the
	// schema bytes so we strip off the empty Record at the end
	arrowSchemaBytes = arrowSchemaBytes[:len(arrowSchemaBytes)-8]

	return arrowSchemaBytes, nil
}

// rowIndexToBatchIndex returns the index of the batch containing the specified row
func (ars *arrowRowScanner) rowIndexToBatchIndex(rowIndex int64) (int, dbsqlerr.DBError) {

	for i := range ars.arrowBatches {
		if ars.arrowBatches[i].contains(rowIndex) {
			return i, nil
		}
	}

	ars.Error().Msg(errArrowRowsInvalidRowIndex(rowIndex))
	return -1, dbsqlerrint.NewDriverError(ars.ctx, errArrowRowsInvalidRowIndex(rowIndex), nil)
}

// tTableSchemaToArrowSchema convers the TTableSchema retrieved by the thrift server into an arrow.Schema instance
func tTableSchemaToArrowSchema(schema *cli_service.TTableSchema, arrowConfig *config.ArrowConfig) (*arrow.Schema, error) {
	columns := schema.GetColumns()
	fields := make([]arrow.Field, len(columns))

	for i := range columns {
		field, err := tColumnDescToArrowField(columns[i], arrowConfig)
		if err != nil {
			return nil, err
		}

		fields[i] = field
	}

	arrowSchema := arrow.NewSchema(fields, nil)

	return arrowSchema, nil
}

// map the thrift data types to the corresponding arrow data type
var toArrowTypeMap map[cli_service.TTypeId]arrow.DataType = map[cli_service.TTypeId]arrow.DataType{
	cli_service.TTypeId_BOOLEAN_TYPE:  arrow.FixedWidthTypes.Boolean,
	cli_service.TTypeId_TINYINT_TYPE:  arrow.PrimitiveTypes.Int8,
	cli_service.TTypeId_SMALLINT_TYPE: arrow.PrimitiveTypes.Int16,
	cli_service.TTypeId_INT_TYPE:      arrow.PrimitiveTypes.Int32,
	cli_service.TTypeId_BIGINT_TYPE:   arrow.PrimitiveTypes.Int64,
	cli_service.TTypeId_FLOAT_TYPE:    arrow.PrimitiveTypes.Float32,
	cli_service.TTypeId_DOUBLE_TYPE:   arrow.PrimitiveTypes.Float64,
	cli_service.TTypeId_STRING_TYPE:   arrow.BinaryTypes.String,
	// cli_service.TTypeId_TIMESTAMP_TYPE:    see tColumnDescToArrowDataType
	cli_service.TTypeId_BINARY_TYPE: arrow.BinaryTypes.Binary,
	// cli_service.TTypeId_ARRAY_TYPE:        see tColumnDescToArrowDataType
	// cli_service.TTypeId_MAP_TYPE:          see tColumnDescToArrowDataType
	// cli_service.TTypeId_STRUCT_TYPE:       see tColumnDescToArrowDataType
	cli_service.TTypeId_UNION_TYPE:        arrow.BinaryTypes.String,
	cli_service.TTypeId_USER_DEFINED_TYPE: arrow.BinaryTypes.String,
	// cli_service.TTypeId_DECIMAL_TYPE:  see tColumnDescToArrowDataType
	cli_service.TTypeId_NULL_TYPE:    arrow.Null,
	cli_service.TTypeId_DATE_TYPE:    arrow.FixedWidthTypes.Date32,
	cli_service.TTypeId_VARCHAR_TYPE: arrow.BinaryTypes.String,
	cli_service.TTypeId_CHAR_TYPE:    arrow.BinaryTypes.String,
	// cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE: see tColumnDescToArrowDataType
	// cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE:   see tColumnDescToArrowDataType
}

func tColumnDescToArrowDataType(tColumnDesc *cli_service.TColumnDesc, arrowConfig *config.ArrowConfig) (arrow.DataType, error) {
	// get the thrift type id
	tType := rowscanner.GetDBTypeID(tColumnDesc)

	if at, ok := toArrowTypeMap[tType]; ok {
		// simple type mapping
		return at, nil
	} else {
		// for some types there isn't a simple 1:1 correspondence to an arrow data type
		if tType == cli_service.TTypeId_DECIMAL_TYPE {
			// if not using arrow native decimal type decimals are returned as strings
			if !arrowConfig.UseArrowNativeDecimal {
				return arrow.BinaryTypes.String, nil
			}

			// Need to construct an instance of arrow DecimalType with the
			// correct scale and precision
			scale, precision, err := getDecimalScalePrecision(tColumnDesc)
			if err != nil {
				return nil, err
			}

			decimalType, err := arrow.NewDecimalType(arrow.DECIMAL128, precision, scale)
			if err != nil {
				return nil, dbsqlerrint.WrapErr(err, errArrowRowsUnableToCreateDecimalType(scale, precision))
			}

			return decimalType, nil

		} else if tType == cli_service.TTypeId_TIMESTAMP_TYPE {
			// if not using arrow native timestamps thrift server returns strings
			if !arrowConfig.UseArrowNativeTimestamp {
				return arrow.BinaryTypes.String, nil
			}

			// timestamp is UTC with microsecond precision
			return arrow.FixedWidthTypes.Timestamp_us, nil
		} else if _, ok := complexTypes[tType]; ok {
			// if not using arrow native complex types thrift server returns strings
			if !arrowConfig.UseArrowNativeComplexTypes {
				return arrow.BinaryTypes.String, nil
			}

			return nil, errors.New(errArrowRowsUnsupportedWithHiveSchema(rowscanner.GetDBTypeName(tColumnDesc)))
		} else if _, ok := intervalTypes[tType]; ok {
			// if not using arrow native complex types thrift server returns strings
			if !arrowConfig.UseArrowNativeIntervalTypes {
				return arrow.BinaryTypes.String, nil
			}

			return nil, errors.New(errArrowRowsUnsupportedWithHiveSchema(rowscanner.GetDBTypeName(tColumnDesc)))
		} else {
			return nil, errors.New(errArrowRowsUnknownDBType)
		}
	}

}

func getDecimalScalePrecision(tColumnDesc *cli_service.TColumnDesc) (scale, precision int32, err error) {
	fail := errors.New(errArrowRowsInvalidDecimalType)

	typeQualifiers := rowscanner.GetDBTypeQualifiers(tColumnDesc)
	if typeQualifiers == nil || typeQualifiers.Qualifiers == nil {
		err = fail
		return
	}

	scaleHolder, ok := typeQualifiers.Qualifiers["scale"]
	if !ok || scaleHolder == nil || scaleHolder.I32Value == nil {
		err = fail
		return
	} else {
		scale = *scaleHolder.I32Value
	}

	precisionHolder, ok := typeQualifiers.Qualifiers["precision"]
	if !ok || precisionHolder == nil || precisionHolder.I32Value == nil {
		err = fail
		return
	} else {
		precision = *precisionHolder.I32Value
	}

	return
}

func tColumnDescToArrowField(columnDesc *cli_service.TColumnDesc, arrowConfig *config.ArrowConfig) (arrow.Field, error) {
	arrowDataType, err := tColumnDescToArrowDataType(columnDesc, arrowConfig)
	if err != nil {
		return arrow.Field{}, err
	}

	arrowField := arrow.Field{
		Name: columnDesc.ColumnName,
		Type: arrowDataType,
	}

	return arrowField, nil
}

type sparkRecordReader struct{}

// Make sure sparkRecordReader fulfills the recordReader interface
var _ recordReader = (*sparkRecordReader)(nil)

func (srr sparkRecordReader) NewRecordFromBytes(arrowSchemaBytes []byte, sparkArrowBatch sparkArrowBatch) (arrow.Record, error) {
	// The arrow batches returned from the thrift server are actually a serialized arrow Record
	// an arrow batch should consist of a Schema and at least one Record.
	// Use a chunked byte reader to concatenate the schema bytes and the record bytes without
	// having to allocate/copy slices.

	br := &chunkedByteReader{chunks: [][]byte{arrowSchemaBytes, sparkArrowBatch.arrowRecordBytes}}
	rdr, err := ipc.NewReader(br)
	if err != nil {
		return nil, err
	}
	defer rdr.Release()

	r, err := rdr.Read()
	if err != nil {
		return nil, err
	}

	r.Retain()

	return r, nil
}

type arrowValueContainerMaker struct{}

var _ valueContainerMaker = (*arrowValueContainerMaker)(nil)

// makeColumnValuesContainers creates appropriately typed  column values holders for each column
func (vcm *arrowValueContainerMaker) makeColumnValuesContainers(ars *arrowRowScanner) error {
	if ars.columnValues == nil {
		ars.columnValues = make([]columnValues, len(ars.colInfo))
		for i, field := range ars.arrowSchema.Fields() {
			holder, err := vcm.makeColumnValueContainer(field.Type, ars.location, ars.toTimestampFn, &ars.colInfo[i])
			if err != nil {
				ars.Error().Msg(err.Error())
				return err
			}

			ars.columnValues[i] = holder
		}
	}
	return nil
}

func (vcm *arrowValueContainerMaker) makeColumnValueContainer(t arrow.DataType, location *time.Location, toTimestampFn timeStampFn, colInfo *colInfo) (columnValues, error) {
	if location == nil {
		location = time.UTC
	}

	switch t := t.(type) {

	case *arrow.BooleanType:
		return &columnValuesTyped[*array.Boolean, bool]{}, nil

	case *arrow.Int8Type:
		return &columnValuesTyped[*array.Int8, int8]{}, nil

	case *arrow.Int16Type:
		return &columnValuesTyped[*array.Int16, int16]{}, nil

	case *arrow.Int32Type:
		return &columnValuesTyped[*array.Int32, int32]{}, nil

	case *arrow.Int64Type:
		return &columnValuesTyped[*array.Int64, int64]{}, nil

	case *arrow.Float32Type:
		return &columnValuesTyped[*array.Float32, float32]{}, nil

	case *arrow.Float64Type:
		return &columnValuesTyped[*array.Float64, float64]{}, nil

	case *arrow.StringType:
		if colInfo != nil && colInfo.dbType == cli_service.TTypeId_TIMESTAMP_TYPE {
			return &timestampStringValueContainer{location: location, fieldName: colInfo.name}, nil
		} else {
			return &columnValuesTyped[*array.String, string]{}, nil
		}

	case *arrow.Decimal128Type:
		return &decimal128Container{scale: t.Scale}, nil

	case *arrow.Date32Type:
		return &dateValueContainer{location: location}, nil

	case *arrow.TimestampType:
		return &timestampValueContainer{location: location, toTimestampFn: toTimestampFn}, nil

	case *arrow.BinaryType:
		return &columnValuesTyped[*array.Binary, []byte]{}, nil

	case *arrow.ListType:
		lvc := &listValueContainer{listArrayType: t}
		var err error
		lvc.values, err = vcm.makeColumnValueContainer(t.Elem(), location, toTimestampFn, nil)
		if err != nil {
			return nil, err
		}
		switch t.Elem().(type) {
		case *arrow.MapType, *arrow.ListType, *arrow.StructType:
			lvc.complexValue = true
		}
		return lvc, nil

	case *arrow.MapType:
		mvc := &mapValueContainer{mapArrayType: t}
		var err error
		mvc.values, err = vcm.makeColumnValueContainer(t.ItemType(), location, toTimestampFn, nil)
		if err != nil {
			return nil, err
		}
		mvc.keys, err = vcm.makeColumnValueContainer(t.KeyType(), location, toTimestampFn, nil)
		if err != nil {
			return nil, err
		}
		switch t.ItemType().(type) {
		case *arrow.MapType, *arrow.ListType, *arrow.StructType:
			mvc.complexValue = true
		}

		return mvc, nil

	case *arrow.StructType:
		svc := &structValueContainer{structArrayType: t}
		svc.fieldNames = make([]string, len(t.Fields()))
		svc.fieldValues = make([]columnValues, len(t.Fields()))
		svc.complexValue = make([]bool, len(t.Fields()))
		for i, f := range t.Fields() {
			svc.fieldNames[i] = f.Name
			c, err := vcm.makeColumnValueContainer(f.Type, location, toTimestampFn, nil)
			if err != nil {
				return nil, err
			}
			svc.fieldValues[i] = c
			switch f.Type.(type) {
			case *arrow.MapType, *arrow.ListType, *arrow.StructType:
				svc.complexValue[i] = true
			}

		}

		return svc, nil

	case *arrow.NullType:
		return nullContainer, nil

	default:
		return nil, errors.Errorf(errArrowRowsUnhandledArrowType(t.String()))
	}
}
