package arrowbased

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/apache/arrow/go/v11/arrow/ipc"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerr "github.com/databricks/databricks-sql-go/internal/err"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
)

var errArrowRowsUnsupportedNativeType = "databricks: arrow native values not yet supported for %s"
var errArrowRowsInvalidBatchIndex = "databricks: invalid arrow batch index index = %d, n = %d"
var errArrowRowsNoArrowBatches = "databricks: result set contains 0 arrow batches"
var errArrowRowsUnableToReadBatch = "databricks: unable to read arrow batch"
var errArrowRowsNilArrowSchema = "databricks: nil arrow.Schema"
var errArrowRowsUnableToWriteArrowSchema = "databricks: unable to write arrow schema"
var errArrowRowsInvalidRowIndex = "databricks: row index %d is not contained in any arrow batch"
var errArrowRowsInvalidDecimalType = "databricks: decimal type with no scale/precision"
var errArrowRowsUnableToCreateDecimalType = "databricks: unable to create decimal type scale: %d, precision: %d"
var errChunkedByteReaderInvalidState1 = "databricks: chunkedByteReader invalid state chunkIndex:%d, byteIndex:%d"
var errChunkedByteReaderOverreadOfNonterminalChunk = "databricks: chunkedByteReader invalid state chunks:%d chunkIndex:%d len:%d byteIndex%d"

// arrowRowScanner handles extracting values from arrow records
type arrowRowScanner struct {
	// configuration of different arrow options for retrieving results
	config.ArrowConfig

	// arrow batches returned by the thrift server
	arrowBatches []*cli_service.TSparkArrowBatch

	// arrow schema corresponding to the TTableSchema
	arrowSchema *arrow.Schema

	// serialized form of arrow format schema
	arrowSchemaBytes []byte

	// database type names for the columns
	colDBTypeNames []string

	// number of rows in the current TRowSet
	nRows int64

	// a TRowSet contains multiple arrow batches
	currentBatchIndex int

	// range of rows in each arrow batch
	batchRanges []batchRange

	// Values for each column
	columnValues []columnValues

	// function to convert arrow timestamp when using native arrow format
	toTimestampFn func(arrow.Timestamp) time.Time

	*dbsqllog.DBSQLLogger
}

// Make sure arrowRowScanner fulfills the RowScanner interface
var _ rowscanner.RowScanner = (*arrowRowScanner)(nil)

// NewArrowRowScanner returns an instance of RowScanner which handles arrow format results
func NewArrowRowScanner(schema *cli_service.TTableSchema, rowSet *cli_service.TRowSet, config *config.Config, logger *dbsqllog.DBSQLLogger) (rowscanner.RowScanner, error) {

	if logger == nil {
		logger = dbsqllog.Logger
	}

	logger.Debug().Msgf("databricks: creating arrow row scanner, nArrowBatches: %d", len(rowSet.ArrowBatches))

	// convert the TTableSchema to an arrow Schema
	arrowSchema, err := tTableSchemaToArrowSchema(schema, &config.ArrowConfig)
	if err != nil {
		logger.Err(err).Msg("databricks: arrow row scanner failed to convert schema")
		return nil, err
	}

	// serialize the arrow schema
	schemaBytes, err := getArrowSchemaBytes(arrowSchema)
	if err != nil {
		logger.Err(err).Msg("databricks: arrow row scanner failed to serialize schema")
		return nil, err
	}

	// get the database type names for each column
	colDBTypes := make([]string, len(schema.Columns))
	for i := range schema.Columns {
		colDBTypes[i] = rowscanner.GetDBTypeName(schema.Columns[i])
	}

	// get the function for converting arrow timestamps to a time.Time
	// time values from the server are returned as UTC with microsecond precision
	ttsf, err := arrow.FixedWidthTypes.Timestamp_us.(*arrow.TimestampType).GetToTimeFunc()
	if err != nil {
		logger.Err(err).Msg("databricks: arrow row scanner failed getting toTimestamp function")
		err = dbsqlerr.WrapErr(err, "databricks: arrow row scanner failed getting toTimestamp function")
		return nil, err
	}

	rs := &arrowRowScanner{
		ArrowConfig:      config.ArrowConfig,
		arrowBatches:     rowSet.ArrowBatches,
		nRows:            countRows(rowSet),
		arrowSchemaBytes: schemaBytes,
		arrowSchema:      arrowSchema,
		toTimestampFn:    ttsf,
		colDBTypeNames:   colDBTypes,
		DBSQLLogger:      logger,
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

var complexTypes map[string]struct{} = map[string]struct{}{"ARRAY": {}, "MAP": {}, "STRUCT": {}}
var intervalTypes map[string]struct{} = map[string]struct{}{"INTERVAL_YEAR_MONTH": {}, "INTERVAL_DAY_TIME": {}}

// ScanRow is called to populate the provided slice with the
// content of the current row. The provided slice will be the same
// size as the number of columns.
// The dest should not be written to outside of ScanRow. Care
// should be taken when closing a RowScanner not to modify
// a buffer held in dest.
func (ars *arrowRowScanner) ScanRow(
	destination []driver.Value,
	rowIndex int64,
	location *time.Location) (err error) {

	// load the error batch for the specified row, if necessary
	err = ars.loadBatchFor(rowIndx(rowIndex))
	if err != nil {
		return err
	}

	// if no location is provided default to UTC
	if location == nil {
		location = time.UTC
	}

	nCols := len(ars.columnValues)

	// loop over the destination slice filling in values
	for i := range destination {
		// clear the destination
		destination[i] = nil

		// if there is a corresponding column and the value for the specified row
		// is not null we put the value in the destination
		if err == nil && i < nCols && !ars.columnValues[i].IsNull(int(rowIndex)) {

			// get the value from the column values holder
			var val any = ars.columnValues[i].Value(int(rowIndex))

			if ars.colDBTypeNames[i] == "DATE" {
				// Need to convert the arrow Date32 value to time.Time
				val = val.(arrow.Date32).ToTime().In(location)
			} else if ars.colDBTypeNames[i] == "TIMESTAMP" {
				if ars.UseArrowNativeTimestamp {
					// Need to convert the arrow timestamp value to a time.Time
					val = ars.toTimestampFn(val.(arrow.Timestamp)).In(location)
				} else {
					// Timestamp is returned as a string so need to parse to time.Time
					val, err = rowscanner.HandleDateTime(val, "TIMESTAMP", ars.arrowSchema.Fields()[i].Name, location)
					if err != nil {
						ars.Err(err).Msg("databrics: arrow row scanner failed to parse date/time")
					}
				}
			}

			destination[i] = val
		} else if ars.colDBTypeNames[i] == "DECIMAL" && ars.UseArrowNativeDecimal {
			//	not yet fully supported
			ars.Error().Msgf(errArrowRowsUnsupportedNativeType, ars.colDBTypeNames[i])
			err = errors.Errorf(errArrowRowsUnsupportedNativeType, ars.colDBTypeNames[i])
		} else if _, ok := complexTypes[ars.colDBTypeNames[i]]; ok && ars.UseArrowNativeComplexTypes {
			//	not yet fully supported
			ars.Error().Msgf(errArrowRowsUnsupportedNativeType, ars.colDBTypeNames[i])
			err = errors.Errorf(errArrowRowsUnsupportedNativeType, ars.colDBTypeNames[i])
		} else if _, ok := intervalTypes[ars.colDBTypeNames[i]]; ok && ars.UseArrowNativeIntervalTypes {
			//	not yet fully supported
			ars.Error().Msgf(errArrowRowsUnsupportedNativeType, ars.colDBTypeNames[i])
			err = errors.Errorf(errArrowRowsUnsupportedNativeType, ars.colDBTypeNames[i])
		}
	}

	return err
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
func (ars *arrowRowScanner) loadBatchFor(rowIndex rowIndx) error {

	// if we haven't loaded the initial batch or the row is not in the current batch
	// we need to load a different batch
	if ars.columnValues == nil || !rowIndex.in(ars.getBatchRanges()[ars.currentBatchIndex]) {
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
func (ars *arrowRowScanner) loadBatch(batchIndex int) error {
	if ars == nil || ars.arrowBatches == nil {
		ars.Error().Msg(errArrowRowsNoArrowBatches)
		return errors.New(errArrowRowsNoArrowBatches)
	}

	// if the batch already loaded we can just return
	if ars.currentBatchIndex == batchIndex && ars.columnValues != nil {
		return nil
	}

	if batchIndex < 0 || batchIndex >= len(ars.arrowBatches) {
		ars.Error().Msgf(errArrowRowsInvalidBatchIndex, batchIndex, len(ars.arrowBatches))
		return errors.Errorf(errArrowRowsInvalidBatchIndex, batchIndex, len(ars.arrowBatches))
	}

	// set up the column values containers
	if ars.columnValues == nil {
		makeColumnValues(ars)
	}

	// The arrow batches returned from the thrift server are actually a serialized arrow Record
	// an arrow batch should consist of a Schema and at least one Record.
	// Use a chunked byte reader to concatenate the schema bytes and the record bytes without
	// having to allocate/copy slices.
	schemaBytes := ars.arrowSchemaBytes

	br := &chunkedByteReader{chunks: [][]byte{schemaBytes, ars.arrowBatches[batchIndex].Batch}}
	rdr, err := ipc.NewReader(br)
	if err != nil {
		ars.Err(err).Msg(errArrowRowsUnableToReadBatch)
		return dbsqlerr.WrapErr(err, errArrowRowsUnableToReadBatch)
	}

	if !rdr.Next() {
		ars.Err(rdr.Err()).Msg(errArrowRowsUnableToReadBatch)
		return dbsqlerr.WrapErr(rdr.Err(), errArrowRowsUnableToReadBatch)
	}

	r := rdr.Record()
	r.Retain()
	defer r.Release()

	// for each column we want to create an arrow array specific to the data type
	for i, col := range r.Columns() {
		col.Retain()
		defer col.Release()

		colData := col.Data()
		colDataType := col.DataType()
		colValsHolder := ars.columnValues[i]

		// release the arrow array already held
		colValsHolder.Release()

		switch colDataType.(type) {
		case *arrow.BooleanType:
			colValsHolder.(*columnValuesTyped[*array.Boolean, bool]).holder = array.NewBooleanData(colData)

		case *arrow.Int8Type:
			colValsHolder.(*columnValuesTyped[*array.Int8, int8]).holder = array.NewInt8Data(colData)

		case *arrow.Int16Type:
			colValsHolder.(*columnValuesTyped[*array.Int16, int16]).holder = array.NewInt16Data(colData)

		case *arrow.Int32Type:
			colValsHolder.(*columnValuesTyped[*array.Int32, int32]).holder = array.NewInt32Data(colData)

		case *arrow.Int64Type:
			colValsHolder.(*columnValuesTyped[*array.Int64, int64]).holder = array.NewInt64Data(colData)

		case *arrow.Float32Type:
			colValsHolder.(*columnValuesTyped[*array.Float32, float32]).holder = array.NewFloat32Data(colData)

		case *arrow.Float64Type:
			colValsHolder.(*columnValuesTyped[*array.Float64, float64]).holder = array.NewFloat64Data(colData)

		case *arrow.StringType:
			colValsHolder.(*columnValuesTyped[*array.String, string]).holder = array.NewStringData(colData)

		case *arrow.Decimal128Type:
			colValsHolder.(*columnValuesTyped[*array.Decimal128, decimal128.Num]).holder = array.NewDecimal128Data(colData)

		case *arrow.Date32Type:
			colValsHolder.(*columnValuesTyped[*array.Date32, arrow.Date32]).holder = array.NewDate32Data(colData)

		case *arrow.TimestampType:
			colValsHolder.(*columnValuesTyped[*array.Timestamp, arrow.Timestamp]).holder = array.NewTimestampData(colData)

		case *arrow.BinaryType:
			colValsHolder.(*columnValuesTyped[*array.Binary, []byte]).holder = array.NewBinaryData(colData)

		default:
			ars.Warn().Msgf("databricks: arrow row scanner unhandled type %s", colDataType.String())

		}
	}

	ars.currentBatchIndex = batchIndex

	return nil
}

// getArrowSchemaBytes returns the serialized schema in ipc format
func getArrowSchemaBytes(schema *arrow.Schema) ([]byte, error) {
	if schema == nil {
		return nil, errors.New(errArrowRowsNilArrowSchema)
	}

	var output bytes.Buffer
	w := ipc.NewWriter(&output, ipc.WithSchema(schema))
	err := w.Close()
	if err != nil {
		return nil, dbsqlerr.WrapErr(err, errArrowRowsUnableToWriteArrowSchema)
	}

	arrowSchemaBytes := output.Bytes()

	// the writer serializes to an arrow batch but we just want the
	// schema bytes so we strip off the empty Record at the end
	arrowSchemaBytes = arrowSchemaBytes[:len(arrowSchemaBytes)-8]

	return arrowSchemaBytes, nil
}

// rowIndexToBatchIndex returns the index of the batch containing the specified row
func (ars *arrowRowScanner) rowIndexToBatchIndex(rowIndex rowIndx) (int, error) {
	ranges := ars.getBatchRanges()
	for i := range ranges {
		if rowIndex.in(ranges[i]) {
			return i, nil
		}
	}

	ars.Error().Msgf(errArrowRowsInvalidRowIndex, rowIndex)
	return -1, errors.Errorf(errArrowRowsInvalidRowIndex, rowIndex)
}

// getBatchRanges does a one time calculation of the row range in each arrow batch
// and returns the ranges
func (ars *arrowRowScanner) getBatchRanges() []batchRange {
	if ars == nil || ars.arrowBatches == nil {
		return []batchRange{}
	}

	if ars.batchRanges == nil {
		var batchOffset rowIndx = 0
		batches := ars.arrowBatches
		ars.batchRanges = make([]batchRange, len(batches))

		for i := range batches {
			rowCount := rowIndx(batches[i].RowCount)
			ars.batchRanges[i] = batchRange{batchOffset, batchOffset + rowCount - 1}
			batchOffset += rowIndx(rowCount)
		}
	}

	return ars.batchRanges
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
	cli_service.TTypeId_BINARY_TYPE:       arrow.BinaryTypes.Binary,
	cli_service.TTypeId_ARRAY_TYPE:        arrow.BinaryTypes.String,
	cli_service.TTypeId_MAP_TYPE:          arrow.BinaryTypes.String,
	cli_service.TTypeId_STRUCT_TYPE:       arrow.BinaryTypes.String,
	cli_service.TTypeId_UNION_TYPE:        arrow.BinaryTypes.String,
	cli_service.TTypeId_USER_DEFINED_TYPE: arrow.BinaryTypes.String,
	// cli_service.TTypeId_DECIMAL_TYPE:  see tColumnDescToArrowDataType
	cli_service.TTypeId_NULL_TYPE:                arrow.Null,
	cli_service.TTypeId_DATE_TYPE:                arrow.FixedWidthTypes.Date32,
	cli_service.TTypeId_VARCHAR_TYPE:             arrow.BinaryTypes.String,
	cli_service.TTypeId_CHAR_TYPE:                arrow.BinaryTypes.String,
	cli_service.TTypeId_INTERVAL_YEAR_MONTH_TYPE: arrow.BinaryTypes.String,
	cli_service.TTypeId_INTERVAL_DAY_TIME_TYPE:   arrow.BinaryTypes.String,
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
			typeQualifiers := rowscanner.GetDBTypeQualifiers(tColumnDesc)
			if typeQualifiers == nil || typeQualifiers.Qualifiers == nil {
				return nil, errors.New(errArrowRowsInvalidDecimalType)
			}

			scale, ok := typeQualifiers.Qualifiers["scale"]
			if !ok {
				return nil, errors.New(errArrowRowsInvalidDecimalType)
			}

			precision, ok := typeQualifiers.Qualifiers["precision"]
			if !ok {
				return nil, errors.New(errArrowRowsInvalidDecimalType)
			}

			decimalType, err := arrow.NewDecimalType(arrow.DECIMAL128, *scale.I32Value, *precision.I32Value)
			if err != nil {
				return nil, dbsqlerr.WrapErr(err, fmt.Sprintf(errArrowRowsUnableToCreateDecimalType, *scale.I32Value, *precision.I32Value))
			}

			return decimalType, nil

		} else if tType == cli_service.TTypeId_TIMESTAMP_TYPE {
			// if not using arrow native timestamps thrift server returns strings
			if !arrowConfig.UseArrowNativeTimestamp {
				return arrow.BinaryTypes.String, nil
			}

			// timestamp is UTC with microsecond precision
			return arrow.FixedWidthTypes.Timestamp_us, nil
		} else {
			return nil, errors.New("unknown data type when converting to arrow type")
		}
	}

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

type rowIndx int64
type batchRange [2]rowIndx

func (ri rowIndx) in(rng batchRange) bool {
	return ri >= rng[0] && ri <= rng[1]
}

// columnValues is the interface for accessing the values for a column
type columnValues interface {
	Value(int) any
	IsNull(int) bool
	Release()
}

// a type constraint for the value types which we handle that are returned in the arrow records
type valueTypes interface {
	bool |
		int8 |
		int16 |
		int32 |
		int64 |
		float32 |
		float64 |
		string |
		arrow.Date32 |
		[]byte |
		decimal128.Num |
		arrow.Timestamp
}

// a type constraint for the arrow array types which we handle that are returned in the arrow records
type arrowArrayTypes interface {
	*array.Boolean |
		*array.Int8 |
		*array.Int16 |
		*array.Int32 |
		*array.Int64 |
		*array.Float32 |
		*array.Float64 |
		*array.String |
		*array.Date32 |
		*array.Binary |
		*array.Decimal128 |
		*array.Timestamp
}

// type constraint for wrapping arrow arrays
type columnValuesHolder[T valueTypes] interface {
	arrowArrayTypes
	Value(int) T
	IsNull(int) bool
	Release()
}

// a generic container for the arrow arrays/value types we handle
type columnValuesTyped[ValueHolderType columnValuesHolder[ValueType], ValueType valueTypes] struct {
	holder ValueHolderType
}

// return the value for the specified row
func (cv *columnValuesTyped[X, T]) Value(rowNum int) any {
	return cv.holder.Value(rowNum)
}

// return true if the value at rowNum is null
func (cv *columnValuesTyped[X, T]) IsNull(rowNum int) bool {
	return cv.holder.IsNull(rowNum)
}

// release the the contained arrow array
func (cv *columnValuesTyped[X, T]) Release() {
	if cv.holder != nil {
		cv.holder.Release()
	}
}

var _ columnValues = (*columnValuesTyped[*array.Int16, int16])(nil)

// makeColumnValues creates appropriately typed  column values holders for each column
func makeColumnValues(ars *arrowRowScanner) {
	if ars.columnValues == nil {
		ars.columnValues = make([]columnValues, len(ars.arrowSchema.Fields()))
		for i, field := range ars.arrowSchema.Fields() {
			switch field.Type.(type) {

			case *arrow.BooleanType:
				ars.columnValues[i] = &columnValuesTyped[*array.Boolean, bool]{}

			case *arrow.Int8Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Int8, int8]{}

			case *arrow.Int16Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Int16, int16]{}

			case *arrow.Int32Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Int32, int32]{}

			case *arrow.Int64Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Int64, int64]{}

			case *arrow.Float32Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Float32, float32]{}

			case *arrow.Float64Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Float64, float64]{}

			case *arrow.StringType:
				ars.columnValues[i] = &columnValuesTyped[*array.String, string]{}

			case *arrow.Decimal128Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Decimal128, decimal128.Num]{}

			case *arrow.Date32Type:
				ars.columnValues[i] = &columnValuesTyped[*array.Date32, arrow.Date32]{}

			case *arrow.TimestampType:
				ars.columnValues[i] = &columnValuesTyped[*array.Timestamp, arrow.Timestamp]{}

			case *arrow.BinaryType:
				ars.columnValues[i] = &columnValuesTyped[*array.Binary, []byte]{}
			}
		}
	}
}

// chunkedByteReader implements the io.Reader interface on a collection
// of byte arrays.
// The TSparkArrowBatch instances returned in TFetchResultsResp contain
// a byte array containing an ipc formatted MessageRecordBatch message.
// The ipc reader expects a bytestream containing a MessageSchema message
// followed by a MessageRecordBatch message.
// chunkedByteReader is used to avoid allocating new byte arrays for each
// TSparkRecordBatch and copying the schema and record bytes.
type chunkedByteReader struct {
	// byte slices to be read as a single slice
	chunks [][]byte
	// index of the chunk being read from
	chunkIndex int
	// index in the current chunk
	byteIndex int
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.
//
// When Read encounters an error or end-of-file condition after
// successfully reading n > 0 bytes, it returns the number of
// bytes read and a non-nil error.  If len(p) is zero Read will
// return 0, nil
//
// Callers should always process the n > 0 bytes returned before
// considering the error err. Doing so correctly handles I/O errors
// that happen after reading some bytes.
func (c *chunkedByteReader) Read(p []byte) (bytesRead int, err error) {
	err = c.isValid()

	for err == nil && bytesRead < len(p) && !c.isEOF() {
		chunk := c.chunks[c.chunkIndex]
		chunkLen := len(chunk)
		source := chunk[c.byteIndex:]

		n := copy(p[bytesRead:], source)
		bytesRead += n

		c.byteIndex += n

		if c.byteIndex >= chunkLen {
			c.byteIndex = 0
			c.chunkIndex += 1
		}
	}

	if err != nil {
		err = dbsqlerr.WrapErr(err, "datbricks: read failure in chunked byte reader")
	} else if c.isEOF() {
		err = io.EOF
	}

	return bytesRead, err
}

// isEOF returns true if the chunkedByteReader is in
// an end-of-file condition.
func (c *chunkedByteReader) isEOF() bool {
	return c.chunkIndex >= len(c.chunks)
}

// reset returns the chunkedByteReader to its initial state
func (c *chunkedByteReader) reset() {
	c.byteIndex = 0
	c.chunkIndex = 0
}

// verify that the chunkedByteReader is in a valid state
func (c *chunkedByteReader) isValid() error {
	if c == nil {
		return errors.New("call to Read on nil chunkedByteReader")
	}
	if c.byteIndex < 0 || c.chunkIndex < 0 {
		return errors.New(fmt.Sprintf(errChunkedByteReaderInvalidState1, c.chunkIndex, c.byteIndex))
	}

	if c.chunkIndex < len(c.chunks)-1 {
		chunkLen := len(c.chunks[c.chunkIndex])
		if 0 < chunkLen && c.byteIndex >= chunkLen {
			return errors.New(fmt.Sprintf(errChunkedByteReaderOverreadOfNonterminalChunk, len(c.chunks), c.chunkIndex, len(c.chunks[c.chunkIndex]), c.byteIndex))
		}
	}
	return nil
}
