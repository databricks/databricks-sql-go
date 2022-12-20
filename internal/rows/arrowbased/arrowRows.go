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
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/pkg/errors"
)

var errChunkedByteReaderInvalidState1 = "chunkedByteReader invalid state chunkIndex:%d, byteIndex:%d"
var errChunkedByteReaderOverreadOfNonterminalChunk = "chunkedByteReader invalid state chunks:%d chunkIndex:%d len:%d byteIndex%d"

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
}

// Make sure arrowRowScanner fulfills the RowScanner interface
var _ rowscanner.RowScanner = (*arrowRowScanner)(nil)

// NewArrowRowScanner returns an instance of RowScanner which handles arrow format results
func NewArrowRowScanner(schema *cli_service.TTableSchema, rowSet *cli_service.TRowSet, config *config.Config) (rowscanner.RowScanner, error) {

	// convert the TTableSchema to an arrow Schema
	arrowSchema, err := tTableSchemaToArrowSchema(schema, &config.ArrowConfig)
	if err != nil {
		return nil, err
	}

	// serialize the arrow schema
	schemaBytes, err := getArrowSchemaBytes(arrowSchema)
	if err != nil {
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
	}

	return rs, nil
}

// Close releases any retained arrow arrays
func (ars *arrowRowScanner) Close() {
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

func (ars *arrowRowScanner) ScanRow(
	dest []driver.Value,
	rowIndex int64,
	location *time.Location) error {

	err := ars.loadBatchFor(rowIndx(rowIndex))
	if err != nil {
		return err
	}

	nCols := len(ars.columnValues)
	if location == nil {
		location = time.UTC
	}

	for i := range dest {
		dest[i] = nil
		if i < nCols && !ars.columnValues[i].IsNull(int(rowIndex)) {
			var val any = ars.columnValues[i].Value(int(rowIndex))

			if ars.colDBTypeNames[i] == "DATE" {
				val = val.(arrow.Date32).ToTime().In(location)
			} else if ars.colDBTypeNames[i] == "TIMESTAMP" {
				if ars.UseArrowNativeTimestamp {
					val = ars.toTimestampFn(val.(arrow.Timestamp)).In(location)
				} else {
					t, err := rowscanner.HandleDateTime(val, "TIMESTAMP", ars.arrowSchema.Fields()[i].Name, location)
					if err != nil {
						return err
					}
					val = t
				}
			} else if ars.UseArrowNativeDecimal && ars.colDBTypeNames[i] == "DECIMAL" {
				val = "0"
			}

			dest[i] = val
		}
	}

	return nil
}

func (ars *arrowRowScanner) loadBatchFor(rowIndex rowIndx) error {

	if ars.columnValues == nil || !rowIndex.in(ars.getBatchRanges()[ars.currentBatchIndex]) {
		batchIndex, err := ars.rowIndexToBatchIndex(rowIndex)
		if err != nil {
			return err
		}

		err = ars.loadBatch(batchIndex)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ars *arrowRowScanner) loadBatch(batchIndex int) error {
	if ars == nil || ars.arrowBatches == nil {
		return errors.New("nil arrow batches")
	}

	if ars.currentBatchIndex == batchIndex && ars.columnValues != nil {
		return nil
	}

	if ars.columnValues == nil {
		makeColumnValues(ars)
	}

	if batchIndex < 0 || batchIndex >= len(ars.arrowBatches) {
		return errors.New("invalid batch index")
	}

	schemaBytes := ars.arrowSchemaBytes

	br := &chunkedByteReader{chunks: [][]byte{schemaBytes, ars.arrowBatches[batchIndex].Batch}}
	rdr, err := ipc.NewReader(br)
	if err != nil {
		return err
	}

	if !rdr.Next() {
		return errors.New("unable to read batch")
	}

	r := rdr.Record()
	r.Retain()
	defer r.Release()

	for i, col := range r.Columns() {
		col.Retain()
		defer col.Release()

		colData := col.Data()
		colDataType := col.DataType()
		colVals := ars.columnValues[i]
		colVals.Release()

		switch colDataType.(type) {
		case *arrow.BooleanType:
			colVals.(*columnValuesTyped[*array.Boolean, bool]).holder = array.NewBooleanData(colData)

		case *arrow.Int8Type:
			colVals.(*columnValuesTyped[*array.Int8, int8]).holder = array.NewInt8Data(colData)

		case *arrow.Int16Type:
			colVals.(*columnValuesTyped[*array.Int16, int16]).holder = array.NewInt16Data(colData)

		case *arrow.Int32Type:
			colVals.(*columnValuesTyped[*array.Int32, int32]).holder = array.NewInt32Data(colData)

		case *arrow.Int64Type:
			colVals.(*columnValuesTyped[*array.Int64, int64]).holder = array.NewInt64Data(colData)

		case *arrow.Float32Type:
			colVals.(*columnValuesTyped[*array.Float32, float32]).holder = array.NewFloat32Data(colData)

		case *arrow.Float64Type:
			colVals.(*columnValuesTyped[*array.Float64, float64]).holder = array.NewFloat64Data(colData)

		case *arrow.StringType:
			colVals.(*columnValuesTyped[*array.String, string]).holder = array.NewStringData(colData)

		case *arrow.Decimal128Type:
			colVals.(*columnValuesTyped[*array.Decimal128, decimal128.Num]).holder = array.NewDecimal128Data(colData)

		case *arrow.Date32Type:
			colVals.(*columnValuesTyped[*array.Date32, arrow.Date32]).holder = array.NewDate32Data(colData)

		case *arrow.TimestampType:
			colVals.(*columnValuesTyped[*array.Timestamp, arrow.Timestamp]).holder = array.NewTimestampData(colData)

		case *arrow.BinaryType:
			colVals.(*columnValuesTyped[*array.Binary, []byte]).holder = array.NewBinaryData(colData)

		}
	}

	ars.currentBatchIndex = batchIndex

	return nil
}

func getArrowSchemaBytes(schema *arrow.Schema) ([]byte, error) {
	if schema == nil {
		return nil, errors.New("nil table schema")
	}

	var output bytes.Buffer
	w := ipc.NewWriter(&output, ipc.WithSchema(schema))
	err := w.Close()
	if err != nil {
		return nil, err
	}

	arrowSchemaBytes := output.Bytes()
	arrowSchemaBytes = arrowSchemaBytes[:len(arrowSchemaBytes)-8]

	return arrowSchemaBytes, nil
}

func (ars *arrowRowScanner) rowIndexToBatchIndex(rowIndex rowIndx) (int, error) {
	ranges := ars.getBatchRanges()
	for i := range ranges {
		if rowIndex.in(ranges[i]) {
			return i, nil
		}
	}

	return -1, errors.New("row index not in any batch")
}

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
	tType := rowscanner.GetDBTypeID(tColumnDesc)

	if tType == cli_service.TTypeId_DECIMAL_TYPE {
		if !arrowConfig.UseArrowNativeDecimal {
			return arrow.BinaryTypes.String, nil
		}

		typeQualifiers := rowscanner.GetDBTypeQualifiers(tColumnDesc)
		if typeQualifiers == nil || typeQualifiers.Qualifiers == nil {
			return nil, errors.New("decimal type with no scale/precision")
		}
		scale, ok := typeQualifiers.Qualifiers["scale"]
		if !ok {
			return nil, errors.New("decimal type with no scale/precision")
		}

		precision, ok := typeQualifiers.Qualifiers["precision"]
		if !ok {
			return nil, errors.New("decimal type with no scale/precision")
		}

		x, _ := arrow.NewDecimalType(arrow.DECIMAL128, *scale.I32Value, *precision.I32Value)
		return x, nil

	} else if tType == cli_service.TTypeId_TIMESTAMP_TYPE {
		if !arrowConfig.UseArrowNativeTimestamp {
			return arrow.BinaryTypes.String, nil
		}

		return arrow.FixedWidthTypes.Timestamp_us, nil
	} else if at, ok := toArrowTypeMap[tType]; ok {
		return at, nil
	} else {
		return nil, errors.New("unknown data type when converting to arrow type")
	}

}

func tColumnDescToArrowField(columnDesc *cli_service.TColumnDesc, arrowConfig *config.ArrowConfig) (arrow.Field, error) {
	arrowDataType, err := tColumnDescToArrowDataType(columnDesc, arrowConfig)
	if err != nil {
		return arrow.Field{}, err
	}

	// TODO: Nullable and Metadata if Field structure
	arrowField := arrow.Field{
		Name: columnDesc.ColumnName,
		Type: arrowDataType,
		// Nullable:
		// Metadata:
	}

	return arrowField, nil
}

type rowIndx int64
type batchRange [2]rowIndx

func (ri rowIndx) in(rng batchRange) bool {
	return ri >= rng[0] && ri <= rng[1]
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

	if err == nil && c.isEOF() {
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
