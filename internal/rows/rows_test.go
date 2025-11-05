package rows

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"

	"github.com/stretchr/testify/assert"
)

func TestRowsNextRowInPage(t *testing.T) {
	t.Parallel()
	var rowSet *rows

	// nil rows instance
	inPage, err := rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "nil rows instance should return false")

	// default rows instance
	rowSet = &rows{schema: &cli_service.TTableSchema{}, ctx: context.Background()}
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "default rows instance should return false")

	fetchResults := &cli_service.TFetchResultsResp{}

	// fetchResults has no TRowSet
	err = rowSet.makeRowScanner(fetchResults)
	assert.EqualError(t, err, "databricks: driver error: "+errRowsUnknowRowType)
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "fetch results with no TRowSet should return false")

	tRowSet := &cli_service.TRowSet{}

	// default TRowSet
	fetchResults.Results = tRowSet
	err = rowSet.makeRowScanner(fetchResults)
	assert.EqualError(t, err, "databricks: driver error: "+errRowsUnknowRowType)
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "fetch results with default TRowSet should return false")

	// Set up a result page starting with row 10 and containing 5 rows
	tRowSet.StartRowOffset = 10
	tColumn := &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}
	tRowSet.Columns = []*cli_service.TColumn{tColumn}

	err = rowSet.makeRowScanner(fetchResults)
	assert.Nil(t, err)

	// next row number is prior to result page
	rowSet.nextRowNumber = 0
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "next row before current page should return false")

	rowSet.nextRowNumber = 9
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "next row before current page should return false")

	// next row number is first row in page
	rowSet.nextRowNumber = 10
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.True(t, inPage, "next row is first row in page should return true")

	// next row is last row in page
	rowSet.nextRowNumber = 14
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.True(t, inPage, "next row is first row in page should return true")

	// next row is in page
	rowSet.nextRowNumber = 12
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.True(t, inPage, "next row is in page should return true")

	// next row immediately follows current page
	rowSet.nextRowNumber = 15
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "next row immediately after current page should return false")

	// next row after current page
	rowSet.nextRowNumber = 100
	inPage, err = rowSet.isNextRowInPage()
	assert.Nil(t, err)
	assert.False(t, inPage, "next row after current page should return false")
}

func TestRowsGetPageStartRowNum(t *testing.T) {
	t.Parallel()
	var sevenRows int64 = 7

	rowSet := &rows{schema: &cli_service.TTableSchema{}}

	err := rowSet.makeRowScanner(&cli_service.TFetchResultsResp{})
	assert.EqualError(t, err, "databricks: driver error: "+errRowsUnknowRowType)

	err = rowSet.makeRowScanner(&cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{}})
	assert.EqualError(t, err, "databricks: driver error: "+errRowsUnknowRowType)

	err = rowSet.makeRowScanner(&cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{StartRowOffset: 7, Columns: []*cli_service.TColumn{}}})
	assert.Nil(t, err)

	start := rowSet.RowScanner.Start()
	assert.Equal(t, sevenRows, start, "rows with TRowSet should return TRowSet.StartRowOffset")
}

func TestRowsFetchMakeRowScanner(t *testing.T) {
	t.Parallel()
	var rowSet *rows

	err := rowSet.fetchResultPage()
	assert.EqualError(t, err, "databricks: driver error: "+errRowsNilRows)

	rowSet = &rows{
		nextRowNumber: -1,
		client:        &cli_service.TCLIServiceClient{},
		schema:        &cli_service.TTableSchema{},
	}

	err = rowSet.makeRowScanner(&cli_service.TFetchResultsResp{})
	assert.EqualError(t, err, "databricks: driver error: "+errRowsUnknowRowType)

	// default TRowSet
	tRowSet := &cli_service.TRowSet{}

	// Set up a result page starting with row 10 and containing 5 rows
	tRowSet.StartRowOffset = 0
	tColumn := &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}
	tRowSet.Columns = []*cli_service.TColumn{tColumn}

	rowSet.nextRowNumber = 5
	var noMoreRows = false

	err = rowSet.makeRowScanner(&cli_service.TFetchResultsResp{Results: tRowSet, HasMoreRows: &noMoreRows})
	assert.Nil(t, err)

}

func TestGetResultMetadataNoDirectResults(t *testing.T) {
	t.Parallel()
	var getMetadataCount, fetchResultsCount int

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}

	metadata, err := rowSet.getResultSetSchema()
	assert.NotNil(t, metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, getMetadataCount)

	// calling again should not call into client
	metadata, err = rowSet.getResultSetSchema()
	assert.NotNil(t, metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, getMetadataCount)
}

func TestGetResultMetadataWithDirectResults(t *testing.T) {
	t.Parallel()
	var getMetadataCount, fetchResultsCount int

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}

	// simulate direct results by setting initial result page and
	// metadata
	req := &cli_service.TFetchResultsReq{
		Orientation: cli_service.TFetchOrientation_FETCH_NEXT,
	}
	firstPage, _ := client.FetchResults(context.Background(), req)
	err := rowSet.makeRowScanner(firstPage)
	assert.Nil(t, err)
	assert.Equal(t, 1, getMetadataCount)
	// fetch results has been called once
	assert.Equal(t, 1, fetchResultsCount)

	req2 := &cli_service.TGetResultSetMetadataReq{}
	metadata, _ := client.GetResultSetMetadata(context.Background(), req2)
	rowSet.schema = metadata.Schema
	// fetch results has been called once
	assert.Equal(t, 1, fetchResultsCount)
	assert.Equal(t, 2, getMetadataCount)

	// calling should not call into client again
	schema, err := rowSet.getResultSetSchema()
	assert.NotNil(t, schema)
	assert.Nil(t, err)
	assert.Equal(t, 2, getMetadataCount)
}

func TestRowsFetchResultPageNoDirectResults(t *testing.T) {
	t.Parallel()

	var getMetadataCount, fetchResultsCount int

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}

	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

	resultPageIterator := rowscanner.NewResultPageIterator(
		ctx,
		rowscanner.NewDelimiter(0, 0),
		1000,
		nil,
		false,
		client,
		rowSet.logger(),
	)
	rowSet.ResultPageIterator = resultPageIterator

	var i64Zero int64

	// next row number is zero so should fetch first result page
	err := rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 1,
		nextRowNumber:     i64Zero,
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is four, still in same result page
	rowSet.nextRowNumber = 4
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 1,
		nextRowNumber:     int64(4),
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is six, should fetch next result page
	rowSet.nextRowNumber = 6
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 2,
		nextRowNumber:     int64(6),
		offset:            int64(5),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is two, can't fetch previous result page
	rowSet.nextRowNumber = 2
	err = rowSet.fetchResultPage()
	assert.ErrorContains(t, err, errRowsOnlyForward)

	// next row number is past end of next result page
	rowSet.nextRowNumber = 15
	err = rowSet.fetchResultPage()
	assert.ErrorContains(t, err, errInvalidRowNumberState)

	rowSet.nextRowNumber = 12
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 3,
		nextRowNumber:     int64(12),
		offset:            int64(10),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	rowSet.nextRowNumber = 15
	err = rowSet.fetchResultPage()
	errMsg := io.EOF.Error()
	assert.EqualError(t, err, errMsg)

	// Once we've hit an EOF state any call to fetchResultPage will return EOF
	rowSet.nextRowNumber = -1
	err = rowSet.fetchResultPage()
	assert.EqualError(t, err, io.EOF.Error())

	// jump back to last page
	rowSet.nextRowNumber = 13
	err = rowSet.fetchResultPage()
	assert.EqualError(t, err, io.EOF.Error())
}

func TestRowsFetchResultPageWithDirectResults(t *testing.T) {
	t.Parallel()

	var getMetadataCount, fetchResultsCount int
	var i64Zero int64

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}

	req := &cli_service.TFetchResultsReq{
		Orientation: cli_service.TFetchOrientation_FETCH_NEXT,
	}
	firstPage, _ := client.FetchResults(context.Background(), req)
	err1 := rowSet.makeRowScanner(firstPage)
	assert.Nil(t, err1)

	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

	resultPageIterator := rowscanner.NewResultPageIterator(
		ctx,
		rowscanner.NewDelimiter(rowSet.RowScanner.Start(), rowSet.RowScanner.Count()),
		1000,
		nil,
		false,
		client,
		rowSet.logger(),
	)
	rowSet.ResultPageIterator = resultPageIterator

	// fetch results and get metadata have been called once
	assert.Equal(t, 1, fetchResultsCount)
	assert.Equal(t, 1, getMetadataCount)

	// next row number is zero so should not fetch a result page again
	err := rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 1,
		nextRowNumber:     i64Zero,
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is four, still in same result page
	rowSet.nextRowNumber = 4
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 1,
		nextRowNumber:     int64(4),
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is six, should fetch next result page
	rowSet.nextRowNumber = 6
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 2,
		nextRowNumber:     int64(6),
		offset:            int64(5),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is two, can't fetch previous result page
	rowSet.nextRowNumber = 2
	err = rowSet.fetchResultPage()
	assert.ErrorContains(t, err, errRowsOnlyForward)

	// next row number is past end of results, should fetch all result pages
	// going forward and then return EOF
	rowSet.nextRowNumber = 15
	err = rowSet.fetchResultPage()
	assert.ErrorContains(t, err, errInvalidRowNumberState)

	rowSet.nextRowNumber = 10
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  1,
		fetchResultsCount: 3,
		nextRowNumber:     int64(10),
		offset:            int64(10),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	rowSet.nextRowNumber = 15
	err = rowSet.fetchResultPage()
	errMsg := io.EOF.Error()
	assert.ErrorContains(t, err, errMsg)

	// jump back to last page
	rowSet.nextRowNumber = 12
	err = rowSet.fetchResultPage()
	assert.ErrorContains(t, err, errMsg)

}

var rowTestColNames []string = []string{
	"bool_col",
	"tinyInt_col",
	"smallInt_col",
	"int_col",
	"bigInt_col",
	"float_col",
	"double_col",
	"string_col",
	"timestamp_col",
	"binary_col",
	"array_col",
	"map_col",
	"struct_col",
	"decimal_col",
	"date_col",
	"interval_ym_col",
	"interval_dt_col",
}

func TestColumnsWithDirectResults(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)

	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

	d, err := NewRows(ctx, nil, client, nil, nil)
	assert.Nil(t, err)

	rowSet := d.(*rows)
	defer rowSet.Close()

	req2 := &cli_service.TGetResultSetMetadataReq{}
	metadata, _ := client.GetResultSetMetadata(context.Background(), req2)
	rowSet.schema = metadata.Schema
	// get metadata has been called once
	assert.Equal(t, 1, getMetadataCount)

	// getting column names should not call into client again
	rowSet.client = client
	colNames := rowSet.Columns()
	assert.NotNil(t, colNames)
	assert.Equal(t, 17, len(colNames))
	assert.Equal(t, rowTestColNames, colNames)
}

func TestColumnsNoDirectResults(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	rowSet := &rows{}

	colNames := rowSet.Columns()
	assert.NotNil(t, colNames)
	assert.Equal(t, 0, len(colNames))

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client
	colNames = rowSet.Columns()
	assert.NotNil(t, colNames)
	assert.Equal(t, 17, len(colNames))
}

func TestNextNoDirectResults(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	var rowSet *rows
	err := rowSet.Next(nil)
	assert.EqualError(t, err, "databricks: driver error: "+errRowsNilRows)

	rowSet = &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

	resultPageIterator := rowscanner.NewResultPageIterator(
		ctx,
		rowscanner.NewDelimiter(0, 0),
		1000,
		nil,
		false,
		client,
		rowSet.logger(),
	)
	rowSet.ResultPageIterator = resultPageIterator

	colNames := rowSet.Columns()
	row := make([]driver.Value, len(colNames))

	err = rowSet.Next(row)
	timestamp, _ := time.Parse(rowscanner.DateTimeFormats["TIMESTAMP"], "2021-07-01 05:43:28")
	date, _ := time.Parse(rowscanner.DateTimeFormats["DATE"], "2021-07-01")
	row0 := []driver.Value{
		true,
		driver.Value(nil),
		int16(0),
		int32(0),
		int64(0),
		float32(0),
		float64(0),
		"s0",
		timestamp,
		[]uint8{uint8(1), uint8(2), uint8(3)},
		"[1, 2, 3]",
		"{\"key1\": 1}",
		"{\"string_field\": \"string_val\", \"array_field\": [1, 2]}",
		"1.1",
		date,
		"100-0",
		"-8 08:13:50.300000000",
	}
	assert.Nil(t, err)
	assert.Equal(t, row0, row)
	assert.Equal(t, int64(1), rowSet.nextRowNumber)
	assert.Equal(t, 1, getMetadataCount)
	assert.Equal(t, 1, fetchResultsCount)
}

func TestNextWithDirectResults(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	rowSet := &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	req := &cli_service.TFetchResultsReq{
		Orientation: cli_service.TFetchOrientation_FETCH_NEXT,
	}
	firstPage, _ := client.FetchResults(context.Background(), req)
	var err error = rowSet.makeRowScanner(firstPage)
	assert.Nil(t, err)
	assert.Equal(t, 1, fetchResultsCount)
	assert.Equal(t, 1, getMetadataCount)

	req2 := &cli_service.TGetResultSetMetadataReq{}
	metadata, _ := client.GetResultSetMetadata(context.Background(), req2)
	rowSet.schema = metadata.Schema
	// get metadata has been called once
	assert.Equal(t, 2, getMetadataCount)

	colNames := rowSet.Columns()
	row := make([]driver.Value, len(colNames))

	err = rowSet.Next(row)

	timestamp, _ := time.Parse(rowscanner.DateTimeFormats["TIMESTAMP"], "2021-07-01 05:43:28")
	date, _ := time.Parse(rowscanner.DateTimeFormats["DATE"], "2021-07-01")
	row0 := []driver.Value{
		true,
		driver.Value(nil),
		int16(0),
		int32(0),
		int64(0),
		float32(0),
		float64(0),
		"s0",
		timestamp,
		[]uint8{uint8(1), uint8(2), uint8(3)},
		"[1, 2, 3]",
		"{\"key1\": 1}",
		"{\"string_field\": \"string_val\", \"array_field\": [1, 2]}",
		"1.1",
		date,
		"100-0",
		"-8 08:13:50.300000000",
	}
	assert.Nil(t, err)
	assert.Equal(t, row0, row)
	assert.Equal(t, int64(1), rowSet.nextRowNumber)
	assert.Equal(t, 2, getMetadataCount)
	assert.Equal(t, 1, fetchResultsCount)
}

func TestGetScanType(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	var rowSet *rows
	cd, err := rowSet.getColumnMetadataByIndex(0)
	assert.Nil(t, cd)
	assert.EqualError(t, err, "databricks: driver error: "+errRowsNilRows)

	rowSet = &rows{}
	cd, err = rowSet.getColumnMetadataByIndex(0)
	assert.Nil(t, cd)
	assert.EqualError(t, err, "databricks: driver error: "+errRowsNoClient)

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	schema, err := rowSet.getResultSetSchema()
	assert.Nil(t, err)

	cols := schema.Columns
	expectedScanTypes := []reflect.Type{
		scanTypeBoolean,
		scanTypeInt8,
		scanTypeInt16,
		scanTypeInt32,
		scanTypeInt64,
		scanTypeFloat32,
		scanTypeFloat64,
		scanTypeString,
		scanTypeDateTime,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeDateTime,
		scanTypeString,
		scanTypeString,
	}

	assert.Equal(t, len(expectedScanTypes), len(cols))

	scanTypes := make([]reflect.Type, len(cols))
	for i := range cols {
		scanTypes[i] = rowSet.ColumnTypeScanType(i)
	}

	assert.Equal(t, expectedScanTypes, scanTypes)
}

func TestColumnTypeNullable(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	rowSet := &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	colNames := rowSet.Columns()
	for i := range colNames {
		nullable, ok := rowSet.ColumnTypeNullable(i)
		assert.False(t, nullable)
		assert.False(t, ok)
	}
}

func TestColumnTypeLength(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	var rowSet *rows
	l, b := rowSet.ColumnTypeLength(0)
	assert.Zero(t, l)
	assert.False(t, b)

	rowSet = &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	colNames := rowSet.Columns()
	for i := range colNames {
		length, ok := rowSet.ColumnTypeLength(i)

		cm, _ := rowSet.getColumnMetadataByIndex(i)
		switch rowscanner.GetDBTypeID(cm) {
		case cli_service.TTypeId_STRING_TYPE,
			cli_service.TTypeId_VARCHAR_TYPE,
			cli_service.TTypeId_BINARY_TYPE,
			cli_service.TTypeId_ARRAY_TYPE,
			cli_service.TTypeId_MAP_TYPE,
			cli_service.TTypeId_STRUCT_TYPE:
			assert.Equal(t, int64(math.MaxInt64), length)
			assert.True(t, ok)
		default:
			assert.Equal(t, int64(0), length)
			assert.False(t, ok)
		}
	}
}

func TestColumnTypeDatabaseTypeName(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	rowSet := &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	schema, err := rowSet.getResultSetSchema()
	assert.Nil(t, err)

	cols := schema.Columns
	expectedScanTypes := []reflect.Type{
		scanTypeBoolean,
		scanTypeInt8,
		scanTypeInt16,
		scanTypeInt32,
		scanTypeInt64,
		scanTypeFloat32,
		scanTypeFloat64,
		scanTypeString,
		scanTypeDateTime,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeRawBytes,
		scanTypeDateTime,
		scanTypeString,
		scanTypeString,
	}

	assert.Equal(t, len(expectedScanTypes), len(cols))

	scanTypes := make([]reflect.Type, len(cols))
	for i := range cols {
		scanTypes[i] = rowSet.ColumnTypeScanType(i)
	}

	assert.Equal(t, expectedScanTypes, scanTypes)
}

func TestRowsCloseOptimization(t *testing.T) {
	var closeCount int
	client := &client.TestClient{
		FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
			closeCount++
			return nil, nil
		},
	}

	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")
	opHandle := &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{GUID: []byte{'f', 'o'}}}
	rowSet, _ := NewRows(ctx, opHandle, client, nil, nil)

	// rowSet has no direct results calling Close should result in call to client to close operation
	err := rowSet.Close()
	assert.Nil(t, err, "rows.Close should not throw an error")
	assert.Equal(t, 1, closeCount)

	// rowSet has direct results, but operation was not closed so it should call client to close operation
	directResults := &cli_service.TSparkDirectResults{
		ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{}},
		ResultSet:         &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{Columns: []*cli_service.TColumn{}}},
	}
	closeCount = 0
	rowSet, _ = NewRows(ctx, opHandle, client, nil, directResults)
	err = rowSet.Close()
	assert.Nil(t, err, "rows.Close should not throw an error")
	assert.Equal(t, 1, closeCount)

	// rowSet has direct results which include a close operation response.  rowSet should be marked as closed
	// and calling Close should not call into the client.
	closeCount = 0
	directResults = &cli_service.TSparkDirectResults{
		CloseOperation:    &cli_service.TCloseOperationResp{},
		ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{Schema: &cli_service.TTableSchema{}},
		ResultSet:         &cli_service.TFetchResultsResp{Results: &cli_service.TRowSet{Columns: []*cli_service.TColumn{}}},
	}
	rowSet, _ = NewRows(ctx, opHandle, client, nil, directResults)
	err = rowSet.Close()
	assert.Nil(t, err, "rows.Close should not throw an error")
	assert.Equal(t, 0, closeCount)
}

type fetch struct {
	direction      cli_service.TFetchOrientation
	resultStartRec int
}

func TestFetchResultsWithRetries(t *testing.T) {
	t.Parallel()

	// Simulate a scenario where network issues and retries cause unexpected jumps
	// across multiple result pages.
	fetches := []fetch{}

	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

	client := getRowsTestSimpleClient2(&fetches)
	rowSet := &rows{client: client}

	resultPageIterator := rowscanner.NewResultPageIterator(
		ctx,
		rowscanner.NewDelimiter(0, 0),
		1000,
		nil,
		false,
		client,
		rowSet.logger(),
	)
	rowSet.ResultPageIterator = resultPageIterator

	// next row number is zero so should fetch first result page
	err := rowSet.fetchResultPage()
	assert.Nil(t, err)
	assert.Len(t, fetches, 1)
	assert.Equal(t, fetches[0].direction, cli_service.TFetchOrientation_FETCH_NEXT)

	// row number four is still in the first page so there should be no calls to fetch
	rowSet.nextRowNumber = 4
	err = rowSet.fetchResultPage()
	assert.Nil(t, err)
	assert.Len(t, fetches, 1)

	rowSet.nextRowNumber = 5
	err = rowSet.fetchResultPage()
	assert.Nil(t, err)
	assert.Len(t, fetches, 5)

}

func TestGetArrowBatches(t *testing.T) {
	t.Run("with direct results", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "directResultsMultipleFetch/ExecuteStatement.json", &executeStatementResp)

		fetchResp1 := cli_service.TFetchResultsResp{}
		loadTestData(t, "directResultsMultipleFetch/FetchResults1.json", &fetchResp1)

		fetchResp2 := cli_service.TFetchResultsResp{}
		loadTestData(t, "directResultsMultipleFetch/FetchResults2.json", &fetchResp2)

		ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
		ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

		client := getSimpleClient([]cli_service.TFetchResultsResp{fetchResp1, fetchResp2})
		cfg := config.WithDefaults()
		rows, err := NewRows(ctx, nil, client, cfg, executeStatementResp.DirectResults)
		assert.Nil(t, err)

		rows2, ok := rows.(dbsqlrows.Rows)
		assert.True(t, ok)

		rs, err2 := rows2.GetArrowBatches(context.Background())
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

	t.Run("without direct results", func(t *testing.T) {
		fetchResp1 := cli_service.TFetchResultsResp{}
		loadTestData(t, "multipleFetch/FetchResults1.json", &fetchResp1)

		fetchResp2 := cli_service.TFetchResultsResp{}
		loadTestData(t, "multipleFetch/FetchResults2.json", &fetchResp2)

		fetchResp3 := cli_service.TFetchResultsResp{}
		loadTestData(t, "multipleFetch/FetchResults3.json", &fetchResp3)

		ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
		ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

		client := getSimpleClient([]cli_service.TFetchResultsResp{fetchResp1, fetchResp2, fetchResp3})
		cfg := config.WithDefaults()
		rows, err := NewRows(ctx, nil, client, cfg, nil)
		assert.Nil(t, err)

		rows2, ok := rows.(dbsqlrows.Rows)
		assert.True(t, ok)

		rs, err2 := rows2.GetArrowBatches(context.Background())
		assert.Nil(t, err2)

		hasNext := rs.HasNext()
		assert.True(t, hasNext)
		r, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[0].RowCount, r.NumRows())
		r.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r2, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp1.Results.ArrowBatches[1].RowCount, r2.NumRows())
		r2.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r3, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[0].RowCount, r3.NumRows())
		r3.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r4, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp2.Results.ArrowBatches[1].RowCount, r4.NumRows())
		r4.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r5, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp3.Results.ArrowBatches[0].RowCount, r5.NumRows())
		r5.Release()

		hasNext = rs.HasNext()
		assert.True(t, hasNext)
		r6, err2 := rs.Next()
		assert.Nil(t, err2)
		assert.Equal(t, fetchResp3.Results.ArrowBatches[1].RowCount, r6.NumRows())
		r6.Release()
	})

	t.Run("with empty result set, no direct results", func(t *testing.T) {
		fetchResp1 := cli_service.TFetchResultsResp{}
		loadTestData(t, "zeroRows/zeroRowsFetchResult.json", &fetchResp1)

		ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
		ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

		client := getSimpleClient([]cli_service.TFetchResultsResp{fetchResp1})
		cfg := config.WithDefaults()
		rows, err := NewRows(ctx, nil, client, cfg, nil)
		assert.Nil(t, err)

		rows2, ok := rows.(dbsqlrows.Rows)
		assert.True(t, ok)

		rs, err2 := rows2.GetArrowBatches(context.Background())
		assert.Nil(t, err2)

		hasNext := rs.HasNext()
		assert.False(t, hasNext)
		r7, err2 := rs.Next()
		assert.Nil(t, r7)
		assert.ErrorContains(t, err2, io.EOF.Error())

	})

	t.Run("with empty result set, direct results", func(t *testing.T) {
		executeStatementResp := cli_service.TExecuteStatementResp{}
		loadTestData(t, "zeroRows/zeroRowsDirectResults.json", &executeStatementResp)
		executeStatementResp.DirectResults.ResultSet.Results.ArrowBatches = []*cli_service.TSparkArrowBatch{}

		ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
		ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

		client := getSimpleClient([]cli_service.TFetchResultsResp{})
		cfg := config.WithDefaults()
		rows, err := NewRows(ctx, nil, client, cfg, executeStatementResp.DirectResults)
		assert.Nil(t, err)

		rows2, ok := rows.(dbsqlrows.Rows)
		assert.True(t, ok)

		rs, err2 := rows2.GetArrowBatches(context.Background())
		assert.Nil(t, err2)

		hasNext := rs.HasNext()
		assert.False(t, hasNext)
		r7, err2 := rs.Next()
		assert.Nil(t, r7)
		assert.ErrorContains(t, err2, io.EOF.Error())
	})
}

type rowTestPagingResult struct {
	getMetadataCount  int
	fetchResultsCount int
	nextRowNumber     int64
	offset            int64
	errMessage        *string
}

func (rt rowTestPagingResult) validatePaging(t *testing.T, rowSet *rows, err error, fetchResultsCount, getMetadataCount int) {
	assert.Equal(t, rt.fetchResultsCount, fetchResultsCount)
	assert.Equal(t, rt.getMetadataCount, getMetadataCount)
	assert.Equal(t, rt.nextRowNumber, rowSet.nextRowNumber)
	assert.Equal(t, rt.offset, rowSet.RowScanner.Start())
	if rt.errMessage == nil {
		assert.Nil(t, err)
	} else {
		assert.EqualError(t, err, *rt.errMessage)
	}
}

// Build a simple test client
func getRowsTestSimpleClient(getMetadataCount, fetchResultsCount *int) cli_service.TCLIService {
	// Metadata for the different types is based on the results returned when querying a table with
	// all the different types which was created in a test shard.
	metadata := &cli_service.TGetResultSetMetadataResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
		Schema: &cli_service.TTableSchema{
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
		},
	}

	getMetadata := func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
		*getMetadataCount = *getMetadataCount + 1
		return metadata, nil
	}

	moreRows := true
	noMoreRows := false
	pages := []*cli_service.TFetchResultsResp{
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &moreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 0,
				Columns: []*cli_service.TColumn{
					// bool_col
					{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}},
					// tinyInt_com
					{ByteVal: &cli_service.TByteColumn{Values: []int8{0, 1, 2, 3, 4}, Nulls: []byte{1}}},
					// smallInt_col
					{I16Val: &cli_service.TI16Column{Values: []int16{0, 1, 2, 3, 4}}},
					// int_col
					{I32Val: &cli_service.TI32Column{Values: []int32{0, 1, 2, 3, 4}}},
					//bigInt_col
					{I64Val: &cli_service.TI64Column{Values: []int64{0, 1, 2, 3, 4}}},
					// float_col
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					// double_col
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					// string_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"s0", "s1", "s2", "s3", "s4"}}},
					// timestamp_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28"}}},
					// binary_col
					{BinaryVal: &cli_service.TBinaryColumn{Values: [][]byte{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}}}},
					// array_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]"}}},
					// map_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}"}}},
					// struct_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}"}}},
					// decimal_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"1.1", "2.2", "3.3", "4.4", "5.5"}}},
					// date_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"2021-07-01", "2021-07-01", "2021-07-01", "2021-07-01", "2021-07-01"}}},
					// interval_ym_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"100-0", "100-0", "100-0", "100-0", "100-0"}}},
					// interval_dt_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000"}}},
				},
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &moreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 5,
				Columns: []*cli_service.TColumn{
					// bool_col
					{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}},
					// tinyInt_com
					{ByteVal: &cli_service.TByteColumn{Values: []int8{0, 1, 2, 3, 4}}},
					// smallInt_col
					{I16Val: &cli_service.TI16Column{Values: []int16{0, 1, 2, 3, 4}}},
					// int_col
					{I32Val: &cli_service.TI32Column{Values: []int32{5, 6, 7, 8, 9}}},
					//bigInt_col
					{I64Val: &cli_service.TI64Column{Values: []int64{0, 1, 2, 3, 4}}},
					// float_col
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					// double_col
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					// string_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"s0", "s1", "s2", "s3", "s4"}}},
					// timestamp_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28"}}},
					// binary_col
					{BinaryVal: &cli_service.TBinaryColumn{Values: [][]byte{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}}}},
					// array_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]"}}},
					// map_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}"}}},
					// struct_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}"}}},
					// decimal_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"1.1", "2.2", "3.3", "4.4", "5.5"}}},
					// date_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"2021-07-01", "2021-07-01", "2021-07-01", "2021-07-01", "2021-07-01"}}},
					// interval_ym_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"100-0", "100-0", "100-0", "100-0", "100-0"}}},
					// interval_dt_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000"}}},
				},
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &noMoreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 10,
				Columns: []*cli_service.TColumn{
					// bool_col
					{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}},
					// tinyInt_com
					{ByteVal: &cli_service.TByteColumn{Values: []int8{0, 1, 2, 3, 4}}},
					// smallInt_col
					{I16Val: &cli_service.TI16Column{Values: []int16{0, 1, 2, 3, 4}}},
					// int_col
					{I32Val: &cli_service.TI32Column{Values: []int32{10, 11, 12, 13, 14}}},
					//bigInt_col
					{I64Val: &cli_service.TI64Column{Values: []int64{0, 1, 2, 3, 4}}},
					// float_col
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}, Nulls: []byte{1}}},
					// double_col
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					// string_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"s0", "s1", "s2", "s3", "s4"}}},
					// timestamp_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28", "2021-07-01 05:43:28"}}},
					// binary_col
					{BinaryVal: &cli_service.TBinaryColumn{Values: [][]byte{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}, {1, 2, 3}}}},
					// array_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]", "[1, 2, 3]"}}},
					// map_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}", "{\"key1\": 1}"}}},
					// struct_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}", "{\"string_field\": \"string_val\", \"array_field\": [1, 2]}"}}},
					// decimal_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"1.1", "2.2", "3.3", "4.4", "5.5"}}},
					// date_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"2021-07-01", "2021-07-01", "2021-07-01", "2021-07-01", "2021-07-01"}}},
					// interval_ym_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"100-0", "100-0", "100-0", "100-0", "100-0"}}},
					// interval_dt_col
					{StringVal: &cli_service.TStringColumn{Values: []string{"-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000", "-8 08:13:50.300000000"}}},
				},
			},
		},
	}

	pageIndex := -1

	fetchResults := func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error) {
		*fetchResultsCount++
		if req.Orientation == cli_service.TFetchOrientation_FETCH_NEXT {
			if pageIndex+1 >= len(pages) {
				return nil, errors.New("can't fetch past end of result set")
			}
			pageIndex++
		} else if req.Orientation == cli_service.TFetchOrientation_FETCH_PRIOR {
			if pageIndex-1 < 0 {
				return nil, errors.New("can't fetch prior to start of result set")
			}
			pageIndex--
		} else {
			return nil, errors.New("invalid fetch results orientation")
		}

		return pages[pageIndex], nil
	}

	client := &client.TestClient{
		FnGetResultSetMetadata: getMetadata,
		FnFetchResults:         fetchResults,
	}

	return client
}

// Build a simple test client
func getRowsTestSimpleClient2(fetches *[]fetch) cli_service.TCLIService {
	// Metadata for the different types is based on the results returned when querying a table with
	// all the different types which was created in a test shard.
	metadata := &cli_service.TGetResultSetMetadataResp{
		Status: &cli_service.TStatus{
			StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
		},
		Schema: &cli_service.TTableSchema{
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
			},
		},
	}

	getMetadata := func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
		return metadata, nil
	}

	moreRows := true
	noMoreRows := false
	colVals := []*cli_service.TColumn{{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}}

	pages := []*cli_service.TFetchResultsResp{
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &moreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 0,
				Columns:        colVals,
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &moreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 5,
				Columns:        colVals,
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &noMoreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 10,
				Columns:        colVals,
			},
		},
		{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			HasMoreRows: &noMoreRows,
			Results: &cli_service.TRowSet{
				StartRowOffset: 15,
				Columns:        []*cli_service.TColumn{},
			},
		},
	}

	// We are simulating the scenario where network errors and retry behaviour cause the fetch
	// result request to be sent multiple times, resulting in jumping past the next/previous result
	// page. Behaviour should be robust enough to handle this by changing the fetch orientation.
	pageSequence := []int{0, 3, 2, 0, 1, 2}
	pageIndex := -1

	fetchResults := func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error) {
		pageIndex++

		p := pages[pageSequence[pageIndex]]
		*fetches = append(*fetches, fetch{direction: req.Orientation, resultStartRec: int(p.Results.StartRowOffset)})
		return p, nil
	}

	client := &client.TestClient{
		FnGetResultSetMetadata: getMetadata,
		FnFetchResults:         fetchResults,
	}

	return client
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

func getSimpleClient(fetchResults []cli_service.TFetchResultsResp) cli_service.TCLIService {
	var resultIndex int

	fetchResultsFn := func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error) {

		p := fetchResults[resultIndex]

		resultIndex++
		return &p, nil
	}

	client := &client.TestClient{
		FnFetchResults: fetchResultsFn,
	}

	return client
}

func getErroringClient(err error) cli_service.TCLIService {
	fetchResultsFn := func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error) {
		return nil, err
	}

	client := &client.TestClient{
		FnFetchResults: fetchResultsFn,
	}

	return client
}

func TestFetchResultPage_PropagatesGetNextPageError(t *testing.T) {
	errorMsg := "Error thrown while calling TFetchResults in getNextPage"
	expectedErr := errors.New(errorMsg)

	client := getErroringClient(expectedErr)

	ctx := driverctx.NewContextWithConnId(context.Background(), "connId")
	ctx = driverctx.NewContextWithCorrelationId(ctx, "corrId")

	executeStatementResp := cli_service.TExecuteStatementResp{}
	cfg := config.WithDefaults()
	rows, _ := NewRows(ctx, nil, client, cfg, executeStatementResp.DirectResults)
	// Call Next and ensure it propagates the error from getNextPage
	actualErr := rows.Next(nil)

	assert.ErrorContains(t, actualErr, errorMsg)
}
