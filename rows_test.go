package dbsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"

	"github.com/stretchr/testify/assert"
)

func TestRowsNextRowInPage(t *testing.T) {
	var rowSet *rows

	// nil rows instance
	inPage := rowSet.isNextRowInPage()
	assert.False(t, inPage, "nil rows instance should return false")

	// default rows instance
	rowSet = &rows{}
	inPage = rowSet.isNextRowInPage()
	assert.False(t, inPage, "default rows instance should return false")

	fetchResults := &cli_service.TFetchResultsResp{}

	// fetchResults has no TRowSet
	rowSet.fetchResults = fetchResults
	inPage = rowSet.isNextRowInPage()
	assert.False(t, inPage, "fetch results with no TRowSet should return false")

	tRowSet := &cli_service.TRowSet{}

	// default TRowSet
	fetchResults.Results = tRowSet
	inPage = rowSet.isNextRowInPage()
	assert.False(t, inPage, "fetch results with default TRowSet should return false")

	// Set up a result page starting with row 10 and containing 5 rows
	tRowSet.StartRowOffset = 10
	tColumn := &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}
	tRowSet.Columns = []*cli_service.TColumn{tColumn}

	// next row number is prior to result page
	rowSet.nextRowNumber = 0
	inPage = rowSet.isNextRowInPage()
	assert.False(t, inPage, "next row before current page should return false")

	rowSet.nextRowNumber = 9
	inPage = rowSet.isNextRowInPage()
	assert.False(t, inPage, "next row before current page should return false")

	// next row number is first row in page
	rowSet.nextRowNumber = 10
	inPage = rowSet.isNextRowInPage()
	assert.True(t, inPage, "next row is first row in page should return true")

	// next row is last row in page
	rowSet.nextRowNumber = 14
	inPage = rowSet.isNextRowInPage()
	assert.True(t, inPage, "next row is first row in page should return true")

	// next row is in page
	rowSet.nextRowNumber = 12
	inPage = rowSet.isNextRowInPage()
	assert.True(t, inPage, "next row is in page should return true")

	// next row immediately follows current page
	rowSet.nextRowNumber = 15
	inPage = rowSet.isNextRowInPage()
	assert.False(t, inPage, "next row immediately after current page should return false")

	// next row after current page
	rowSet.nextRowNumber = 100
	inPage = rowSet.isNextRowInPage()
	assert.False(t, inPage, "next row after current page should return false")
}

func TestRowsGetPageFetchDirection(t *testing.T) {
	var rowSet *rows

	// nil rows instance
	direction := rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "nil rows instance should return forward direction")

	// default rows instance
	rowSet = &rows{}
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "default rows instance should return forward direction")

	fetchResults := &cli_service.TFetchResultsResp{}

	// fetchResults has no TRowSet
	rowSet.fetchResults = fetchResults
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "fetchResults has no TRowSet should return forward direction")

	tRowSet := &cli_service.TRowSet{}

	// default TRowSet
	fetchResults.Results = tRowSet
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "fetchResults has no TRowSet should return forward direction")

	// Set up a result page starting with row 10 and containing 5 rows
	tRowSet.StartRowOffset = 10
	tColumn := &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}
	tRowSet.Columns = []*cli_service.TColumn{tColumn}

	// next row number is prior to result page
	rowSet.nextRowNumber = 0
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_PRIOR, direction, "next row number is prior to result page should return reverse direction")

	// next row number is immediately prior to result page
	rowSet.nextRowNumber = 9
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_PRIOR, direction, "next row number is immediately prior to result page should return reverse direction")

	// next row number is first row in page
	rowSet.nextRowNumber = 10
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "fetchResults has no TRowSet should return forward direction")

	// next row is last row in page
	rowSet.nextRowNumber = 14
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "fetchResults has no TRowSet should return forward direction")

	// next row is in page
	rowSet.nextRowNumber = 12
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "fetchResults has no TRowSet should return forward direction")

	// next row immediately follows current page
	rowSet.nextRowNumber = 15
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "fetchResults has no TRowSet should return forward direction")

	// next row after current page
	rowSet.nextRowNumber = 100
	direction = rowSet.getPageFetchDirection()
	assert.Equal(t, cli_service.TFetchOrientation_FETCH_NEXT, direction, "fetchResults has no TRowSet should return forward direction")
}

func TestRowsGetPageStartRowNum(t *testing.T) {
	var noRows int64 = 0
	var sevenRows int64 = 7

	var rowSet *rows
	start := rowSet.getPageStartRowNum()
	assert.Equal(t, noRows, start, "nil rows instance should return 0")

	rowSet = &rows{}

	start = rowSet.getPageStartRowNum()
	assert.Equal(t, noRows, start, "rows with no page should return 0")

	rowSet.fetchResults = &cli_service.TFetchResultsResp{}

	start = rowSet.getPageStartRowNum()
	assert.Equal(t, noRows, start, "rows with no TRowSet should return 0")

	rowSet.fetchResults.Results = &cli_service.TRowSet{}
	start = rowSet.getPageStartRowNum()
	assert.Equal(t, noRows, start, "rows with default TRowSet should return 0")

	rowSet.fetchResults.Results.StartRowOffset = 7
	start = rowSet.getPageStartRowNum()
	assert.Equal(t, sevenRows, start, "rows with TRowSet should return TRowSet.StartRowOffset")
}

func TestRowsFetchResultPageErrors(t *testing.T) {
	var rowSet *rows

	err := rowSet.fetchResultPage()
	assert.EqualError(t, err, errRowsNilRows.Error())

	rowSet = &rows{
		nextRowNumber: -1,
		client:        &cli_service.TCLIServiceClient{},
	}
	err = rowSet.fetchResultPage()
	assert.EqualError(t, err, errRowsFetchPriorToStart.Error(), "negative row number should return error")

	rowSet.fetchResults = &cli_service.TFetchResultsResp{}

	// default TRowSet
	tRowSet := &cli_service.TRowSet{}
	rowSet.fetchResults.Results = tRowSet

	// Set up a result page starting with row 10 and containing 5 rows
	tRowSet.StartRowOffset = 0
	tColumn := &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}
	tRowSet.Columns = []*cli_service.TColumn{tColumn}

	rowSet.nextRowNumber = 5
	var noMoreRows = false
	rowSet.fetchResults.HasMoreRows = &noMoreRows

	//
	err = rowSet.fetchResultPage()
	assert.EqualError(t, err, io.EOF.Error(), "row number past end of result set should return EOF")
}

func TestGetResultMetadataNoDirectResults(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}

	metadata, err := rowSet.getResultMetadata()
	assert.NotNil(t, metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, getMetadataCount)

	// calling again should not call into client
	metadata, err = rowSet.getResultMetadata()
	assert.NotNil(t, metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, getMetadataCount)
}

func TestGetResultMetadataWithDirectResults(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}

	// simulate direct results by setting initial result page and
	// metadata
	req := &cli_service.TFetchResultsReq{
		Orientation: cli_service.TFetchOrientation_FETCH_NEXT,
	}
	firstPage, _ := client.FetchResults(context.Background(), req)
	rowSet.fetchResults = firstPage
	// fetch results has been called once
	assert.Equal(t, 1, fetchResultsCount)

	req2 := &cli_service.TGetResultSetMetadataReq{}
	metadata, _ := client.GetResultSetMetadata(context.Background(), req2)
	rowSet.fetchResultsMetadata = metadata
	// fetch results has been called once
	assert.Equal(t, 1, fetchResultsCount)

	// calling should not call into client again
	metadata, err := rowSet.getResultMetadata()
	assert.NotNil(t, metadata)
	assert.Nil(t, err)
	assert.Equal(t, 1, getMetadataCount)
}

func TestRowsFetchResultPageNoDirectResults(t *testing.T) {

	var getMetadataCount, fetchResultsCount int

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}

	var i64Zero int64

	// next row number is zero so should fetch first result page
	err := rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 1,
		nextRowIndex:      i64Zero,
		nextRowNumber:     i64Zero,
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is four, still in same result page
	rowSet.nextRowNumber = 4
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 1,
		nextRowIndex:      int64(4),
		nextRowNumber:     int64(4),
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is six, should fetch next result page
	rowSet.nextRowNumber = 6
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 2,
		nextRowIndex:      int64(1),
		nextRowNumber:     int64(6),
		offset:            int64(5),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is two, should fetch previous result page
	rowSet.nextRowNumber = 2
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 3,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(2),
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is past end of results, should fetch all result pages
	// going forward and then return EOF
	rowSet.nextRowNumber = 15
	err = rowSet.fetchResultPage()
	errMsg := io.EOF.Error()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 5,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(15),
		offset:            int64(10),
		errMessage:        &errMsg,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is before start of results, should fetch all result pages
	// going forward and then return EOF
	rowSet.nextRowNumber = -1
	err = rowSet.fetchResultPage()
	errMsg = errRowsFetchPriorToStart.Error()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 7,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(-1),
		offset:            i64Zero,
		errMessage:        &errMsg,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// jump back to last page
	rowSet.nextRowNumber = 12
	err = rowSet.fetchResultPage()
	errMsg = "unable to fetch row page prior to start of results"
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 9,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(12),
		offset:            int64(10),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)
}

func TestRowsFetchResultPageWithDirectResults(t *testing.T) {

	var getMetadataCount, fetchResultsCount int
	var i64Zero int64

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet := &rows{client: client}
	req := &cli_service.TFetchResultsReq{
		Orientation: cli_service.TFetchOrientation_FETCH_NEXT,
	}
	firstPage, _ := client.FetchResults(context.Background(), req)
	rowSet.fetchResults = firstPage
	// fetch results has been called once
	assert.Equal(t, 1, fetchResultsCount)

	// next row number is zero so should not fetch a result page again
	err := rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 1,
		nextRowIndex:      i64Zero,
		nextRowNumber:     i64Zero,
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is four, still in same result page
	rowSet.nextRowNumber = 4
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 1,
		nextRowIndex:      int64(4),
		nextRowNumber:     int64(4),
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is six, should fetch next result page
	rowSet.nextRowNumber = 6
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 2,
		nextRowIndex:      int64(1),
		nextRowNumber:     int64(6),
		offset:            int64(5),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is two, should fetch previous result page
	rowSet.nextRowNumber = 2
	err = rowSet.fetchResultPage()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 3,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(2),
		offset:            i64Zero,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is past end of results, should fetch all result pages
	// going forward and then return EOF
	rowSet.nextRowNumber = 15
	err = rowSet.fetchResultPage()
	errMsg := io.EOF.Error()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 5,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(15),
		offset:            int64(10),
		errMessage:        &errMsg,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// next row number is before start of results, should fetch all result pages
	// going forward and then return EOF
	rowSet.nextRowNumber = -1
	err = rowSet.fetchResultPage()
	errMsg = errRowsFetchPriorToStart.Error()
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 7,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(-1),
		offset:            i64Zero,
		errMessage:        &errMsg,
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)

	// jump back to last page
	rowSet.nextRowNumber = 12
	err = rowSet.fetchResultPage()
	errMsg = "unable to fetch row page prior to start of results"
	rowTestPagingResult{
		getMetadataCount:  0,
		fetchResultsCount: 9,
		nextRowIndex:      int64(2),
		nextRowNumber:     int64(12),
		offset:            int64(10),
	}.validatePaging(t, rowSet, err, fetchResultsCount, getMetadataCount)
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

	rowSet := &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)

	req := &cli_service.TFetchResultsReq{
		Orientation: cli_service.TFetchOrientation_FETCH_NEXT,
	}
	firstPage, _ := client.FetchResults(context.Background(), req)
	rowSet.fetchResults = firstPage
	// fetch results has been called once
	assert.Equal(t, 1, fetchResultsCount)

	req2 := &cli_service.TGetResultSetMetadataReq{}
	metadata, _ := client.GetResultSetMetadata(context.Background(), req2)
	rowSet.fetchResultsMetadata = metadata
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
	assert.EqualError(t, err, errRowsNilRows.Error())

	rowSet = &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	colNames := rowSet.Columns()
	row := make([]driver.Value, len(colNames))

	err = rowSet.Next(row)
	timestamp, _ := time.Parse(TimestampFormat, "2021-07-01 05:43:28")
	date, _ := time.Parse(DateFormat, "2021-07-01")
	row0 := []driver.Value{
		true,
		driver.Value(nil),
		int16(0),
		int32(0),
		int64(0),
		float64(0),
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
	assert.Equal(t, int64(1), rowSet.nextRowIndex)
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
	rowSet.fetchResults = firstPage
	// fetch results has been called once
	assert.Equal(t, 1, fetchResultsCount)

	req2 := &cli_service.TGetResultSetMetadataReq{}
	metadata, _ := client.GetResultSetMetadata(context.Background(), req2)
	rowSet.fetchResultsMetadata = metadata
	// get metadata has been called once
	assert.Equal(t, 1, getMetadataCount)

	colNames := rowSet.Columns()
	row := make([]driver.Value, len(colNames))

	err := rowSet.Next(row)

	timestamp, _ := time.Parse(TimestampFormat, "2021-07-01 05:43:28")
	date, _ := time.Parse(DateFormat, "2021-07-01")
	row0 := []driver.Value{
		true,
		driver.Value(nil),
		int16(0),
		int32(0),
		int64(0),
		float64(0),
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
	assert.Equal(t, int64(1), rowSet.nextRowIndex)
	assert.Equal(t, 1, getMetadataCount)
	assert.Equal(t, 1, fetchResultsCount)
}

func TestGetScanType(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	var rowSet *rows
	cd, err := rowSet.getColumnMetadataByIndex(0)
	assert.Nil(t, cd)
	assert.EqualError(t, err, errRowsNilRows.Error())

	rowSet = &rows{}
	cd, err = rowSet.getColumnMetadataByIndex(0)
	assert.Nil(t, cd)
	assert.EqualError(t, err, errRowsNoClient.Error())

	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	resp, err := rowSet.getResultMetadata()
	assert.Nil(t, err)

	cols := resp.Schema.Columns
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

func TestLengthTRowSet(t *testing.T) {
	rowSet := &cli_service.TRowSet{}
	assert.Equal(t, int64(0), getNRows(rowSet))

	rowSet.Columns = make([]*cli_service.TColumn, 1)

	bc := make([]bool, 3)
	rowSet.Columns[0] = &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: bc}}
	assert.Equal(t, int64(len(bc)), getNRows(rowSet))

	by := make([]int8, 5)
	rowSet.Columns[0] = &cli_service.TColumn{ByteVal: &cli_service.TByteColumn{Values: by}}
	assert.Equal(t, int64(len(by)), getNRows(rowSet))

	i16 := make([]int16, 7)
	rowSet.Columns[0] = &cli_service.TColumn{I16Val: &cli_service.TI16Column{Values: i16}}
	assert.Equal(t, int64(len(i16)), getNRows(rowSet))

	i32 := make([]int32, 11)
	rowSet.Columns[0] = &cli_service.TColumn{I32Val: &cli_service.TI32Column{Values: i32}}
	assert.Equal(t, int64(len(i32)), getNRows(rowSet))

	i64 := make([]int64, 13)
	rowSet.Columns[0] = &cli_service.TColumn{I64Val: &cli_service.TI64Column{Values: i64}}
	assert.Equal(t, int64(len(i64)), getNRows(rowSet))

	str := make([]string, 17)
	rowSet.Columns[0] = &cli_service.TColumn{StringVal: &cli_service.TStringColumn{Values: str}}
	assert.Equal(t, int64(len(str)), getNRows(rowSet))

	dbl := make([]float64, 19)
	rowSet.Columns[0] = &cli_service.TColumn{DoubleVal: &cli_service.TDoubleColumn{Values: dbl}}
	assert.Equal(t, int64(len(dbl)), getNRows(rowSet))

	bin := make([][]byte, 23)
	rowSet.Columns[0] = &cli_service.TColumn{BinaryVal: &cli_service.TBinaryColumn{Values: bin}}
	assert.Equal(t, int64(len(bin)), getNRows(rowSet))
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
		switch getDBTypeID(cm) {
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

type rowTestPagingResult struct {
	getMetadataCount  int
	fetchResultsCount int
	nextRowIndex      int64
	nextRowNumber     int64
	offset            int64
	errMessage        *string
}

func (rt rowTestPagingResult) validatePaging(t *testing.T, rowSet *rows, err error, fetchResultsCount, getMetadataCount int) {
	assert.Equal(t, rt.fetchResultsCount, fetchResultsCount)
	assert.Equal(t, rt.getMetadataCount, getMetadataCount)
	assert.NotNil(t, rowSet.fetchResults)
	assert.Equal(t, rt.nextRowIndex, rowSet.nextRowIndex)
	assert.Equal(t, rt.nextRowNumber, rowSet.nextRowNumber)
	assert.Equal(t, rt.offset, rowSet.getPageStartRowNum())
	if rt.errMessage == nil {
		assert.Nil(t, err)
	} else {
		assert.EqualError(t, err, *rt.errMessage)
	}
}

type rowTestClient struct {
	fOpenSession           func(ctx context.Context, req *cli_service.TOpenSessionReq) (_r *cli_service.TOpenSessionResp, _err error)
	fCloseSession          func(ctx context.Context, req *cli_service.TCloseSessionReq) (_r *cli_service.TCloseSessionResp, _err error)
	fGetInfo               func(ctx context.Context, req *cli_service.TGetInfoReq) (_r *cli_service.TGetInfoResp, _err error)
	fExecuteStatement      func(ctx context.Context, req *cli_service.TExecuteStatementReq) (_r *cli_service.TExecuteStatementResp, _err error)
	fGetTypeInfo           func(ctx context.Context, req *cli_service.TGetTypeInfoReq) (_r *cli_service.TGetTypeInfoResp, _err error)
	fGetCatalogs           func(ctx context.Context, req *cli_service.TGetCatalogsReq) (_r *cli_service.TGetCatalogsResp, _err error)
	fGetSchemas            func(ctx context.Context, req *cli_service.TGetSchemasReq) (_r *cli_service.TGetSchemasResp, _err error)
	fGetTables             func(ctx context.Context, req *cli_service.TGetTablesReq) (_r *cli_service.TGetTablesResp, _err error)
	fGetTableTypes         func(ctx context.Context, req *cli_service.TGetTableTypesReq) (_r *cli_service.TGetTableTypesResp, _err error)
	fGetColumns            func(ctx context.Context, req *cli_service.TGetColumnsReq) (_r *cli_service.TGetColumnsResp, _err error)
	fGetFunctions          func(ctx context.Context, req *cli_service.TGetFunctionsReq) (_r *cli_service.TGetFunctionsResp, _err error)
	fGetPrimaryKeys        func(ctx context.Context, req *cli_service.TGetPrimaryKeysReq) (_r *cli_service.TGetPrimaryKeysResp, _err error)
	fGetCrossReference     func(ctx context.Context, req *cli_service.TGetCrossReferenceReq) (_r *cli_service.TGetCrossReferenceResp, _err error)
	fGetOperationStatus    func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (_r *cli_service.TGetOperationStatusResp, _err error)
	fCancelOperation       func(ctx context.Context, req *cli_service.TCancelOperationReq) (_r *cli_service.TCancelOperationResp, _err error)
	fCloseOperation        func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error)
	fGetResultSetMetadata  func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error)
	fFetchResults          func(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error)
	fGetDelegationToken    func(ctx context.Context, req *cli_service.TGetDelegationTokenReq) (_r *cli_service.TGetDelegationTokenResp, _err error)
	fCancelDelegationToken func(ctx context.Context, req *cli_service.TCancelDelegationTokenReq) (_r *cli_service.TCancelDelegationTokenResp, _err error)
	fRenewDelegationToken  func(ctx context.Context, req *cli_service.TRenewDelegationTokenReq) (_r *cli_service.TRenewDelegationTokenResp, _err error)
}

var _ cli_service.TCLIService = (*rowTestClient)(nil)

func (c *rowTestClient) OpenSession(ctx context.Context, req *cli_service.TOpenSessionReq) (_r *cli_service.TOpenSessionResp, _err error) {
	if c.fOpenSession != nil {
		return c.fOpenSession(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) CloseSession(ctx context.Context, req *cli_service.TCloseSessionReq) (_r *cli_service.TCloseSessionResp, _err error) {
	if c.fCloseSession != nil {
		return c.fCloseSession(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetInfo(ctx context.Context, req *cli_service.TGetInfoReq) (_r *cli_service.TGetInfoResp, _err error) {
	if c.fGetInfo != nil {
		return c.fGetInfo(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) ExecuteStatement(ctx context.Context, req *cli_service.TExecuteStatementReq) (_r *cli_service.TExecuteStatementResp, _err error) {
	if c.fExecuteStatement != nil {
		return c.fExecuteStatement(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetTypeInfo(ctx context.Context, req *cli_service.TGetTypeInfoReq) (_r *cli_service.TGetTypeInfoResp, _err error) {
	if c.fGetTypeInfo != nil {
		return c.fGetTypeInfo(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetCatalogs(ctx context.Context, req *cli_service.TGetCatalogsReq) (_r *cli_service.TGetCatalogsResp, _err error) {
	if c.fGetCatalogs != nil {
		return c.fGetCatalogs(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetSchemas(ctx context.Context, req *cli_service.TGetSchemasReq) (_r *cli_service.TGetSchemasResp, _err error) {
	if c.fGetSchemas != nil {
		return c.fGetSchemas(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetTables(ctx context.Context, req *cli_service.TGetTablesReq) (_r *cli_service.TGetTablesResp, _err error) {
	if c.fGetTables != nil {
		return c.fGetTables(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetTableTypes(ctx context.Context, req *cli_service.TGetTableTypesReq) (_r *cli_service.TGetTableTypesResp, _err error) {
	if c.fGetTableTypes != nil {
		return c.fGetTableTypes(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetColumns(ctx context.Context, req *cli_service.TGetColumnsReq) (_r *cli_service.TGetColumnsResp, _err error) {
	if c.fGetColumns != nil {
		return c.fGetColumns(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetFunctions(ctx context.Context, req *cli_service.TGetFunctionsReq) (_r *cli_service.TGetFunctionsResp, _err error) {
	if c.fGetFunctions != nil {
		return c.fGetFunctions(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetPrimaryKeys(ctx context.Context, req *cli_service.TGetPrimaryKeysReq) (_r *cli_service.TGetPrimaryKeysResp, _err error) {
	if c.fGetPrimaryKeys != nil {
		return c.fGetPrimaryKeys(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetCrossReference(ctx context.Context, req *cli_service.TGetCrossReferenceReq) (_r *cli_service.TGetCrossReferenceResp, _err error) {
	if c.fGetCrossReference != nil {
		return c.fGetCrossReference(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetOperationStatus(ctx context.Context, req *cli_service.TGetOperationStatusReq) (_r *cli_service.TGetOperationStatusResp, _err error) {
	if c.fGetOperationStatus != nil {
		return c.fGetOperationStatus(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) CancelOperation(ctx context.Context, req *cli_service.TCancelOperationReq) (_r *cli_service.TCancelOperationResp, _err error) {
	if c.fCancelOperation != nil {
		return c.fCancelOperation(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) CloseOperation(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
	if c.fCloseOperation != nil {
		return c.fCloseOperation(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetResultSetMetadata(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
	if c.fGetResultSetMetadata != nil {
		return c.fGetResultSetMetadata(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) FetchResults(ctx context.Context, req *cli_service.TFetchResultsReq) (_r *cli_service.TFetchResultsResp, _err error) {
	if c.fFetchResults != nil {
		return c.fFetchResults(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) GetDelegationToken(ctx context.Context, req *cli_service.TGetDelegationTokenReq) (_r *cli_service.TGetDelegationTokenResp, _err error) {
	if c.fGetDelegationToken != nil {
		return c.fGetDelegationToken(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) CancelDelegationToken(ctx context.Context, req *cli_service.TCancelDelegationTokenReq) (_r *cli_service.TCancelDelegationTokenResp, _err error) {
	if c.fCancelDelegationToken != nil {
		return c.fCancelDelegationToken(ctx, req)
	}
	return nil, ErrNotImplemented
}
func (c *rowTestClient) RenewDelegationToken(ctx context.Context, req *cli_service.TRenewDelegationTokenReq) (_r *cli_service.TRenewDelegationTokenResp, _err error) {
	if c.fRenewDelegationToken != nil {
		return c.fRenewDelegationToken(ctx, req)
	}
	return nil, ErrNotImplemented
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

	client := &rowTestClient{
		fGetResultSetMetadata: getMetadata,
		fFetchResults:         fetchResults,
	}

	return client
}
