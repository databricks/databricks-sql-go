package dbsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"

	"github.com/stretchr/testify/assert"
)

func TestRowsNextRowInPage(t *testing.T) {
	var rowSet *rows

	// nil rows instance
	inPage := rowSet.nextRowInPage()
	assert.False(t, inPage, "nil rows instance should return false")

	// default rows instance
	rowSet = &rows{}
	inPage = rowSet.nextRowInPage()
	assert.False(t, inPage, "default rows instance should return false")

	fetchResults := &cli_service.TFetchResultsResp{}

	// fetchResults has no TRowSet
	rowSet.fetchResults = fetchResults
	inPage = rowSet.nextRowInPage()
	assert.False(t, inPage, "fetch results with no TRowSet should return false")

	tRowSet := &cli_service.TRowSet{}

	// default TRowSet
	fetchResults.Results = tRowSet
	inPage = rowSet.nextRowInPage()
	assert.False(t, inPage, "fetch results with default TRowSet should return false")

	// Set up a result page starting with row 10 and containing 5 rows
	tRowSet.StartRowOffset = 10
	tColumn := &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}}
	tRowSet.Columns = []*cli_service.TColumn{tColumn}

	// next row number is prior to result page
	rowSet.nextRowNumber = 0
	inPage = rowSet.nextRowInPage()
	assert.False(t, inPage, "next row before current page should return false")

	rowSet.nextRowNumber = 9
	inPage = rowSet.nextRowInPage()
	assert.False(t, inPage, "next row before current page should return false")

	// next row number is first row in page
	rowSet.nextRowNumber = 10
	inPage = rowSet.nextRowInPage()
	assert.True(t, inPage, "next row is first row in page should return true")

	// next row is last row in page
	rowSet.nextRowNumber = 14
	inPage = rowSet.nextRowInPage()
	assert.True(t, inPage, "next row is first row in page should return true")

	// next row is in page
	rowSet.nextRowNumber = 12
	inPage = rowSet.nextRowInPage()
	assert.True(t, inPage, "next row is in page should return true")

	// next row immediately follows current page
	rowSet.nextRowNumber = 15
	inPage = rowSet.nextRowInPage()
	assert.False(t, inPage, "next row immediately after current page should return false")

	// next row after current page
	rowSet.nextRowNumber = 100
	inPage = rowSet.nextRowInPage()
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
	assert.Nil(t, err, "nil rows instance should return nil")

	rowSet = &rows{
		nextRowNumber: -1,
	}
	err = rowSet.fetchResultPage()
	assert.EqualError(t, err, "unable to fetch row page prior to start of results", "negative row number should return error")

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
	assert.EqualError(t, err, "EOF", "row number past end of result set should return EOF")
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
	errMsg := "EOF"
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
	errMsg = "unable to fetch row page prior to start of results"
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
	errMsg := "EOF"
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
	errMsg = "unable to fetch row page prior to start of results"
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
	"char_col",
	"varchar_col",
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
	assert.Equal(t, 10, len(colNames))
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
	assert.Equal(t, 10, len(colNames))
}

func TestNextNoDirectResults(t *testing.T) {
	var getMetadataCount, fetchResultsCount int

	rowSet := &rows{}
	client := getRowsTestSimpleClient(&getMetadataCount, &fetchResultsCount)
	rowSet.client = client

	colNames := rowSet.Columns()
	row := make([]driver.Value, len(colNames))

	err := rowSet.Next(row)
	row0 := []driver.Value{true, int8(0), int16(0), int32(0), int64(0), float64(0), float64(0), "s0", "s0", "s0"}
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
	row0 := []driver.Value{true, int8(0), int16(0), int32(0), int64(0), float64(0), float64(0), "s0", "s0", "s0"}
	assert.Nil(t, err)
	assert.Equal(t, row0, row)
	assert.Equal(t, int64(1), rowSet.nextRowNumber)
	assert.Equal(t, int64(1), rowSet.nextRowIndex)
	assert.Equal(t, 1, getMetadataCount)
	assert.Equal(t, 1, fetchResultsCount)
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
					ColumnName: "char_col",
					TypeDesc: &cli_service.TTypeDesc{
						Types: []*cli_service.TTypeEntry{
							{
								PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
									Type: cli_service.TTypeId_CHAR_TYPE,
								},
							},
						},
					},
				},
				{
					ColumnName: "varchar_col",
					TypeDesc: &cli_service.TTypeDesc{
						Types: []*cli_service.TTypeEntry{
							{
								PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{
									Type: cli_service.TTypeId_VARCHAR_TYPE,
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
					{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}},
					{ByteVal: &cli_service.TByteColumn{Values: []int8{0, 1, 2, 3, 4}}},
					{I16Val: &cli_service.TI16Column{Values: []int16{0, 1, 2, 3, 4}}},
					{I32Val: &cli_service.TI32Column{Values: []int32{0, 1, 2, 3, 4}}},
					{I64Val: &cli_service.TI64Column{Values: []int64{0, 1, 2, 3, 4}}},
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s0", "s1", "s2", "s3", "s4"}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s0", "s1", "s2", "s3", "s4"}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s0", "s1", "s2", "s3", "s4"}}},
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
					{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}},
					{ByteVal: &cli_service.TByteColumn{Values: []int8{5, 6, 7, 8, 9}}},
					{I16Val: &cli_service.TI16Column{Values: []int16{5, 6, 7, 8, 9}}},
					{I32Val: &cli_service.TI32Column{Values: []int32{5, 6, 7, 8, 9}}},
					{I64Val: &cli_service.TI64Column{Values: []int64{5, 6, 7, 8, 9}}},
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s5", "s6", "s7", "s8", "s9"}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s5", "s6", "s7", "s8", "s9"}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s5", "s6", "s7", "s8", "s9"}}},
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
					{BoolVal: &cli_service.TBoolColumn{Values: []bool{true, false, true, false, true}}},
					{ByteVal: &cli_service.TByteColumn{Values: []int8{10, 11, 12, 13, 14}}},
					{I16Val: &cli_service.TI16Column{Values: []int16{10, 11, 12, 13, 14}}},
					{I32Val: &cli_service.TI32Column{Values: []int32{10, 11, 12, 13, 14}}},
					{I64Val: &cli_service.TI64Column{Values: []int64{10, 11, 12, 13, 14}}},
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					{DoubleVal: &cli_service.TDoubleColumn{Values: []float64{0, 1.1, 2.2, 3.3, 4.4}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s10", "s11", "s12", "s13", "s14"}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s10", "s11", "s12", "s13", "s14"}}},
					{StringVal: &cli_service.TStringColumn{Values: []string{"s10", "s11", "s12", "s13", "s14"}}},
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
