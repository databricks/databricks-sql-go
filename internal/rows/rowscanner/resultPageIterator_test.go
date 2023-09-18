package rowscanner

import (
	"context"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
)

func TestFetchResultPageination(t *testing.T) {
	t.Parallel()

	fetches := []fetch{}
	pageSequence := []int{0, 3, 2, 0, 1, 2}
	client := getSimpleClient(&fetches, pageSequence)

	rf := &resultPageIterator{
		Delimiter:     NewDelimiter(0, 0),
		client:        client,
		logger:        dbsqllog.WithContext("connId", "correlationId", ""),
		connectionId:  "connId",
		correlationId: "correlationId",
	}

	// next row number is zero so should fetch first result page
	_, err := rf.Next()
	assert.Nil(t, err)
	assert.Len(t, fetches, 1)
	assert.Equal(t, fetches[0].direction, cli_service.TFetchOrientation_FETCH_NEXT)

	// The test client returns rows
	_, err = rf.Next()
	assert.Nil(t, err)
	assert.Len(t, fetches, 5)
	expected := []fetch{
		{direction: cli_service.TFetchOrientation_FETCH_NEXT, resultStartRec: 0},
		{direction: cli_service.TFetchOrientation_FETCH_NEXT, resultStartRec: 15},
		{direction: cli_service.TFetchOrientation_FETCH_PRIOR, resultStartRec: 10},
		{direction: cli_service.TFetchOrientation_FETCH_PRIOR, resultStartRec: 0},
		{direction: cli_service.TFetchOrientation_FETCH_NEXT, resultStartRec: 5},
	}
	assert.Equal(t, expected, fetches)
}

type fetch struct {
	direction      cli_service.TFetchOrientation
	resultStartRec int
}

// Build a simple test client
func getSimpleClient(fetches *[]fetch, pageSequence []int) cli_service.TCLIService {
	// We are simulating the scenario where network errors and retry behaviour cause the fetch
	// result request to be sent multiple times, resulting in jumping past the next/previous result
	// page. Behaviour should be robust enough to handle this by changing the fetch orientation.

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
