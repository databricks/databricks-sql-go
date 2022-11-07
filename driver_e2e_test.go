package dbsql

import (
	"context"
	"database/sql"
	"net/url"
	"strconv"
	"testing"

	ts "github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestQueryContextDirectResultsSuccess(t *testing.T) {
	cfg := config.WithDefaults()
	// set up server

	var executeStatement = func(ctx context.Context, req *ts.TExecuteStatementReq) (*ts.TExecuteStatementResp, error) {
		return &ts.TExecuteStatementResp{
			Status: &ts.TStatus{
				StatusCode: ts.TStatusCode_SUCCESS_STATUS,
			},
			OperationHandle: &ts.TOperationHandle{
				OperationId: &ts.THandleIdentifier{
					GUID:   []byte("2"),
					Secret: []byte("b"),
				},
			},
			DirectResults: &ts.TSparkDirectResults{
				OperationStatus: &ts.TGetOperationStatusResp{
					Status: &ts.TStatus{
						StatusCode: ts.TStatusCode_SUCCESS_STATUS,
					},
					OperationState: ts.TOperationStatePtr(ts.TOperationState_FINISHED_STATE),
				},
				ResultSetMetadata: &ts.TGetResultSetMetadataResp{
					Status: &ts.TStatus{
						StatusCode: ts.TStatusCode_SUCCESS_STATUS,
					},
				},
				ResultSet: &ts.TFetchResultsResp{
					Status: &ts.TStatus{
						StatusCode: ts.TStatusCode_SUCCESS_STATUS,
					},
				},
			},
		}, nil
	}

	cancelOperationCalled := false
	var cancelOperation = func(ctx context.Context, req *ts.TCancelOperationReq) (*ts.TCancelOperationResp, error) {
		cancelOperationCalled = true
		return &ts.TCancelOperationResp{
			Status: &ts.TStatus{
				StatusCode: ts.TStatusCode_SUCCESS_STATUS,
			},
		}, nil
	}
	getOperationStatusCalled := false
	var getOperationStatus = func(ctx context.Context, req *ts.TGetOperationStatusReq) (*ts.TGetOperationStatusResp, error) {
		getOperationStatusCalled = true
		return &ts.TGetOperationStatusResp{
			Status: &ts.TStatus{
				StatusCode: ts.TStatusCode_STILL_EXECUTING_STATUS,
			},
		}, nil
	}

	ts := initThriftTestServer(cfg, &serverHandler{
		executeStatement:   executeStatement,
		getOperationStatus: getOperationStatus,
		cancelOperation:    cancelOperation,
	})

	defer ts.Close()

	r, err := url.Parse(ts.URL)
	assert.NoError(t, err)
	cfg.Host = "localhost"
	port, err := strconv.Atoi(r.Port())
	assert.NoError(t, err)
	cfg.Port = port

	connector := &connector{cfg}

	db := sql.OpenDB(connector)

	rows, err := db.QueryContext(context.Background(), `select * from dummy`)
	assert.NoError(t, err)
	assert.NotNil(t, rows)
	assert.False(t, cancelOperationCalled)
	assert.False(t, getOperationStatusCalled)
	// TODO
	// columns, err1 := rows.Columns()
	// assert.NoError(t, err1)
	// assert.Equal(t, columns, []string{"col1", "col2"})
	// for rows.Next() {

	// }

	// rows.Close()
}

func TestQueryContextDirectResultsError(t *testing.T) {
	cfg := config.WithDefaults()
	// set up server

	var executeStatement = func(ctx context.Context, req *ts.TExecuteStatementReq) (*ts.TExecuteStatementResp, error) {
		return &ts.TExecuteStatementResp{
			Status: &ts.TStatus{
				StatusCode: ts.TStatusCode_SUCCESS_STATUS,
			},
			OperationHandle: &ts.TOperationHandle{
				OperationId: &ts.THandleIdentifier{
					GUID:   []byte("2"),
					Secret: []byte("b"),
				},
			},
			DirectResults: &ts.TSparkDirectResults{
				OperationStatus: &ts.TGetOperationStatusResp{
					Status: &ts.TStatus{
						StatusCode: ts.TStatusCode_SUCCESS_STATUS,
					},
					OperationState: ts.TOperationStatePtr(ts.TOperationState_ERROR_STATE),
					ErrorMessage:   strPtr("not valid"),
				},
				ResultSetMetadata: &ts.TGetResultSetMetadataResp{
					Status: &ts.TStatus{
						StatusCode: ts.TStatusCode_SUCCESS_STATUS,
					},
				},
				ResultSet: &ts.TFetchResultsResp{
					Status: &ts.TStatus{
						StatusCode: ts.TStatusCode_SUCCESS_STATUS,
					},
				},
			},
		}, nil
	}

	cancelOperationCalled := false
	var cancelOperation = func(ctx context.Context, req *ts.TCancelOperationReq) (*ts.TCancelOperationResp, error) {
		cancelOperationCalled = true
		return &ts.TCancelOperationResp{
			Status: &ts.TStatus{
				StatusCode: ts.TStatusCode_SUCCESS_STATUS,
			},
		}, nil
	}
	getOperationStatusCalled := false
	var getOperationStatus = func(ctx context.Context, req *ts.TGetOperationStatusReq) (*ts.TGetOperationStatusResp, error) {
		getOperationStatusCalled = true
		return &ts.TGetOperationStatusResp{
			Status: &ts.TStatus{
				StatusCode: ts.TStatusCode_STILL_EXECUTING_STATUS,
			},
		}, nil
	}

	ts := initThriftTestServer(cfg, &serverHandler{
		executeStatement:   executeStatement,
		getOperationStatus: getOperationStatus,
		cancelOperation:    cancelOperation,
	})

	defer ts.Close()

	r, err := url.Parse(ts.URL)
	assert.NoError(t, err)
	cfg.Host = "localhost"
	port, err := strconv.Atoi(r.Port())
	assert.NoError(t, err)
	cfg.Port = port

	connector := &connector{cfg}

	db := sql.OpenDB(connector)

	rows, err := db.QueryContext(context.Background(), `select * from dummy`)
	assert.ErrorContains(t, err, "not valid")
	assert.Nil(t, rows)
	assert.False(t, cancelOperationCalled)
	assert.False(t, getOperationStatusCalled)
	// TODO
	// columns, err1 := rows.Columns()
	// assert.NoError(t, err1)
	// assert.Equal(t, columns, []string{"col1", "col2"})
	// for rows.Next() {

	// }

	// rows.Close()
}

func strPtr(s string) *string {
	return &s
}
