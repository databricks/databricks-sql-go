package dbsql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestQueryContextDirectResultsSuccess(t *testing.T) {
	cfg := config.WithDefaults()
	// set up server

	var executeStatement = func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
		return &cli_service.TExecuteStatementResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			OperationHandle: &cli_service.TOperationHandle{
				OperationId: &cli_service.THandleIdentifier{
					GUID:   []byte("2"),
					Secret: []byte("b"),
				},
			},
			DirectResults: &cli_service.TSparkDirectResults{
				OperationStatus: &cli_service.TGetOperationStatusResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				},
				ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
				},
				ResultSet: &cli_service.TFetchResultsResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
				},
			},
		}, nil
	}

	cancelOperationCalled := false
	var cancelOperation = func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
		cancelOperationCalled = true
		return &cli_service.TCancelOperationResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
		}, nil
	}
	getOperationStatusCalled := false
	var getOperationStatus = func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
		getOperationStatusCalled = true
		return &cli_service.TGetOperationStatusResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
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
	cfg.Protocol = "http"
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

func TestQueryContextTimeouts(t *testing.T) {
	cfg := config.WithDefaults()
	// set up server

	var executeStatement = func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
		return &cli_service.TExecuteStatementResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			OperationHandle: &cli_service.TOperationHandle{
				OperationId: &cli_service.THandleIdentifier{
					GUID:   []byte("2"),
					Secret: []byte("b"),
				},
			},
			DirectResults: &cli_service.TSparkDirectResults{
				OperationStatus: &cli_service.TGetOperationStatusResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
					},
					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_PENDING_STATE),
				},
				ResultSetMetadata: nil,
				ResultSet:         nil,
			},
		}, nil
	}

	cancelOperationCalled := false
	var cancelOperation = func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
		cancelOperationCalled = true
		return &cli_service.TCancelOperationResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
		}, nil
	}
	getOperationStatusCalled := false
	var getOperationStatus = func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
		// this is very important. If the context gets canceled while getting the operation we should still call cancel
		time.Sleep(200 * time.Millisecond)
		getOperationStatusCalled = true
		return &cli_service.TGetOperationStatusResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
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
	cfg.Protocol = "http"
	cfg.Host = "localhost"
	port, err := strconv.Atoi(r.Port())
	assert.NoError(t, err)
	cfg.Port = port
	cfg.PollInterval = 200 * time.Millisecond

	connector := &connector{cfg}

	db := sql.OpenDB(connector)

	ctx, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()

	rows, err := db.QueryContext(ctx, `select * from dummy`)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	_, ok := err.(stackTracer)
	assert.True(t, ok)
	_, ok1 := err.(causer)
	assert.True(t, ok1)

	fmt.Printf("%+v", err)
	time.Sleep(40 * time.Millisecond)
	assert.Nil(t, rows)
	assert.True(t, cancelOperationCalled)
	assert.True(t, getOperationStatusCalled)

}

func TestQueryContextDirectResultsError(t *testing.T) {
	cfg := config.WithDefaults()
	// set up server

	var executeStatement = func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
		return &cli_service.TExecuteStatementResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			OperationHandle: &cli_service.TOperationHandle{
				OperationId: &cli_service.THandleIdentifier{
					GUID:   []byte("2"),
					Secret: []byte("b"),
				},
			},
			DirectResults: &cli_service.TSparkDirectResults{
				OperationStatus: &cli_service.TGetOperationStatusResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
					ErrorMessage:   strPtr("error message"),
					DisplayMessage: strPtr("display message"),
				},
				ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
				},
				ResultSet: &cli_service.TFetchResultsResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
				},
			},
		}, nil
	}

	cancelOperationCalled := false
	var cancelOperation = func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
		cancelOperationCalled = true
		return &cli_service.TCancelOperationResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
		}, nil
	}
	getOperationStatusCalled := false
	var getOperationStatus = func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
		getOperationStatusCalled = true
		return &cli_service.TGetOperationStatusResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
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
	cfg.Protocol = "http"
	cfg.Host = "localhost"
	port, err := strconv.Atoi(r.Port())
	assert.NoError(t, err)
	cfg.Port = port

	connector := &connector{cfg}

	db := sql.OpenDB(connector)

	rows, err := db.QueryContext(context.Background(), `select * from dummy`)
	assert.ErrorContains(t, err, "display message")
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
