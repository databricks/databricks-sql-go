package dbsql

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStmt_Close(t *testing.T) {
	t.Run("Close is not applicable", func(t *testing.T) {
		testStmt := stmt{
			conn:  &conn{},
			query: "query string",
		}
		err := testStmt.Close()
		assert.Nil(t, err)
	})
}

func TestStmt_NumInput(t *testing.T) {
	t.Run("NumInput is not applicable", func(t *testing.T) {
		testStmt := stmt{
			conn:  &conn{},
			query: "query string",
		}
		numInput := testStmt.NumInput()
		assert.Equal(t, -1, numInput)
	})
}

func TestStmt_Exec(t *testing.T) {
	t.Run("Exec is not implemented", func(t *testing.T) {
		testStmt := stmt{
			conn:  &conn{},
			query: "query string",
		}
		res, err := testStmt.Exec([]driver.Value{})
		assert.Nil(t, res)
		assert.Error(t, err)
	})
}

func TestStmt_Query(t *testing.T) {
	t.Run("Query is not implemented", func(t *testing.T) {
		testStmt := stmt{
			conn:  &conn{},
			query: "query string",
		}
		res, err := testStmt.Query([]driver.Value{})
		assert.Nil(t, res)
		assert.Error(t, err)
	})
}

// func TestStmt_ExecContext(t *testing.T) {
// 	t.Run("ExecContext returns number of rows modified when execution is successful", func(t *testing.T) {
// 		var executeStatementCount, getOperationStatusCount int
// 		var savedQueryString string
// 		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
// 			executeStatementCount++
// 			savedQueryString = req.Statement
// 			executeStatementResp := &cli_service.TExecuteStatementResp{
// 				Status: &cli_service.TStatus{
// 					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 				},
// 				OperationHandle: &cli_service.TOperationHandle{
// 					OperationId: &cli_service.THandleIdentifier{
// 						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
// 						Secret: []byte("b"),
// 					},
// 				},
// 			}
// 			return executeStatementResp, nil
// 		}

// 		getExecutionStatus := func(ctx context.Context, req *client.GetExecutionStatusReq) (r *client.GetExecutionStatusResp, err error) {
// 			getOperationStatusCount++
// 			getOperationStatusResp := &client.GetExecutionStatusResp{
// 				ExecutionStatus: client.ExecutionStatus{
// 					ExecutionState:  cli_service.TOperationState_FINISHED_STATE.String(),
// 					NumModifiedRows: int64(10),
// 				},
// 			}
// 			return getOperationStatusResp, nil
// 		}

// 		testClient := &client.TestClient{
// 			FnExecuteStatement:   executeStatement,
// 			FnGetExecutionStatus: getExecutionStatus,
// 		}
// 		testConn := &conn{
// 			session: getTestSession(),
// 			client:  testClient,
// 			cfg:     config.WithDefaults(),
// 		}
// 		testQuery := "insert 10"
// 		testStmt := &stmt{
// 			conn:  testConn,
// 			query: testQuery,
// 		}
// 		res, err := testStmt.ExecContext(context.Background(), []driver.NamedValue{})

// 		assert.NoError(t, err)
// 		assert.NotNil(t, res)
// 		rowsAffected, _ := res.RowsAffected()
// 		assert.Equal(t, int64(10), rowsAffected)
// 		assert.Equal(t, 1, executeStatementCount)
// 		assert.Equal(t, testQuery, savedQueryString)
// 	})
// }

// func TestStmt_QueryContext(t *testing.T) {
// 	t.Run("QueryContext returns rows object upon successful query", func(t *testing.T) {
// 		var executeStatementCount, getOperationStatusCount int
// 		var savedQueryString string
// 		executeStatement := func(ctx context.Context, req *client.ExecuteStatementReq) (r *client.ExecuteStatementResp, err error) {
// 			executeStatementCount++
// 			savedQueryString = req.Statement
// 			executeStatementResp := &client.ExecuteStatementResp{
// 				Status: &client.RequestStatus{
// 					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS.String(),
// 				},
// 				ExecutionHandle: &cli_service.TOperationHandle{
// 					OperationId: &cli_service.THandleIdentifier{
// 						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
// 						Secret: []byte("b"),
// 					},
// 				},
// 			}
// 			return executeStatementResp, nil
// 		}

// 		getExecutionStatus := func(ctx context.Context, req *client.GetExecutionStatusReq) (r *client.GetExecutionStatusResp, err error) {
// 			getOperationStatusCount++
// 			getExecutionStatusResp := &client.GetExecutionStatusResp{
// 				ExecutionStatus: client.ExecutionStatus{
// 					ExecutionState:  cli_service.TOperationState_FINISHED_STATE.String(),
// 					NumModifiedRows: int64(10),
// 				},
// 			}
// 			return getExecutionStatusResp, nil
// 		}

// 		testClient := &client.TestClient{
// 			FnExecuteStatement:   executeStatement,
// 			FnGetExecutionStatus: getExecutionStatus,
// 		}
// 		testConn := &conn{
// 			session: getTestSession(),
// 			client:  testClient,
// 			cfg:     config.WithDefaults(),
// 		}
// 		testQuery := "select 1"
// 		testStmt := &stmt{
// 			conn:  testConn,
// 			query: testQuery,
// 		}
// 		rows, err := testStmt.QueryContext(context.Background(), []driver.NamedValue{})

// 		assert.NoError(t, err)
// 		assert.NotNil(t, rows)
// 		assert.Equal(t, 1, executeStatementCount)
// 		assert.Equal(t, testQuery, savedQueryString)
// 	})
// }
