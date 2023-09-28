package dbsql

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
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

func TestStmt_ExecContext(t *testing.T) {
	t.Run("ExecContext returns number of rows modified when execution is successful", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		var savedQueryString string
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			savedQueryString = req.Statement
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: thrift.Int64Ptr(10),
			}
			return getOperationStatusResp, nil
		}
		fetchResultSetMetadata := func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
			var b = false
			return &cli_service.TGetResultSetMetadataResp{IsStagingOperation: &b}, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:     executeStatement,
			FnGetOperationStatus:   getOperationStatus,
			FnGetResultSetMetadata: fetchResultSetMetadata,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		testQuery := "insert 10"
		testStmt := &stmt{
			conn:  testConn,
			query: testQuery,
		}
		res, err := testStmt.ExecContext(context.Background(), []driver.NamedValue{})

		assert.NoError(t, err)
		assert.NotNil(t, res)
		rowsAffected, _ := res.RowsAffected()
		assert.Equal(t, int64(10), rowsAffected)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, testQuery, savedQueryString)
	})
}

func TestStmt_QueryContext(t *testing.T) {
	t.Run("QueryContext returns rows object upon successful query", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		var savedQueryString string
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			savedQueryString = req.Statement
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: thrift.Int64Ptr(10),
			}
			return getOperationStatusResp, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		testQuery := "select 1"
		testStmt := &stmt{
			conn:  testConn,
			query: testQuery,
		}
		rows, err := testStmt.QueryContext(context.Background(), []driver.NamedValue{})

		assert.NoError(t, err)
		assert.NotNil(t, rows)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, testQuery, savedQueryString)
	})
}
