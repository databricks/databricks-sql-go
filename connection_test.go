package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pkg/errors"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	thriftbackend "github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestConn_ExecContext(t *testing.T) {
	t.Parallel()
	t.Run("ExecContext currently does not support query parameters", func(t *testing.T) {
		var executeStatementCount int

		testClient := &client.TestClient{}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		res, err := testConn.ExecContext(context.Background(), "select 1", []driver.NamedValue{
			{Value: 1, Name: "name"},
		})

		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, 0, executeStatementCount)
	})

	t.Run("ExecContext returns err when client.ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_ERROR_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, fmt.Errorf("error")
		}

		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
				ctxErr := ctx.Err()
				assert.NoError(t, ctxErr)
				return &cli_service.TCloseOperationResp{}, nil
			},
		}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		res, err := testConn.ExecContext(context.Background(), "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("ExecContext returns number of rows modified when execution is successful", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
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
		getResultSetMetadata := func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
			var b = false
			return &cli_service.TGetResultSetMetadataResp{IsStagingOperation: &b}, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
				ctxErr := ctx.Err()
				assert.NoError(t, ctxErr)
				return &cli_service.TCloseOperationResp{}, nil
			},
			FnGetResultSetMetadata: getResultSetMetadata,
		}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		res, err := testConn.ExecContext(context.Background(), "insert 10", []driver.NamedValue{})

		assert.NoError(t, err)
		assert.NotNil(t, res)
		rowsAffected, _ := res.RowsAffected()
		assert.Equal(t, int64(10), rowsAffected)
		assert.Equal(t, 1, executeStatementCount)
	})
	t.Run("ExecContext uses new context to close operation", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount, closeOperationCount, cancelOperationCount int
		var cancel context.CancelFunc
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
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
			cancel()
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: thrift.Int64Ptr(10),
			}
			return getOperationStatusResp, nil
		}

		getResultSetMetadata := func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
			var b = false
			return &cli_service.TGetResultSetMetadataResp{IsStagingOperation: &b}, nil
		}

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
				closeOperationCount++
				ctxErr := ctx.Err()
				assert.NoError(t, ctxErr)
				return &cli_service.TCloseOperationResp{}, nil
			},
			FnCancelOperation: func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
				cancelOperationCount++
				cancelOperationResp := &cli_service.TCancelOperationResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
				}
				return cancelOperationResp, nil
			},
			FnGetResultSetMetadata: getResultSetMetadata,
		}

		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		ctx := context.Background()
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		res, err := testConn.ExecContext(ctx, "insert 10", []driver.NamedValue{})
		// GetOperationStatus reports FINISHED in the same call that cancels the
		// context. Because the operation completed, the sentinel must report
		// success and must NOT cancel a finished operation, regardless of
		// scheduler timing — this assertion previously raced on cancelOperationCount.
		assert.NoError(t, err)
		assert.NotNil(t, res)
		rowsAffected, _ := res.RowsAffected()
		assert.Equal(t, int64(10), rowsAffected)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, cancelOperationCount)
		assert.Equal(t, 1, getOperationStatusCount)
		// CloseOperation must still run, on a fresh (non-cancelled) context, even
		// though the context passed to ExecContext was cancelled mid-poll. Its
		// FnCloseOperation asserts ctx.Err() == nil.
		assert.Equal(t, 1, closeOperationCount)
	})
}

func TestConn_QueryContext(t *testing.T) {
	t.Parallel()
	t.Run("QueryContext currently does not support query parameters", func(t *testing.T) {
		var executeStatementCount int

		testClient := &client.TestClient{}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		res, err := testConn.QueryContext(context.Background(), "select 1", []driver.NamedValue{
			{Value: 1, Name: "name"},
		})

		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, 0, executeStatementCount)
	})

	t.Run("QueryContext returns err when client.ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_ERROR_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, fmt.Errorf("error")
		}

		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		res, err := testConn.QueryContext(context.Background(), "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("QueryContext returns rows object upon successful query", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
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
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		rows, err := testConn.QueryContext(context.Background(), "select 1", []driver.NamedValue{})

		assert.NoError(t, err)
		assert.NotNil(t, rows)
		assert.Equal(t, 1, executeStatementCount)
	})
}

func TestConn_Ping(t *testing.T) {
	t.Run("ping returns ErrBadConn when executeStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_ERROR_STATUS,
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

		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		err := testConn.Ping(context.Background())

		assert.Error(t, err)
		assert.True(t, errors.Is(err, driver.ErrBadConn))
		assert.True(t, errors.Is(err, dbsqlerr.ExecutionError))
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("ping returns nil error when driver can establish connection", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
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
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: thrift.Int64Ptr(10),
			}
			return getOperationStatusResp, nil
		}

		var closeCount int
		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
				closeCount++
				return nil, nil
			},
		}

		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		err := testConn.Ping(context.Background())

		assert.Nil(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, closeCount)
	})
}

func TestConn_Begin(t *testing.T) {
	t.Run("Begin not supported", func(t *testing.T) {
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults()),
		}
		res, err := testConn.Begin()
		assert.Nil(t, res)
		assert.Error(t, err)
	})
}

func TestConn_BeginTx(t *testing.T) {
	t.Run("BeginTx not supported", func(t *testing.T) {
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults()),
		}
		res, err := testConn.BeginTx(context.Background(), driver.TxOptions{})
		assert.Nil(t, res)
		assert.Error(t, err)
	})
}

func TestConn_ResetSession(t *testing.T) {
	t.Run("ResetSession not currently supported", func(t *testing.T) {
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults()),
		}
		res := testConn.ResetSession(context.Background())
		assert.Nil(t, res)
	})
}

func TestConn_Close(t *testing.T) {
	t.Run("Close will call CloseSession", func(t *testing.T) {
		var closeSessionCount int

		closeSession := func(ctx context.Context, req *cli_service.TCloseSessionReq) (r *cli_service.TCloseSessionResp, err error) {
			closeSessionCount++
			closeSessionResp := &cli_service.TCloseSessionResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return closeSessionResp, nil
		}

		testClient := &client.TestClient{
			FnCloseSession: closeSession,
		}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		err := testConn.Close()

		assert.NoError(t, err)
		assert.Equal(t, 1, closeSessionCount)
	})

	t.Run("Close will err when CloseSession fails", func(t *testing.T) {
		var closeSessionCount int

		closeSession := func(ctx context.Context, req *cli_service.TCloseSessionReq) (r *cli_service.TCloseSessionResp, err error) {
			closeSessionCount++
			closeSessionResp := &cli_service.TCloseSessionResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_ERROR_STATUS,
				},
			}
			return closeSessionResp, fmt.Errorf("error")
		}

		testClient := &client.TestClient{
			FnCloseSession: closeSession,
		}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		err := testConn.Close()

		assert.Error(t, err)
		assert.Equal(t, 1, closeSessionCount)
	})
}

func TestConn_Prepare(t *testing.T) {
	t.Run("Prepare returns stmt struct", func(t *testing.T) {
		testClient := &client.TestClient{}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		stmt, err := testConn.Prepare("query string")
		assert.NoError(t, err)
		assert.NotNil(t, stmt)
	})
}

func TestConn_PrepareContext(t *testing.T) {
	t.Run("PrepareContext returns stmt struct", func(t *testing.T) {
		testClient := &client.TestClient{}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}
		stmt, err := testConn.PrepareContext(context.Background(), "query string")
		assert.NoError(t, err)
		assert.NotNil(t, stmt)
	})
}

func TestConn_execStagingOperation(t *testing.T) {
	t.Run("handles nil IsStagingOperation from DirectResults", func(t *testing.T) {
		testClient := &client.TestClient{}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}

		// Create response with nil IsStagingOperation in DirectResults
		exStmtResp := &cli_service.TExecuteStatementResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			OperationHandle: &cli_service.TOperationHandle{
				OperationId: &cli_service.THandleIdentifier{
					GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 223, 34, 54},
					Secret: []byte("b"),
				},
			},
			DirectResults: &cli_service.TSparkDirectResults{
				ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
					Status: &cli_service.TStatus{
						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
					},
					// IsStagingOperation is nil
				},
			},
		}

		// Mock GetResultSetMetadata to return false for IsStagingOperation
		var getResultSetMetadataCount int
		getResultSetMetadata := func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
			getResultSetMetadataCount++
			var falseVal = false
			return &cli_service.TGetResultSetMetadataResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				IsStagingOperation: &falseVal,
			}, nil
		}
		testClient.FnGetResultSetMetadata = getResultSetMetadata

		ctx := context.Background()
		op := thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()).OperationForTest(exStmtResp, nil)
		err := testConn.execStagingOperation(op, ctx)

		assert.Nil(t, err)
		assert.Equal(t, 0, getResultSetMetadataCount) // should not be called since DirectResults.ResultSetMetadata exists
	})

	t.Run("handles nil IsStagingOperation from GetResultSetMetadata", func(t *testing.T) {
		testClient := &client.TestClient{}
		testConn := &conn{
			cfg:     config.WithDefaults(),
			backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		}

		// Create response with nil DirectResults.ResultSetMetadata
		exStmtResp := &cli_service.TExecuteStatementResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
			OperationHandle: &cli_service.TOperationHandle{
				OperationId: &cli_service.THandleIdentifier{
					GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 223, 34, 54},
					Secret: []byte("b"),
				},
			},
			// DirectResults.ResultSetMetadata is nil
		}

		// Mock GetResultSetMetadata to return nil for IsStagingOperation
		var getResultSetMetadataCount int
		getResultSetMetadata := func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (_r *cli_service.TGetResultSetMetadataResp, _err error) {
			getResultSetMetadataCount++
			return &cli_service.TGetResultSetMetadataResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				// IsStagingOperation is nil
			}, nil
		}
		testClient.FnGetResultSetMetadata = getResultSetMetadata

		ctx := context.Background()
		op := thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()).OperationForTest(exStmtResp, nil)
		err := testConn.execStagingOperation(op, ctx)

		assert.Nil(t, err)
		assert.Equal(t, 1, getResultSetMetadataCount) // should be called since DirectResults.ResultSetMetadata is nil
	})
}

func TestChunkTimingAccumulator_Record(t *testing.T) {
	tests := []struct {
		name       string
		latencies  []int64
		wantInit   int64
		wantSlow   int64
		wantSum    int64
		wantReturn []bool
	}{
		{"zero latency skipped", []int64{0}, 0, 0, 0, []bool{false}},
		{"negative skipped", []int64{-5}, 0, 0, 0, []bool{false}},
		{"single positive", []int64{10}, 10, 10, 10, []bool{true}},
		{"initial preserved across calls", []int64{10, 20}, 10, 20, 30, []bool{true, true}},
		{"slowest tracks max not last", []int64{30, 10, 50}, 30, 50, 90, []bool{true, true, true}},
		{"zero interleaved skipped", []int64{10, 0, 20}, 10, 20, 30, []bool{true, false, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var a chunkTimingAccumulator
			for i, lat := range tt.latencies {
				got := a.record(lat)
				if got != tt.wantReturn[i] {
					t.Errorf("record(%d) = %v, want %v", lat, got, tt.wantReturn[i])
				}
			}
			if a.initialMs != tt.wantInit {
				t.Errorf("initialMs = %d, want %d", a.initialMs, tt.wantInit)
			}
			if a.slowestMs != tt.wantSlow {
				t.Errorf("slowestMs = %d, want %d", a.slowestMs, tt.wantSlow)
			}
			if a.sumMs != tt.wantSum {
				t.Errorf("sumMs = %d, want %d", a.sumMs, tt.wantSum)
			}
		})
	}
}

func TestChunkTimingAccumulator_CloudFetchFileCount(t *testing.T) {
	var a chunkTimingAccumulator
	a.cloudFetchFileCount++
	a.record(0) // sub-ms download — still counted but not timed
	a.cloudFetchFileCount++
	a.record(5)

	if a.cloudFetchFileCount != 2 {
		t.Errorf("cloudFetchFileCount = %d, want 2", a.cloudFetchFileCount)
	}
	if a.initialMs != 5 {
		t.Errorf("initialMs = %d, want 5 (zero-latency file should not set initial)", a.initialMs)
	}
}

func getTestSession() *cli_service.TOpenSessionResp {
	return &cli_service.TOpenSessionResp{SessionHandle: &cli_service.TSessionHandle{
		SessionId: &cli_service.THandleIdentifier{
			GUID: []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
		},
	}}
}

// TestConn_handleStagingRetry verifies that the staging-operation HTTP wrappers
// (handleStagingPut/Get/Remove) retry transient S3 errors. ES-1911239: FactSet
// hit intermittent HTTP 503 SlowDown on PUT against a Unity Catalog external
// volume; pre-fix any single 5xx failed the entire SQL statement.
//
// Retry behavior mirrors the CloudFetch fix from ES-1892645 / PR #355:
//   - Retryable statuses: 408/429/500/502/503/504.
//   - Exponential backoff with equal jitter, honoring RetryMax/RetryWaitMin/
//     RetryWaitMax from the connection config.
//   - Integer Retry-After response header is honored (capped at RetryWaitMax).
//   - Non-retryable statuses (e.g. 403) fail on the first attempt.
//   - Context cancellation aborts backoff promptly.
func TestConn_handleStagingRetry(t *testing.T) {
	// retryCfg returns a fast-backoff config so tests don't burn wall-clock
	// on sleeps. RetryMax leaves room for several retries; RetryWaitMin/Max
	// keep the worst-case test runtime under a second.
	retryCfg := func() *config.Config {
		cfg := config.WithDefaults()
		cfg.RetryMax = 4
		cfg.RetryWaitMin = 1 * time.Millisecond
		cfg.RetryWaitMax = 5 * time.Millisecond
		return cfg
	}

	t.Run("PUT retries transient 503 and eventually succeeds", func(t *testing.T) {
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("<Error><Code>SlowDown</Code></Error>"))
				return
			}
			// Drain the body so we exercise the retry-replay path for PUTs.
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "payload.parquet")
		if err := os.WriteFile(localFile, []byte("parquet bytes"), 0600); err != nil {
			t.Fatalf("write local file: %v", err)
		}

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingPut(context.Background(), server.URL, nil, localFile)
		assert.Nil(t, err)
		assert.Equal(t, int32(3), atomic.LoadInt32(&attempts), "expected 2 retries before success")
	})

	t.Run("GET retries transient 503 and eventually succeeds", func(t *testing.T) {
		var attempts int32
		body := []byte("downloaded bytes")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "out.bin")

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingGet(context.Background(), server.URL, nil, localFile)
		assert.Nil(t, err)
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))

		got, readErr := os.ReadFile(localFile) //nolint:gosec
		assert.Nil(t, readErr)
		assert.Equal(t, body, got, "GET should write the final-attempt body to local file")
	})

	t.Run("REMOVE retries transient 503 and eventually succeeds", func(t *testing.T) {
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingRemove(context.Background(), server.URL, nil)
		assert.Nil(t, err)
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))
	})

	t.Run("REMOVE treats 503-then-404 as success (idempotent delete)", func(t *testing.T) {
		// The first DELETE may have applied server-side even though the
		// response was 503 (load balancer dies mid-response, etc.). The
		// retry then sees 404 — the object is already gone. The caller's
		// post-condition ("object absent") is satisfied, so this is success.
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingRemove(context.Background(), server.URL, nil)
		assert.Nil(t, err, "503 then 404 on REMOVE should succeed: the object is absent, which is the caller's intent")
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))
	})

	t.Run("REMOVE treats first-attempt 404 as success (idempotent delete)", func(t *testing.T) {
		// DELETE on a non-existent object is success: the post-condition
		// ("object absent") is already true. Documents the behavior change
		// vs. the pre-retry implementation, which surfaced 404 as failure.
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingRemove(context.Background(), server.URL, nil)
		assert.Nil(t, err, "404 on REMOVE should always be success")
		assert.Equal(t, int32(1), atomic.LoadInt32(&attempts), "404 must not trigger a retry")
	})

	t.Run("PUT first-attempt 404 still fails (only REMOVE treats 404 as success)", func(t *testing.T) {
		// Guard against the 404-as-success behavior leaking into the other
		// handlers. PUT/GET must still treat 404 as a terminal failure.
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "payload.parquet")
		if err := os.WriteFile(localFile, []byte("data"), 0600); err != nil {
			t.Fatalf("write local file: %v", err)
		}

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingPut(context.Background(), server.URL, nil, localFile)
		assert.NotNil(t, err, "404 on PUT must remain a terminal failure")
		assert.ErrorContains(t, err, "404")
		assert.Equal(t, int32(1), atomic.LoadInt32(&attempts))
	})

	t.Run("PUT retries transient HTTP 500", func(t *testing.T) {
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&attempts, 1)
			if n < 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "payload.parquet")
		if err := os.WriteFile(localFile, []byte("data"), 0600); err != nil {
			t.Fatalf("write local file: %v", err)
		}

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingPut(context.Background(), server.URL, nil, localFile)
		assert.Nil(t, err)
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))
	})

	t.Run("PUT fails after exhausting retries on persistent 503", func(t *testing.T) {
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "payload.parquet")
		if err := os.WriteFile(localFile, []byte("data"), 0600); err != nil {
			t.Fatalf("write local file: %v", err)
		}

		cfg := retryCfg()
		cfg.RetryMax = 2
		c := &conn{cfg: cfg}
		err := c.handleStagingPut(context.Background(), server.URL, nil, localFile)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "503")
		// initial attempt + RetryMax retries
		assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	})

	t.Run("PUT does not retry non-retryable status (403)", func(t *testing.T) {
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusForbidden)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "payload.parquet")
		if err := os.WriteFile(localFile, []byte("data"), 0600); err != nil {
			t.Fatalf("write local file: %v", err)
		}

		c := &conn{cfg: retryCfg()}
		started := time.Now()
		err := c.handleStagingPut(context.Background(), server.URL, nil, localFile)
		elapsed := time.Since(started)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "403")
		assert.Equal(t, int32(1), atomic.LoadInt32(&attempts), "non-retryable status must fail on first attempt")
		// retryCfg's RetryWaitMin is 1ms; if a backoff fired by mistake we'd
		// observe at least that. 50ms gives headroom for slow CI without
		// masking an accidental retry.
		assert.Less(t, elapsed, 50*time.Millisecond, "non-retryable status must not trigger backoff")
	})

	t.Run("PUT replays the file body on each retry", func(t *testing.T) {
		// Verifies that the retry implementation correctly handles the request
		// body lifecycle: an os.File consumed by attempt N must be rewound or
		// re-opened before attempt N+1, otherwise the server sees a zero-length
		// body on retries.
		var (
			mu            sync.Mutex
			receivedSizes []int64
		)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			receivedSizes = append(receivedSizes, int64(len(body)))
			n := len(receivedSizes)
			mu.Unlock()
			if n < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "payload.parquet")
		payload := []byte("important parquet data that must be re-sent on each retry")
		if err := os.WriteFile(localFile, payload, 0600); err != nil {
			t.Fatalf("write local file: %v", err)
		}

		c := &conn{cfg: retryCfg()}
		err := c.handleStagingPut(context.Background(), server.URL, nil, localFile)
		assert.Nil(t, err)
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, 3, len(receivedSizes), "expected 3 PUT attempts")
		for i, sz := range receivedSizes {
			assert.Equal(t, int64(len(payload)), sz, "attempt %d received %d bytes, expected full payload of %d bytes", i+1, sz, len(payload))
		}
	})

	t.Run("PUT respects context cancellation during backoff", func(t *testing.T) {
		var attempts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&attempts, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		tmpDir := t.TempDir()
		localFile := filepath.Join(tmpDir, "payload.parquet")
		if err := os.WriteFile(localFile, []byte("data"), 0600); err != nil {
			t.Fatalf("write local file: %v", err)
		}

		cfg := retryCfg()
		cfg.RetryMax = 5
		cfg.RetryWaitMin = 500 * time.Millisecond
		cfg.RetryWaitMax = 1 * time.Second

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		c := &conn{cfg: cfg}
		started := time.Now()
		err := c.handleStagingPut(ctx, server.URL, nil, localFile)
		elapsed := time.Since(started)

		assert.NotNil(t, err)
		// Cancellation should land well before the full retry budget elapses.
		// 5 retries * 500ms+ minimum backoff = 2.5s+ without cancellation.
		// 2s gives generous headroom on slow CI runners without masking a
		// regression where cancellation is honored only at retry boundaries.
		assert.Less(t, elapsed, 2*time.Second, "context cancel should abort PUT retry backoff promptly")
	})
}
