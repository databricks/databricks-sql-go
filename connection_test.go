package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestConn_executeStatement(t *testing.T) {
	t.Parallel()
	t.Run("executeStatement should err when client.ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			return nil, fmt.Errorf("error")
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		_, err := testConn.executeStatement(context.Background(), "select 1", []driver.NamedValue{})
		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("executeStatement should return TExecuteStatementResp on success", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54},
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
			}
			return executeStatementResp, nil
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		_, err := testConn.executeStatement(context.Background(), "select 1", []driver.NamedValue{})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("ExecStatement should close operation on success", func(t *testing.T) {
		var executeStatementCount, closeOperationCount int
		executeStatementResp := &cli_service.TExecuteStatementResp{
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
		}

		testClient := &client.TestClient{
			FnExecuteStatement: func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
				executeStatementCount++
				return executeStatementResp, nil
			},
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
				closeOperationCount++
				return &cli_service.TCloseOperationResp{}, nil
			},
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}

		type opStateTest struct {
			state               cli_service.TOperationState
			err                 string
			closeOperationCount int
		}

		// test behaviour with all terminal operation states
		operationStateTests := []opStateTest{
			{state: cli_service.TOperationState_ERROR_STATE, err: "error state", closeOperationCount: 1},
			{state: cli_service.TOperationState_FINISHED_STATE, err: "", closeOperationCount: 1},
			{state: cli_service.TOperationState_CANCELED_STATE, err: "cancelled state", closeOperationCount: 1},
			{state: cli_service.TOperationState_CLOSED_STATE, err: "closed state", closeOperationCount: 0},
			{state: cli_service.TOperationState_TIMEDOUT_STATE, err: "timeout state", closeOperationCount: 1},
		}

		for _, opTest := range operationStateTests {
			closeOperationCount = 0
			executeStatementCount = 0
			executeStatementResp.DirectResults.OperationStatus.OperationState = &opTest.state
			executeStatementResp.DirectResults.OperationStatus.DisplayMessage = &opTest.err
			_, err := testConn.ExecContext(context.Background(), "select 1", []driver.NamedValue{})
			if opTest.err == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, opTest.err)
			}
			assert.Equal(t, 1, executeStatementCount)
			assert.Equal(t, opTest.closeOperationCount, closeOperationCount)
		}

		// if the execute statement response contains direct results with a non-nil CloseOperation member
		// we shouldn't call close
		closeOperationCount = 0
		executeStatementCount = 0
		executeStatementResp.DirectResults.CloseOperation = &cli_service.TCloseOperationResp{}
		finished := cli_service.TOperationState_FINISHED_STATE
		executeStatementResp.DirectResults.OperationStatus.OperationState = &finished
		_, err := testConn.ExecContext(context.Background(), "select 1", []driver.NamedValue{})
		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, closeOperationCount)
	})

	t.Run("executeStatement should not call cancel if not needed", func(t *testing.T) {
		var executeStatementCount int
		var cancelOperationCount int
		var cancel context.CancelFunc
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			cancel()
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
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
					CloseOperation: &cli_service.TCloseOperationResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
					},
				},
			}
			return executeStatementResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
			FnCancelOperation:  cancelOperation,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}

		ctx := context.Background()
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		_, err := testConn.executeStatement(ctx, "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, cancelOperationCount)
	})
	t.Run("executeStatement should call cancel if needed", func(t *testing.T) {
		var executeStatementCount int
		var cancelOperationCount int
		var cancel context.CancelFunc
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			cancel()
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
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
			}
			return executeStatementResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
			FnCancelOperation:  cancelOperation,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		ctx := context.Background()
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		_, err := testConn.executeStatement(ctx, "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, cancelOperationCount)
	})

}

func TestConn_pollOperation(t *testing.T) {
	t.Parallel()
	t.Run("pollOperation returns finished state response when query finishes", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 4, 7, 8, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns closed state response when query has been closed", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns closed state response when query has been closed", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns unknown state response when query state is unknown", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_UKNOWN_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_UKNOWN_STATE),
		}, *res)
	})

	t.Run("pollOperation returns error state response when query errors", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
		}, *res)
	})

	t.Run("pollOperation returns finished state response after query cycles through various states", func(t *testing.T) {
		var getOperationStatusCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.pollOperation(context.Background(), &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 3, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, getOperationStatusCount)
		assert.Equal(t, cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
		}, *res)
	})

	t.Run("pollOperation returns cancel err when context times out before get operation", func(t *testing.T) {
		var getOperationStatusCount, cancelOperationCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
			FnCancelOperation:    cancelOperation,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		res, err := testConn.pollOperation(ctx, &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.Error(t, err)
		assert.Equal(t, 0, getOperationStatusCount)
		assert.Equal(t, 1, cancelOperationCount)
		assert.Nil(t, res)
	})

	t.Run("pollOperation returns cancel err when context times out before get operation", func(t *testing.T) {
		var getOperationStatusCount, cancelOperationCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
			FnCancelOperation:    cancelOperation,
		}
		cfg := config.WithDefaults()
		cfg.PollInterval = 100 * time.Millisecond
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     cfg,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		res, err := testConn.pollOperation(ctx, &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})

		assert.Error(t, err)
		assert.GreaterOrEqual(t, getOperationStatusCount, 1)
		assert.Equal(t, 1, cancelOperationCount)
		assert.Nil(t, res)
	})

	t.Run("pollOperation returns cancel err when context is cancelled", func(t *testing.T) {
		var getOperationStatusCount, cancelOperationCount int
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			operationStates := [4]cli_service.TOperationState{cli_service.TOperationState_INITIALIZED_STATE, cli_service.TOperationState_PENDING_STATE,
				cli_service.TOperationState_RUNNING_STATE, cli_service.TOperationState_FINISHED_STATE}
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(operationStates[getOperationStatusCount-1]),
			}
			return getOperationStatusResp, nil
		}
		cancelOperation := func(ctx context.Context, req *cli_service.TCancelOperationReq) (r *cli_service.TCancelOperationResp, err error) {
			cancelOperationCount++
			cancelOperationResp := &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
			}
			return cancelOperationResp, nil
		}
		testClient := &client.TestClient{
			FnGetOperationStatus: getOperationStatus,
			FnCancelOperation:    cancelOperation,
		}
		cfg := config.WithDefaults()
		cfg.PollInterval = 100 * time.Millisecond
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     cfg,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			time.Sleep(150 * time.Millisecond)
			cancel()
		}()
		res, err := testConn.pollOperation(ctx, &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
				Secret: []byte("b"),
			},
		})
		assert.Error(t, err)
		assert.GreaterOrEqual(t, getOperationStatusCount, 1)
		assert.GreaterOrEqual(t, 1, cancelOperationCount)
		assert.Nil(t, res)
	})
}

func TestConn_runQuery(t *testing.T) {
	t.Parallel()
	t.Run("runQuery should err when client.ExecuteStatement fails", func(t *testing.T) {
		var executeStatementCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			return nil, fmt.Errorf("error")
		}
		testClient := &client.TestClient{
			FnExecuteStatement: executeStatement,
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})
		assert.Error(t, err)
		assert.Nil(t, exStmtResp)
		assert.Nil(t, opStatusResp)
		assert.Equal(t, 1, executeStatementCount)
	})

	t.Run("runQuery should err when pollOperation fails", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 3, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
			}
			return getOperationStatusResp, fmt.Errorf("error on get operation status")
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
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.Nil(t, opStatusResp)
	})

	t.Run("runQuery should return resp when query is finished", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 4, 4, 223, 34, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}
		var numModRows int64 = 2

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: &numModRows,
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
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
		assert.Equal(t, &numModRows, opStatusResp.NumModifiedRows)
	})

	t.Run("runQuery should return resp and error when query is canceled", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 4, 4, 223, 34, 23, 54},
						Secret: []byte("b"),
					},
				},
			}
			return executeStatementResp, nil
		}
		var numModRows int64 = 3

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_CANCELED_STATE),
				NumModifiedRows: &numModRows,
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
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
		assert.Equal(t, &numModRows, opStatusResp.NumModifiedRows)
	})

	t.Run("runQuery should return resp when query is finished with DirectResults", func(t *testing.T) {
		var executeStatementCount, getOperationStatusCount int
		executeStatement := func(ctx context.Context, req *cli_service.TExecuteStatementReq) (r *cli_service.TExecuteStatementResp, err error) {
			executeStatementCount++
			executeStatementResp := &cli_service.TExecuteStatementResp{
				Status: &cli_service.TStatus{
					StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
				},
				OperationHandle: &cli_service.TOperationHandle{
					OperationId: &cli_service.THandleIdentifier{
						GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 4, 4, 223, 34, 54, 87},
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
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
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
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, getOperationStatusCount) // GetOperationStatus should not be called, already provided in DirectResults
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})

	t.Run("runQuery should return resp and err when query is cancelled with DirectResults", func(t *testing.T) {
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
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CANCELED_STATE),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
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
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 0, getOperationStatusCount) // GetOperationStatus should not be called, already provided in DirectResults
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})

	t.Run("runQuery should return resp when query is finished but DirectResults still live", func(t *testing.T) {
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
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_INITIALIZED_STATE),
					},
				},
			}
			return executeStatementResp, nil
		}
		var numModRows int64 = 3
		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState:  cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				NumModifiedRows: &numModRows,
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
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})

		assert.NoError(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, &numModRows, opStatusResp.NumModifiedRows)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})

	t.Run("runQuery should return resp and err when query is cancelled after DirectResults still live", func(t *testing.T) {
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
				DirectResults: &cli_service.TSparkDirectResults{
					OperationStatus: &cli_service.TGetOperationStatusResp{
						Status: &cli_service.TStatus{
							StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
						},
						OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_INITIALIZED_STATE),
					},
				},
			}
			return executeStatementResp, nil
		}

		getOperationStatus := func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (r *cli_service.TGetOperationStatusResp, err error) {
			getOperationStatusCount++
			getOperationStatusResp := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CANCELED_STATE),
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
		exStmtResp, opStatusResp, err := testConn.runQuery(context.Background(), "select 1", []driver.NamedValue{})

		assert.Error(t, err)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.NotNil(t, exStmtResp)
		assert.NotNil(t, opStatusResp)
	})
}

func TestConn_ExecContext(t *testing.T) {
	t.Parallel()
	t.Run("ExecContext currently does not support query parameters", func(t *testing.T) {
		var executeStatementCount int

		testClient := &client.TestClient{}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (_r *cli_service.TCloseOperationResp, _err error) {
				ctxErr := ctx.Err()
				assert.NoError(t, ctxErr)
				return &cli_service.TCloseOperationResp{}, nil
			},
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
		}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		ctx := context.Background()
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		res, err := testConn.ExecContext(ctx, "insert 10", []driver.NamedValue{})
		assert.Error(t, err)
		assert.Nil(t, res)
		assert.Equal(t, 1, executeStatementCount)
		assert.Equal(t, 1, cancelOperationCount)
		assert.Equal(t, 1, getOperationStatusCount)
		assert.Equal(t, 1, closeOperationCount)
	})
}

func TestConn_QueryContext(t *testing.T) {
	t.Parallel()
	t.Run("QueryContext currently does not support query parameters", func(t *testing.T) {
		var executeStatementCount int

		testClient := &client.TestClient{}
		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		err := testConn.Ping(context.Background())

		assert.Error(t, err)
		assert.Equal(t, driver.ErrBadConn, err)
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

		testClient := &client.TestClient{
			FnExecuteStatement:   executeStatement,
			FnGetOperationStatus: getOperationStatus,
		}

		testConn := &conn{
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		err := testConn.Ping(context.Background())

		assert.Nil(t, err)
		assert.Equal(t, 1, executeStatementCount)
	})
}

func TestConn_Begin(t *testing.T) {
	t.Run("Begin not supported", func(t *testing.T) {
		testConn := &conn{
			session: getTestSession(),
			client:  &client.TestClient{},
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.Begin()
		assert.Nil(t, res)
		assert.Error(t, err)
	})
}

func TestConn_BeginTx(t *testing.T) {
	t.Run("BeginTx not supported", func(t *testing.T) {
		testConn := &conn{
			session: getTestSession(),
			client:  &client.TestClient{},
			cfg:     config.WithDefaults(),
		}
		res, err := testConn.BeginTx(context.Background(), driver.TxOptions{})
		assert.Nil(t, res)
		assert.Error(t, err)
	})
}

func TestConn_ResetSession(t *testing.T) {
	t.Run("ResetSession not currently supported", func(t *testing.T) {
		testConn := &conn{
			session: getTestSession(),
			client:  &client.TestClient{},
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
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
			session: getTestSession(),
			client:  testClient,
			cfg:     config.WithDefaults(),
		}
		stmt, err := testConn.PrepareContext(context.Background(), "query string")
		assert.NoError(t, err)
		assert.NotNil(t, stmt)
	})
}

func getTestSession() *cli_service.TOpenSessionResp {
	return &cli_service.TOpenSessionResp{SessionHandle: &cli_service.TSessionHandle{
		SessionId: &cli_service.THandleIdentifier{
			GUID: []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
		},
	}}
}
