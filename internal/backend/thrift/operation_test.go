package thrift

import (
	"context"
	"errors"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// opHandle builds a minimal operation handle for tests.
func opHandle() *cli_service.TOperationHandle {
	return &cli_service.TOperationHandle{
		OperationId: &cli_service.THandleIdentifier{
			GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34},
			Secret: []byte("s"),
		},
	}
}

// TestOperationClose pins the close-decision matrix: Close must issue the
// CloseOperation RPC (closed=true) only for a live terminal op with an open
// handle, and skip it (closed=false) for no-handle / already-closed-via-direct-
// results / already-CLOSED_STATE. The returned `closed` bool gates CLOSE_STATEMENT
// telemetry in conn, so the skip branches are load-bearing.
func TestOperationClose(t *testing.T) {
	makeOp := func(exResp *cli_service.TExecuteStatementResp, opStatus *cli_service.TGetOperationStatusResp, onClose func()) *thriftOperation {
		tc := &client.TestClient{
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
				if onClose != nil {
					onClose()
				}
				return &cli_service.TCloseOperationResp{}, nil
			},
		}
		be := NewForTest(tc, getTestSession(), config.WithDefaults())
		return be.OperationForTest(exResp, opStatus)
	}

	t.Run("no handle -> skip RPC, closed=false", func(t *testing.T) {
		called := false
		op := makeOp(&cli_service.TExecuteStatementResp{}, nil, func() { called = true })
		closed, err := op.Close(context.Background())
		assert.False(t, closed)
		assert.NoError(t, err)
		assert.False(t, called, "CloseOperation must not be called without a handle")
	})

	t.Run("already closed via direct results -> skip RPC, closed=false", func(t *testing.T) {
		called := false
		exResp := &cli_service.TExecuteStatementResp{
			OperationHandle: opHandle(),
			DirectResults: &cli_service.TSparkDirectResults{
				CloseOperation: &cli_service.TCloseOperationResp{},
			},
		}
		op := makeOp(exResp, nil, func() { called = true })
		closed, err := op.Close(context.Background())
		assert.False(t, closed)
		assert.NoError(t, err)
		assert.False(t, called, "must not re-close an operation the server already closed")
	})

	t.Run("already in CLOSED_STATE -> skip RPC, closed=false", func(t *testing.T) {
		called := false
		exResp := &cli_service.TExecuteStatementResp{OperationHandle: opHandle()}
		opStatus := &cli_service.TGetOperationStatusResp{
			OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_CLOSED_STATE),
		}
		op := makeOp(exResp, opStatus, func() { called = true })
		closed, err := op.Close(context.Background())
		assert.False(t, closed)
		assert.NoError(t, err)
		assert.False(t, called, "must not re-close an operation already in CLOSED_STATE")
	})

	// Live terminal states with an open handle: Close issues the RPC.
	for _, state := range []cli_service.TOperationState{
		cli_service.TOperationState_FINISHED_STATE,
		cli_service.TOperationState_ERROR_STATE,
		cli_service.TOperationState_CANCELED_STATE,
		cli_service.TOperationState_TIMEDOUT_STATE,
	} {
		t.Run("open handle in "+state.String()+" -> issue RPC, closed=true", func(t *testing.T) {
			called := false
			exResp := &cli_service.TExecuteStatementResp{OperationHandle: opHandle()}
			opStatus := &cli_service.TGetOperationStatusResp{OperationState: cli_service.TOperationStatePtr(state)}
			op := makeOp(exResp, opStatus, func() { called = true })
			closed, err := op.Close(context.Background())
			assert.True(t, closed, "a live op with an open handle must be closed")
			assert.NoError(t, err)
			assert.True(t, called, "CloseOperation RPC must be issued")
		})
	}

	t.Run("nil opStatus with open handle -> issue RPC (state defaults, not CLOSED)", func(t *testing.T) {
		called := false
		exResp := &cli_service.TExecuteStatementResp{OperationHandle: opHandle()}
		op := makeOp(exResp, nil, func() { called = true })
		closed, err := op.Close(context.Background())
		assert.True(t, closed)
		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("RPC error is returned with closed=true", func(t *testing.T) {
		tc := &client.TestClient{
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
				return nil, errors.New("boom")
			},
		}
		be := NewForTest(tc, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{OperationHandle: opHandle()}, nil)
		closed, err := op.Close(context.Background())
		assert.True(t, closed, "closed=true reflects that the RPC was attempted, so telemetry records it")
		assert.Error(t, err)
	})
}

// TestOperationStatementID pins the accessor contract: empty string when the
// Operation has no server handle, and the formatted GUID when it does. The
// value is memoized after the first read, so repeated calls return the same
// string cheaply.
func TestOperationStatementID(t *testing.T) {
	be := NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults())

	t.Run("empty when there is no handle", func(t *testing.T) {
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{}, nil)
		assert.Equal(t, "", op.StatementID())
	})

	t.Run("formatted GUID when the handle is present", func(t *testing.T) {
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{OperationHandle: opHandle()}, nil)
		got := op.StatementID()
		// opHandle's GUID is 16 bytes, so SprintGuid renders it dashed.
		assert.Equal(t, "01020304-0217-0402-0301-02030404df22", got)
		// Repeated calls hit the memo and return the same value.
		assert.Equal(t, got, op.StatementID())
	})
}

// TestOperationAffectedRows pins the accessor contract on the success path:
// the value is read from opStatusResp.NumModifiedRows via the Thrift getter,
// which returns 0 when the field is unset. AffectedRows is defined only on
// the success path (per backend.Operation), where runQuery has guaranteed a
// non-nil opStatusResp, so a nil-opStatusResp input is out of contract and
// not tested.
func TestOperationAffectedRows(t *testing.T) {
	be := NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults())

	t.Run("returns NumModifiedRows on the success path", func(t *testing.T) {
		rows := int64(42)
		op := be.OperationForTest(
			&cli_service.TExecuteStatementResp{OperationHandle: opHandle()},
			&cli_service.TGetOperationStatusResp{NumModifiedRows: &rows},
		)
		assert.Equal(t, int64(42), op.AffectedRows())
	})

	t.Run("returns 0 when NumModifiedRows is unset", func(t *testing.T) {
		op := be.OperationForTest(
			&cli_service.TExecuteStatementResp{OperationHandle: opHandle()},
			&cli_service.TGetOperationStatusResp{},
		)
		assert.Equal(t, int64(0), op.AffectedRows())
	})
}

// TestOperationClose_Idempotency pins the interface contract: a second Close
// call on the same Operation must not re-issue the CloseOperation RPC. This
// guards a defer + explicit-close double-invocation pattern (and any consumer
// that calls Close twice) from re-firing CLOSE_STATEMENT telemetry or wasting
// an RPC.
func TestOperationClose_Idempotency(t *testing.T) {
	t.Run("second Close on a live op is a no-op", func(t *testing.T) {
		closeCalls := 0
		tc := &client.TestClient{
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
				closeCalls++
				return &cli_service.TCloseOperationResp{}, nil
			},
		}
		be := NewForTest(tc, getTestSession(), config.WithDefaults())
		exResp := &cli_service.TExecuteStatementResp{OperationHandle: opHandle()}
		opStatus := &cli_service.TGetOperationStatusResp{OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE)}
		op := be.OperationForTest(exResp, opStatus)

		closed1, err1 := op.Close(context.Background())
		assert.True(t, closed1)
		require.NoError(t, err1)

		closed2, err2 := op.Close(context.Background())
		assert.False(t, closed2, "second Close must not re-issue the RPC")
		require.NoError(t, err2)

		assert.Equal(t, 1, closeCalls, "CloseOperation RPC must be issued exactly once")
	})

	t.Run("second Close after RPC error is a no-op", func(t *testing.T) {
		closeCalls := 0
		tc := &client.TestClient{
			FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
				closeCalls++
				return nil, errors.New("boom")
			},
		}
		be := NewForTest(tc, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{OperationHandle: opHandle()}, nil)

		closed1, err1 := op.Close(context.Background())
		assert.True(t, closed1)
		assert.Error(t, err1)

		closed2, err2 := op.Close(context.Background())
		assert.False(t, closed2, "second Close after a failed first Close must not retry the RPC")
		require.NoError(t, err2)

		assert.Equal(t, 1, closeCalls, "a failed close attempt is not retried by a second Close call")
	})
}

// TestOperationIsStaging covers the two branches: answer from direct-results
// metadata (no RPC) vs. GetResultSetMetadata RPC when direct results are absent.
func TestOperationIsStaging(t *testing.T) {
	truePtr := true
	falsePtr := false

	t.Run("from direct results, no RPC", func(t *testing.T) {
		metaCalls := 0
		tc := &client.TestClient{
			FnGetResultSetMetadata: func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
				metaCalls++
				return &cli_service.TGetResultSetMetadataResp{}, nil
			},
		}
		be := NewForTest(tc, getTestSession(), config.WithDefaults())
		exResp := &cli_service.TExecuteStatementResp{
			OperationHandle: opHandle(),
			DirectResults: &cli_service.TSparkDirectResults{
				ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{IsStagingOperation: &truePtr},
			},
		}
		op := be.OperationForTest(exResp, nil)
		staging, err := op.IsStaging(context.Background())
		assert.True(t, staging)
		assert.NoError(t, err)
		assert.Equal(t, 0, metaCalls, "direct-results metadata must be used without an RPC")
	})

	t.Run("falls back to GetResultSetMetadata RPC", func(t *testing.T) {
		metaCalls := 0
		tc := &client.TestClient{
			FnGetResultSetMetadata: func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
				metaCalls++
				return &cli_service.TGetResultSetMetadataResp{IsStagingOperation: &falsePtr}, nil
			},
		}
		be := NewForTest(tc, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{OperationHandle: opHandle()}, nil)
		staging, err := op.IsStaging(context.Background())
		assert.False(t, staging)
		assert.NoError(t, err)
		assert.Equal(t, 1, metaCalls, "absent direct-results metadata must trigger one RPC")
	})

	t.Run("no handle -> false, no RPC", func(t *testing.T) {
		be := NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{}, nil)
		staging, err := op.IsStaging(context.Background())
		assert.False(t, staging)
		require.NoError(t, err)
	})
}

// TestOperationExecutionError pins the exact user-visible composed error string
// returned when a statement finishes in a non-FINISHED terminal state, plus the
// sqlstate carried on the wrapped DBExecutionError. The string is the driver's
// stable contract for callers that match on it; the composition spans four
// pieces (databricksError.Error's "databricks: execution error: " prefix,
// ErrQueryExecution "failed to execute query", unexpectedOperationState's
// "unexpected operation state STATE" wrap, and opStatusResp.DisplayMessage) so
// any of them regressing must fail this test.
func TestOperationExecutionError(t *testing.T) {
	t.Run("nil cause -> nil error", func(t *testing.T) {
		be := NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{}, nil)
		assert.NoError(t, op.ExecutionError(context.Background(), nil))
	})

	for _, state := range []cli_service.TOperationState{
		cli_service.TOperationState_ERROR_STATE,
		cli_service.TOperationState_CANCELED_STATE,
		cli_service.TOperationState_TIMEDOUT_STATE,
		cli_service.TOperationState_CLOSED_STATE,
	} {
		t.Run("composed message + sqlstate for "+state.String(), func(t *testing.T) {
			displayMsg := "the server said no"
			sqlState := "42000"
			opStatus := &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(state),
				DisplayMessage: &displayMsg,
				SqlState:       &sqlState,
			}
			be := NewForTest(&client.TestClient{}, getTestSession(), config.WithDefaults())
			op := be.OperationForTest(&cli_service.TExecuteStatementResp{OperationHandle: opHandle()}, opStatus)
			err := op.ExecutionError(context.Background(), unexpectedOperationState(opStatus))
			require.Error(t, err)
			assert.EqualError(t, err,
				"databricks: execution error: failed to execute query: unexpected operation state "+
					state.String()+": "+displayMsg)
			var execErr dbsqlerr.DBExecutionError
			require.True(t, errors.As(err, &execErr), "wrapped error must satisfy DBExecutionError")
			assert.Equal(t, sqlState, execErr.SqlState(), "sqlstate must propagate from opStatusResp")
		})
	}
}
