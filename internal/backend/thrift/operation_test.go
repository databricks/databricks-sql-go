package thrift

import (
	"context"
	"errors"
	"testing"

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
		be := newTestBackend(tc, getTestSession(), config.WithDefaults())
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
		be := newTestBackend(tc, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{OperationHandle: opHandle()}, nil)
		closed, err := op.Close(context.Background())
		assert.True(t, closed, "closed=true reflects that the RPC was attempted, so telemetry records it")
		assert.Error(t, err)
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
		be := newTestBackend(tc, getTestSession(), config.WithDefaults())
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
		be := newTestBackend(tc, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{OperationHandle: opHandle()}, nil)
		staging, err := op.IsStaging(context.Background())
		assert.False(t, staging)
		assert.NoError(t, err)
		assert.Equal(t, 1, metaCalls, "absent direct-results metadata must trigger one RPC")
	})

	t.Run("no handle -> false, no RPC", func(t *testing.T) {
		be := newTestBackend(&client.TestClient{}, getTestSession(), config.WithDefaults())
		op := be.OperationForTest(&cli_service.TExecuteStatementResp{}, nil)
		staging, err := op.IsStaging(context.Background())
		assert.False(t, staging)
		require.NoError(t, err)
	})
}
