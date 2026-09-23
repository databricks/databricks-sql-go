package thrift

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	apachethrift "github.com/apache/thrift/lib/go/thrift"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clientTimeoutExecuteResponse() *cli_service.TExecuteStatementResp {
	return &cli_service.TExecuteStatementResp{
		Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
		OperationHandle: &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{
			GUID:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Secret: []byte("secret"),
		}},
	}
}

func clientTimeoutFinishedExecuteResponse() *cli_service.TExecuteStatementResp {
	resp := clientTimeoutExecuteResponse()
	resp.DirectResults = &cli_service.TSparkDirectResults{OperationStatus: &cli_service.TGetOperationStatusResp{
		Status:         &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
		OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
	}}
	return resp
}

func timeoutTestClient(
	execute func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error),
	status func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error),
	cancelled, closed chan<- struct{},
) *client.TestClient {
	return &client.TestClient{
		FnExecuteStatement:   execute,
		FnGetOperationStatus: status,
		FnCancelOperation: func(context.Context, *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
			if cancelled != nil {
				cancelled <- struct{}{}
			}
			return &cli_service.TCancelOperationResp{}, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			if closed != nil {
				closed <- struct{}{}
			}
			return &cli_service.TCloseOperationResp{}, nil
		},
	}
}

func timeoutTestConfig(timeout, pollInterval time.Duration) *config.Config {
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout
	cfg.PollInterval = pollInterval
	return cfg
}

func awaitTimeoutCleanup(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for asynchronous cleanup")
	}
}

func TestClientQueryTimeoutStopsBeforeNextPollAndCleansUp(t *testing.T) {
	var statusCalls, metadataCalls, closeCalls atomic.Int32
	foreground := timeoutTestClient(
		func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return clientTimeoutExecuteResponse(), nil
		},
		func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			statusCalls.Add(1)
			return nil, nil
		}, nil, nil,
	)
	foreground.FnGetResultSetMetadata = func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
		metadataCalls.Add(1)
		return nil, nil
	}
	foreground.FnCloseOperation = func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
		closeCalls.Add(1)
		return nil, nil
	}

	cancelled, closed := make(chan struct{}, 1), make(chan struct{}, 1)
	cleanup := timeoutTestClient(nil, nil, cancelled, closed)
	be := NewForTest(foreground, getTestSession(), timeoutTestConfig(20*time.Millisecond, 200*time.Millisecond))
	be.newClient = func() (cli_service.TCLIService, error) { return cleanup, nil }

	op, err := be.Execute(context.Background(), backend.ExecRequest{Query: "select 1"})
	require.ErrorIs(t, err, errClientQueryTimeout)
	assert.Zero(t, statusCalls.Load(), "no status RPC should start after the deadline")
	assert.Equal(t, dbsqlerrint.CategoryStatementTimeout, dbsqlerrint.CategoryFromError(err))
	var executionErr dbsqlerr.DBExecutionError
	require.ErrorAs(t, op.ExecutionError(context.Background(), err), &executionErr)
	assert.Equal(t, "HYT00", executionErr.SqlState())
	assert.False(t, executionErr.IsRetryable())

	staging, stagingErr := op.IsStaging(context.Background())
	require.NoError(t, stagingErr)
	closedForeground, closeErr := op.Close(context.Background())
	require.NoError(t, closeErr)
	assert.False(t, staging)
	assert.False(t, closedForeground)
	assert.Zero(t, metadataCalls.Load())
	assert.Zero(t, closeCalls.Load())
	awaitTimeoutCleanup(t, cancelled)
	awaitTimeoutCleanup(t, closed)
}

func TestClientQueryTimeoutStatusGrace(t *testing.T) {
	for _, test := range []struct {
		name        string
		state       cli_service.TOperationState
		wantTimeout bool
		wantError   bool
	}{
		{name: "terminal success wins", state: cli_service.TOperationState_FINISHED_STATE},
		{name: "terminal server error wins", state: cli_service.TOperationState_ERROR_STATE, wantError: true},
		{name: "live status loses", state: cli_service.TOperationState_RUNNING_STATE, wantTimeout: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cancelled, closed := make(chan struct{}, 1), make(chan struct{}, 1)
			cli := timeoutTestClient(
				func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
					return clientTimeoutExecuteResponse(), nil
				},
				func(ctx context.Context, _ *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					assert.InDelta(t, clientQueryStatusRPCGrace, time.Until(deadline), float64(100*time.Millisecond))
					time.Sleep(40 * time.Millisecond) // Return after the 20ms execution deadline.
					message := "server terminal result"
					return &cli_service.TGetOperationStatusResp{
						OperationState: cli_service.TOperationStatePtr(test.state),
						DisplayMessage: &message,
					}, nil
				}, cancelled, closed,
			)
			be := NewForTest(cli, getTestSession(), timeoutTestConfig(20*time.Millisecond, time.Millisecond))

			_, status, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
			assert.Equal(t, test.wantError, err != nil)
			assert.Equal(t, test.wantTimeout, errors.Is(err, errClientQueryTimeout))
			if test.wantTimeout {
				require.NotNil(t, status)
				assert.Equal(t, "HYT00", status.GetSqlState())
				awaitTimeoutCleanup(t, cancelled)
				awaitTimeoutCleanup(t, closed)
			}
		})
	}
}

func TestClientQueryTimeoutGivesInitialExecuteNoGrace(t *testing.T) {
	cancelled, closed := make(chan struct{}, 1), make(chan struct{}, 1)
	cli := timeoutTestClient(
		func(ctx context.Context, _ *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			_, hasDeadline := ctx.Deadline()
			assert.True(t, hasDeadline)
			time.Sleep(40 * time.Millisecond) // Simulate a transport that ignores cancellation.
			return clientTimeoutFinishedExecuteResponse(), nil
		}, nil, cancelled, closed,
	)
	be := NewForTest(cli, getTestSession(), timeoutTestConfig(20*time.Millisecond, time.Millisecond))

	_, status, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
	require.ErrorIs(t, err, errClientQueryTimeout)
	require.NotNil(t, status)
	assert.Equal(t, "HYT00", status.GetSqlState())
	awaitTimeoutCleanup(t, cancelled)
	awaitTimeoutCleanup(t, closed)
}

func TestUnlimitedClientQueryTimeoutDoesNotCreateThriftDeadline(t *testing.T) {
	for _, timeout := range []time.Duration{0, unlimitedClientQueryTimeout} {
		t.Run(timeout.String(), func(t *testing.T) {
			cli := timeoutTestClient(func(ctx context.Context, _ *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
				_, hasDeadline := ctx.Deadline()
				assert.False(t, hasDeadline)
				return clientTimeoutFinishedExecuteResponse(), nil
			}, nil, nil, nil)
			be := NewForTest(cli, getTestSession(), timeoutTestConfig(timeout, time.Millisecond))
			_, _, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
			require.NoError(t, err)
		})
	}
}

func TestClientQueryTimeoutLeavesThriftTransportReusable(t *testing.T) {
	var executeCalls atomic.Int32
	handler := &client.TestClient{FnExecuteStatement: func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
		if executeCalls.Add(1) == 1 {
			time.Sleep(250 * time.Millisecond)
		}
		return clientTimeoutFinishedExecuteResponse(), nil
	}}
	processor := cli_service.NewTCLIServiceProcessor(handler)
	protocolFactory := apachethrift.NewTBinaryProtocolFactoryConf(nil)
	server := httptest.NewServer(http.HandlerFunc(apachethrift.NewThriftHandlerFunc(processor, protocolFactory, protocolFactory)))
	t.Cleanup(server.Close)

	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(endpoint.Port())
	require.NoError(t, err)
	cfg := timeoutTestConfig(20*time.Millisecond, time.Millisecond)
	cfg.Protocol, cfg.Host, cfg.Port, cfg.HTTPPath = endpoint.Scheme, endpoint.Hostname(), port, "/"
	cli, err := client.InitThriftClient(cfg, server.Client())
	require.NoError(t, err)
	be := NewForTest(cli, getTestSession(), cfg)

	start := time.Now()
	_, _, err = be.runQuery(context.Background(), backend.ExecRequest{Query: "select slow"})
	require.ErrorIs(t, err, errClientQueryTimeout)
	assert.Less(t, time.Since(start), 150*time.Millisecond)
	_, _, err = be.runQuery(context.Background(), backend.ExecRequest{Query: "select fast"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), executeCalls.Load())
}
