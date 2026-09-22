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

func clientTimeoutOperationHandle() *cli_service.TOperationHandle {
	return &cli_service.TOperationHandle{
		OperationId: &cli_service.THandleIdentifier{
			GUID:   []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			Secret: []byte("secret"),
		},
	}
}

func clientTimeoutExecuteResponse() *cli_service.TExecuteStatementResp {
	return &cli_service.TExecuteStatementResp{
		Status:          &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
		OperationHandle: clientTimeoutOperationHandle(),
	}
}

func timeoutCleanupClient(
	execute func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error),
	status func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error),
	cancelled chan<- struct{},
	closed chan<- struct{},
) *client.TestClient {
	return &client.TestClient{
		FnExecuteStatement:   execute,
		FnGetOperationStatus: status,
		FnCancelOperation: func(context.Context, *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
			if cancelled != nil {
				cancelled <- struct{}{}
			}
			return &cli_service.TCancelOperationResp{Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}}, nil
		},
		FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			if closed != nil {
				closed <- struct{}{}
			}
			return &cli_service.TCloseOperationResp{Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}}, nil
		},
	}
}

func awaitCleanupSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for asynchronous %s", name)
	}
}

func TestClientQueryTimeoutBeforeNextThriftPoll(t *testing.T) {
	timeout := 20 * time.Millisecond
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout
	cfg.PollInterval = 200 * time.Millisecond

	cancelled := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	statusCalls := 0
	cli := timeoutCleanupClient(
		func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return clientTimeoutExecuteResponse(), nil
		},
		func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			statusCalls++
			return nil, nil
		},
		cancelled,
		closed,
	)
	be := NewForTest(cli, getTestSession(), cfg)

	op, err := be.Execute(context.Background(), backend.ExecRequest{Query: "select 1"})
	require.Error(t, err)
	assert.Equal(t, 0, statusCalls, "no status RPC should start after the deadline")
	assert.Equal(t, dbsqlerrint.CategoryStatementTimeout, dbsqlerrint.CategoryFromError(err))

	wrapped := op.ExecutionError(context.Background(), err)
	var executionErr dbsqlerr.DBExecutionError
	require.True(t, errors.As(wrapped, &executionErr))
	assert.Equal(t, "HYT00", executionErr.SqlState())
	assert.False(t, executionErr.IsRetryable())

	awaitCleanupSignal(t, cancelled, "cancel")
	awaitCleanupSignal(t, closed, "close")
}

func TestClientQueryTimeoutLetsTerminalThriftPollWinDuringGrace(t *testing.T) {
	for _, test := range []struct {
		name      string
		state     cli_service.TOperationState
		wantError bool
	}{
		{name: "success", state: cli_service.TOperationState_FINISHED_STATE},
		{name: "server error", state: cli_service.TOperationState_ERROR_STATE, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			timeout := 20 * time.Millisecond
			cfg := config.WithDefaults()
			cfg.ClientQueryTimeout = &timeout
			cfg.PollInterval = time.Millisecond

			cli := timeoutCleanupClient(
				func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
					return clientTimeoutExecuteResponse(), nil
				},
				func(ctx context.Context, _ *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
					graceDeadline, ok := ctx.Deadline()
					require.True(t, ok)
					assert.InDelta(t, 5*time.Second, time.Until(graceDeadline), float64(100*time.Millisecond))
					time.Sleep(40 * time.Millisecond)
					message := "server terminal result"
					return &cli_service.TGetOperationStatusResp{
						OperationState: cli_service.TOperationStatePtr(test.state),
						DisplayMessage: &message,
					}, nil
				},
				nil,
				nil,
			)
			be := NewForTest(cli, getTestSession(), cfg)

			_, _, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
			if test.wantError {
				require.Error(t, err)
				assert.False(t, errors.Is(err, errClientQueryTimeout))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestClientQueryTimeoutRejectsLiveThriftPollAfterDeadline(t *testing.T) {
	timeout := 20 * time.Millisecond
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout
	cfg.PollInterval = time.Millisecond

	cancelled := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	cli := timeoutCleanupClient(
		func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return clientTimeoutExecuteResponse(), nil
		},
		func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			time.Sleep(40 * time.Millisecond)
			return &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_RUNNING_STATE),
			}, nil
		},
		cancelled,
		closed,
	)
	be := NewForTest(cli, getTestSession(), cfg)

	_, status, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
	require.Error(t, err)
	require.NotNil(t, status)
	assert.Equal(t, cli_service.TOperationState_TIMEDOUT_STATE, status.GetOperationState())
	assert.Equal(t, "HYT00", status.GetSqlState())
	awaitCleanupSignal(t, cancelled, "cancel")
	awaitCleanupSignal(t, closed, "close")
}

func TestClientQueryTimeoutPreservesInvalidThriftStatus(t *testing.T) {
	timeout := time.Second
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout
	cfg.PollInterval = time.Millisecond

	cli := timeoutCleanupClient(
		func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return clientTimeoutExecuteResponse(), nil
		},
		func(context.Context, *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			return &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_UKNOWN_STATE),
			}, nil
		},
		nil,
		nil,
	)
	be := NewForTest(cli, getTestSession(), cfg)

	_, status, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, errClientQueryTimeout))
	require.NotNil(t, status)
	assert.Equal(t, cli_service.TOperationState_UKNOWN_STATE, status.GetOperationState())
}

func TestClientQueryTimeoutPreservesTerminalStatusOnCallerCancellation(t *testing.T) {
	timeout := time.Second
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout
	cfg.PollInterval = time.Millisecond

	statusCtx := make(chan context.Context, 1)
	release := make(chan struct{})
	cli := timeoutCleanupClient(
		func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return clientTimeoutExecuteResponse(), nil
		},
		func(ctx context.Context, _ *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			statusCtx <- ctx
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return &cli_service.TGetOperationStatusResp{
					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				}, nil
			}
		},
		nil,
		nil,
	)
	be := NewForTest(cli, getTestSession(), cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := be.runQuery(ctx, backend.ExecRequest{Query: "select 1"})
		done <- err
	}()

	inFlightCtx := <-statusCtx
	cancel()
	statusCanceled := false
	select {
	case <-inFlightCtx.Done():
		statusCanceled = true
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	assert.False(t, statusCanceled, "caller cancellation reached an in-flight status RPC")
	require.NoError(t, <-done)
}

func TestClientQueryTimeoutBoundsInitialThriftExecute(t *testing.T) {
	timeout := 20 * time.Millisecond
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout

	cli := timeoutCleanupClient(
		func(ctx context.Context, _ *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		nil,
		nil,
		nil,
	)
	be := NewForTest(cli, getTestSession(), cfg)

	start := time.Now()
	_, status, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
	elapsed := time.Since(start)
	require.Error(t, err)
	require.NotNil(t, status)
	assert.Equal(t, "HYT00", status.GetSqlState())
	assert.Less(t, elapsed, time.Second)
}

func TestClientQueryTimeoutDoesNotGiveInitialExecuteGrace(t *testing.T) {
	timeout := 20 * time.Millisecond
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout

	cancelled := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	cli := timeoutCleanupClient(
		func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			time.Sleep(40 * time.Millisecond)
			resp := clientTimeoutExecuteResponse()
			resp.DirectResults = &cli_service.TSparkDirectResults{
				OperationStatus: &cli_service.TGetOperationStatusResp{
					Status:         &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				},
			}
			return resp, nil
		},
		nil,
		cancelled,
		closed,
	)
	be := NewForTest(cli, getTestSession(), cfg)

	_, status, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
	require.ErrorIs(t, err, errClientQueryTimeout)
	require.NotNil(t, status)
	assert.Equal(t, "HYT00", status.GetSqlState())
	awaitCleanupSignal(t, cancelled, "cancel")
	awaitCleanupSignal(t, closed, "close")
}

func TestClientQueryTimeoutLeavesForegroundOperationToBackgroundCleanup(t *testing.T) {
	timeout := 20 * time.Millisecond
	cfg := config.WithDefaults()
	cfg.ClientQueryTimeout = &timeout
	cfg.PollInterval = 200 * time.Millisecond

	metadataCalls := 0
	foregroundCloseCalls := 0
	foreground := timeoutCleanupClient(
		func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return clientTimeoutExecuteResponse(), nil
		},
		nil,
		nil,
		nil,
	)
	foreground.FnGetResultSetMetadata = func(context.Context, *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
		metadataCalls++
		return nil, nil
	}
	foreground.FnCloseOperation = func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
		foregroundCloseCalls++
		return nil, nil
	}

	cancelled := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	cleanup := timeoutCleanupClient(nil, nil, cancelled, closed)
	be := NewForTest(foreground, getTestSession(), cfg)
	be.newClient = func() (cli_service.TCLIService, error) { return cleanup, nil }

	op, err := be.Execute(context.Background(), backend.ExecRequest{Query: "select 1"})
	require.ErrorIs(t, err, errClientQueryTimeout)
	isStaging, stagingErr := op.IsStaging(context.Background())
	require.NoError(t, stagingErr)
	assert.False(t, isStaging)
	didClose, closeErr := op.Close(context.Background())
	require.NoError(t, closeErr)
	assert.False(t, didClose)
	assert.Zero(t, metadataCalls)
	assert.Zero(t, foregroundCloseCalls)
	awaitCleanupSignal(t, cancelled, "cancel")
	awaitCleanupSignal(t, closed, "close")
}

func TestUnlimitedClientQueryTimeoutDoesNotCreateThriftDeadline(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Duration(1<<63 - 1)} {
		t.Run(timeout.String(), func(t *testing.T) {
			cfg := config.WithDefaults()
			cfg.ClientQueryTimeout = &timeout
			cli := timeoutCleanupClient(
				func(ctx context.Context, _ *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
					if _, hasDeadline := ctx.Deadline(); hasDeadline {
						t.Fatal("unlimited timeout added a Thrift context deadline")
					}
					resp := clientTimeoutExecuteResponse()
					resp.DirectResults = &cli_service.TSparkDirectResults{
						OperationStatus: &cli_service.TGetOperationStatusResp{
							OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
						},
					}
					return resp, nil
				},
				nil,
				nil,
				nil,
			)
			be := NewForTest(cli, getTestSession(), cfg)
			_, _, err := be.runQuery(context.Background(), backend.ExecRequest{Query: "select 1"})
			require.NoError(t, err)
		})
	}
}

func TestClientQueryTimeoutLeavesThriftTransportReusable(t *testing.T) {
	var executeCalls atomic.Int32
	handler := &client.TestClient{
		FnExecuteStatement: func(context.Context, *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			if executeCalls.Add(1) == 1 {
				time.Sleep(250 * time.Millisecond)
			}
			resp := clientTimeoutExecuteResponse()
			resp.DirectResults = &cli_service.TSparkDirectResults{
				OperationStatus: &cli_service.TGetOperationStatusResp{
					Status:         &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
				},
			}
			return resp, nil
		},
	}
	processor := cli_service.NewTCLIServiceProcessor(handler)
	protocolFactory := apachethrift.NewTBinaryProtocolFactoryConf(nil)
	server := httptest.NewServer(http.HandlerFunc(apachethrift.NewThriftHandlerFunc(processor, protocolFactory, protocolFactory)))
	t.Cleanup(server.Close)

	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(endpoint.Port())
	require.NoError(t, err)
	timeout := 20 * time.Millisecond
	cfg := config.WithDefaults()
	cfg.Protocol = endpoint.Scheme
	cfg.Host = endpoint.Hostname()
	cfg.Port = port
	cfg.HTTPPath = "/"
	cfg.ClientQueryTimeout = &timeout

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
