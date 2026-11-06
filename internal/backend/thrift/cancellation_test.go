package thrift

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/require"
)

type cancellationContextKey struct{}

type cancellationRequest struct {
	handle *cli_service.TOperationHandle
	value  any
	err    error
}

func TestPollOperationCancelsBlockedStatusRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), cancellationContextKey{}, "request-value"))
	defer cancel()
	assertBlockedPollCancellation(t, ctx, cancel)
}

func TestPollOperationHonorsDeadlineDuringStatusRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), cancellationContextKey{}, "request-value"), 250*time.Millisecond)
	defer cancel()
	assertBlockedPollCancellation(t, ctx, nil)
}

func assertBlockedPollCancellation(t *testing.T, ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	requests := make(chan cancellationRequest, 2)
	finished := make(chan error, 1)
	cfg := config.WithDefaults()
	cfg.PollInterval = time.Millisecond
	be := NewForTest(blockedStatusClient(started, release, requests), getTestSession(), cfg)
	handle := &cli_service.TOperationHandle{OperationId: &cli_service.THandleIdentifier{GUID: make([]byte, 16)}}
	defer close(release)
	go func() {
		_, err := be.pollOperation(ctx, handle)
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("status request did not start")
	}
	if cancel != nil {
		cancel()
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("query context was not cancelled")
	}
	select {
	case request := <-requests:
		require.Equal(t, handle, request.handle)
		require.Equal(t, "request-value", request.value)
		require.NoError(t, request.err, "remote cancellation must use a live context")
	case <-time.After(250 * time.Millisecond):
		t.Fatal("CancelOperation was not called promptly after context cancellation")
	}
	select {
	case err := <-finished:
		require.ErrorIs(t, err, ctx.Err())
	case <-time.After(time.Second):
		t.Fatal("pollOperation did not return after cancellation")
	}
	require.Empty(t, requests, "remote cancellation must be sent once")
}

func blockedStatusClient(started, release chan struct{}, requests chan cancellationRequest) *client.TestClient {
	var start sync.Once
	return &client.TestClient{
		FnGetOperationStatus: func(ctx context.Context, _ *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			start.Do(func() { close(started) })
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return nil, errors.New("test released blocked status request")
			}
		},
		FnCancelOperation: func(ctx context.Context, request *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
			requests <- cancellationRequest{request.OperationHandle, ctx.Value(cancellationContextKey{}), ctx.Err()}
			return &cli_service.TCancelOperationResp{
				Status: &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS},
			}, nil
		},
	}
}
