package thrift

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/stretchr/testify/assert"
)

// runningStatusClient keeps the operation in RUNNING_STATE so pollOperation
// keeps polling until the context fires.
func runningStatusClient() *client.TestClient {
	return &client.TestClient{
		FnGetOperationStatus: func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			return &cli_service.TGetOperationStatusResp{
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_RUNNING_STATE),
			}, nil
		},
	}
}

func pollTestOpHandle() *cli_service.TOperationHandle {
	return &cli_service.TOperationHandle{
		OperationId: &cli_service.THandleIdentifier{GUID: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
	}
}

// A query cancelled during status polling must be tagged
// execute_statement_cancelled, and must still satisfy errors.Is(context.Canceled).
func TestPollOperationCancelledCarriesCategory(t *testing.T) {
	be := NewForTest(runningStatusClient(), getTestSession(), config.WithDefaults())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := be.pollOperation(ctx, pollTestOpHandle())
	assert.NotNil(t, err)
	assert.Equal(t, dbsqlerrint.CategoryStatementCancelled, dbsqlerrint.CategoryFromError(err))
	assert.True(t, errors.Is(err, context.Canceled), "cancellation identity must be preserved for the thrift layer")
}

// A deadline exceeded during polling must NOT be tagged cancelled — it keeps its
// existing (untagged) classification.
func TestPollOperationDeadlineNotTaggedCancelled(t *testing.T) {
	be := NewForTest(runningStatusClient(), getTestSession(), config.WithDefaults())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	defer cancel()

	_, err := be.pollOperation(ctx, pollTestOpHandle())
	assert.NotNil(t, err)
	assert.Equal(t, dbsqlerrint.ErrorCategory(""), dbsqlerrint.CategoryFromError(err))
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}
