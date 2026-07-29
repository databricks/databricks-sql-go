package rowscanner

import (
	"context"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// A FetchResults failure surfaced from the result page iterator must carry
// CategoryResultSet so telemetry reports "result_set_error".
func TestResultPageFetchErrorCarriesCategory(t *testing.T) {
	failingClient := &client.TestClient{
		FnFetchResults: func(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return nil, errors.New("boom")
		},
	}

	rpf := &resultPageIterator{
		Delimiter:     NewDelimiter(0, 0),
		client:        failingClient,
		ctx:           context.Background(), // live ctx, like production, so the guard's real branch runs
		logger:        dbsqllog.WithContext("connId", "correlationId", ""),
		connectionId:  "connId",
		correlationId: "correlationId",
	}

	_, err := rpf.Next()
	assert.NotNil(t, err)
	assert.Equal(t, dbsqlerrint.CategoryResultSet, dbsqlerrint.CategoryFromError(err))
}

// A page fetch that fails when the results context is already aborted must NOT
// be tagged result_set_error.
func TestResultPageFetchCancelNotTaggedResultSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	failingClient := &client.TestClient{
		FnFetchResults: func(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return nil, errors.New("boom")
		},
	}

	rpf := &resultPageIterator{
		Delimiter:     NewDelimiter(0, 0),
		client:        failingClient,
		ctx:           ctx,
		logger:        dbsqllog.WithContext("connId", "correlationId", ""),
		connectionId:  "connId",
		correlationId: "correlationId",
	}

	_, err := rpf.Next()
	assert.NotNil(t, err)
	assert.Equal(t, dbsqlerrint.ErrorCategory(""), dbsqlerrint.CategoryFromError(err))
}
