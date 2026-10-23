package rowscanner

import (
	"context"
	"io"
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

// Err() must return nil when the result set is consumed normally (io.EOF is
// not an error).
func TestResultPageIteratorErrNilOnNormalFinish(t *testing.T) {
	hasMoreRows := false
	calls := 0
	c := &client.TestClient{
		FnFetchResults: func(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			calls++
			return &cli_service.TFetchResultsResp{
				HasMoreRows: &hasMoreRows,
				Results: &cli_service.TRowSet{
					StartRowOffset: 0,
					ArrowBatches:   []*cli_service.TSparkArrowBatch{{RowCount: 5}},
				},
			}, nil
		},
	}

	rpf := NewResultPageIterator(context.Background(), NewDelimiter(0, 0), 100, nil, false, c,
		dbsqllog.WithContext("connId", "correlationId", ""))

	assert.True(t, rpf.HasNext())
	page, err := rpf.Next()
	assert.Nil(t, err)
	assert.NotNil(t, page)

	assert.False(t, rpf.HasNext())
	assert.Nil(t, rpf.Err())
	_, err = rpf.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rpf.Err())
	assert.Equal(t, 1, calls)
}

// After a failed page fetch, subsequent HasNext() calls must keep the stored
// error rather than overwriting it with io.EOF.
func TestResultPageIteratorPreservesFetchError(t *testing.T) {
	calls := 0
	c := &client.TestClient{
		FnFetchResults: func(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			calls++
			return nil, errors.New("connection reset")
		},
	}

	rpf := NewResultPageIterator(context.Background(), NewDelimiter(0, 0), 100, nil, false, c,
		dbsqllog.WithContext("connId", "correlationId", ""))

	assert.False(t, rpf.HasNext())
	assert.ErrorContains(t, rpf.Err(), "connection reset")

	// A second HasNext() must not mask the error as a normal end-of-results.
	assert.False(t, rpf.HasNext())
	assert.ErrorContains(t, rpf.Err(), "connection reset")
	_, err := rpf.Next()
	assert.ErrorContains(t, err, "connection reset")
	assert.Equal(t, 1, calls)
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
