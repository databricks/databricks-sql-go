package rows

import (
	"context"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// A GetResultSetMetadata failure must carry CategoryResultSet, and (regression
// for the former err/err2 bug) must wrap the real underlying cause rather than
// dropping it.
func TestResultSetMetadataErrorCarriesCategoryAndCause(t *testing.T) {
	cause := errors.New("metadata rpc failed")
	failingClient := &client.TestClient{
		FnGetResultSetMetadata: func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			return nil, cause
		},
	}

	r := &rows{client: failingClient, ctx: context.Background()}

	_, err := r.getResultSetSchema()
	assert.NotNil(t, err)
	assert.Equal(t, dbsqlerrint.CategoryResultSet, dbsqlerrint.CategoryFromError(err))
	// The real cause must be preserved (previously the wrong variable was wrapped).
	assert.True(t, errors.Is(err, cause))
}

// A metadata fetch that fails when the results context is already aborted must
// NOT be tagged result_set_error.
func TestResultSetMetadataCancelNotTaggedResultSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	failingClient := &client.TestClient{
		FnGetResultSetMetadata: func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			return nil, errors.New("boom")
		},
	}

	r := &rows{client: failingClient, ctx: ctx}

	_, err := r.getResultSetSchema()
	assert.NotNil(t, err)
	assert.Equal(t, dbsqlerrint.ErrorCategory(""), dbsqlerrint.CategoryFromError(err))
}
