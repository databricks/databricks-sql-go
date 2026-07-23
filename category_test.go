package dbsql

import (
	"context"
	"database/sql/driver"
	"testing"

	thriftbackend "github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// tagStatementClosed tags a non-nil close error statement_closed; nil stays nil.
func TestTagStatementClosed(t *testing.T) {
	assert.Nil(t, tagStatementClosed(context.Background(), nil))

	tagged := tagStatementClosed(context.Background(), errors.New("close rpc failed"))
	assert.NotNil(t, tagged)
	assert.Equal(t, dbsqlerrint.CategoryStatementClosed, dbsqlerrint.CategoryFromError(tagged))
}

// A failed conn.Close must still return a driver.ErrBadConn (pool eviction),
// even though it tags a telemetry-only session_closed copy.
func TestConnCloseFailureReturnsBadConn(t *testing.T) {
	testClient := &client.TestClient{
		FnCloseSession: func(ctx context.Context, req *cli_service.TCloseSessionReq) (*cli_service.TCloseSessionResp, error) {
			return nil, errors.New("close session rpc failed")
		},
	}
	c := &conn{
		cfg:     config.WithDefaults(),
		backend: thriftbackend.NewForTest(testClient, getTestSession(), config.WithDefaults()),
		// telemetry nil: the returned-error path is outside the telemetry block.
	}

	err := c.Close()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, driver.ErrBadConn), "a failed close must surface as ErrBadConn")
	assert.Equal(t, dbsqlerrint.ErrorCategory(""), dbsqlerrint.CategoryFromError(err),
		"the returned error must not carry the telemetry category")
}
