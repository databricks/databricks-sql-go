package dbsql

import (
	"context"
	"testing"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/stretchr/testify/assert"
)

// TestEnrichQueryId pins the queryId-enrichment semantics preserved from the
// removed client.LoggerAndContext: (1) a caller-set queryId is never
// overwritten, and (2) when the context has no queryId, the derived id is always
// applied via NewContextWithQueryId — even when empty — so a registered
// QueryIdCallback fires. The pre-refactor code relied on both; a naive
// "if id != ” { set }" rewrite broke both.
func TestEnrichQueryId(t *testing.T) {
	t.Run("preserves a caller-set queryId", func(t *testing.T) {
		ctx := driverctx.NewContextWithQueryId(context.Background(), "caller-set")
		ctx = enrichQueryId(ctx, "statement-guid")
		assert.Equal(t, "caller-set", driverctx.QueryIdFromContext(ctx),
			"a queryId the caller already set must not be overwritten")
	})

	t.Run("fills the statement id when the context has none", func(t *testing.T) {
		ctx := enrichQueryId(context.Background(), "statement-guid")
		assert.Equal(t, "statement-guid", driverctx.QueryIdFromContext(ctx))
	})

	t.Run("fires the QueryIdCallback even with an empty derived id", func(t *testing.T) {
		var fired bool
		var got string
		ctx := driverctx.NewContextWithQueryIdCallback(context.Background(), func(id string) {
			fired = true
			got = id
		})
		// No handle -> empty statement id. The callback must still fire (old
		// LoggerAndContext always called NewContextWithQueryId on the empty branch).
		_ = enrichQueryId(ctx, "")
		assert.True(t, fired, "QueryIdCallback must fire on the no-queryId path even with an empty id")
		assert.Equal(t, "", got)
	})

	t.Run("fires the QueryIdCallback with the derived id", func(t *testing.T) {
		var got string
		ctx := driverctx.NewContextWithQueryIdCallback(context.Background(), func(id string) { got = id })
		_ = enrichQueryId(ctx, "statement-guid")
		assert.Equal(t, "statement-guid", got)
	})
}
