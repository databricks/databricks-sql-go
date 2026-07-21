package errors

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCategoryFromError(t *testing.T) {
	t.Run("nil error has no category", func(t *testing.T) {
		assert.Equal(t, ErrorCategory(""), CategoryFromError(nil))
	})

	t.Run("untagged error has no category", func(t *testing.T) {
		err := NewDriverError(context.TODO(), "boom", errors.New("cause"))
		assert.Equal(t, ErrorCategory(""), CategoryFromError(err))
	})

	t.Run("returns the category set at the source", func(t *testing.T) {
		err := NewDriverError(context.TODO(), "boom", errors.New("cause")).
			WithCategory(CategoryChunkDownload)
		assert.Equal(t, CategoryChunkDownload, CategoryFromError(err))
	})

	t.Run("walks the chain to a tagged inner error", func(t *testing.T) {
		// errors.As would stop at the untagged outer wrapper; the walk must not.
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryResultSet)
		wrapped := errors.Wrap(inner, "outer")
		assert.Equal(t, CategoryResultSet, CategoryFromError(wrapped))
	})

	t.Run("innermost (source) category wins over an outer tag", func(t *testing.T) {
		// Pins innermost-wins: a future outer-wrapper tag must not mask the source.
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryResultSet)
		outer := NewDriverError(context.TODO(), "outer", inner).
			WithCategory(CategoryChunkDownload)
		assert.Equal(t, CategoryResultSet, CategoryFromError(outer))
	})

	t.Run("a generic outer tag does not mask a specific inner one", func(t *testing.T) {
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryChunkDownload)
		outer := NewDriverError(context.TODO(), "outer", inner).
			WithCategory(CategoryGeneric)
		assert.Equal(t, CategoryChunkDownload, CategoryFromError(outer))
	})

	t.Run("empty outer category does not shadow a tagged inner", func(t *testing.T) {
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryResultSet)
		outer := NewExecutionError(context.TODO(), "outer", inner, nil)
		assert.Equal(t, CategoryResultSet, CategoryFromError(outer))
	})

	t.Run("finds the tag through a fmt.Errorf %w wrapper", func(t *testing.T) {
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryResultSet)
		wrapped := fmt.Errorf("outer: %w", inner)
		assert.Equal(t, CategoryResultSet, CategoryFromError(wrapped))
	})

	t.Run("finds the tag inside a tree-shaped chain", func(t *testing.T) {
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryResultSet)
		joined := stderrors.Join(errors.New("sibling"), inner)
		assert.Equal(t, CategoryResultSet, CategoryFromError(joined))

		multiWrap := fmt.Errorf("a: %w, b: %w", errors.New("sibling"), inner)
		assert.Equal(t, CategoryResultSet, CategoryFromError(multiWrap))
	})
}

// TestCategoryStringValues pins every category to its exact wire string (a
// dashboard dimension) so a typo fails the build instead of shipping silently.
func TestCategoryStringValues(t *testing.T) {
	cases := []struct {
		category ErrorCategory
		want     string
	}{
		{CategoryTimeout, "timeout"},
		{CategoryCancelled, "cancelled"},
		{CategoryConnectionError, "connection_error"},
		{CategoryAuthError, "auth_error"},
		{CategoryPermissionError, "permission_error"},
		{CategoryNotFound, "not_found"},
		{CategorySyntaxError, "syntax_error"},
		{CategoryInvalidRequest, "invalid_request"},
		{CategoryGeneric, "error"},
		{CategoryChunkDownload, "chunk_download_error"},
		{CategoryDecompression, "decompression_error"},
		{CategoryArrowSchemaParsing, "arrow_schema_parsing_error"},
		{CategoryResultSet, "result_set_error"},
		{CategoryUnsupportedOperation, "unsupported_operation"},
		{CategoryRateLimitExceeded, "rate_limit_exceeded"},
		{CategorySSLHandshake, "ssl_handshake_error"},
		{CategoryStatementTimeout, "statement_execution_timeout"},
		{CategoryExecuteStatement, "execute_statement_failed"},
		{CategoryStatementCancelled, "execute_statement_cancelled"},
		{CategorySessionClosed, "session_closed"},
		{CategoryStatementClosed, "statement_closed"},
	}
	assert.Len(t, cases, 21, "every ErrorCategory constant must be pinned here")
	for _, tc := range cases {
		assert.Equal(t, tc.want, string(tc.category))
	}
}

// WithCategory must not change the concrete type, so every public interface
// stays reachable.
func TestWithCategoryPreservesInterfaces(t *testing.T) {
	t.Run("execution error", func(t *testing.T) {
		err := NewExecutionError(context.TODO(), "exec", errors.New("cause"), nil).
			WithCategory(CategoryStatementTimeout)

		var ee dbsqlerr.DBExecutionError
		assert.True(t, errors.As(err, &ee))
		assert.Equal(t, CategoryStatementTimeout, CategoryFromError(err))

		wrapped := errors.Wrap(err, "wrapped")
		assert.True(t, errors.As(wrapped, &ee))
		assert.Equal(t, CategoryStatementTimeout, CategoryFromError(wrapped))
	})

	t.Run("driver error", func(t *testing.T) {
		err := NewDriverError(context.TODO(), "driver", errors.New("cause")).
			WithCategory(CategoryChunkDownload)
		var de dbsqlerr.DBDriverError
		assert.True(t, errors.As(err, &de))
	})

	t.Run("request error", func(t *testing.T) {
		err := NewRequestError(context.TODO(), "request", errors.New("cause")).
			WithCategory(CategoryResultSet)
		var re dbsqlerr.DBRequestError
		assert.True(t, errors.As(err, &re))
	})
}
