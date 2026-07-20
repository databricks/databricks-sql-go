package errors

import (
	"context"
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

	t.Run("outermost non-empty category wins", func(t *testing.T) {
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryResultSet)
		outer := NewDriverError(context.TODO(), "outer", inner).
			WithCategory(CategoryChunkDownload)
		assert.Equal(t, CategoryChunkDownload, CategoryFromError(outer))
	})

	t.Run("empty outer category does not shadow a tagged inner", func(t *testing.T) {
		inner := NewRequestError(context.TODO(), "inner", errors.New("cause")).
			WithCategory(CategoryResultSet)
		outer := NewExecutionError(context.TODO(), "outer", inner, nil)
		assert.Equal(t, CategoryResultSet, CategoryFromError(outer))
	})
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
