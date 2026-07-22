package telemetry

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

// With no source category set, classifyError must return the same values it
// does today (the message-substring fallback).
func TestClassifyErrorFallback(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want string
	}{
		{"timeout", "operation timeout exceeded", "timeout"},
		{"cancelled", "context cancelled by caller", "cancelled"},
		{"connection", "connection refused", "connection_error"},
		{"authentication", "authentication failed", "auth_error"},
		{"unauthorized", "unauthorized", "auth_error"},
		{"forbidden", "forbidden", "permission_error"},
		{"not found", "table not found", "not_found"},
		{"syntax", "syntax error near 'select'", "syntax_error"},
		{"invalid", "invalid parameter", "invalid_request"},
		{"first match wins", "connection timeout", "timeout"},
		{"unmatched is generic", "something unexpected happened", "error"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, classifyError(errors.New(c.msg)))
		})
	}
}

func TestClassifyErrorNil(t *testing.T) {
	assert.Equal(t, "", classifyError(nil))
}

func TestClassifyErrorPrefersSourceCategory(t *testing.T) {
	// Message would match "not found", but the source category wins.
	err := dbsqlerrint.NewRequestError(context.TODO(), "table not found", errors.New("cause")).
		WithCategory(dbsqlerrint.CategoryChunkDownload)
	assert.Equal(t, "chunk_download_error", classifyError(err))
}

// The category must still be found when the tagged error is wrapped before
// reaching the telemetry hook.
func TestClassifyErrorSourceCategorySurvivesWrapping(t *testing.T) {
	err := dbsqlerrint.NewExecutionError(context.TODO(), "boom", errors.New("cause"), nil).
		WithCategory(dbsqlerrint.CategoryStatementTimeout)
	wrapped := errors.Wrap(err, "while executing")
	assert.Equal(t, "statement_execution_timeout", classifyError(wrapped))
}

func TestClassifyErrorUntaggedDbErrorUsesFallback(t *testing.T) {
	err := dbsqlerrint.NewRequestError(context.TODO(), "syntax error", errors.New("cause"))
	assert.Equal(t, "syntax_error", classifyError(err))
}

// The staging unsupported-operation message classifies as the generic "error"
// on its own; the source tag is what makes it report "unsupported_operation".
func TestClassifyErrorUnsupportedOperation(t *testing.T) {
	msg := "operation COPY is not supported. Supported operations are GET, PUT, and REMOVE"
	assert.Equal(t, "error", classifyError(dbsqlerrint.NewDriverError(context.TODO(), msg, nil)))
	assert.Equal(t, "unsupported_operation",
		classifyError(dbsqlerrint.NewDriverError(context.TODO(), msg, nil).WithCategory(dbsqlerrint.CategoryUnsupportedOperation)))
}
