package errors

import (
	"context"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDbSqlErrors(t *testing.T) {

	t.Run("errors.Is/As works with execution error values", func(t *testing.T) {
		// Create an execution error and wrap it in a regular error
		cause := errors.New("cause")
		var execError error = NewExecutionError(context.TODO(), "exec error", cause, nil)
		e := errors.Wrap(execError, "is wrapped")

		m := e.Error()
		assert.NotNil(t, m)
		assert.Equal(t, m, "is wrapped: databricks: execution error: exec error: cause")

		// Should return true for is sentinel value
		assert.True(t, errors.Is(e, dbsqlerr.ExecutionError))

		// should return true for actual execution error
		assert.True(t, errors.Is(e, execError))

		// should return true for cause if execution error is unwrapping correctly
		assert.True(t, errors.Is(e, cause))

		// should succesfully retrieve execError as an instance of DBExecutionError
		var ee dbsqlerr.DBExecutionError
		assert.True(t, errors.As(e, &ee))
		assert.Equal(t, ee, execError)
	})

	t.Run("errors.Is/As works with system fault values", func(t *testing.T) {
		// Create a system fault and wrap it in a regular error
		cause := errors.New("cause")
		var systemFault error = NewSystemFault(context.TODO(), "system fault", cause)
		e := errors.Wrap(systemFault, "is wrapped")

		m := e.Error()
		assert.NotNil(t, m)
		assert.Equal(t, m, "is wrapped: databricks: system fault: system fault: cause")

		// Should return true for is sentinel value
		assert.True(t, errors.Is(e, dbsqlerr.SystemFault))

		// should return true for actual system fault
		assert.True(t, errors.Is(e, systemFault))

		// should return true for cause if system fault is unwrapping correctly
		assert.True(t, errors.Is(e, cause))

		// should succesfully retrieve systemFault as an instance of DBSystemFault
		var ee dbsqlerr.DBSystemFault
		assert.True(t, errors.As(e, &ee))
		assert.Equal(t, ee, systemFault)
	})

	t.Run("errors.Is/As works with request error values", func(t *testing.T) {
		// Create a request error and wrap it in a regular error
		cause := errors.New("cause")
		var requestError error = NewRequestError(context.TODO(), "request error", cause)
		e := errors.Wrap(requestError, "is wrapped")

		m := e.Error()
		assert.NotNil(t, m)
		assert.Equal(t, "is wrapped: databricks: request error: request error: cause", m)

		// Should return true for is sentinel value
		assert.True(t, errors.Is(e, dbsqlerr.RequestError))

		// should return true for actual request error
		assert.True(t, errors.Is(e, requestError))

		// should return true for cause if request error is unwrapping correctly
		assert.True(t, errors.Is(e, cause))

		// should succesfully retrieve requestErrors as an instance of DBRequestError
		var ee dbsqlerr.DBRequestError
		assert.True(t, errors.As(e, &ee))
		assert.Equal(t, ee, requestError)
	})

	t.Run("stack trace should be added if not already there", func(t *testing.T) {
		// create a causative error with a stack trace
		cause := errors.New("cause")
		var requestError dbsqlerr.DBRequestError = NewRequestError(context.TODO(), "request error", cause)

		// stack trace should not have been added since cause already has one
		st := requestError.StackTrace()
		assert.NotNil(t, st)

		var str stackTracer
		ok := errors.As(requestError.Cause(), &str)
		assert.True(t, ok)
		assert.NotEqual(t, requestError, str)
		assert.Equal(t, cause, str)

		cause = &boringError{}
		requestError = NewRequestError(context.TODO(), "request error", cause)

		st = requestError.StackTrace()
		assert.NotNil(t, st)

		ok = errors.As(requestError.Cause(), &str)
		assert.True(t, ok)
		assert.NotEqual(t, requestError, str)
		assert.NotEqual(t, cause, str)
	})

	t.Run("WrapErr and WrapErrf should only add stack trace if not already there", func(t *testing.T) {

		var str stackTracer

		// create a causative error with a stack trace
		cause := errors.New("cause")

		e := WrapErr(cause, "new message")
		assert.NotEqual(t, cause, e)

		ok := errors.As(e, &str)
		assert.True(t, ok)
		assert.NotEqual(t, e, str)
		assert.Equal(t, cause, str)

		e = WrapErrf(cause, "new message %s", "foo")
		assert.NotEqual(t, cause, e)

		ok = errors.As(e, &str)
		assert.True(t, ok)
		assert.NotEqual(t, e, str)
		assert.Equal(t, cause, str)

		cause = &boringError{}
		e = WrapErr(cause, "new message")
		assert.NotEqual(t, cause, e)

		ok = errors.As(e, &str)
		assert.True(t, ok)
		assert.Equal(t, e, str)
		assert.NotEqual(t, cause, str)

		cause = &boringError{}
		e = WrapErrf(cause, "new message %s", "foo")
		assert.NotEqual(t, cause, e)

		ok = errors.As(e, &str)
		assert.True(t, ok)
		assert.Equal(t, e, str)
		assert.NotEqual(t, cause, str)
	})
}

type boringError struct{}

func (be *boringError) Error() string {
	return "boring"
}
