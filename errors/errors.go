package errors

import "github.com/pkg/errors"

// value to be used with errors.Is() to determine if an error chain contains a request error
var RequestError error = errors.New("Request Error")

// value to be used with errors.Is() to determine if an error chain contains a system fault
var SystemFault error = errors.New("System Fault")

// value to be used with errors.Is() to determine if an error chain contains an execution error
var ExecutionError error = errors.New("Execution Error")

// Base interface for driver errors
type DatabricksError interface {
	// Descriptive message describing the error
	Error() string

	// ErrorType() DBsqlErrorType

	// User specified id to track what happens under a request. Useful to track multiple connections in the same request.
	// Appears in log messages as field corrId.  See driverctx.NewContextWithCorrelationId()
	CorrelationId() string

	// Internal id to track what happens under a connection. Connections can be reused so this would track across queries.
	// Appears in log messages as field connId.
	ConnectionId() string

	// Stack trace associated with the error.  May be nil.
	StackTrace() errors.StackTrace

	// Underlying causative error. May be nil.
	Cause() error
}

// An error that is caused by an invalid request.
// Example: permission denied, or the user tries to access a warehouse that doesn’t exist
type DBRequestError interface {
	DatabricksError
}

// A fault that is caused by Databricks services
type DBSystemFault interface {
	DatabricksError

	IsRetryable() bool
}

// Any error that occurs after the SQL statement has been accepted (e.g. SQL syntax error).
type DBExecutionError interface {
	DatabricksError

	// Internal id to track what happens under a query.
	// Appears in log messages as field queryId.
	QueryId() string

	// Optional portable error identifier across SQL engines.
	// See https://github.com/apache/spark/tree/master/core/src/main/resources/error#ansiiso-standard
	SqlState() string
}
