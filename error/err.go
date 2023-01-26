package error

import (
	"context"
	"fmt"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/pkg/errors"
)

// Error messages
const (
	// Driver error messages
	ErrNotImplemented           = "not implemented"
	ErrTransactionsNotSupported = "transactions are not supported"
	ErrParametersNotSupported   = "query parameters are not supported"
	ErrInvalidOperationState    = "invalid operation state. This should not have happened"
	ErrReadQueryStatus          = "could not read query status"

	// Authentication error messages
	ErrNoAuthenticationMethod = "no authentication method set"
	ErrInvalidDSNFormat       = "invalid DSN: invalid format"
	ErrInvalidDSNPort         = "invalid DSN: invalid DSN port"
	ErrInvalidDSNTokenIsEmpty = "invalid DSN: empty token"
	ErrBasicAuthNotSupported  = "invalid DSN: basic auth not enabled"
	ErrInvalidDSNMaxRows      = "invalid DSN: maxRows param is not an integer"
	ErrInvalidDSNTimeout      = "invalid DSN: timeout param is not an integer"

	// QueryFailure error messages
	ErrQueryExecution = "failed to execute query"

	// Network error messages

	// Connection error messages
	ErrCloseConnection = "failed to close connection"
	ErrThriftClient    = "error initializing thrift client"
	ErrInvalidURL      = "invalid URL"
)

type DatabricksError interface {
	Error() string
	ErrorType() string
	CorrelationId() string
	ConnectionId() string
	Message() string
	ErrorCondition() string
}

type databricksError struct {
	msg     string
	err     error
	corrId  string
	connId  string
	errType dbsqlErrorType
}

func newDatabricksError(ctx context.Context, msg string, err error, errType dbsqlErrorType) databricksError {
	return databricksError{
		msg:     msg,
		err:     errors.WithStack(err),
		corrId:  driverctx.CorrelationIdFromContext(ctx),
		connId:  driverctx.ConnIdFromContext(ctx),
		errType: errType,
	}
}

type dbsqlErrorType int64

// Error types
const (
	Unknown dbsqlErrorType = iota
	Driver
	Authentication
	QueryFailure
	Network
	Connection
)

func (t dbsqlErrorType) string() string {
	switch t {
	case Driver:
		return "driver"
	case Authentication:
		return "authentication"
	case QueryFailure:
		return "query failure"
	case Network:
		return "network"
	case Connection:
		return "connection"
	}
	return "unknown"
}

type DatabricksErrorWithQuery interface {
	QueryId() string
	ErrorCondition() string
}

func (e databricksError) Error() string {
	return fmt.Sprintf("databricks: %s error: %s: %v", e.errType.string(), e.msg, e.err)
}

func (e databricksError) Unwrap() error {
	return e.err
}

func (e databricksError) Message() string {
	return e.msg
}

func (e databricksError) CorrelationId() string {
	return e.corrId
}

func (e databricksError) ConnectionId() string {
	return e.connId
}

func (e databricksError) ErrorType() string {
	return e.errType.string()
}

// DriverError are issues with the driver, e.g. not supported operations, driver specific non-recoverable failures
type DriverError struct {
	databricksError
}

func NewDriverError(ctx context.Context, msg string, err error) *DriverError {
	return &DriverError{newDatabricksError(ctx, msg, err, Driver)}
}

// AuthenticationError are issues with the driver, e.g. not supported operations, driver specific non-recoverable failures
type AuthenticationError struct {
	databricksError
}

func NewAuthenticationError(ctx context.Context, msg string, err error) *AuthenticationError {
	return &AuthenticationError{newDatabricksError(ctx, msg, err, Driver)}
}

// QueryFailureError are errors with the query such as invalid syntax, etc
type QueryFailureError struct {
	databricksError
	queryId      string
	errCondition string
}

func (q *QueryFailureError) QueryId() string {
	return q.queryId
}

func (q *QueryFailureError) ErrorCondition() string {
	return q.errCondition
}

func NewQueryFailureError(ctx context.Context, msg string, err error, errCondition string) *QueryFailureError {
	dbsqlErr := newDatabricksError(ctx, msg, err, QueryFailure)
	return &QueryFailureError{dbsqlErr, driverctx.QueryIdFromContext(ctx), errCondition}
}

// ConnectionError are issues with the connection
type ConnectionError struct {
	databricksError
}

func NewConnectionError(ctx context.Context, msg string, err error) *DriverError {
	return &DriverError{newDatabricksError(ctx, msg, err, Driver)}
}
