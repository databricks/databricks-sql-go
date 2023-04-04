package errors

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/pkg/errors"
)

// Error messages
const (
	// System Fault (driver errors, system failures)
	ErrNotImplemented           = "not implemented"
	ErrTransactionsNotSupported = "transactions are not supported"
	ErrParametersNotSupported   = "query parameters are not supported"
	ErrInvalidOperationState    = "invalid operation state. This should not have happened"

	ErrReadQueryStatus = "could not read query status"

	// Execution error messages (query failure)
	ErrQueryExecution = "failed to execute query"

	// Request error messages (connection, authentication, network error)
	ErrCloseConnection = "failed to close connection"
	ErrThriftClient    = "error initializing thrift client"
	ErrInvalidURL      = "invalid URL"

	ErrNoAuthenticationMethod = "no authentication method set"
	ErrInvalidDSNFormat       = "invalid DSN: invalid format"
	ErrInvalidDSNPort         = "invalid DSN: invalid DSN port"
	ErrInvalidDSNPATIsEmpty   = "invalid DSN: empty token"
	ErrBasicAuthNotSupported  = "invalid DSN: basic auth not enabled"
	ErrInvalidDSNMaxRows      = "invalid DSN: maxRows param is not an integer"
	ErrInvalidDSNTimeout      = "invalid DSN: timeout param is not an integer"
)

type databricksError struct {
	err           error
	correlationId string
	connectionId  string
	errType       string
}

var _ error = (*databricksError)(nil)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func newDatabricksError(ctx context.Context, msg string, err error) databricksError {
	// create an error with the new message
	if err == nil {
		err = errors.New(msg)
	} else {
		err = errors.WithMessage(err, msg)
	}

	// if the source error does not have a stack trace in its
	// error chain add a stack trace
	var st stackTracer
	if ok := errors.As(err, &st); !ok {
		err = errors.WithStack(err)
	}

	return databricksError{
		err:           err,
		correlationId: driverctx.CorrelationIdFromContext(ctx),
		connectionId:  driverctx.ConnIdFromContext(ctx),
		errType:       "unknown",
	}
}

func (e databricksError) Error() string {
	return fmt.Sprintf("databricks: %s: %s", e.errType, e.err.Error())
}

func (e databricksError) Cause() error {
	return e.err
}

func (e databricksError) StackTrace() errors.StackTrace {
	var st stackTracer
	if ok := errors.As(e.err, &st); ok {
		return st.StackTrace()
	}

	return nil
}

func (e databricksError) CorrelationId() string {
	return e.correlationId
}

func (e databricksError) ConnectionId() string {
	return e.connectionId
}

// systemFault are issues with the driver or server, e.g. not supported operations, driver specific non-recoverable failures
type systemFault struct {
	databricksError
	isRetryable bool
}

var _ dbsqlerr.DBSystemFault = (*systemFault)(nil)

func (e systemFault) Is(err error) bool {
	return err == dbsqlerr.SystemFault
}

func (e systemFault) Unwrap() error {
	return e.err
}

func (e systemFault) IsRetryable() bool {
	return e.isRetryable
}

func NewSystemFault(ctx context.Context, msg string, err error) *systemFault {
	dbErr := newDatabricksError(ctx, msg, err)
	dbErr.errType = "system fault"
	return &systemFault{databricksError: dbErr, isRetryable: false}
}

// requestError are errors caused by invalid requests, e.g. permission denied, warehouse not found
type requestError struct {
	databricksError
}

var _ dbsqlerr.DBRequestError = (*requestError)(nil)

func (e requestError) Is(err error) bool {
	return err == dbsqlerr.RequestError
}

func (e requestError) Unwrap() error {
	return e.err
}

func NewRequestError(ctx context.Context, msg string, err error) *requestError {
	dbErr := newDatabricksError(ctx, msg, err)
	dbErr.errType = "request error"
	return &requestError{databricksError: dbErr}
}

// executionError are errors occurring after the query has been submitted, e.g. invalid syntax, query timeout
type executionError struct {
	databricksError
	queryId  string
	sqlState string
}

var _ dbsqlerr.DBExecutionError = (*executionError)(nil)

func (e executionError) Is(err error) bool {
	return err == dbsqlerr.ExecutionError
}

func (e executionError) Unwrap() error {
	return e.err
}

func (e executionError) QueryId() string {
	return e.queryId
}

func (e executionError) SqlState() string {
	return e.sqlState
}

func NewExecutionError(ctx context.Context, msg string, err error, opStatusResp *cli_service.TGetOperationStatusResp) *executionError {
	dbErr := newDatabricksError(ctx, msg, err)
	dbErr.errType = "execution error"
	var sqlState string
	if opStatusResp != nil {
		sqlState = opStatusResp.GetSqlState()
	}

	return &executionError{databricksError: dbErr, queryId: driverctx.QueryIdFromContext(ctx), sqlState: sqlState}
}

// wraps an error and adds trace if not already present
func WrapErr(err error, msg string) error {
	var st stackTracer
	if ok := errors.As(err, &st); ok {
		// wrap passed in error in a new error with the message
		return errors.WithMessage(err, msg)
	}

	// wrap passed in error in errors with the message and a stack trace
	return errors.Wrap(err, msg)
}

// adds a stack trace if not already present
func WrapErrf(err error, format string, args ...interface{}) error {
	var st stackTracer
	if ok := errors.As(err, &st); ok {
		// wrap passed in error in a new error with the formatted message
		return errors.WithMessagef(err, format, args...)
	}

	// wrap passed in error in errors with the formatted message and a stack trace
	return errors.Wrapf(err, format, args...)
}
