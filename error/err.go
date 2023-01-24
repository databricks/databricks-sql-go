package error

import (
	"fmt"
	"github.com/pkg/errors"
)

type DatabricksError struct {
	msg          string
	err          error
	corrId       string
	connId       string
	queryId      string
	errCondition string
	errType      DatabricksErrorType
}

type DatabricksErrorType int64

// Error types
const (
	Unknown DatabricksErrorType = iota
	Driver
	Authentication
	QueryFailure
	Network
	Connection
)

func (t DatabricksErrorType) String() string {
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

// Error messages
const (
	// Driver error messages

	// Authentication error messages

	// QueryFailure error messages
	ErrNotImplemented           = "not implemented"
	ErrTransactionsNotSupported = "transactions are not supported"
	ErrParametersNotSupported   = "query parameters are not supported"

	// Network error messages

	// Connection error messages
)

func NewDatabricksError(msg string, err error, corrId string, connId string, queryId string, errCondition string, errType DatabricksErrorType) *DatabricksError {
	return &DatabricksError{msg, errors.WithStack(err), corrId, connId, queryId, errCondition, errType}
}

func (e *DatabricksError) Error() string {
	return fmt.Sprintf("databricks: %s error: %s\n%v", e.errType.String(), e.msg, e.err)
}

func (e *DatabricksError) Unwrap() error {
	return e.err
}

func (e *DatabricksError) Message() string {
	return e.msg
}

func (e *DatabricksError) CorrelationId() string {
	return e.corrId
}

func (e *DatabricksError) ConnectionId() string {
	return e.connId
}

func (e *DatabricksError) QueryId() string {
	return e.queryId
}

func (e *DatabricksError) ErrorCondition() string {
	return e.errCondition
}

func (e *DatabricksError) ErrorType() string {
	return e.errType.String()
}
