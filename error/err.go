package error

import (
	"fmt"
)

type DatabricksError struct {
	msg          string
	err          error
	corrId       string
	connId       string
	queryId      string
	errCondition string
	errType      databricksErrorType
}

type databricksErrorType int64

const (
	Unknown databricksErrorType = iota
	Driver
	Authentication
	QueryFailure
	Network
)

func (t databricksErrorType) String() string {
	switch t {
	case Driver:
		return "driver"
	case Authentication:
		return "authentication"
	case QueryFailure:
		return "query failure"
	case Network:
		return "network"
	}
	return "unknown"
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
