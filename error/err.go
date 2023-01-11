package error

import (
	"fmt"
)

// ConnectionError is an error in the connection to the server
type ConnectionError struct {
	Msg string
	Err error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("databricks: connection error: %s\n%v", e.Msg, e.Err)
}

// OperationStatusError is returned when query operation moves to an error state
type OperationStatusError struct {
	Msg string
	Err error
}

func (e *OperationStatusError) Error() string {
	return fmt.Sprintf("databricks: operation status error: %s\n%v", e.Msg, e.Err)
}
