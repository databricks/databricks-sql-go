package dbsql

import (
	"github.com/pkg/errors"
)

var ErrNotImplemented = "databricks: not implemented"
var ErrTransactionsNotSupported = "databricks: transactions are not supported"
var ErrParametersNotSupported = "databricks: query parameters are not supported"

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// adds a stack trace if not already present
func WithStack(err error) error {
	if _, ok := err.(stackTracer); ok {
		return err
	}

	return errors.WithStack(err)
}

type causer interface {
	Cause() error
}
