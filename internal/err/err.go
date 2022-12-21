package err

import (
	"github.com/pkg/errors"
)

var ErrNotImplemented = "databricks: not implemented"
var ErrTransactionsNotSupported = "databricks: transactions are not supported"
var ErrParametersNotSupported = "databricks: query parameters are not supported"

type StackTracer interface {
	StackTrace() errors.StackTrace
}

// wraps an error and adds trace if not already present
func WrapErr(err error, msg string) error {
	if _, ok := err.(StackTracer); ok {
		return err
	}

	return errors.Wrap(err, msg)
}

// adds a stack trace if not already present
func WrapErrf(err error, format string, args ...interface{}) error {
	if _, ok := err.(StackTracer); ok {
		return err
	}

	return errors.Wrapf(err, format, args...)
}

type Causer interface {
	Cause() error
}
