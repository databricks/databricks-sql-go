package dbsql

import (
	"github.com/pkg/errors"
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// wraps an error and adds trace if not already present
func wrapErr(err error, msg string) error {
	if _, ok := err.(stackTracer); ok {
		return err
	}

	return errors.Wrap(err, msg)
}

// adds a stack trace if not already present
func wrapErrf(err error, format string, args ...interface{}) error {
	if _, ok := err.(stackTracer); ok {
		return err
	}

	return errors.Wrapf(err, format, args...)
}

type causer interface {
	Cause() error
}
