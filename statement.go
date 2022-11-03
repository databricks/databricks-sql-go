package dbsql

import (
	"context"
	"database/sql/driver"
)

type dbsqlStmt struct {
}

func (s *dbsqlStmt) Close() error {
	return ErrNotImplemented
}

func (s *dbsqlStmt) NumInput() int {
	return 0
}

// Deprecated: Use StmtExecContext instead.
func (s *dbsqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrNotImplemented
}

// Deprecated: Use StmtQueryContext instead.
func (s *dbsqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, ErrNotImplemented
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *dbsqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, ErrNotImplemented
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *dbsqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return nil, ErrNotImplemented
}

var _ driver.Connector = (*connector)(nil)
