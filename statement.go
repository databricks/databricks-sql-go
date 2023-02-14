package dbsql

import (
	"context"
	"database/sql/driver"
	"errors"

	dbsqlerr "github.com/databricks/databricks-sql-go/internal/err"
)

type stmt struct {
	conn  *conn
	query string
}

// Close closes the statement.
func (s *stmt) Close() error {
	// no-op
	return nil
}

// NumInput returns -1 and the sql package will not sanity check Exec or Query argument counts.
func (s *stmt) NumInput() int {
	return -1
}

// Exec is not implemented.
//
// Deprecated: Use StmtExecContext instead.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New(dbsqlerr.ErrNotImplemented)
}

// Query is not implemented.
//
// Deprecated: Use StmtQueryContext instead.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New(dbsqlerr.ErrNotImplemented)
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext honors the context timeout and return when it is canceled.
// Statement ExecContext is the same as connection ExecContext
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext honors the context timeout and return when it is canceled.
// Statement QueryContext is the same as connection QueryContext
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

var _ driver.Stmt = (*stmt)(nil)
