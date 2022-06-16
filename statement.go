package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/hive"
)

// Stmt is statement
type Stmt struct {
	stmt string

	conn *Conn
}

// Close statement. No-op
func (s *Stmt) Close() error {
	return nil
}

// NumInput returns number of inputs
func (s *Stmt) NumInput() int {
	return -1
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (s *Stmt) CheckNamedValue(val *driver.NamedValue) error {
	t, ok := val.Value.(time.Time)
	if ok {
		val.Value = t.Format(hive.TimestampFormat)
		return nil
	}
	return driver.ErrSkip
}

// Exec executes a query that doesn't return rows
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	nargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nargs[i] = driver.NamedValue{Ordinal: i, Value: arg}
	}
	return s.ExecContext(context.Background(), nargs)
}

// Query executes a query that may return rows
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	nargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nargs[i] = driver.NamedValue{Ordinal: i, Value: arg}
	}
	return s.QueryContext(context.Background(), nargs)
}

// QueryContext executes a query that may return rows
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	session, err := s.conn.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	stmt, err := statement(s.stmt, args)
	if err != nil {
		return nil, err
	}
	return query(ctx, session, stmt)
}

// ExecContext executes a query that doesn't return rows
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	session, err := s.conn.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	stmt, err := statement(s.stmt, args)
	if err != nil {
		return nil, err
	}
	return exec(ctx, session, stmt)
}

func template(query string) string {
	ordinal := 1
	for {
		idx := strings.Index(query, "?")
		if idx == -1 {
			break
		}
		placeholder := fmt.Sprintf("@p%d", ordinal)
		query = strings.Replace(query, "?", placeholder, 1)
		ordinal++
	}
	return query
}

func statement(tmpl string, args []driver.NamedValue) (string, error) {
	stmt := tmpl
	for _, arg := range args {
		var re *regexp.Regexp
		if arg.Name != "" {
			re = regexp.MustCompile(fmt.Sprintf("@%s%s", arg.Name, `\b`))
		} else {
			re = regexp.MustCompile(fmt.Sprintf("@p%d%s", arg.Ordinal, `\b`))
		}
		escaped, err := EscapeArg(arg)
		if err != nil {
			return "", err
		}
		val := fmt.Sprintf("%v", escaped)
		stmt = re.ReplaceAllString(stmt, val)
	}
	return stmt, nil
}

func query(ctx context.Context, session *hive.Session, stmt string) (driver.Rows, error) {
	operation, err := session.ExecuteStatement(ctx, stmt)
	if err != nil {
		return nil, err
	}

	schema, err := operation.GetResultSetMetadata(ctx)
	if err != nil {
		return nil, err
	}

	rs, err := operation.FetchResults(ctx, schema)
	if err != nil {
		return nil, err
	}

	return &Rows{
		rs:      rs,
		schema:  schema,
		closefn: func() error { return operation.Close(ctx) },
	}, nil
}

func exec(ctx context.Context, session *hive.Session, stmt string) (driver.Result, error) {
	operation, err := session.ExecuteStatement(ctx, stmt)
	if err != nil {
		return nil, err
	}

	if err := operation.Close(ctx); err != nil {
		return nil, err
	}

	return driver.ResultNoRows, nil
}
