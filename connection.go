package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/hive"
)

// Connection
type Conn struct {
	t       thrift.TTransport
	session *hive.Session
	client  *hive.Client
	log     *log.Logger
}

func (c *Conn) Ping(ctx context.Context) error {
	session, err := c.OpenSession(ctx)
	if err != nil {
		return err
	}

	if err := session.Ping(ctx); err != nil {
		return err
	}

	return nil
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (c *Conn) CheckNamedValue(val *driver.NamedValue) error {
	t, ok := val.Value.(time.Time)
	if ok {
		val.Value = t.Format(hive.TimestampFormat)
		return nil
	}
	return driver.ErrSkip
}

// Prepare returns prepared statement
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns prepared statement
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &Stmt{
		conn: c,
		stmt: template(query),
	}, nil
}

// QueryContext executes a query that may return rows
func (c *Conn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	session, err := c.OpenSession(ctx)
	if err != nil {
		return nil, err
	}

	tmpl := template(q)
	stmt, err := statement(tmpl, args)
	if err != nil {
		return nil, err
	}
	return query(ctx, session, stmt)
}

// ExecContext executes a query that doesn't return rows
func (c *Conn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	session, err := c.OpenSession(ctx)
	if err != nil {
		return nil, err
	}

	tmpl := template(q)
	stmt, err := statement(tmpl, args)
	if err != nil {
		return nil, err
	}
	return exec(ctx, session, stmt)
}

// Begin is not supported
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

// OpenSession ensure opened session
func (c *Conn) OpenSession(ctx context.Context) (*hive.Session, error) {
	if c.session == nil {
		session, err := c.client.OpenSession(ctx)
		if err != nil {
			c.log.Printf("failed to open session: %v", err)
			return nil, fmt.Errorf("%v: %v", driver.ErrBadConn, err)
		}
		c.session = session
	}
	return c.session, nil
}

// ResetSession closes hive session
func (c *Conn) ResetSession(ctx context.Context) error {
	if c.session != nil {
		if err := c.session.Close(ctx); err != nil {
			return err
		}
		c.session = nil
	}
	return nil
}

// Close connection
func (c *Conn) Close() error {
	c.log.Printf("close connection")
	return c.t.Close()
}
