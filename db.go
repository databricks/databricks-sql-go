package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/pkg/errors"
)

type DatabricksDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, Execution, error)
	CancelExecution(ctx context.Context, exc Execution) error
	GetExecutionRows(ctx context.Context, exc Execution) (*sql.Rows, error)
	CheckExecution(ctx context.Context, exc Execution) (Execution, error)
	Close() error
}

type databricksDB struct {
	sqldb *sql.DB
}

func OpenDB(c driver.Connector) DatabricksDB {
	cnnr := c.(*connector)
	cnnr.cfg.RunAsync = true
	db := sql.OpenDB(c)
	return &databricksDB{db}
}

func (db *databricksDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, Execution, error) {
	exc := Execution{}
	ctx2 := newContextWithExecution(ctx, &exc)
	ret, err := db.sqldb.QueryContext(ctx2, query, args...)
	return ret, exc, err
}

func (db *databricksDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, string, error) {
	// db.sqldb.ExecContext()
	return nil, "", errors.New(ErrNotImplemented)
}

func (db *databricksDB) Close() error {
	return db.sqldb.Close()
}

func (db *databricksDB) CancelExecution(ctx context.Context, exc Execution) error {
	con, err := db.sqldb.Conn(ctx)
	if err != nil {
		return err
	}
	return con.Raw(func(driverConn any) error {
		dbsqlcon, ok := driverConn.(*Conn)
		if !ok {
			return errors.New("invalid connection type")
		}
		return dbsqlcon.cancelOperation(ctx, exc)
	})
}

func (db *databricksDB) CheckExecution(ctx context.Context, exc Execution) (Execution, error) {
	con, err := db.sqldb.Conn(ctx)
	if err != nil {
		return exc, err
	}
	exRet := exc
	err = con.Raw(func(driverConn any) error {
		dbsqlcon, ok := driverConn.(*Conn)
		if !ok {
			return errors.New("invalid connection type")
		}
		exRet, err = dbsqlcon.getOperationStatus(ctx, exc)
		return err
	})
	return exRet, err
}

func (db *databricksDB) GetExecutionRows(ctx context.Context, exc Execution) (*sql.Rows, error) {
	return db.sqldb.QueryContext(ctx, "", exc)
}

func (db *databricksDB) GetExecutionResult(ctx context.Context, exc Execution) (sql.Result, error) {
	return db.sqldb.ExecContext(ctx, "", exc)
}

type Execution struct {
	Status       ExecutionStatus
	Id           string
	Secret       []byte
	HasResultSet bool
}

type ExecutionStatus string

const (
	// live state Initialized
	ExecutionInitialized ExecutionStatus = "Initialized"
	// live state Running
	ExecutionRunning ExecutionStatus = "Running"
	// terminal state Finished
	ExecutionFinished ExecutionStatus = "Finished"
	// terminal state Canceled
	ExecutionCanceled ExecutionStatus = "Canceled"
	// terminal state Closed
	ExecutionClosed ExecutionStatus = "Closed"
	// terminal state Error
	ExecutionError   ExecutionStatus = "Error"
	ExecutionUnknown ExecutionStatus = "Unknown"
	// live state Pending
	ExecutionPending ExecutionStatus = "Pending"
	// terminal state TimedOut
	ExecutionTimedOut ExecutionStatus = "TimedOut"
)

func newContextWithExecution(ctx context.Context, exc *Execution) context.Context {
	return context.WithValue(ctx, driverctx.ExecutionContextKey, exc)
}

func excFromContext(ctx context.Context) *Execution {
	excId, ok := ctx.Value(driverctx.ExecutionContextKey).(*Execution)
	if !ok {
		return nil
	}
	return excId
}
