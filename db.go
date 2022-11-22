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
	CancelExecution(ctx context.Context, exec Execution) error
	GetExecutionRows(ctx context.Context, exec Execution) (*sql.Rows, error)
	CheckExecution(ctx context.Context, exec Execution) (Execution, error)
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
	exec := Execution{}
	ctx2 := newContextWithExec(ctx, &exec)
	ret, err := db.sqldb.QueryContext(ctx2, query, args...)
	return ret, exec, err
}

func (db *databricksDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, string, error) {
	// db.sqldb.ExecContext()
	return nil, "", errors.New(ErrNotImplemented)
}

func (db *databricksDB) Close() error {
	return db.sqldb.Close()
}

func (db *databricksDB) CancelExecution(ctx context.Context, exec Execution) error {
	con, err := db.sqldb.Conn(ctx)
	if err != nil {
		return err
	}
	return con.Raw(func(driverConn any) error {
		dbsqlcon, ok := driverConn.(*Conn)
		if !ok {
			return errors.New("invalid connection type")
		}
		return dbsqlcon.cancelOperation(ctx, exec)
	})
}

func (db *databricksDB) CheckExecution(ctx context.Context, exec Execution) (Execution, error) {
	con, err := db.sqldb.Conn(ctx)
	if err != nil {
		return exec, err
	}
	exRet := exec
	err = con.Raw(func(driverConn any) error {
		dbsqlcon, ok := driverConn.(*Conn)
		if !ok {
			return errors.New("invalid connection type")
		}
		exRet, err = dbsqlcon.getOperationStatus(ctx, exec)
		return err
	})
	return exRet, err
}

func (db *databricksDB) GetExecutionRows(ctx context.Context, exec Execution) (*sql.Rows, error) {
	return db.sqldb.QueryContext(ctx, "", exec)
}

func (db *databricksDB) GetExecutionResult(ctx context.Context, exec Execution) (sql.Result, error) {
	return db.sqldb.ExecContext(ctx, "", exec)
}

type Execution struct {
	Status       string
	Id           string
	Secret       []byte
	HasResultSet bool
}

func newContextWithExec(ctx context.Context, exec *Execution) context.Context {
	return context.WithValue(ctx, driverctx.ExecutionContextKey, exec)
}

func execFromContext(ctx context.Context) *Execution {
	execId, ok := ctx.Value(driverctx.ExecutionContextKey).(*Execution)
	if !ok {
		return nil
	}
	return execId
}
