package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

type DatabricksDB interface {
	QueryContextAsync(ctx context.Context, query string, args ...any) (rows *sql.Rows, queryId string, err error)
}

type databricksDB struct {
	db *sql.DB
}

func OpenDB(c driver.Connector) DatabricksDB {
	db := sql.OpenDB(c)
	return &databricksDB{db}
}

func (db *databricksDB) QueryContextAsync(ctx context.Context, query string, args ...any) (rows *sql.Rows, queryId string, err error) {
	return nil, "", nil
}

func (db *databricksDB) ExecContextAsync(ctx context.Context, query string, args ...any) (result sql.Result, queryId string) {
	//go do something
	return nil, ""
}

func (db *databricksDB) CancelQuery(ctx context.Context, queryId string) error {
	//go do something
	return nil
}

func (db *databricksDB) GetQueryStatus(ctx context.Context, queryId string) error {
	//go do something
	return nil
}

func (db *databricksDB) FetchRows(ctx context.Context, queryId string) (rows *sql.Rows, err error) {
	//go do something
	return nil, nil
}

func (db *databricksDB) FetchResult(ctx context.Context, queryId string) (rows sql.Result, err error) {
	//go do something
	return nil, nil
}
