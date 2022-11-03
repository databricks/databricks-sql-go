package dbsql

import "database/sql/driver"

type dbsqlResult struct {
	affectedRows int64
	insertId     int64
}

var _ driver.Result = (*dbsqlResult)(nil)

func (res *dbsqlResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *dbsqlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
