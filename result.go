package dbsql

import "database/sql/driver"

type result struct {
	AffectedRows int64
	InsertId     int64
}

var _ driver.Result = (*result)(nil)

func (res *result) LastInsertId() (int64, error) {
	return res.InsertId, nil
}

func (res *result) RowsAffected() (int64, error) {
	return res.AffectedRows, nil
}
