package dbsql

import (
	"database/sql/driver"

	dbsqlerr "github.com/databricks/databricks-sql-go/v2/errors"
	"github.com/databricks/databricks-sql-go/v2/internal/backend"
)

type result struct {
	AffectedRows int64
	InsertId     int64
}

var _ driver.Result = (*result)(nil)

type unknownRowsAffectedResult struct {
	result
}

func newResult(op backend.Operation, reportUnknown bool) driver.Result {
	count := op.AffectedRows()
	if reportUnknown {
		if reporter, ok := op.(backend.AffectedRowsReporter); ok && !reporter.AffectedRowsKnown() {
			return &unknownRowsAffectedResult{result{AffectedRows: count}}
		}
	}
	return &result{AffectedRows: count}
}

func (res *unknownRowsAffectedResult) RowsAffected() (int64, error) {
	return 0, dbsqlerr.ErrRowsAffectedUnknown
}

// LastInsertId returns the database's auto-generated ID after an insert into a table.
// This is currently not really implemented for this driver and will always return 0.
func (res *result) LastInsertId() (int64, error) {
	return res.InsertId, nil
}

// RowsAffected returns the number of rows affected by the query.
func (res *result) RowsAffected() (int64, error) {
	return res.AffectedRows, nil
}
