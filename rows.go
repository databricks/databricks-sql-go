package dbsql

import (
	"database/sql/driver"
	"reflect"

	"github.com/databricks/databricks-sql-go/hive"
)

// Rows is an iterator over an executed query's results.
type Rows struct {
	rs      *hive.ResultSet
	schema  *hive.TableSchema
	closefn func() error
}

// Close closes rows iterator
func (r *Rows) Close() error {
	return r.closefn()
}

// Columns returns the names of the columns
func (r *Rows) Columns() []string {
	var cols []string
	for _, col := range r.schema.Columns {
		cols = append(cols, col.Name)
	}
	return cols
}

// ColumnTypeScanType returns column's native type
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	return r.schema.Columns[index].ScanType
}

// ColumnTypeDatabaseTypeName returns column's database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.schema.Columns[index].DatabaseTypeName
}

// Next prepares next row for scanning
func (r *Rows) Next(dest []driver.Value) error {
	return r.rs.Next(dest)
}
