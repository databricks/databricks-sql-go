package columnbased

import (
	"database/sql/driver"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
)

// row scanner for query results in column based format
type columnRowScanner struct {
	*dbsqllog.DBSQLLogger
	// TRowSet with query results in column format
	rowSet *client.ResultData
	schema *client.ResultSchema

	// number of rows in the current TRowSet
	nRows int64

	location *time.Location
}

var _ rowscanner.RowScanner = (*columnRowScanner)(nil)

// NewColumnRowScanner returns a columnRowScanner initialized with the provided
// values.
func NewColumnRowScanner(schema *client.ResultSchema, rowSet *client.ResultData, cfg *config.Config, logger *dbsqllog.DBSQLLogger) (rowscanner.RowScanner, error) {
	if logger == nil {
		logger = dbsqllog.Logger
	}

	var location *time.Location = time.UTC
	if cfg != nil {
		if cfg.Location != nil {
			location = cfg.Location
		}
	}

	logger.Debug().Msg("databricks: creating column row scanner")
	rs := &columnRowScanner{
		schema:      schema,
		rowSet:      rowSet,
		nRows:       countRows(rowSet),
		DBSQLLogger: logger,
		location:    location,
	}

	return rs, nil
}

// Close is called when the Rows instance is closed.
func (crs *columnRowScanner) Close() {}

// NRows returns the number or rows in the current TRowSet
func (crs *columnRowScanner) NRows() int64 {
	if crs == nil {
		return 0
	}
	return crs.nRows
}

// ScanRow is called to populate the provided slice with the
// content of the current row. The provided slice will be the same
// size as the number of columns.
// The dest should not be written to outside of ScanRow. Care
// should be taken when closing a RowScanner not to modify
// a buffer held in dest.
func (crs *columnRowScanner) ScanRow(
	dest []driver.Value,
	rowIndex int64) error {

	// populate the destinatino slice
	for i := range dest {
		val, err := crs.value(crs.rowSet.Columns[i], crs.schema.Columns[i], rowIndex)

		if err != nil {
			return err
		}

		dest[i] = val
	}

	return nil
}

// value retrieves the value for the specified colum/row
func (crs *columnRowScanner) value(tColumn *cli_service.TColumn, columnInfo *client.ColumnInfo, rowNum int64) (val interface{}, err error) {
	// default to UTC time
	if crs.location == nil {
		crs.location = time.UTC
	}

	// Database type name
	dbtype := columnInfo.Type

	if tVal := tColumn.GetStringVal(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
		// DATE and TIMESTAMP are returned as strings so we need to handle that possibility
		val, err = rowscanner.HandleDateTime(val, dbtype, columnInfo.Name, crs.location)
		if err != nil {
			crs.Err(err).Msg("databrics: column row scanner failed to parse date/time")
		}
	} else if tVal := tColumn.GetByteVal(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetI16Val(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetI32Val(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetI64Val(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetBoolVal(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	} else if tVal := tColumn.GetDoubleVal(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		if dbtype == client.FLOAT_TYPE {
			// database types FLOAT and DOUBLE are both returned as a float64
			// convert to a float32 is valid because the FLOAT type would have
			// only been four bytes on the server
			val = float32(tVal.Values[rowNum])
		} else {
			val = tVal.Values[rowNum]
		}
	} else if tVal := tColumn.GetBinaryVal(); tVal != nil && !rowscanner.IsNull(tVal.Nulls, rowNum) {
		val = tVal.Values[rowNum]
	}

	return val, err
}

// countRows returns the number of rows in the TRowSet
func countRows(rowSet *client.ResultData) int64 {
	if rowSet == nil || rowSet.Columns == nil {
		return 0
	}

	// Find a column/values and return the number of values.
	for _, col := range rowSet.Columns {
		if col.BoolVal != nil {
			return int64(len(col.BoolVal.Values))
		}
		if col.ByteVal != nil {
			return int64(len(col.ByteVal.Values))
		}
		if col.I16Val != nil {
			return int64(len(col.I16Val.Values))
		}
		if col.I32Val != nil {
			return int64(len(col.I32Val.Values))
		}
		if col.I64Val != nil {
			return int64(len(col.I64Val.Values))
		}
		if col.StringVal != nil {
			return int64(len(col.StringVal.Values))
		}
		if col.DoubleVal != nil {
			return int64(len(col.DoubleVal.Values))
		}
		if col.BinaryVal != nil {
			return int64(len(col.BinaryVal.Values))
		}
	}
	return 0
}
