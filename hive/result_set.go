package hive

import (
	"database/sql/driver"
	"io"
	"time"

	"github.com/databricks/databricks-sql-go/cli_service"
)

// ResultSet ...
type ResultSet struct {
	idx     int
	length  int
	fetchfn func() (*cli_service.TFetchResultsResp, error)
	schema  *TableSchema

	operation *Operation
	result    *cli_service.TRowSet
	more      bool
}

// Next ...
func (rs *ResultSet) Next(dest []driver.Value) error {
	if rs.idx >= rs.length {
		if !rs.more {
			return io.EOF
		}

		resp, err := rs.fetchfn()
		if err != nil {
			return err
		}
		rs.result = resp.Results
		rs.more = resp.GetHasMoreRows()
		rs.idx = 0
		rs.length = length(rs.result)
	}

	if rs.length == 0 {
		return io.EOF
	}

	for i := range dest {
		val, err := value(rs.result.Columns[i], rs.schema.Columns[i], rs.idx)
		if err != nil {
			return err
		}
		dest[i] = val
	}
	rs.idx++
	return nil
}

func isNull(nulls []byte, position int) bool {
	index := position / 8
	if len(nulls) > index {
		b := nulls[index]
		return (b & (1 << (uint)(position%8))) != 0
	}
	return false
}

func value(col *cli_service.TColumn, cd *ColDesc, i int) (interface{}, error) {
	switch cd.DatabaseTypeName {
	case "STRING", "CHAR", "VARCHAR":
		if isNull(col.StringVal.Nulls, i) {
			return nil, nil
		}
		return col.StringVal.Values[i], nil
	case "TINYINT":
		if isNull(col.ByteVal.Nulls, i) {
			return nil, nil
		}
		return col.ByteVal.Values[i], nil
	case "SMALLINT":
		if isNull(col.I16Val.Nulls, i) {
			return nil, nil
		}
		return col.I16Val.Values[i], nil
	case "INT":
		if isNull(col.I32Val.Nulls, i) {
			return nil, nil
		}
		return col.I32Val.Values[i], nil
	case "BIGINT":
		if isNull(col.I64Val.Nulls, i) {
			return nil, nil
		}
		return col.I64Val.Values[i], nil
	case "BOOLEAN":
		if isNull(col.BoolVal.Nulls, i) {
			return nil, nil
		}
		return col.BoolVal.Values[i], nil
	case "FLOAT", "DOUBLE":
		if isNull(col.DoubleVal.Nulls, i) {
			return nil, nil
		}
		return col.DoubleVal.Values[i], nil
	case "TIMESTAMP", "DATETIME":
		if isNull(col.StringVal.Nulls, i) {
			return nil, nil
		}
		t, err := time.Parse(TimestampFormat, col.StringVal.Values[i])
		if err != nil {
			return nil, err
		}
		return t, nil
	default:
		if isNull(col.StringVal.Nulls, i) {
			return nil, nil
		}
		return col.StringVal.Values[i], nil
	}
}

func length(rs *cli_service.TRowSet) int {
	if rs == nil {
		return 0
	}
	for _, col := range rs.Columns {
		if col.BoolVal != nil {
			return len(col.BoolVal.Values)
		}
		if col.ByteVal != nil {
			return len(col.ByteVal.Values)
		}
		if col.I16Val != nil {
			return len(col.I16Val.Values)
		}
		if col.I32Val != nil {
			return len(col.I32Val.Values)
		}
		if col.I32Val != nil {
			return len(col.I32Val.Values)
		}
		if col.I64Val != nil {
			return len(col.I64Val.Values)
		}
		if col.StringVal != nil {
			return len(col.StringVal.Values)
		}
		if col.DoubleVal != nil {
			return len(col.DoubleVal.Values)
		}
	}
	return 0
}
