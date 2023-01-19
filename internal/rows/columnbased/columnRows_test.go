package columnbased

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/stretchr/testify/assert"
)

func TestNRowsColumnRowScanner(t *testing.T) {
	var crs *columnRowScanner
	assert.Equal(t, int64(0), crs.NRows())

	rowSet := &cli_service.TRowSet{}
	var rs rowscanner.RowScanner
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)

	assert.Equal(t, int64(0), rs.NRows())

	rowSet.Columns = make([]*cli_service.TColumn, 1)

	bc := make([]bool, 3)
	rowSet.Columns[0] = &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: bc}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(bc)), rs.NRows())

	by := make([]int8, 5)
	rowSet.Columns[0] = &cli_service.TColumn{ByteVal: &cli_service.TByteColumn{Values: by}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(by)), rs.NRows())

	i16 := make([]int16, 7)
	rowSet.Columns[0] = &cli_service.TColumn{I16Val: &cli_service.TI16Column{Values: i16}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(i16)), rs.NRows())

	i32 := make([]int32, 11)
	rowSet.Columns[0] = &cli_service.TColumn{I32Val: &cli_service.TI32Column{Values: i32}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(i32)), rs.NRows())

	i64 := make([]int64, 13)
	rowSet.Columns[0] = &cli_service.TColumn{I64Val: &cli_service.TI64Column{Values: i64}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(i64)), rs.NRows())

	str := make([]string, 17)
	rowSet.Columns[0] = &cli_service.TColumn{StringVal: &cli_service.TStringColumn{Values: str}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(str)), rs.NRows())

	dbl := make([]float64, 19)
	rowSet.Columns[0] = &cli_service.TColumn{DoubleVal: &cli_service.TDoubleColumn{Values: dbl}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(dbl)), rs.NRows())

	bin := make([][]byte, 23)
	rowSet.Columns[0] = &cli_service.TColumn{BinaryVal: &cli_service.TBinaryColumn{Values: bin}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil, nil)
	assert.Equal(t, int64(len(bin)), rs.NRows())
}
