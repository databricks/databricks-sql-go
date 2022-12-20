package columnbased

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/rows/rowscanner"
	"github.com/stretchr/testify/assert"
)

func TestNRowsColumnRowScanner(t *testing.T) {
	var crs *columnRowScanner
	assert.Equal(t, int64(0), crs.NRows())

	rowSet := &cli_service.TRowSet{}
	var rs rowscanner.RowScanner
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)

	assert.Equal(t, int64(0), rs.NRows())

	rowSet.Columns = make([]*cli_service.TColumn, 1)

	bc := make([]bool, 3)
	rowSet.Columns[0] = &cli_service.TColumn{BoolVal: &cli_service.TBoolColumn{Values: bc}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(bc)), rs.NRows())

	by := make([]int8, 5)
	rowSet.Columns[0] = &cli_service.TColumn{ByteVal: &cli_service.TByteColumn{Values: by}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(by)), rs.NRows())

	i16 := make([]int16, 7)
	rowSet.Columns[0] = &cli_service.TColumn{I16Val: &cli_service.TI16Column{Values: i16}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(i16)), rs.NRows())

	i32 := make([]int32, 11)
	rowSet.Columns[0] = &cli_service.TColumn{I32Val: &cli_service.TI32Column{Values: i32}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(i32)), rs.NRows())

	i64 := make([]int64, 13)
	rowSet.Columns[0] = &cli_service.TColumn{I64Val: &cli_service.TI64Column{Values: i64}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(i64)), rs.NRows())

	str := make([]string, 17)
	rowSet.Columns[0] = &cli_service.TColumn{StringVal: &cli_service.TStringColumn{Values: str}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(str)), rs.NRows())

	dbl := make([]float64, 19)
	rowSet.Columns[0] = &cli_service.TColumn{DoubleVal: &cli_service.TDoubleColumn{Values: dbl}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(dbl)), rs.NRows())

	bin := make([][]byte, 23)
	rowSet.Columns[0] = &cli_service.TColumn{BinaryVal: &cli_service.TBinaryColumn{Values: bin}}
	rs, _ = NewColumnRowScanner(nil, rowSet, nil)
	assert.Equal(t, int64(len(bin)), rs.NRows())
}

func TestHandlingDateTime(t *testing.T) {
	t.Run("should do nothing if data is not a date/time", func(t *testing.T) {
		val, err := rowscanner.HandleDateTime("this is not a date", "STRING", "string_col", time.UTC)
		assert.Nil(t, err, "handleDateTime should do nothing if a column is not a date/time")
		assert.Equal(t, "this is not a date", val)
	})

	t.Run("should error on invalid date/time value", func(t *testing.T) {
		_, err := rowscanner.HandleDateTime("this is not a date", "DATE", "date_col", time.UTC)
		assert.NotNil(t, err)
		assert.True(t, strings.HasPrefix(err.Error(), fmt.Sprintf(rowscanner.ErrRowsParseValue, "DATE", "this is not a date", "date_col")))
	})

	t.Run("should parse valid date", func(t *testing.T) {
		dt, err := rowscanner.HandleDateTime("2006-12-22", "DATE", "date_col", time.UTC)
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2006, 12, 22, 0, 0, 0, 0, time.UTC), dt)
	})

	t.Run("should parse valid timestamp", func(t *testing.T) {
		dt, err := rowscanner.HandleDateTime("2006-12-22 17:13:11.000001000", "TIMESTAMP", "timestamp_col", time.UTC)
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2006, 12, 22, 17, 13, 11, 1000, time.UTC), dt)
	})

	t.Run("should parse date with negative year", func(t *testing.T) {
		expectedTime := time.Date(-2006, 12, 22, 0, 0, 0, 0, time.UTC)
		dateStrings := []string{
			"-2006-12-22",
			"\u22122006-12-22",
			"\x2D2006-12-22",
		}

		for _, s := range dateStrings {
			dt, err := rowscanner.HandleDateTime(s, "DATE", "date_col", time.UTC)
			assert.Nil(t, err)
			assert.Equal(t, expectedTime, dt)
		}
	})

	t.Run("should parse timestamp with negative year", func(t *testing.T) {
		expectedTime := time.Date(-2006, 12, 22, 17, 13, 11, 1000, time.UTC)

		timestampStrings := []string{
			"-2006-12-22 17:13:11.000001000",
			"\u22122006-12-22 17:13:11.000001000",
			"\x2D2006-12-22 17:13:11.000001000",
		}

		for _, s := range timestampStrings {
			dt, err := rowscanner.HandleDateTime(s, "TIMESTAMP", "timestamp_col", time.UTC)
			assert.Nil(t, err)
			assert.Equal(t, expectedTime, dt)
		}
	})
}
