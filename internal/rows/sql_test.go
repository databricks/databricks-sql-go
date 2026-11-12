package rows

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLRowsReportsCloseOperationError(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "flag", Type: arrow.FixedWidthTypes.Boolean}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
	record := builder.NewRecord()
	defer record.Release()
	var data bytes.Buffer
	writer := ipc.NewWriter(&data, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(data.Bytes())
	}))
	defer server.Close()
	page := func(offset int64) *cli_service.TRowSet {
		return &cli_service.TRowSet{StartRowOffset: offset, ResultLinks: []*cli_service.TSparkArrowResultLink{{
			FileLink: server.URL, StartRowOffset: offset, RowCount: 2,
			ExpiryTime: time.Now().Add(time.Hour).Unix(),
		}}}
	}
	metadata := &cli_service.TGetResultSetMetadataResp{
		Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
			ColumnName: "flag",
			TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
				PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_BOOLEAN_TYPE},
			}}},
		}}},
	}
	for _, tc := range []struct {
		name        string
		hasMoreRows bool
		earlyClose  bool
		wantFetches int
		wantRows    []bool
	}{
		{name: "complete direct results", wantRows: []bool{true, false}},
		{name: "paginated results", hasMoreRows: true, wantFetches: 1, wantRows: []bool{true, false, true, false}},
		{name: "explicit early close", hasMoreRows: true, earlyClose: true, wantRows: []bool{true}},
	} {
		for _, closeErr := range []error{nil, errors.New("close failed")} {
			t.Run(fmt.Sprintf("%s/close=%v", tc.name, closeErr), func(t *testing.T) {
				fetches, closes, callbacks := 0, 0, 0
				noMoreRows := false
				testClient := &client.TestClient{
					FnFetchResults: func(context.Context, *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
						fetches++
						require.LessOrEqual(t, fetches, tc.wantFetches)
						return &cli_service.TFetchResultsResp{HasMoreRows: &noMoreRows, Results: page(2)}, nil
					},
					FnCloseOperation: func(context.Context, *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
						closes++
						return nil, closeErr
					},
				}
				cfg := config.WithDefaults()
				cfg.UseLz4Compression = false
				dr, err := NewRows(context.Background(), nil, testClient, cfg, &cli_service.TSparkDirectResults{
					ResultSetMetadata: metadata,
					ResultSet: &cli_service.TFetchResultsResp{
						HasMoreRows: &tc.hasMoreRows, Results: page(0),
					},
				}, &TelemetryCallbacks{OnClose: func(_ int64, count int, iterErr, rpcErr error) {
					callbacks++
					assert.Equal(t, 1, closes)
					assert.Equal(t, 1+tc.wantFetches, count)
					assert.NoError(t, iterErr)
					assert.ErrorIs(t, rpcErr, closeErr)
				}})
				require.NoError(t, err)
				defer dr.Close() //nolint:errcheck
				db := sql.OpenDB(&sqlRowsTestDriver{result: dr})
				t.Cleanup(func() { assert.NoError(t, db.Close()) })
				result, queryErr := db.QueryContext(context.Background(), "select flag")
				require.NoError(t, queryErr)
				defer result.Close() //nolint:errcheck
				var got []bool
				for result.Next() {
					var value bool
					require.NoError(t, result.Scan(&value))
					got = append(got, value)
					assert.Zero(t, closes)
					if tc.earlyClose {
						break
					}
				}
				assert.Equal(t, tc.wantRows, got)
				if tc.earlyClose {
					assert.ErrorIs(t, result.Close(), closeErr)
				} else {
					assert.ErrorIs(t, result.Err(), closeErr)
					assert.Equal(t, 1, closes)
					assert.NoError(t, result.Close())
				}
				assert.ErrorIs(t, result.Err(), closeErr)
				assert.ErrorIs(t, dr.Close(), closeErr)
				assert.Equal(t, tc.wantFetches, fetches)
				assert.Equal(t, 1, closes)
				assert.Equal(t, 1, callbacks)
			})
		}
	}
}

type sqlRowsTestDriver struct {
	result driver.Rows
}

func (d *sqlRowsTestDriver) Connect(context.Context) (driver.Conn, error) { return d, nil }
func (d *sqlRowsTestDriver) Driver() driver.Driver                        { return d }
func (d *sqlRowsTestDriver) Open(string) (driver.Conn, error)             { return d, nil }
func (d *sqlRowsTestDriver) Close() error                                 { return nil }
func (d *sqlRowsTestDriver) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is unsupported")
}
func (d *sqlRowsTestDriver) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are unsupported")
}
func (d *sqlRowsTestDriver) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return d.result, nil
}
