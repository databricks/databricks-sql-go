package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureWarnLogs routes the driver logger to a buffer at the default (warn)
// level for the duration of the test.
func captureWarnLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	prev := logger.Logger.GetLevel()
	var buf bytes.Buffer
	logger.SetLogOutput(&buf)
	require.NoError(t, logger.SetLogLevel("warn"))
	t.Cleanup(func() {
		logger.SetLogOutput(os.Stderr)
		_ = logger.SetLogLevel(prev.String())
	})
	return &buf
}

func maxDownloadThreadsWarnings(logs string) []string {
	var lines []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "maxDownloadThreads") {
			lines = append(lines, line)
		}
	}
	return lines
}

// A non-positive CloudFetch download-thread count degrades to the default with
// one warning instead of starting no download workers (issue #439).
func TestMaxDownloadThreadsNonPositiveDegradesToDefault(t *testing.T) {
	cases := []struct {
		value    int
		want     int
		wantWarn bool
	}{
		{value: -1, want: config.DefaultMaxDownloadThreads, wantWarn: true},
		{value: 0, want: config.DefaultMaxDownloadThreads, wantWarn: true},
		{value: 1, want: 1},
		{value: 15, want: 15},
		{value: 100000, want: 100000},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("option %d", tc.value), func(t *testing.T) {
			logs := captureWarnLogs(t)
			con, err := NewConnector(
				WithServerHostname("example.cloud.databricks.com"),
				WithHTTPPath("/sql/1.0/warehouses/abc"),
				WithAccessToken("token"),
				WithMaxDownloadThreads(tc.value),
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want, con.(*connector).cfg.MaxDownloadThreads)
			assertMaxDownloadThreadsWarning(t, logs.String(), tc.value, tc.wantWarn)
		})

		t.Run(fmt.Sprintf("dsn %d", tc.value), func(t *testing.T) {
			logs := captureWarnLogs(t)
			dsn := fmt.Sprintf("token:secret@example.cloud.databricks.com:443/sql/1.0/warehouses/abc?maxDownloadThreads=%d", tc.value)
			con, err := (&databricksDriver{}).OpenConnector(dsn)
			require.NoError(t, err)
			assert.Equal(t, tc.want, con.(*connector).cfg.MaxDownloadThreads)
			assertMaxDownloadThreadsWarning(t, logs.String(), tc.value, tc.wantWarn)
		})
	}

	t.Run("unset keeps the default without a warning", func(t *testing.T) {
		logs := captureWarnLogs(t)
		con, err := NewConnector(WithServerHostname("example.cloud.databricks.com"))
		require.NoError(t, err)
		assert.Equal(t, config.DefaultMaxDownloadThreads, con.(*connector).cfg.MaxDownloadThreads)
		assert.Empty(t, maxDownloadThreadsWarnings(logs.String()))
	})
}

func assertMaxDownloadThreadsWarning(t *testing.T, logs string, value int, wantWarn bool) {
	t.Helper()
	warnings := maxDownloadThreadsWarnings(logs)
	if !wantWarn {
		assert.Empty(t, warnings)
		return
	}
	require.Len(t, warnings, 1, "want exactly one warning, got logs: %s", logs)
	assert.Contains(t, warnings[0], `"level":"warn"`)
	assert.Contains(t, warnings[0], "WithMaxDownloadThreads")
	assert.Contains(t, warnings[0], fmt.Sprintf("value %d", value))
}

// TestCloudFetchReadWithNonPositiveMaxDownloadThreads drives a full CloudFetch
// read through the real connector, Thrift client and download pool against a
// fake server. Before the fix the first rows.Next failed with
// "row number 0 is not contained in any arrow batch: EOF".
func TestCloudFetchReadWithNonPositiveMaxDownloadThreads(t *testing.T) {
	ids := []int64{7, 8, 9}
	schemaBytes, fileBytes := cloudFetchArrowFixture(t, ids)

	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(fileBytes)
	}))
	defer fileServer.Close()

	state := &callState{}
	loadTestData(t, "OpenSessionSuccess.json", &state.openSessionResp)
	loadTestData(t, "CloseSessionSuccess.json", &state.closeSessionResp)
	loadTestData(t, "CloseOperationSuccess.json", &state.closeOperationResp)
	state.executeStatementResp = cloudFetchExecuteResponse(schemaBytes, fileServer.URL, int64(len(ids)))
	ts := getServer(state)
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)

	open := map[string]func(t *testing.T, threads int) *sql.DB{
		"option": func(t *testing.T, threads int) *sql.DB {
			con, err := NewConnector(
				WithServerHostname("localhost"),
				WithPort(port),
				WithAccessToken("token"),
				WithMaxDownloadThreads(threads),
			)
			require.NoError(t, err)
			return sql.OpenDB(con)
		},
		"dsn": func(t *testing.T, threads int) *sql.DB {
			db, err := sql.Open("databricks", fmt.Sprintf("http://token:secret@localhost:%d/?maxDownloadThreads=%d", port, threads))
			require.NoError(t, err)
			return db
		},
	}

	for name, openDB := range open {
		for _, threads := range []int{-1, 0, 2} {
			t.Run(fmt.Sprintf("%s %d", name, threads), func(t *testing.T) {
				db := openDB(t, threads)
				defer db.Close() //nolint:errcheck

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				rows, err := db.QueryContext(ctx, "SELECT id FROM range(3)")
				require.NoError(t, err)
				defer rows.Close() //nolint:errcheck

				var got []int64
				for rows.Next() {
					var id int64
					require.NoError(t, rows.Scan(&id))
					got = append(got, id)
				}
				require.NoError(t, rows.Err())
				assert.Equal(t, ids, got)
			})
		}
	}
}

// cloudFetchArrowFixture returns the result's Arrow schema message and a
// CloudFetch file holding one BIGINT column with the given values.
func cloudFetchArrowFixture(t *testing.T, ids []int64) (schemaBytes, fileBytes []byte) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	var schemaBuf bytes.Buffer
	sw := ipc.NewWriter(&schemaBuf, ipc.WithSchema(schema))
	require.NoError(t, sw.Close())

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	record := builder.NewRecordBatch()
	defer record.Release()

	var fileBuf bytes.Buffer
	fw := ipc.NewWriter(&fileBuf, ipc.WithSchema(schema))
	require.NoError(t, fw.Write(record))
	require.NoError(t, fw.Close())

	return schemaBuf.Bytes(), fileBuf.Bytes()
}

// cloudFetchExecuteResponse is a finished ExecuteStatement whose direct results
// carry a single CloudFetch link and no inline rows.
func cloudFetchExecuteResponse(schemaBytes []byte, fileURL string, rowCount int64) cli_service.TExecuteStatementResp {
	success := &cli_service.TStatus{StatusCode: cli_service.TStatusCode_SUCCESS_STATUS}
	hasMoreRows := false
	return cli_service.TExecuteStatementResp{
		Status: success,
		OperationHandle: &cli_service.TOperationHandle{
			OperationId: &cli_service.THandleIdentifier{
				GUID:   []byte("0123456789abcdef"),
				Secret: []byte("fedcba9876543210"),
			},
			OperationType: cli_service.TOperationType_EXECUTE_STATEMENT,
			HasResultSet:  true,
		},
		DirectResults: &cli_service.TSparkDirectResults{
			OperationStatus: &cli_service.TGetOperationStatusResp{
				Status:         success,
				OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
			},
			ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
				Status: success,
				Schema: &cli_service.TTableSchema{Columns: []*cli_service.TColumnDesc{{
					ColumnName: "id",
					TypeDesc: &cli_service.TTypeDesc{Types: []*cli_service.TTypeEntry{{
						PrimitiveEntry: &cli_service.TPrimitiveTypeEntry{Type: cli_service.TTypeId_BIGINT_TYPE},
					}}},
					Position: 1,
				}}},
				ResultFormat: cli_service.TSparkRowSetTypePtr(cli_service.TSparkRowSetType_URL_BASED_SET),
				ArrowSchema:  schemaBytes,
			},
			ResultSet: &cli_service.TFetchResultsResp{
				Status:      success,
				HasMoreRows: &hasMoreRows,
				Results: &cli_service.TRowSet{
					ResultLinks: []*cli_service.TSparkArrowResultLink{{
						FileLink:   fileURL,
						ExpiryTime: time.Now().Add(time.Hour).Unix(),
						RowCount:   rowCount,
					}},
				},
			},
			CloseOperation: &cli_service.TCloseOperationResp{Status: success},
		},
	}
}
