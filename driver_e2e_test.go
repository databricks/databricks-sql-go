package dbsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowExample(t *testing.T) {

	// these are global variables that get changed depending on the the expected response
	var (
		catalog = "hive_metastore"
		schema  = "default"
	)
	state := &callState{}
	// load basic responses
	loadTestData(t, "OpenSessionSuccess.json", &state.openSessionResp)
	loadTestData(t, "CloseSessionSuccess.json", &state.closeSessionResp)
	loadTestData(t, "CloseOperationSuccess.json", &state.closeOperationResp)

	ts := getServer(state)

	defer ts.Close()

	r, err := url.Parse(ts.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(r.Port())
	require.NoError(t, err)

	connector, err := NewConnector(
		WithServerHostname("localhost"),
		WithPort(port),
		WithHTTPPath(""),
		WithAccessToken(""),
		WithSessionParams(map[string]string{"timezone": "America/Sao_Paulo", "ansi_mode": "true"}),
		WithUserAgentEntry("workflow-example"),
		WithInitialNamespace(catalog, schema),
		WithTimeout(time.Minute),
		WithMaxRows(10),
	)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := driverctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()

	// set up basic responses for ping
	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement1.json", &state.executeStatementResp)
	if err := db.PingContext(ctx1); err != nil {
		require.NoError(t, err)
	}
	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement4.json", &state.executeStatementResp)
	if _, err := db.ExecContext(ogCtx, `CREATE TABLE IF NOT EXISTS diamonds USING CSV LOCATION '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv' options (header = true, inferSchema = true)`); err != nil {
		require.NoError(t, err)
	}
	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement5.json", &state.executeStatementResp)
	var max float64
	if err := db.QueryRowContext(ogCtx, `select max(carat) from diamonds`).Scan(&max); err != nil {
		require.NoError(t, err)
	} else {
		require.Equal(t, 5.01, max)
	}
	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement7.json", &state.executeStatementResp)
	state.fetchResultsResp = cli_service.TFetchResultsResp{}
	loadTestData(t, "FetchResults8.json", &state.fetchResultsResp)

	ctx2, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()

	if rows, err := db.QueryContext(ctx2, "select * from diamonds limit 19"); err != nil {
		require.NoError(t, err)
	} else {

		expectedColumnNames := []string{"_c0", "carat", "cut", "color", "clarity", "depth", "table", "price", "x", "y", "z"}
		expectedDatabaseType := []string{"INT", "DOUBLE", "STRING", "STRING", "STRING", "DOUBLE", "DOUBLE", "INT", "DOUBLE", "DOUBLE", "DOUBLE"}
		expectedNullable := []bool{false, false, false, false, false, false, false, false, false, false, false}

		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, expectedColumnNames, cols)

		types, err := rows.ColumnTypes()
		require.NoError(t, err)

		for i, v := range types {
			assert.Equal(t, expectedColumnNames[i], v.Name())
			assert.Equal(t, expectedDatabaseType[i], v.DatabaseTypeName())
			nullable, ok := v.Nullable()
			assert.False(t, ok)
			assert.Equal(t, expectedNullable[i], nullable)
		}

		type row struct {
			_c0     int
			carat   float64
			cut     string
			color   string
			clarity string
			depth   sql.NullFloat64
			table   sql.NullInt64
			price   int
			x       float64
			y       float64
			z       float64
		}
		expectedRows := []row{
			{_c0: 1, carat: 0.23, cut: "Ideal", color: "E", clarity: "SI2", depth: sql.NullFloat64{Float64: 61.5, Valid: true}, table: sql.NullInt64{Int64: 55, Valid: true}, price: 326, x: 3.95, y: 3.98, z: 2.43},
			{_c0: 2, carat: 0.21, cut: "Premium", color: "E", clarity: "SI1", depth: sql.NullFloat64{Float64: 59.8, Valid: true}, table: sql.NullInt64{Int64: 61, Valid: true}, price: 326, x: 3.89, y: 3.84, z: 2.31},
			{_c0: 3, carat: 0.23, cut: "Good", color: "E", clarity: "VS1", depth: sql.NullFloat64{Float64: 56.9, Valid: true}, table: sql.NullInt64{Int64: 65, Valid: true}, price: 327, x: 4.05, y: 4.07, z: 2.31},
			{_c0: 4, carat: 0.29, cut: "Premium", color: "I", clarity: "VS2", depth: sql.NullFloat64{Float64: 62.4, Valid: true}, table: sql.NullInt64{Int64: 58, Valid: true}, price: 334, x: 4.2, y: 4.23, z: 2.63},
			{_c0: 5, carat: 0.31, cut: "Good", color: "J", clarity: "SI2", depth: sql.NullFloat64{Float64: 63.3, Valid: true}, table: sql.NullInt64{Int64: 58, Valid: true}, price: 335, x: 4.34, y: 4.35, z: 2.75},
			{_c0: 6, carat: 0.24, cut: "Very Good", color: "J", clarity: "VVS2", depth: sql.NullFloat64{Float64: 62.8, Valid: true}, table: sql.NullInt64{Int64: 57, Valid: true}, price: 336, x: 3.94, y: 3.96, z: 2.48},
			{_c0: 7, carat: 0.24, cut: "Very Good", color: "I", clarity: "VVS1", depth: sql.NullFloat64{Float64: 62.3, Valid: true}, table: sql.NullInt64{Int64: 57, Valid: true}, price: 336, x: 3.95, y: 3.98, z: 2.47},
			{_c0: 8, carat: 0.26, cut: "Very Good", color: "H", clarity: "SI1", depth: sql.NullFloat64{Float64: 61.9, Valid: true}, table: sql.NullInt64{Int64: 55, Valid: true}, price: 337, x: 4.07, y: 4.11, z: 2.53},
			{_c0: 9, carat: 0.22, cut: "Fair", color: "E", clarity: "VS2", depth: sql.NullFloat64{Float64: 65.1, Valid: true}, table: sql.NullInt64{Int64: 61, Valid: true}, price: 337, x: 3.87, y: 3.78, z: 2.49},
			{_c0: 10, carat: 0.23, cut: "Very Good", color: "H", clarity: "VS1", depth: sql.NullFloat64{Float64: 59.4, Valid: true}, table: sql.NullInt64{Int64: 61, Valid: true}, price: 338, x: 4, y: 4.05, z: 2.39},
			{_c0: 11, carat: 0.3, cut: "Good", color: "J", clarity: "SI1", depth: sql.NullFloat64{Float64: 64, Valid: true}, table: sql.NullInt64{Int64: 55, Valid: true}, price: 339, x: 4.25, y: 4.28, z: 2.73},
			{_c0: 12, carat: 0.23, cut: "Ideal", color: "J", clarity: "VS1", depth: sql.NullFloat64{Float64: 62.8, Valid: true}, table: sql.NullInt64{Int64: 56, Valid: true}, price: 340, x: 3.93, y: 3.9, z: 2.46},
			{_c0: 13, carat: 0.22, cut: "Premium", color: "F", clarity: "SI1", depth: sql.NullFloat64{Float64: 60.4, Valid: true}, table: sql.NullInt64{Int64: 61, Valid: true}, price: 342, x: 3.88, y: 3.84, z: 2.33},
			{_c0: 14, carat: 0.31, cut: "Ideal", color: "J", clarity: "SI2", depth: sql.NullFloat64{Float64: 62.2, Valid: true}, table: sql.NullInt64{Int64: 54, Valid: true}, price: 344, x: 4.35, y: 4.37, z: 2.71},
			{_c0: 15, carat: 0.2, cut: "Premium", color: "E", clarity: "SI2", depth: sql.NullFloat64{Float64: 60.2, Valid: true}, table: sql.NullInt64{Int64: 62, Valid: true}, price: 345, x: 3.79, y: 3.75, z: 2.27},
			{_c0: 16, carat: 0.32, cut: "Premium", color: "E", clarity: "I1", depth: sql.NullFloat64{Float64: 60.9, Valid: true}, table: sql.NullInt64{Int64: 58, Valid: true}, price: 345, x: 4.38, y: 4.42, z: 2.68},
			{_c0: 17, carat: 0.3, cut: "Ideal", color: "I", clarity: "SI2", depth: sql.NullFloat64{Float64: 62, Valid: true}, table: sql.NullInt64{Int64: 54, Valid: true}, price: 348, x: 4.31, y: 4.34, z: 2.68},
			{_c0: 18, carat: 0.3, cut: "Good", color: "J", clarity: "SI1", depth: sql.NullFloat64{Float64: 63.4, Valid: true}, table: sql.NullInt64{Int64: 54, Valid: true}, price: 351, x: 4.23, y: 4.29, z: 2.7},
			{_c0: 19, carat: 0.3, cut: "Good", color: "J", clarity: "SI1", depth: sql.NullFloat64{Float64: 63.8, Valid: true}, table: sql.NullInt64{Int64: 56, Valid: true}, price: 351, x: 4.23, y: 4.26, z: 2.71},
		}
		rowIdx := 0
		for rows.Next() {
			// After row 10 this will cause one fetch call, as 10 rows (maxRows config) will come from the first execute statement call.
			r := row{}
			if err := rows.Scan(&r._c0, &r.carat, &r.cut, &r.color, &r.clarity, &r.depth, &r.table, &r.price, &r.x, &r.y, &r.z); err != nil {
				require.NoError(t, err)
			} else {
				assert.Equal(t, expectedRows[rowIdx], r)
			}
			rowIdx++
		}
	}

	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement10.json", &state.executeStatementResp)

	// // timezones are also supported
	var curTimestamp time.Time
	var curDate time.Time
	var curTimezone string
	loc, err := time.LoadLocation("America/Sao_Paulo")
	require.NoError(t, err)

	if err := db.QueryRowContext(ogCtx, `select current_date(), current_timestamp(), current_timezone()`).Scan(&curDate, &curTimestamp, &curTimezone); err != nil {
		require.NoError(t, err)
	} else {
		assert.Equal(t, "America/Sao_Paulo", curTimezone)
		assert.Equal(t, time.Date(2022, time.Month(11), 17, 14, 33, 15, 771000000, loc), curTimestamp)
		assert.Equal(t, time.Date(2022, time.Month(11), 17, 0, 0, 0, 0, loc), curDate)
	}

	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement14.json", &state.executeStatementResp)
	state.getOperationStatusResp = cli_service.TGetOperationStatusResp{}
	loadTestData(t, "GetOperationStatusFinished.json", &state.getOperationStatusResp)
	// // we can also create and query a table with complex data types
	if _, err := db.ExecContext(
		ogCtx,
		`create table if not exists array_map_struct (
			array_col array < int >,
			map_col map < string, int >,
			struct_col struct < string_field string, array_field array < int > >)`); err != nil {
		require.NoError(t, err)
	}

	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement19.json", &state.executeStatementResp)
	if res, err := db.ExecContext(
		context.Background(),
		`insert into table array_map_struct (array_col, map_col, struct_col) VALUES (
				array(1, 2, 3),
				map('key1', 1),
				struct('string_val', array(4, 5, 6)))`); err != nil {
		require.NoError(t, err)
	} else {
		iRows, err := res.RowsAffected()
		if err != nil {
			require.NoError(t, err)
		}
		assert.Equal(t, int64(1), iRows)
	}

	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement20.json", &state.executeStatementResp)
	if rows, err := db.QueryContext(ogCtx, "select * from array_map_struct"); err != nil {
		require.NoError(t, err)
	} else {
		// complex data types are returned as string
		type row struct {
			arrayVal  string
			mapVal    string
			structVal string
		}
		expectedColumnNames := []string{"array_col", "map_col", "struct_col"}
		expectedDatabaseType := []string{"ARRAY", "MAP", "STRUCT"}
		expectedNullable := []bool{false, false, false}

		cols, err := rows.Columns()
		require.NoError(t, err)
		require.Equal(t, expectedColumnNames, cols)

		types, err := rows.ColumnTypes()
		require.NoError(t, err)

		for i, v := range types {
			assert.Equal(t, expectedColumnNames[i], v.Name())
			assert.Equal(t, expectedDatabaseType[i], v.DatabaseTypeName())
			nullable, ok := v.Nullable()
			assert.False(t, ok)
			assert.Equal(t, expectedNullable[i], nullable)
		}

		expectedRows := []row{{arrayVal: "[1,2,3]", mapVal: "{\"key1\":1}", structVal: "{\"string_field\":\"string_val\",\"array_field\":[4,5,6]}"}}
		rowIdx := 0
		for rows.Next() {
			r := row{}
			if err := rows.Scan(&r.arrayVal, &r.mapVal, &r.structVal); err != nil {
				require.NoError(t, err)
			} else {
				assert.Equal(t, expectedRows[rowIdx], r)
			}
			rowIdx++
		}
	}
}

func TestContextTimeoutExample(t *testing.T) {

	_ = logger.SetLogLevel("debug")
	state := &callState{}
	// load basic responses
	loadTestData(t, "OpenSessionSuccess.json", &state.openSessionResp)
	loadTestData(t, "CloseSessionSuccess.json", &state.closeSessionResp)
	loadTestData(t, "CloseOperationSuccess.json", &state.closeOperationResp)

	ts := getServer(state)

	defer ts.Close()

	r, err := url.Parse(ts.URL)
	require.NoError(t, err)

	db, err := sql.Open("databricks", fmt.Sprintf("http://localhost:%s", r.Port()))
	require.NoError(t, err)
	defer db.Close()

	ogCtx := driverctx.NewContextWithCorrelationId(context.Background(), "context-timeout-example")

	state.executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement21.json", &state.executeStatementResp)
	state.getOperationStatusResp = cli_service.TGetOperationStatusResp{}
	loadTestData(t, "GetOperationStatusRunning.json", &state.getOperationStatusResp)
	loadTestData(t, "CancelOperationSuccess.json", &state.cancelOperationResp)

	ctx1, cancel := context.WithTimeout(ogCtx, 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx1, `SELECT id FROM RANGE(100000000) ORDER BY RANDOM() + 2 asc`)
	require.ErrorContains(t, err, context.DeadlineExceeded.Error())
	require.Nil(t, rows)
	_, ok := err.(causer)
	assert.True(t, ok)
	_, ok = err.(stackTracer)
	assert.True(t, ok)
	assert.Equal(t, 1, state.executeStatementCalls)
	assert.GreaterOrEqual(t, state.getOperationStatusCalls, 1)
	assert.Equal(t, 1, state.cancelOperationCalls)

}

func strPtr(s string) *string {
	return &s
}

func loadTestData(t *testing.T, name string, v any) {
	if f, err := os.ReadFile(fmt.Sprintf("testdata/%s", name)); err != nil {
		t.Errorf("could not read data from: %s", name)
	} else {
		if err := json.Unmarshal(f, v); err != nil {
			t.Errorf("could not load data from: %s", name)
		}
	}
}

type callState struct {
	openSessionCalls int
	openSessionResp  cli_service.TOpenSessionResp
	openSessionError error

	closeSessionCalls int
	closeSessionResp  cli_service.TCloseSessionResp
	closeSessionError error

	executeStatementCalls int
	executeStatementSleep time.Duration
	executeStatementResp  cli_service.TExecuteStatementResp
	executeStatementError error

	cancelOperationCalls int
	cancelOperationResp  cli_service.TCancelOperationResp
	cancelOperationError error

	closeOperationCalls int
	closeOperationResp  cli_service.TCloseOperationResp
	closeOperationError error

	fetchResultsCalls int
	fetchResultsResp  cli_service.TFetchResultsResp
	fetchResultsError error

	getResultSetMetadataCalls int
	getResultSetMetadataResp  cli_service.TGetResultSetMetadataResp
	getResultSetMetadataError error

	getOperationStatusCalls int
	getOperationStatusResp  cli_service.TGetOperationStatusResp
	getOperationStatusError error
}

func getServer(state *callState) *httptest.Server {
	return initThriftTestServer(&client.TestClient{
		FnOpenSession: func(ctx context.Context, req *cli_service.TOpenSessionReq) (*cli_service.TOpenSessionResp, error) {
			state.openSessionCalls++
			return &state.openSessionResp, state.openSessionError
		},
		FnCloseSession: func(ctx context.Context, req *cli_service.TCloseSessionReq) (*cli_service.TCloseSessionResp, error) {
			state.closeSessionCalls++
			return &state.closeSessionResp, state.closeSessionError
		},
		FnExecuteStatement: func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			state.executeStatementCalls++
			if state.executeStatementSleep != 0 {
				time.Sleep(state.executeStatementSleep)
			}
			return &state.executeStatementResp, state.executeStatementError
		},
		FnGetOperationStatus: func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			state.getOperationStatusCalls++
			return &state.getOperationStatusResp, state.getOperationStatusError
		},
		FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			state.closeOperationCalls++
			return &state.closeOperationResp, state.closeOperationError
		},
		FnCancelOperation: func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
			state.cancelOperationCalls++
			return &state.cancelOperationResp, state.cancelOperationError
		},
		FnFetchResults: func(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			state.fetchResultsCalls++
			return &state.fetchResultsResp, state.fetchResultsError
		},
		FnGetResultSetMetadata: func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			state.getResultSetMetadataCalls++
			return &state.getResultSetMetadataResp, state.getResultSetMetadataError
		},
	})
}
