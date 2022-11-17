package dbsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/stretchr/testify/require"
)

// func TestQueryContextDirectResultsSuccess(t *testing.T) {
// 	cfg := config.WithDefaults()
// 	// set up server

// 	var executeStatement = func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
// 		return &cli_service.TExecuteStatementResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 			},
// 			OperationHandle: &cli_service.TOperationHandle{
// 				OperationId: &cli_service.THandleIdentifier{
// 					GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 1, 2, 3, 4, 4, 223, 34, 54},
// 					Secret: []byte("b"),
// 				},
// 			},
// 			DirectResults: &cli_service.TSparkDirectResults{
// 				OperationStatus: &cli_service.TGetOperationStatusResp{
// 					Status: &cli_service.TStatus{
// 						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 					},
// 					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_FINISHED_STATE),
// 				},
// 				ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
// 					Status: &cli_service.TStatus{
// 						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 					},
// 				},
// 				ResultSet: &cli_service.TFetchResultsResp{
// 					Status: &cli_service.TStatus{
// 						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 					},
// 				},
// 			},
// 		}, nil
// 	}

// 	cancelOperationCalled := false
// 	var cancelOperation = func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
// 		cancelOperationCalled = true
// 		return &cli_service.TCancelOperationResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 			},
// 		}, nil
// 	}
// 	getOperationStatusCalled := false
// 	var getOperationStatus = func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
// 		getOperationStatusCalled = true
// 		return &cli_service.TGetOperationStatusResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
// 			},
// 		}, nil
// 	}

// 	ts := initThriftTestServer(&client.TestClient{
// 		FnExecuteStatement:   executeStatement,
// 		FnGetOperationStatus: getOperationStatus,
// 		FnCancelOperation:    cancelOperation,
// 	})

// 	defer ts.Close()

// 	r, err := url.Parse(ts.URL)
// 	assert.NoError(t, err)
// 	cfg.Protocol = "http"
// 	cfg.Host = "localhost"
// 	port, err := strconv.Atoi(r.Port())
// 	assert.NoError(t, err)
// 	cfg.Port = port

// 	connector := &connector{cfg}

// 	db := sql.OpenDB(connector)

// 	rows, err := db.QueryContext(context.Background(), `select * from dummy`)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, rows)
// 	assert.False(t, cancelOperationCalled)
// 	assert.False(t, getOperationStatusCalled)
// }
// func TestQueryContextTimeouts(t *testing.T) {
// 	cfg := config.WithDefaults()
// 	// set up server

// 	var executeStatement = func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
// 		return &cli_service.TExecuteStatementResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 			},
// 			OperationHandle: &cli_service.TOperationHandle{
// 				OperationId: &cli_service.THandleIdentifier{
// 					GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 3, 2, 3, 4, 4, 223, 34, 54},
// 					Secret: []byte("b"),
// 				},
// 			},
// 			DirectResults: &cli_service.TSparkDirectResults{
// 				OperationStatus: &cli_service.TGetOperationStatusResp{
// 					Status: &cli_service.TStatus{
// 						StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
// 					},
// 					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_PENDING_STATE),
// 				},
// 				ResultSetMetadata: nil,
// 				ResultSet:         nil,
// 			},
// 		}, nil
// 	}

// 	cancelOperationCalled := false
// 	var cancelOperation = func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
// 		cancelOperationCalled = true
// 		return &cli_service.TCancelOperationResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 			},
// 		}, nil
// 	}
// 	getOperationStatusCalled := false
// 	var getOperationStatus = func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
// 		// this is very important. If the context gets canceled while getting the operation we should still call cancel
// 		time.Sleep(200 * time.Millisecond)
// 		getOperationStatusCalled = true
// 		return &cli_service.TGetOperationStatusResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
// 			},
// 		}, nil
// 	}

// 	ts := initThriftTestServer(&client.TestClient{
// 		FnExecuteStatement:   executeStatement,
// 		FnGetOperationStatus: getOperationStatus,
// 		FnCancelOperation:    cancelOperation,
// 	})

// 	defer ts.Close()

// 	r, err := url.Parse(ts.URL)
// 	assert.NoError(t, err)
// 	cfg.Protocol = "http"
// 	cfg.Host = "localhost"
// 	port, err := strconv.Atoi(r.Port())
// 	assert.NoError(t, err)
// 	cfg.Port = port
// 	cfg.PollInterval = 200 * time.Millisecond

// 	connector := &connector{cfg}

// 	db := sql.OpenDB(connector)

// 	ctx, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
// 	defer cancel1()

// 	rows, err := db.QueryContext(ctx, `select * from dummy`)
// 	assert.ErrorIs(t, err, context.DeadlineExceeded)
// 	_, ok := err.(stackTracer)
// 	assert.True(t, ok)
// 	_, ok1 := err.(causer)
// 	assert.True(t, ok1)

// 	fmt.Printf("%+v", err)
// 	time.Sleep(40 * time.Millisecond)
// 	assert.Nil(t, rows)
// 	assert.True(t, cancelOperationCalled)
// 	assert.True(t, getOperationStatusCalled)
// }

// func TestQueryContextDirectResultsError(t *testing.T) {
// 	cfg := config.WithDefaults()
// 	// set up server

// 	var executeStatement = func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
// 		return &cli_service.TExecuteStatementResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 			},
// 			OperationHandle: &cli_service.TOperationHandle{
// 				OperationId: &cli_service.THandleIdentifier{
// 					GUID:   []byte{1, 2, 3, 4, 2, 23, 4, 2, 31, 122, 3, 4, 4, 223, 34, 54},
// 					Secret: []byte("b"),
// 				},
// 			},
// 			DirectResults: &cli_service.TSparkDirectResults{
// 				OperationStatus: &cli_service.TGetOperationStatusResp{
// 					Status: &cli_service.TStatus{
// 						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 					},
// 					OperationState: cli_service.TOperationStatePtr(cli_service.TOperationState_ERROR_STATE),
// 					ErrorMessage:   strPtr("error message"),
// 					DisplayMessage: strPtr("display message"),
// 				},
// 				ResultSetMetadata: &cli_service.TGetResultSetMetadataResp{
// 					Status: &cli_service.TStatus{
// 						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 					},
// 				},
// 				ResultSet: &cli_service.TFetchResultsResp{
// 					Status: &cli_service.TStatus{
// 						StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 					},
// 				},
// 			},
// 		}, nil
// 	}

// 	cancelOperationCalled := false
// 	var cancelOperation = func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
// 		cancelOperationCalled = true
// 		return &cli_service.TCancelOperationResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
// 			},
// 		}, nil
// 	}
// 	getOperationStatusCalled := false
// 	var getOperationStatus = func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
// 		getOperationStatusCalled = true
// 		return &cli_service.TGetOperationStatusResp{
// 			Status: &cli_service.TStatus{
// 				StatusCode: cli_service.TStatusCode_STILL_EXECUTING_STATUS,
// 			},
// 		}, nil
// 	}

// 	ts := initThriftTestServer(&client.TestClient{
// 		FnExecuteStatement:   executeStatement,
// 		FnGetOperationStatus: getOperationStatus,
// 		FnCancelOperation:    cancelOperation,
// 	})

// 	defer ts.Close()

// 	r, err := url.Parse(ts.URL)
// 	assert.NoError(t, err)
// 	cfg.Protocol = "http"
// 	cfg.Host = "localhost"
// 	port, err := strconv.Atoi(r.Port())
// 	assert.NoError(t, err)
// 	cfg.Port = port

// 	connector := &connector{cfg}

// 	db := sql.OpenDB(connector)

// 	rows, err := db.QueryContext(context.Background(), `select * from dummy`)
// 	assert.ErrorContains(t, err, "display message")
// 	assert.Nil(t, rows)
// 	assert.False(t, cancelOperationCalled)
// 	assert.False(t, getOperationStatusCalled)
// }

func TestWorkflowExample(t *testing.T) {

	// these are global variables that get changed depending on the the expected response
	var (
		catalog = "hive_metastore"
		schema  = "default"

		openSessionResp  cli_service.TOpenSessionResp
		openSessionError error

		closeSessionResp  cli_service.TCloseSessionResp
		closeSessionError error

		executeStatementResp  cli_service.TExecuteStatementResp
		executeStatementError error

		cancelOperationResp  cli_service.TCancelOperationResp
		cancelOperationError error

		closeOperationResp  cli_service.TCloseOperationResp
		closeOperationError error

		fetchResultsResp  cli_service.TFetchResultsResp
		fetchResultsError error

		getResultSetMetadataResp  cli_service.TGetResultSetMetadataResp
		getResultSetMetadataError error

		getOperationStatusResp  cli_service.TGetOperationStatusResp
		getOperationStatusError error
	)
	// load basic responses
	loadTestData(t, "OpenSessionSuccess.json", &openSessionResp)
	loadTestData(t, "CloseSessionSuccess.json", &closeSessionResp)
	loadTestData(t, "CloseOperationSuccess.json", &closeOperationResp)

	ts := initThriftTestServer(&client.TestClient{
		FnOpenSession: func(ctx context.Context, req *cli_service.TOpenSessionReq) (*cli_service.TOpenSessionResp, error) {
			return &openSessionResp, openSessionError
		},
		FnCloseSession: func(ctx context.Context, req *cli_service.TCloseSessionReq) (*cli_service.TCloseSessionResp, error) {
			return &closeSessionResp, closeSessionError
		},
		FnExecuteStatement: func(ctx context.Context, req *cli_service.TExecuteStatementReq) (*cli_service.TExecuteStatementResp, error) {
			return &executeStatementResp, executeStatementError
		},
		FnGetOperationStatus: func(ctx context.Context, req *cli_service.TGetOperationStatusReq) (*cli_service.TGetOperationStatusResp, error) {
			return &getOperationStatusResp, getOperationStatusError
		},
		FnCloseOperation: func(ctx context.Context, req *cli_service.TCloseOperationReq) (*cli_service.TCloseOperationResp, error) {
			return &closeOperationResp, closeOperationError
		},
		FnCancelOperation: func(ctx context.Context, req *cli_service.TCancelOperationReq) (*cli_service.TCancelOperationResp, error) {
			return &cancelOperationResp, cancelOperationError
		},
		FnFetchResults: func(ctx context.Context, req *cli_service.TFetchResultsReq) (*cli_service.TFetchResultsResp, error) {
			return &fetchResultsResp, fetchResultsError
		},
		FnGetResultSetMetadata: func(ctx context.Context, req *cli_service.TGetResultSetMetadataReq) (*cli_service.TGetResultSetMetadataResp, error) {
			return &getResultSetMetadataResp, getResultSetMetadataError
		},
	})

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
	executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement1.json", &executeStatementResp)
	if err := db.PingContext(ctx1); err != nil {
		require.NoError(t, err)
	}
	executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement4.json", &executeStatementResp)
	if _, err := db.ExecContext(ogCtx, `CREATE TABLE IF NOT EXISTS diamonds USING CSV LOCATION '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv' options (header = true, inferSchema = true)`); err != nil {
		require.NoError(t, err)
	}
	executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement5.json", &executeStatementResp)
	var max float64
	if err := db.QueryRowContext(ogCtx, `select max(carat) from diamonds`).Scan(&max); err != nil {
		require.NoError(t, err)
	} else {
		require.Equal(t, 5.01, max)
	}
	executeStatementResp = cli_service.TExecuteStatementResp{}
	loadTestData(t, "ExecuteStatement7.json", &executeStatementResp)
	fetchResultsResp = cli_service.TFetchResultsResp{}
	loadTestData(t, "FetchRows8.json", &fetchResultsResp)

	ctx2, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()

	if rows, err := db.QueryContext(ctx2, "select * from diamonds limit 19"); err != nil {
		t.Error(err)
	} else {
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

		cols, err := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		types, err := rows.ColumnTypes()
		if err != nil {
			t.Error(err)
		}
		for i, c := range cols {
			fmt.Printf("column %d is %s and has type %v\n", i, c, types[i].DatabaseTypeName())
		}
		res := []row{}
		for rows.Next() {
			// After row 10 this will cause one fetch call, as 10 rows (maxRows config) will come from the first execute statement call.
			r := row{}
			if err := rows.Scan(&r._c0, &r.carat, &r.cut, &r.color, &r.clarity, &r.depth, &r.table, &r.price, &r.x, &r.y, &r.z); err != nil {
				t.Error(err)
			}
			res = append(res, r)
		}
		for _, r := range res {
			fmt.Printf("%+v\n", r)
		}
	}

	// // timezones are also supported
	// var curTimestamp time.Time
	// var curDate time.Time
	// var curTimezone string

	// if err := db.QueryRowContext(ogCtx, `select current_date(), current_timestamp(), current_timezone()`).Scan(&curDate, &curTimestamp, &curTimezone); err != nil {
	// 	panic(err)
	// } else {
	// 	// this will print now at timezone America/Sao_Paulo is: 2022-11-16 20:25:15.282 -0300 -03
	// 	fmt.Printf("current timestamp at timezone %s is: %s\n", curTimezone, curTimestamp)
	// 	fmt.Printf("current date at timezone %s is: %s\n", curTimezone, curDate)
	// }

	// // we can also create and query a table with complex data types
	// if _, err := db.ExecContext(
	// 	ogCtx,
	// 	`create table if not exists array_map_struct (
	// 		array_col array < int >,
	// 		map_col map < string, int >,
	// 		struct_col struct < string_field string, array_field array < int > >)`); err != nil {
	// 	panic(err)
	// }
	// var numRows int
	// if err := db.QueryRowContext(ogCtx, `select count(*) from array_map_struct`).Scan(&numRows); err != nil {
	// 	panic(err)
	// } else {
	// 	fmt.Printf("table has %d rows\n", numRows)
	// }
	// if numRows == 0 {

	// 	if res, err := db.ExecContext(
	// 		context.Background(),
	// 		`insert into table array_map_struct (array_col, map_col, struct_col) VALUES (
	// 			array(1, 2, 3),
	// 			map('key1', 1),
	// 			struct('string_val', array(4, 5, 6)))`); err != nil {
	// 		panic(err)
	// 	} else {
	// 		i, err1 := res.RowsAffected()
	// 		if err1 != nil {
	// 			panic(err1)
	// 		}
	// 		fmt.Printf("inserted %d rows", i)
	// 	}
	// }

	// if rows, err := db.QueryContext(ogCtx, "select * from array_map_struct"); err != nil {
	// 	panic(err)
	// } else {
	// 	// complex data types are returned as string
	// 	type row struct {
	// 		arrayVal  string
	// 		mapVal    string
	// 		structVal string
	// 	}
	// 	res := []row{}
	// 	cols, err := rows.Columns()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	types, err := rows.ColumnTypes()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	for i, c := range cols {
	// 		fmt.Printf("column %d is %s and has type %v\n", i, c, types[i].DatabaseTypeName())
	// 	}

	// 	for rows.Next() {
	// 		r := row{}
	// 		if err := rows.Scan(&r.arrayVal, &r.mapVal, &r.structVal); err != nil {
	// 			panic(err)
	// 		}
	// 		res = append(res, r)
	// 	}
	// 	// below will print
	// 	//{arrayVal:[1,2,3] mapVal:{"key1":1} structVal:{"string_field":"string_val","array_field":[4,5,6]}}
	// 	for _, r := range res {
	// 		fmt.Printf("%+v\n", r)
	// 	}
	// }
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
