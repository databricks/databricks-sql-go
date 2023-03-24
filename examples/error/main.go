package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/joho/godotenv"
)

// NOTE: these tests print errors to the console and are for demonstration purposes, not unit test coverage
func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}
	// accessTokenInvalid()
	// accessTokenMissing()
	// serverHostnameInvalid()
	// httpPathInvalid()
	// portInvalid()
	// userDoesNotHavePermission()
	// tableIdentifierIsInvalid()
	// tableNotFound()
	invalidSqlCommand()
	// connectorMissingServerHostname()
	// connectorMissingAccessToken()
	// invalidDsnFormat()
	// dsnTokenEmpty()
	// dsnTokenMaxRowsInvalid()
	// dsnTokenTimeoutInvalid()
	// dsnTokenInvalidNoSuchHost()
}

// Returns:
// databricks: request error: error connecting: host=e2-dogfood.staging.cloud.databricks.com port=443, httpPath=/sql/1.0/warehouses/77636a5b5b0e395a: open session request error: Post "https://e2-dogfood.staging.cloud.databricks.com:443/sql/1.0/warehouses/77636a5b5b0e395a": Invalid access token.
// TODO: error should describe invalid access token
func accessTokenInvalid() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")+"invalid"),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: request error: error connecting: host=e2-dogfood.staging.cloud.databricks.com port=443, httpPath=/sql/1.0/warehouses/77636a5b5b0e395a: open session request error: Post "https://e2-dogfood.staging.cloud.databricks.com:443/sql/1.0/warehouses/77636a5b5b0e395a": 401 Unauthorized
// TODO: error should describe missing access token
func accessTokenMissing() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}

// Returns:
// ERR request failed (x5)
// databricks: request error: error connecting: host=e2-dogfood-invalid.staging.cloud.databricks.com port=443, httpPath=/sql/1.0/warehouses/77636a5b5b0e395a: open session request error: Post "https://e2-dogfood-invalid.staging.cloud.databricks.com:443/sql/1.0/warehouses/77636a5b5b0e395a": dial tcp: lookup e2-dogfood-invalid.staging.cloud.databricks.com: no such host
// TODO: what is dial tcp?
func serverHostnameInvalid() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname("e2-dogfood-invalid.staging.cloud.databricks.com"),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: request error: error connecting: host=e2-dogfood.staging.cloud.databricks.com port=443, httpPath=/sql/1.0/warehouses/77636a5b5b0e395ainvalid: open session request error: Post "https://e2-dogfood.staging.cloud.databricks.com:443/sql/1.0/warehouses/77636a5b5b0e395ainvalid": MALFORMED_REQUEST: Path /sql/1.0/warehouses/77636a5b5b0e395ainvalid must match pattern /sql/1.0/endpoints/<endpointId> or /sql/1.0/warehouses/<warehouseId>
func httpPathInvalid() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")+"invalid"),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}

// Returns:
// ERR request failed (x3)
// databricks: request error: error connecting: host=e2-dogfood.staging.cloud.databricks.com port=444, httpPath=/sql/1.0/warehouses/77636a5b5b0e395a: open session request error: Post "https://e2-dogfood.staging.cloud.databricks.com:444/sql/1.0/warehouses/77636a5b5b0e395a": net/http: TLS handshake timeout
func portInvalid() {
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(444),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 60*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: execution error: failed to execute query: [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
// User does not have permission SELECT on any file.
func userDoesNotHavePermission() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	// ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	// defer cancel()
	// if err := db.PingContext(ctx1); err != nil {
	// 	log.Fatal(err)
	// }

	// Exec
	if _, err := db.ExecContext(ogCtx, `CREATE TABLE IF NOT EXISTS diamonds USING CSV LOCATION '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv' options (header = true, inferSchema = true)`); err != nil {
		var queryFailureError dbsqlerr.DBExecutionError
		if errors.As(err, &queryFailureError) {
			fmt.Println("ExecutionError for query", queryFailureError.QueryId(), ".", queryFailureError)
		}
	}
}

// Returns:
// ERR databricks: query state: ERROR_STATE connId=01eda22e-e9dc-1d8f-8160-de421eaf201e corrId=workflow-example queryId=01eda22e-ea07-15bb-9920-2ed8e2e8cf1e
// ERR org.apache.hive.service.cli.HiveSQLException: Error running query: [INVALID_IDENTIFIER] org.apache.spark.sql.catalyst.parser.ParseException:
// [INVALID_IDENTIFIER] The identifier diamonds-invalid is invalid. Please, consider quoting it with back-quotes as `diamonds-invalid`.(line 1, pos 31)
//
// == SQL ==
// select max(carat) from diamonds-invalid
// -------------------------------^^^
// < spark stack trace >
// ERR databricks: failed to run query error="\n[INVALID_IDENTIFIER] The identifier diamonds-invalid is invalid. Please, consider quoting it with back-quotes as `diamonds-invalid`.(line 1, pos 31)\n\n== SQL ==\nselect max(carat) from diamonds-invalid\n-------------------------------^^^\n" connId=01eda22e-e9dc-1d8f-8160-de421eaf201e corrId=workflow-example queryId=01eda22e-ea07-15bb-9920-2ed8e2e8cf1e
// databricks: execution error: failed to execute query:
// [INVALID_IDENTIFIER] The identifier diamonds-invalid is invalid. Please, consider quoting it with back-quotes as `diamonds-invalid`.(line 1, pos 31)
//
// == SQL ==
// select max(carat) from diamonds-invalid
// -------------------------------^^^
func tableIdentifierIsInvalid() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}

	// Query row
	var max float64
	if err := db.QueryRowContext(ogCtx, "select max(carat) from diamonds-invalid").Scan(&max); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("max carat in dataset is: %f\n", max)
	}
}

// Returns:
// ERR databricks: query state: ERROR_STATE connId=01eda22f-fde5-1b4c-904f-1f763f17f056 corrId=workflow-example queryId=01eda22f-fe0e-196c-a431-52e3d4f33fc4
// ERR org.apache.hive.service.cli.HiveSQLException: Error running query: [TABLE_OR_VIEW_NOT_FOUND] org.apache.spark.sql.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `diamonds-invalid` cannot be found. Verify the spelling and correctness of the schema and catalog.
// If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
// To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS.; line 1 pos 23
// < spark stack trace >
// < spark stack trace again >
// ERR databricks: execution error: failed to execute query: [TABLE_OR_VIEW_NOT_FOUND] The table or view `diamonds-invalid` cannot be found. Verify the spelling and correctness of the schema and catalog.
// query failure error: failed to execute query: [TABLE_OR_VIEW_NOT_FOUND] The table or view `diamonds-invalid` cannot be found. Verify the spelling and correctness of the schema and catalog.
// If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
// To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS.; line 1 pos 23
func tableNotFound() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	// ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	// defer cancel()
	// if err := db.PingContext(ctx1); err != nil {
	// 	log.Fatal(err)
	// }

	// Query row
	var max float64
	if err := db.QueryRowContext(ogCtx, "select max(carat) from `diamonds-invalid`").Scan(&max); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("max carat in dataset is: %f\n", max)
	}
}

// Returns:
// ERR databricks: query state: ERROR_STATE connId=01eda22f-fde5-1b4c-904f-1f763f17f056 corrId=workflow-example queryId=01eda22f-fe0e-196c-a431-52e3d4f33fc4
// ERR org.apache.hive.service.cli.HiveSQLException: Error running query: [PARSE_SYNTAX_ERROR] org.apache.spark.sql.catalyst.parser.ParseException:
// [PARSE_SYNTAX_ERROR] Syntax error at or near 'Selec'(line 1, pos 0)
//
// == SQL ==
// Selec id from range(100)
// ^^^
// < spark stack trace > (x2)
// ERR databricks: failed to execute query: query Selec id from range(100) error="\n[PARSE_SYNTAX_ERROR] Syntax error at or near 'Selec'(line 1, pos 0)\n\n== SQL ==\nSelec id from range(100)\n^^^\n" connId=01eda243-917b-1806-b517-4067df2495f4 corrId=workflow-example queryId=01eda243-91cd-1929-9ae7-d6382952c217
// databricks: execution error: failed to execute query:
// [PARSE_SYNTAX_ERROR] Syntax error at or near 'Selec'(line 1, pos 0)
//
// == SQL ==
// Selec id from range(100)
// ^^^
func invalidSqlCommand() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}

	// Exec
	if _, err := db.ExecContext(ogCtx, `Selec id from range(100)`); err != nil {
		log.Fatal(err)
	}
}

// Returns:
//
//	databricks: request error: error initializing thrift client: databricks: missing Hostname
func connectorMissingServerHostname() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}

// Returns:
//
//	databricks: request error: error connecting: host=e2-dogfood.staging.cloud.databricks.com port=443, httpPath=/sql/1.0/warehouses/77636a5b5b0e395a: open session request error: Post "https://e2-dogfood.staging.cloud.databricks.com:443/sql/1.0/warehouses/77636a5b5b0e395a": 401 Unauthorized
func connectorMissingAccessToken() {
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: request error: invalid DSN: invalid DSN port: strconv.Atoi: parsing "": invalid syntax
func invalidDsnFormat() {
	dsn := fmt.Sprintf("token%s%s%s%s", os.Getenv("DATABRICKS_ACCESSTOKEN"), os.Getenv("DATABRICKS_HOST"), os.Getenv("DATABRICKS_PORT"), os.Getenv("DATABRICKS_HTTPPATH"))
	_, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: request error: invalid DSN: empty token
func dsnTokenEmpty() {
	dsn := fmt.Sprintf("token:@%s:%s%s", os.Getenv("DATABRICKS_HOST"), os.Getenv("DATABRICKS_PORT"), os.Getenv("DATABRICKS_HTTPPATH"))
	_, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: request error: invalid DSN: maxRows param is not an integer: strconv.Atoi: parsing "abc": invalid syntax
func dsnTokenMaxRowsInvalid() {
	dsn := fmt.Sprintf("token:%s@%s:%s%s?maxRows=abc", os.Getenv("DATABRICKS_ACCESSTOKEN"), os.Getenv("DATABRICKS_HOST"), os.Getenv("DATABRICKS_PORT"), os.Getenv("DATABRICKS_HTTPPATH"))
	_, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: request error: invalid DSN: timeout param is not an integer: strconv.Atoi: parsing "abc": invalid syntax
func dsnTokenTimeoutInvalid() {
	dsn := fmt.Sprintf("token:%s@%s:%s%s?timeout=abc", os.Getenv("DATABRICKS_ACCESSTOKEN"), os.Getenv("DATABRICKS_HOST"), os.Getenv("DATABRICKS_PORT"), os.Getenv("DATABRICKS_HTTPPATH"))
	_, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatal(err)
	}
}

// Returns:
// databricks: request error: error connecting: host=no_host port=443, httpPath=/sql/1.0/warehouses/77636a5b5b0e395a: open session request error: Post "https://no_host:443/sql/1.0/warehouses/77636a5b5b0e395a": dial tcp: lookup no_host: no such host
func dsnTokenInvalidNoSuchHost() {
	dsn := fmt.Sprintf("token:%s@%s:%s%s?maxRows=5", os.Getenv("DATABRICKS_ACCESSTOKEN"), "no_host", os.Getenv("DATABRICKS_PORT"), os.Getenv("DATABRICKS_HTTPPATH"))
	db, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// Ping
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}
}
