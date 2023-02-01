package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerror "github.com/databricks/databricks-sql-go/error"
	"github.com/joho/godotenv"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	// Setup
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}
	accessTokenInvalid()
	//userDoesNotHavePermission()
}

// Returns:
// databricks: driver error: error connecting: host=e2-dogfood.staging.cloud.databricks.com port=443, httpPath=/sql/1.0/warehouses/e5edb16633f87f9b: open session request error: HTTP Response code: 403
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
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}

	// Exec
	if _, err := db.ExecContext(ogCtx, `CREATE TABLE IF NOT EXISTS diamonds USING CSV LOCATION '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv' options (header = true, inferSchema = true)`); err != nil {
		var queryFailureError *dbsqlerror.QueryFailureError
		if errors.As(err, &queryFailureError) {
			fmt.Println("QueryFailureError for query", queryFailureError.QueryId(), ".", queryFailureError)
		}
	}
}
