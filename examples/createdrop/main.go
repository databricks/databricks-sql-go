package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strconv"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/joho/godotenv"
)

func main() {
	// use this package to set up logging. By default logging level is `warn`. If you want to disable logging, use `disabled`
	if err := dbsqllog.SetLogLevel("debug"); err != nil {
		log.Fatal(err)
	}
	// sets the logging output. By default it will use os.Stderr. If running in terminal, it will use ConsoleWriter to make it pretty
	// dbsqllog.SetLogOutput(os.Stdout)

	// this is just to make it easy to load all variables
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err)
	}

	// programmatically initializes the connector
	// another way is to use a DNS. In this case the equivalent DNS would be:
	// "token:<my_token>@hostname:port/http_path?catalog=hive_metastore&schema=default&timeout=60&maxRows=10&&timezone=America/Sao_Paulo&ANSI_MODE=true"
	connector, err := dbsql.NewConnector(
		// minimum configuration
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		//optional configuration
		dbsql.WithSessionParams(map[string]string{"timezone": "America/Sao_Paulo", "ansi_mode": "true"}),
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
		dbsql.WithTimeout(time.Minute), // defaults to no timeout. Global timeout. Any query will be canceled if taking more than this time.
		dbsql.WithMaxRows(10),          // defaults to 10000
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)

	}
	// Opening a driver typically will not attempt to connect to the database.
	db := sql.OpenDB(connector)
	// make sure to close it later
	defer db.Close()

	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "createdrop-example")

	// sets the timeout to 30 seconds. More than that we ping will fail. The default is 15 seconds
	ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx1); err != nil {
		log.Fatal(err)
	}

	// create a table with some data. This has no context timeout, it will follow the timeout of one minute set for the connection.
	if _, err := db.ExecContext(ogCtx, `CREATE TABLE IF NOT EXISTS diamonds USING CSV LOCATION '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv' options (header = true, inferSchema = true)`); err != nil {
		log.Fatal(err)
	}

	if _, err := db.ExecContext(ogCtx, `DROP TABLE diamonds `); err != nil {
		log.Fatal(err)
	}
}
