package main

import (
	"context"
	"fmt"
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
		panic(err)
	}
	// sets the logging output. By default it will use os.Stderr. If running in terminal, it will use ConsoleWriter to make it pretty
	// dbsqllog.SetLogOutput(os.Stdout)

	// this is just to make it easy to load all variables
	if err := godotenv.Load(); err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		panic(err)
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
		dbsql.WithUserAgentEntry("workflow-example"),
		dbsql.WithInitialNamespace("hive_metastore", "default"),
		dbsql.WithTimeout(time.Minute), // defaults to no timeout. Global timeout. Any query will be canceled if taking more than this time.
		dbsql.WithMaxRows(10),          // defaults to 10000
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		panic(err)

	}
	// Opening a driver typically will not attempt to connect to the database.
	db := dbsql.OpenDB(connector)
	// make sure to close it later
	defer db.Close()

	db.SetMaxOpenConns(1)

	// the "github.com/databricks/databricks-sql-go/driverctx" has some functions to help set the context for the driver
	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "asyncWorkflow-example")

	// for _, v := range []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} {
	// i := v
	// go func() {
	// _, exc, err := db.QueryContext(ogCtx, fmt.Sprintf("select %s", i))
	rs, exc, err := db.QueryContext(ogCtx, `SELECT id FROM RANGE(100000000) ORDER BY RANDOM() + 2 asc`)
	if err != nil {
		panic(err)
	}
	for {
		if exc.Status.Terminal() {
			break
		} else {
			// TODO: how to prevent the connection being locked when rows has no data??
			exc, err = db.CheckExecution(ogCtx, exc)
			if err != nil {
				log.Fatal(err)
			}
		}
		fmt.Println(db.Stats())
		time.Sleep(time.Second)
	}

	fmt.Println(exc.Status)
	if exc.Status == dbsql.ExecutionFinished {
		rs, err = db.GetExecutionRows(ogCtx, exc)
		if err != nil {
			panic(err)
		}
		defer rs.Close()
		var res string
		i := 0
		for rs.Next() {
			err := rs.Scan(&res)
			if err != nil {
				fmt.Println(err)
				rs.Close()
				return
			}
			fmt.Println(res)
			if i < 10 {
				i++
			} else {
				rs.Close()
			}
		}
	}
	// }
	// timezones are also supported
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

}
