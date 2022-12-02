package main

import (
	"context"
	"database/sql"
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

	// 	the log looks like this:
	// ```
	// {"level":"debug","connId":"01ed6545-5669-1ec7-8c7e-6d8a1ea0ab16","corrId":"workflow-example","queryId":"01ed6545-57cc-188a-bfc5-d9c0eaf8e189","time":1668558402,"message":"QueryContext elapsed time: 1.298712292s"}
	// ```
	// This format makes it very easy to parse and create metrics in logging
	// tools.

	// **Connection ID**
	// Pretty much everything we do with the driver is based on a connection
	// id. This is the internal id to track what happens under that connection.
	// Remember that connections can be reused so this would track across
	// queries.

	// **Correlation ID**
	// If you have an external ID, such as a request ID, we provide a way
	// to add it to the context so we can log with it. This helps in cases
	// where multiple connections are used in the same request.

	// **Query ID**
	// If we have a query ID, we also add to the logs so you can have another
	// layer of tracking, as sometimes the same query can be used with multiple
	// connections.

	// the "github.com/databricks/databricks-sql-go/driverctx" has some functions to help set the context for the driver
	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

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

	// QueryRowContext is a shortcut function to get a single value
	var max float64
	if err := db.QueryRowContext(ogCtx, `select max(carat) from diamonds`).Scan(&max); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("max carat in dataset is: %f\n", max)
	}

	// sets the timeout to 30 seconds. The default is the one set for DB (in this example 1 minute)
	ctx2, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	defer cancel()

	if rows, err := db.QueryContext(ctx2, "select * from diamonds limit 19"); err != nil {
		log.Fatal(err)
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
			log.Fatal(err)
		}
		types, err := rows.ColumnTypes()
		if err != nil {
			log.Fatal(err)
		}
		for i, c := range cols {
			fmt.Printf("column %d is %s and has type %v\n", i, c, types[i].DatabaseTypeName())
		}
		res := []row{}
		for rows.Next() {
			// After row 10 this will cause one fetch call, as 10 rows (maxRows config) will come from the first execute statement call.
			r := row{}
			if err := rows.Scan(&r._c0, &r.carat, &r.cut, &r.color, &r.clarity, &r.depth, &r.table, &r.price, &r.x, &r.y, &r.z); err != nil {
				log.Fatal(err)
			}
			res = append(res, r)
		}
		for _, r := range res {
			fmt.Printf("%+v\n", r)
		}
	}

	// timezones are also supported
	var curTimestamp time.Time
	var curDate time.Time
	var curTimezone string

	if err := db.QueryRowContext(ogCtx, `select current_date(), current_timestamp(), current_timezone()`).Scan(&curDate, &curTimestamp, &curTimezone); err != nil {
		log.Fatal(err)
	} else {
		// this will print now at timezone America/Sao_Paulo is: 2022-11-16 20:25:15.282 -0300 -03
		fmt.Printf("current timestamp at timezone %s is: %s\n", curTimezone, curTimestamp)
		fmt.Printf("current date at timezone %s is: %s\n", curTimezone, curDate)
	}

	// we can also create and query a table with complex data types
	if _, err := db.ExecContext(
		ogCtx,
		`create table if not exists array_map_struct (
			array_col array < int >,
			map_col map < string, int >,
			struct_col struct < string_field string, array_field array < int > >)`); err != nil {
		log.Fatal(err)
	}
	var numRows int
	if err := db.QueryRowContext(ogCtx, `select count(*) from array_map_struct`).Scan(&numRows); err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("table has %d rows\n", numRows)
	}
	if numRows == 0 {

		if res, err := db.ExecContext(
			context.Background(),
			`insert into table array_map_struct (array_col, map_col, struct_col) VALUES (
				array(1, 2, 3),
				map('key1', 1),
				struct('string_val', array(4, 5, 6)))`); err != nil {
			log.Fatal(err)
		} else {
			i, err1 := res.RowsAffected()
			if err1 != nil {
				log.Fatal(err1)
			}
			fmt.Printf("inserted %d rows", i)
		}
	}

	if rows, err := db.QueryContext(ogCtx, "select * from array_map_struct"); err != nil {
		log.Fatal(err)
	} else {
		// complex data types are returned as string
		type row struct {
			arrayVal  string
			mapVal    string
			structVal string
		}
		res := []row{}
		cols, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}
		types, err := rows.ColumnTypes()
		if err != nil {
			log.Fatal(err)
		}
		for i, c := range cols {
			fmt.Printf("column %d is %s and has type %v\n", i, c, types[i].DatabaseTypeName())
		}

		for rows.Next() {
			r := row{}
			if err := rows.Scan(&r.arrayVal, &r.mapVal, &r.structVal); err != nil {
				log.Fatal(err)
			}
			res = append(res, r)
		}
		// below will print
		//{arrayVal:[1,2,3] mapVal:{"key1":1} structVal:{"string_field":"string_val","array_field":[4,5,6]}}
		for _, r := range res {
			fmt.Printf("%+v\n", r)
		}
	}

}
