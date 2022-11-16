package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/logger"
	"github.com/joho/godotenv"
)

func main() {
	if err := logger.SetLogLevel("debug"); err != nil {
		panic(err)
	}
	if err := godotenv.Load(); err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		panic(err)
	}
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
		dbsql.WithTimeout(time.Minute), // defaults to 0
		dbsql.WithMaxRows(10),          // defaults to 10000
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		panic(err)

	}
	// Opening a driver typically will not attempt to connect to the database.
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		panic(err)
	}

	// create a table with some data
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS diamonds USING CSV LOCATION '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv' options (header = true, inferSchema = true)`); err != nil {
		panic(err)
	}

	var max float64
	if err := db.QueryRowContext(ctx, `select max(carat) from diamonds`).Scan(&max); err != nil {
		panic(err)
	} else {
		fmt.Printf("max carat in dataset is: %f\n", max)
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
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if rows, err := db.QueryContext(ctx, "select * from diamonds limit 19"); err != nil {
		panic(err)
	} else {
		cols, err := rows.Columns()
		if err != nil {
			panic(err)
		}
		types, err := rows.ColumnTypes()
		if err != nil {
			panic(err)
		}
		for i, c := range cols {
			fmt.Printf("column %d is %s and has type %v\n", i, c, types[i].DatabaseTypeName())
		}
		res := []row{}
		for rows.Next() {
			r := row{}
			if err := rows.Scan(&r._c0, &r.carat, &r.cut, &r.color, &r.clarity, &r.depth, &r.table, &r.price, &r.x, &r.y, &r.z); err != nil {
				panic(err)
			}
			res = append(res, r)
		}
		for _, r := range res {
			fmt.Printf("%+v\n", r)
		}
	}

}
