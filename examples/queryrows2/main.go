package main

import (
	"context"
	"database/sql"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	// "github.com/pkg/profile"

	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/client"

	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/joho/godotenv"
)

func main() {

	// defer profile.Start(profile.CPUProfile, profile.NoShutdownHook).Stop()

	// use this package to set up logging. By default logging level is `warn`. If you want to disable logging, use `disabled`
	if err := dbsqllog.SetLogLevel("warn"); err != nil {
		log.Fatal(err)
	}
	// sets
	client.RecordResults = true

	// Opening a driver typically will not attempt to connect to the database.
	err := godotenv.Load()

	if err != nil {
		log.Fatal(err.Error())
	}
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err.Error())
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithArrowBatches(true),
		dbsql.WithMaxRows(100000),
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// the "github.com/databricks/databricks-sql-go/driverctx" has some functions to help set the context for the driver
	ogCtx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

	// sets the timeout to 30 seconds. More than that we ping will fail. The default is 15 seconds
	// ctx1, cancel := context.WithTimeout(ogCtx, 30*time.Second)
	// defer cancel()
	// if err := db.PingContext(ctx1); err != nil {
	// 	log.Fatal(err)
	// }

	// create a table with some data. This has no context timeout, it will follow the timeout of one minute set for the connection.
	if _, err := db.ExecContext(ogCtx, `CREATE TABLE IF NOT EXISTS diamonds USING CSV LOCATION '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv' options (header = true, inferSchema = true)`); err != nil {
		log.Fatal(err)
	}

	type row struct {
		_c0     int
		carat   float64
		cut     string
		color   string
		clarity string
		depth   sql.NullFloat64
		table   sql.NullFloat64
		price   int
		x       float64
		y       float64
		z       float64
	}

	// cols, err := rows.Columns()
	// if err != nil {
	// 	panic(err)
	// }
	// types, err := rows.ColumnTypes()
	// if err != nil {
	// 	panic(err)
	// }
	// for i, c := range cols {
	// 	fmt.Printf("column %d is %s and has type %v\n", i, c, types[i].DatabaseTypeName())
	// }

	// res := []row{}

	nRuns := 1
	times := make([]float64, nRuns)

	for i := 0; i < nRuns; i++ {
		start := time.Now()

		rows, err1 := db.QueryContext(ogCtx, `select * from diamonds`)
		if err1 != nil {
			log.Fatal(err1)
		}

		for rows.Next() {
			// After row 10 this will cause one fetch call, as 10 rows (maxRows config) will come from the first execute statement call.
			r := row{}
			if err := rows.Scan(&r._c0, &r.carat, &r.cut, &r.color, &r.clarity, &r.depth, &r.table, &r.price, &r.x, &r.y, &r.z); err != nil {
				log.Fatal(err)
			}
			// res = append(res, r)
		}
		d := time.Since(start)
		times[i] = float64(d)
		log.Printf(" *** %v:  %v ***", i, time.Duration(d))
	}

	// log.Printf("mean time *** %v ***", time.Duration(mean(times)))
	// min, max := minMax(times)
	// log.Printf("max: %v, min: %v", time.Duration(max), time.Duration(min))
	// log.Printf("stdDev *** %v ***", time.Duration(stddev(times)))

	// log.Printf("mean within 2 *** %v ***", time.Duration(mean(within(times, 2))))
	// log.Printf("mean within 3 *** %v ***", time.Duration(mean(within(times, 3))))

	// for _, r := range res {
	// 	fmt.Printf("%+v\n", r)
	// 	// fmt.Printf("date_col: %s, timestamp_col: %s", r.date_col.String(), r.timestamp_col.String())
	// }

	// if _, err := db.ExecContext(ogCtx, `drop table default.primitive_types`); err != nil {
	// 	log.Fatal(err)
	// }
}

func stddev(times []float64) float64 {
	m := mean(times)
	var sd float64
	for i := range times {
		sd += math.Pow(times[i]-m, 2)
	}
	sd = math.Sqrt(sd / float64(len(times)))

	return sd
}

func within(times []float64, x int) []float64 {
	m := mean(times)
	sd := stddev(times)
	r := sd * float64(x)
	var wt []float64
	for i := range times {
		tf := math.Abs(float64(times[i]) - m)
		if tf < r {
			wt = append(wt, times[i])
		}
	}

	log.Printf(" within %v len %v", x, len(wt))

	return wt
}

func mean(times []float64) float64 {
	var ttotal float64
	for i := range times {
		ttotal = ttotal + times[i]
	}
	avgTime := ttotal / float64(len(times))
	return avgTime
}

func minMax(times []float64) (min float64, max float64) {
	min = times[0]
	max = times[0]
	for i := 1; i < len(times); i++ {
		if times[i] < min {
			min = times[i]
		}
		if times[i] > max {
			max = times[i]
		}
	}
	return
}
