package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	// "strconv"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/joho/godotenv"
	// dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
)

func main() {
	// Read from file
	dat, err := os.ReadFile("queries.txt")
	if err != nil {
		panic(err)
	}
	queries := strings.Split(string(dat), "\n")
	for _, query := range queries {
		executeQuery(query, 100, 10800, 100000)
	}
}

func executeQuery(query string, maxIters int, timeoutSecs int, maxRows int) {
	fmt.Println("Query:", query)
	if err := dbsqllog.SetLogLevel("debug"); err != nil {
		panic(err)
	}
	if err := godotenv.Load(); err != nil {
		log.Fatal(err)
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithUserAgentEntry("benchmark"),
		dbsql.WithTimeout(time.Duration(timeoutSecs*1000)*time.Millisecond),
		dbsql.WithMaxRows(maxRows),
	)
	if err != nil {
		panic(err)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	defer db.Close()

	if err := db.Ping(); err != nil {
		panic(err)
	}
	ctx := context.Background()
	testStartTime := time.Now()
	numErrs := 0
	totalElapsedTime := time.Duration(0) * time.Millisecond
	elapsedTime := time.Duration(0) * time.Millisecond

	for totalOps := 0; totalOps < maxIters; totalOps++ {
		elapsedTime = time.Since(testStartTime)
		fmt.Println("elapsedTime:", elapsedTime)
		if elapsedTime > time.Duration(timeoutSecs*1000)*time.Millisecond {
			break
		}
		type row struct {
			id int
		}
		type taxi struct {
			col_0 string
			col_1 string
			col_2 float64
			col_3 float64
			col_4 int
			col_5 int
		}
		isTaxi := strings.Contains(query, "nyctaxi")
		if isTaxi {
			res := []taxi{}
			opStartTime := time.Now()
			rows, err := db.QueryContext(ctx, query)
			defer rows.Close()
			if err != nil {
				fmt.Println(err)
				numErrs++
			} else {
				for rows.Next() {
					r := taxi{}
					if err := rows.Scan(&r.col_0, &r.col_1, &r.col_2, &r.col_3, &r.col_4, &r.col_5); err != nil {
						panic(err)
					}
					res = append(res, r)
				}
			}
			duration := time.Since(opStartTime)
			fmt.Println("sanity check. # of rows:", len(res))
			fmt.Println("op", totalOps, "duration:", duration)
			totalElapsedTime += duration
			fmt.Println("total elapsed:", totalElapsedTime)
			fmt.Println("errs so far:", numErrs)
		} else {
			res := []row{}
			opStartTime := time.Now()
			rows, err := db.QueryContext(ctx, query)
			defer rows.Close()
			if err != nil {
				fmt.Println(err)
				numErrs++
			} else {
				for rows.Next() {
					r := row{}
					if err := rows.Scan(&r.id); err != nil {
						panic(err)
					}
					res = append(res, r)
				}
			}
			duration := time.Since(opStartTime)
			fmt.Println("sanity check. # of rows:", len(res))
			fmt.Println("op", totalOps, "duration:", duration)
			totalElapsedTime += duration
			fmt.Println("total elapsed:", totalElapsedTime)
			fmt.Println("errs so far:", numErrs)
		}
	}
	fmt.Println("total errors:", numErrs)
	meanTime := float64(totalElapsedTime.Microseconds()) / float64(maxIters-numErrs)
	fmt.Println("mean time:", meanTime/1000, "milliseconds")
	f, err := os.OpenFile("results_go.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%.4f\n", meanTime/1000)); err != nil {
		panic(err)
	}
}
