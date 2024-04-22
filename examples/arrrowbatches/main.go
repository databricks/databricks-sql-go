package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/joho/godotenv"
)

func main() {
	// Opening a driver typically will not attempt to connect to the database.
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err.Error())
	}

	// dbsqllog.SetLogLevel("debug")

	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err.Error())
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithMaxRows(10000),
	)

	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	loopWithHasNext(db)
	loopWithNext(db)
}

func loopWithHasNext(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, _ := db.Conn(ctx)
	defer conn.Close()

	query := `select * from main.default.diamonds`

	var rows driver.Rows
	var err error
	err = conn.Raw(func(d interface{}) error {
		rows, err = d.(driver.QueryerContext).QueryContext(ctx, query, nil)
		return err
	})

	if err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}
	defer rows.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	batches, err := rows.(dbsqlrows.Rows).GetArrowBatches(ctx2)
	if err != nil {
		log.Fatalf("unable to get arrow batches. err: %v", err)
	}

	var iBatch, nRows int
	for batches.HasNext() {
		b, err := batches.Next()
		if err != nil {
			log.Fatalf("Failure retrieving batch. err: %v", err)
		}

		log.Printf("batch %v: nRecords=%v\n", iBatch, b.NumRows())
		iBatch += 1
		nRows += int(b.NumRows())
		b.Release()
	}
	log.Printf("NRows: %v\n", nRows)
}

func loopWithNext(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, _ := db.Conn(ctx)
	defer conn.Close()

	query := `select * from main.default.diamonds`

	var rows driver.Rows
	var err error

	err = conn.Raw(func(d interface{}) error {
		rows, err = d.(driver.QueryerContext).QueryContext(ctx, query, nil)
		return err
	})
	if err != nil {
		log.Fatalf("unable to run the query. err: %v", err)
	}
	defer rows.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	batches, err := rows.(dbsqlrows.Rows).GetArrowBatches(ctx2)
	if err != nil {
		log.Fatalf("unable to get arrow batches. err: %v", err)
	}

	var iBatch, nRows int
	var b arrow.Record
	for b, err = batches.Next(); err == nil; b, err = batches.Next() {
		log.Printf("batch %v: nRecords=%v\n", iBatch, b.NumRows())
		iBatch += 1
		nRows += int(b.NumRows())
		b.Release()
	}

	log.Printf("NRows: %v\n", nRows)
	if err == io.EOF {
		log.Println("normal loop termination")
	} else {
		log.Printf("loop terminated with error: %v", err)
	}
}
