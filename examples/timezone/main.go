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
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/joho/godotenv"
)

func main() {
	// Opening a driver typically will not attempt to connect to the database.
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err.Error())
	}

	if err := dbsqllog.SetLogLevel("info"); err != nil {
		log.Println(err)
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
		dbsql.WithSessionParams(map[string]string{"timezone": "America/Sao_Paulo", "ansi_mode": "true"}),
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)

	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()
	ctx := context.Background()
	resa := &time.Time{}
	resb := &time.Time{}
	resc := &time.Time{}
	if qerr := db.QueryRowContext(ctx, `select now()`).Scan(resa); qerr != nil {
		fmt.Printf("err: %+v\n", qerr)
	} else {
		fmt.Println(resa)
	}
	if qerr := db.QueryRowContext(ctx, `select now()`).Scan(resb); qerr != nil {
		fmt.Printf("err: %v\n", qerr)
	} else {
		fmt.Println(resb)
	}
	if qerr := db.QueryRowContext(ctx, `select now()`).Scan(resc); qerr != nil {
		fmt.Printf("err: %v\n", qerr)

	} else {
		fmt.Println(resc)
	}

	connector1, err1 := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithSessionParams(map[string]string{"timezone": "Europe/Amsterdam", "ansi_mode": "true"}),
	)
	if err1 != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err1)
	}
	db1 := sql.OpenDB(connector1)
	defer db1.Close()

	res1 := &time.Time{}
	res2 := &time.Time{}
	if qerr := db1.QueryRowContext(ctx, `select now()`).Scan(res1); qerr != nil {
		fmt.Printf("err: %v\n", qerr)

	} else {
		fmt.Println(res1)
	}
	if qerr := db1.QueryRowContext(ctx, `select now()`).Scan(res2); qerr != nil {
		fmt.Printf("err: %v\n", qerr)

	} else {
		fmt.Println(res2)
	}
}
