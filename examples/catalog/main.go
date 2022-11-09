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
	"github.com/joho/godotenv"
)

func main() {
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
		dbsql.WithInitialNamespace("quickstart_catalog1", "quickstart_schema1"),
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Println(err)
	}

	rows, err1 := db.QueryContext(context.Background(), `select * from quickstart_table1`)
	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Println(err1.Error())
			return
		}
	}
	var res string
	for rows.Next() {
		err := rows.Scan(&res)
		if err != nil {
			fmt.Println(err)
			rows.Close()
			return
		}
		fmt.Println(res)
	}

}
