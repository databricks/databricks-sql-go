package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth/oauth/u2m"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal(err.Error())
	}

	authenticator, err := u2m.NewAuthenticator(os.Getenv("DATABRICKS_HOST"), 1*time.Minute)

	if err != nil {
		log.Fatal(err.Error())
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAuthenticator(authenticator),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Pinging should require logging in
	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var res int

	// Running query should not require logging in as we should have a token
	// from when ping was called.
	err1 := db.QueryRowContext(ctx, `select 1`).Scan(&res)

	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Printf("err: %v\n", err1)
		}
	}
	fmt.Println(res)
}
