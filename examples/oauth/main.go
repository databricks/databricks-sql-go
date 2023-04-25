package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth/oauth/dev"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()

	if err != nil {
		log.Fatal(err.Error())
	}
	// devAuthenticator := m2m.NewClient(
	// 	os.Getenv("DATABRICKS_CLIENT_ID"),
	// 	os.Getenv("DATABRICKS_CLIENT_SECRET"),
	// 	fmt.Sprintf("https://%s/oidc", os.Getenv("DATABRICKS_HOST")),
	// 	[]string{"sql", "offline_access"},
	// )
	devAuthenticator := dev.NewDevAuthenticator(
		os.Getenv("DATABRICKS_CLIENT_ID"),
		os.Getenv("DATABRICKS_HOST"),
		"offline_access",
	)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAuthenticator(devAuthenticator),
	)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// ctx := context.Background()
	var res int
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

	err1 = db.QueryRowContext(ctx, `select 1`).Scan(&res)

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
