package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/joho/godotenv"
)

func main() {

	_ = godotenv.Load()

	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err.Error())
	}

	// Connection-level query tags: applied to all queries in this session.
	// WithQueryTags accepts a map and handles serialization automatically.
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
		dbsql.WithQueryTags(map[string]string{
			"team":   "engineering",
			"test":   "query-tags",
			"driver": "go",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Example 1: Connection-level query tags (set during connection)
	fmt.Println("=== Connection-level query tags ===")
	ctx := context.Background()
	var result int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		log.Printf("err: %v\n", err)
	} else {
		fmt.Println(result)
	}

	// Example 2: Statement-level query tags (per-query override via context)
	fmt.Println("=== Statement-level query tags ===")
	ctx = driverctx.NewContextWithQueryTags(context.Background(), map[string]string{
		"team":        "data-eng",
		"application": "etl-pipeline",
		"env":         "production",
	})
	err = db.QueryRowContext(ctx, "SELECT 2").Scan(&result)
	if err != nil {
		log.Printf("err: %v\n", err)
	} else {
		fmt.Println(result)
	}

	// Example 3: Different query tags for a different statement
	fmt.Println("=== Different statement-level query tags ===")
	ctx = driverctx.NewContextWithQueryTags(context.Background(), map[string]string{
		"team": "analytics",
		"job":  "daily-report",
	})
	err = db.QueryRowContext(ctx, "SELECT 3").Scan(&result)
	if err != nil {
		log.Printf("err: %v\n", err)
	} else {
		fmt.Println(result)
	}
}
