package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth/oauth/u2m"
)

func main() {
	// Example of using the token cache with the Go driver
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname("your-workspace.cloud.databricks.com"),
		dbsql.WithPort(443),
		dbsql.WithHTTPPath("/sql/1.0/warehouses/your-warehouse-id"),
		// Enable token caching (enabled by default)
		dbsql.WithTokenCache(true),
		// Optional: Specify a custom path for the token cache
		// dbsql.WithTokenCachePath("/path/to/custom/token/cache"),
		// Set up OAuth U2M authentication with options
		dbsql.WithUserAuthenticationOptions(
			2*time.Minute, // Timeout for browser authentication
			// Optional: Specify custom scopes
			u2m.WithScopes([]string{"offline_access", "sql"}),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create a database connection
	db := sql.OpenDB(connector)
	defer db.Close()

	// Test the connection
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	// Run a simple query
	rows, err := db.QueryContext(ctx, "SELECT current_date()")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Process the results
	for rows.Next() {
		var currentDate string
		if err := rows.Scan(&currentDate); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Current date: %s\n", currentDate)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connection successful! The token has been cached for future use.")
}