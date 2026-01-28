// Example: Browser OAuth (U2M) Authentication
//
// This example demonstrates User-to-Machine (U2M) OAuth authentication,
// which opens a browser for the user to log in interactively.
//
// Environment variables:
//   - DATABRICKS_HOST: Databricks workspace hostname
//   - DATABRICKS_HTTPPATH: SQL warehouse HTTP path
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
	// Load .env file if present
	if err := godotenv.Load(); err != nil {
		log.Printf("Note: .env file not found")
	}

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")

	if host == "" || httpPath == "" {
		log.Fatal("Required: DATABRICKS_HOST and DATABRICKS_HTTPPATH")
	}

	fmt.Println("Browser OAuth (U2M) Example")
	fmt.Println("===========================")
	fmt.Printf("Host: %s\n", host)
	fmt.Printf("Path: %s\n\n", httpPath)

	// Create U2M authenticator - this will open a browser for login
	authenticator, err := u2m.NewAuthenticator(host, 2*time.Minute)
	if err != nil {
		log.Fatalf("Failed to create authenticator: %v", err)
	}

	// Create database connector
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithAuthenticator(authenticator),
	)
	if err != nil {
		log.Fatalf("Failed to create connector: %v", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Test connection - this triggers browser OAuth flow
	fmt.Println("Connecting (browser will open for login)...")
	if err := db.Ping(); err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	fmt.Println("✓ Connected successfully")

	// Run test queries
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var result int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("✓ SELECT 1 = %d\n", result)

	var user string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&user); err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("✓ Logged in as: %s\n", user)
}
