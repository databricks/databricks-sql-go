// Example: Token Provider Authentication
//
// This example demonstrates different ways to provide authentication tokens:
// 1. Static token - hardcoded/env token
// 2. External token - dynamic token from a function
// 3. Custom provider - full TokenProvider implementation
//
// Environment variables:
//   - DATABRICKS_HOST: Databricks workspace hostname
//   - DATABRICKS_HTTPPATH: SQL warehouse HTTP path
//   - DATABRICKS_ACCESS_TOKEN: Access token for authentication
//   - TOKEN_EXAMPLE: Which example to run (static, external, custom)
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth/tokenprovider"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Printf("Note: .env file not found")
	}

	example := os.Getenv("TOKEN_EXAMPLE")
	if example == "" {
		example = "static"
	}

	switch example {
	case "static":
		runStaticExample()
	case "external":
		runExternalExample()
	case "custom":
		runCustomProviderExample()
	default:
		log.Fatalf("Unknown example: %s (use: static, external, custom)", example)
	}
}

// runStaticExample uses a static token from environment variable
func runStaticExample() {
	fmt.Println("Static Token Example")
	fmt.Println("====================")

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	token := os.Getenv("DATABRICKS_ACCESS_TOKEN")

	if host == "" || httpPath == "" || token == "" {
		log.Fatal("Required: DATABRICKS_HOST, DATABRICKS_HTTPPATH, DATABRICKS_ACCESS_TOKEN")
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithStaticToken(token),
	)
	if err != nil {
		log.Fatal(err)
	}

	testConnection(connector)
}

// runExternalExample uses a function that returns tokens on-demand
func runExternalExample() {
	fmt.Println("External Token Example")
	fmt.Println("======================")

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")

	if host == "" || httpPath == "" {
		log.Fatal("Required: DATABRICKS_HOST, DATABRICKS_HTTPPATH")
	}

	// Token function - called each time a token is needed
	// In practice, this could read from a file, call an API, etc.
	tokenFunc := func() (string, error) {
		token := os.Getenv("DATABRICKS_ACCESS_TOKEN")
		if token == "" {
			return "", fmt.Errorf("DATABRICKS_ACCESS_TOKEN not set")
		}
		fmt.Println("  → Token fetched from external source")
		return token, nil
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithExternalToken(tokenFunc),
	)
	if err != nil {
		log.Fatal(err)
	}

	testConnection(connector)
}

// runCustomProviderExample uses a custom TokenProvider implementation
func runCustomProviderExample() {
	fmt.Println("Custom Provider Example")
	fmt.Println("=======================")

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	token := os.Getenv("DATABRICKS_ACCESS_TOKEN")

	if host == "" || httpPath == "" || token == "" {
		log.Fatal("Required: DATABRICKS_HOST, DATABRICKS_HTTPPATH, DATABRICKS_ACCESS_TOKEN")
	}

	// Custom provider with expiry tracking
	provider := &ExpiringTokenProvider{
		token:    token,
		lifetime: 1 * time.Hour,
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithTokenProvider(provider),
	)
	if err != nil {
		log.Fatal(err)
	}

	testConnection(connector)
	fmt.Printf("  Token expires: %s\n", provider.expiresAt.Format(time.RFC3339))
}

// testConnection verifies the connection works
func testConnection(connector driver.Connector) {
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var result int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	fmt.Printf("✓ Connected, SELECT 1 = %d\n", result)
}

// ExpiringTokenProvider is a custom TokenProvider with expiry support
type ExpiringTokenProvider struct {
	token     string
	lifetime  time.Duration
	expiresAt time.Time
}

func (p *ExpiringTokenProvider) GetToken(ctx context.Context) (*tokenprovider.Token, error) {
	p.expiresAt = time.Now().Add(p.lifetime)
	return &tokenprovider.Token{
		AccessToken: p.token,
		TokenType:   "Bearer",
		ExpiresAt:   p.expiresAt,
	}, nil
}

func (p *ExpiringTokenProvider) Name() string {
	return "expiring-token"
}
