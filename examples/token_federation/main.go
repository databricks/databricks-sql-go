package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth/tokenprovider"
)

func main() {
	// Get configuration from environment
	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		port = 443
	}

	fmt.Println("Token Federation Examples")
	fmt.Println("=========================")

	// Choose which example to run based on environment variable
	example := os.Getenv("TOKEN_EXAMPLE")
	if example == "" {
		example = "static"
	}

	switch example {
	case "static":
		runStaticTokenExample(host, httpPath, port)
	case "external":
		runExternalTokenExample(host, httpPath, port)
	case "cached":
		runCachedTokenExample(host, httpPath, port)
	case "custom":
		runCustomProviderExample(host, httpPath, port)
	case "oauth":
		runOAuthServiceExample(host, httpPath, port)
	default:
		log.Fatalf("Unknown example: %s", example)
	}
}

// Example 1: Static token (simplest case)
func runStaticTokenExample(host, httpPath string, port int) {
	fmt.Println("\nExample 1: Static Token Provider")
	fmt.Println("---------------------------------")

	token := os.Getenv("DATABRICKS_ACCESS_TOKEN")
	if token == "" {
		log.Fatal("DATABRICKS_ACCESS_TOKEN not set")
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithStaticToken(token),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Test the connection
	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Connected successfully using static token\n")
	fmt.Printf("✓ Test query result: %d\n", result)
}

// Example 2: External token provider (token passthrough)
func runExternalTokenExample(host, httpPath string, port int) {
	fmt.Println("\nExample 2: External Token Provider (Passthrough)")
	fmt.Println("------------------------------------------------")

	// Simulate getting token from external source
	tokenFunc := func() (string, error) {
		// In real scenario, this could:
		// - Read from a file
		// - Call another service
		// - Retrieve from a secret manager
		// - Get from environment variable
		token := os.Getenv("DATABRICKS_ACCESS_TOKEN")
		if token == "" {
			return "", fmt.Errorf("no token available")
		}
		fmt.Println("  → Fetching token from external source...")
		return token, nil
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithExternalToken(tokenFunc),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Test the connection
	var result int
	err = db.QueryRow("SELECT 2").Scan(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Connected successfully using external token provider\n")
	fmt.Printf("✓ Test query result: %d\n", result)
}

// Example 3: Cached token provider
func runCachedTokenExample(host, httpPath string, port int) {
	fmt.Println("\nExample 3: Cached Token Provider")
	fmt.Println("--------------------------------")

	callCount := 0
	// Create a token provider that tracks how many times it's called
	baseProvider := tokenprovider.NewExternalTokenProvider(func() (string, error) {
		callCount++
		fmt.Printf("  → Token provider called (count: %d)\n", callCount)
		token := os.Getenv("DATABRICKS_ACCESS_TOKEN")
		if token == "" {
			return "", fmt.Errorf("no token available")
		}
		return token, nil
	})

	// Wrap with caching
	cachedProvider := tokenprovider.NewCachedTokenProvider(baseProvider)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithTokenProvider(cachedProvider),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Run multiple queries - token should only be fetched once due to caching
	for i := 1; i <= 3; i++ {
		var result int
		err = db.QueryRow(fmt.Sprintf("SELECT %d", i)).Scan(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("✓ Query %d result: %d\n", i, result)
	}

	fmt.Printf("✓ Token was fetched %d time(s) (should be 1 due to caching)\n", callCount)
}

// Example 4: Custom token provider with expiry
func runCustomProviderExample(host, httpPath string, port int) {
	fmt.Println("\nExample 4: Custom Token Provider with Expiry")
	fmt.Println("--------------------------------------------")

	// Custom provider that simulates token with expiry
	provider := &CustomExpiringTokenProvider{
		baseToken: os.Getenv("DATABRICKS_ACCESS_TOKEN"),
		expiry:    1 * time.Hour,
	}

	// Wrap with caching to handle refresh
	cachedProvider := tokenprovider.NewCachedTokenProvider(provider)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithTokenProvider(cachedProvider),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT 42").Scan(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Connected with custom provider\n")
	fmt.Printf("✓ Token expires at: %s\n", provider.lastToken.ExpiresAt.Format(time.RFC3339))
	fmt.Printf("✓ Test query result: %d\n", result)
}

// Example 5: OAuth service token provider
func runOAuthServiceExample(host, httpPath string, port int) {
	fmt.Println("\nExample 5: OAuth Service Token Provider")
	fmt.Println("---------------------------------------")

	oauthEndpoint := os.Getenv("OAUTH_TOKEN_ENDPOINT")
	clientID := os.Getenv("OAUTH_CLIENT_ID")
	clientSecret := os.Getenv("OAUTH_CLIENT_SECRET")

	if oauthEndpoint == "" || clientID == "" || clientSecret == "" {
		fmt.Println("⚠ Skipping OAuth example (OAUTH_TOKEN_ENDPOINT, OAUTH_CLIENT_ID, or OAUTH_CLIENT_SECRET not set)")
		return
	}

	provider := &OAuthServiceTokenProvider{
		endpoint:     oauthEndpoint,
		clientID:     clientID,
		clientSecret: clientSecret,
	}

	// Wrap with caching for efficiency
	cachedProvider := tokenprovider.NewCachedTokenProvider(provider)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithTokenProvider(cachedProvider),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT 'OAuth Success'").Scan(&result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("✓ Connected with OAuth service token\n")
	fmt.Printf("✓ Test query result: %s\n", result)
}

// CustomExpiringTokenProvider simulates a provider with token expiry
type CustomExpiringTokenProvider struct {
	baseToken string
	expiry    time.Duration
	lastToken *tokenprovider.Token
}

func (p *CustomExpiringTokenProvider) GetToken(ctx context.Context) (*tokenprovider.Token, error) {
	if p.baseToken == "" {
		return nil, fmt.Errorf("no base token configured")
	}

	fmt.Println("  → Generating new token with expiry...")
	p.lastToken = &tokenprovider.Token{
		AccessToken: p.baseToken,
		TokenType:   "Bearer",
		ExpiresAt:   time.Now().Add(p.expiry),
	}

	return p.lastToken, nil
}

func (p *CustomExpiringTokenProvider) Name() string {
	return "custom-expiring"
}

// OAuthServiceTokenProvider gets tokens from an OAuth service
type OAuthServiceTokenProvider struct {
	endpoint     string
	clientID     string
	clientSecret string
}

func (p *OAuthServiceTokenProvider) GetToken(ctx context.Context) (*tokenprovider.Token, error) {
	fmt.Printf("  → Fetching token from OAuth service: %s\n", p.endpoint)

	// Create OAuth request
	req, err := http.NewRequestWithContext(ctx, "POST", p.endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(p.clientID, p.clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Make request
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("OAuth service returned %d: %s", resp.StatusCode, body)
	}

	// Parse response
	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, err
	}

	token := &tokenprovider.Token{
		AccessToken: tokenResp.AccessToken,
		TokenType:   tokenResp.TokenType,
	}

	if tokenResp.ExpiresIn > 0 {
		token.ExpiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	}

	return token, nil
}

func (p *OAuthServiceTokenProvider) Name() string {
	return "oauth-service"
}
