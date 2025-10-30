package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth/oauth/u2m"
	"github.com/databricks/databricks-sql-go/auth/tokenprovider"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}

	fmt.Println("Browser OAuth with Token Federation Test")
	fmt.Println("=========================================")
	fmt.Println()

	// Get test mode from environment
	testMode := os.Getenv("TEST_MODE")
	if testMode == "" {
		fmt.Println("TEST_MODE not set. Available modes:")
		fmt.Println("  passthrough        - Account-wide WIF Auth_Flow=0 (Token passthrough)")
		fmt.Println("  u2m_federation     - Account-wide WIF Auth_Flow=2 (U2M with federation)")
		fmt.Println("  u2m_native         - Native U2M without federation (baseline)")
		fmt.Println("  external_token     - Manual token passthrough (for testing exchange)")
		os.Exit(1)
	}

	switch testMode {
	case "passthrough":
		testTokenPassthrough()
	case "u2m_federation":
		testU2MWithFederation()
	case "u2m_native":
		testU2MNative()
	case "external_token":
		testExternalTokenWithFederation()
	default:
		log.Fatalf("Unknown test mode: %s", testMode)
	}
}

// testU2MNative tests native U2M OAuth without token federation (baseline)
func testU2MNative() {
	fmt.Println("Test: Native U2M OAuth (Baseline - No Federation)")
	fmt.Println("--------------------------------------------------")
	fmt.Println("This uses Databricks' built-in OAuth without token exchange")
	fmt.Println()

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")

	if host == "" || httpPath == "" {
		log.Fatal("DATABRICKS_HOST and DATABRICKS_HTTPPATH must be set")
	}

	fmt.Printf("Host: %s\n", host)
	fmt.Printf("HTTP Path: %s\n", httpPath)
	fmt.Println()

	// Create U2M authenticator
	authenticator, err := u2m.NewAuthenticator(host, 2*time.Minute)
	if err != nil {
		log.Fatal(err)
	}

	// Create connector with native OAuth
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithAuthenticator(authenticator),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Test connection
	fmt.Println("Testing connection with browser OAuth...")

	// First try ping to see if we can establish connection
	fmt.Println("Attempting to ping database...")
	if err := db.Ping(); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("✓ Ping successful")

	if err := testConnection(db); err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}

	fmt.Println()
	fmt.Println("✓ Native U2M OAuth test PASSED")
}

// testU2MWithFederation tests U2M OAuth with token federation (Account-wide WIF)
func testU2MWithFederation() {
	fmt.Println("Test: U2M OAuth with Token Federation (Account-wide WIF)")
	fmt.Println("---------------------------------------------------------")
	fmt.Println("This tests Auth_Flow=2: Browser OAuth token → Federation exchange → Databricks token")
	fmt.Println()

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	externalIdpHost := os.Getenv("EXTERNAL_IDP_HOST")

	if host == "" || httpPath == "" {
		log.Fatal("DATABRICKS_HOST and DATABRICKS_HTTPPATH must be set")
	}

	if externalIdpHost == "" {
		fmt.Println("WARNING: EXTERNAL_IDP_HOST not set, using Databricks host")
		externalIdpHost = host
	}

	fmt.Printf("Databricks Host: %s\n", host)
	fmt.Printf("HTTP Path: %s\n", httpPath)
	fmt.Printf("External IdP Host: %s\n", externalIdpHost)
	fmt.Println()

	// Step 1: Get token from external IdP using browser OAuth
	fmt.Println("Step 1: Getting token from external IdP via browser OAuth...")
	baseAuthenticator, err := u2m.NewAuthenticator(externalIdpHost, 2*time.Minute)
	if err != nil {
		log.Fatalf("Failed to create U2M authenticator: %v", err)
	}

	// Wrap U2M authenticator as a token provider
	u2mProvider := &U2MTokenProvider{authenticator: baseAuthenticator}

	// Step 2: Wrap with federation provider for automatic token exchange
	fmt.Println("Step 2: Setting up federation provider for automatic token exchange...")
	federationProvider := tokenprovider.NewFederationProvider(u2mProvider, host)
	cachedProvider := tokenprovider.NewCachedTokenProvider(federationProvider)

	// Create connector with federated authentication
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithTokenProvider(cachedProvider),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Test connection
	fmt.Println()
	fmt.Println("Step 3: Testing connection (will trigger browser OAuth and token exchange)...")
	if err := testConnection(db); err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}

	fmt.Println()
	fmt.Println("✓ U2M with Token Federation test PASSED")
	fmt.Println()
	fmt.Println("Token flow: Browser OAuth → External IdP Token → Token Exchange → Databricks Token")
}

// testTokenPassthrough tests manual token passthrough with federation
func testTokenPassthrough() {
	fmt.Println("Test: Token Passthrough with Federation (Auth_Flow=0)")
	fmt.Println("------------------------------------------------------")
	fmt.Println("This tests passing an external token that gets exchanged automatically")
	fmt.Println()

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	externalToken := os.Getenv("EXTERNAL_TOKEN")

	if host == "" || httpPath == "" {
		log.Fatal("DATABRICKS_HOST and DATABRICKS_HTTPPATH must be set")
	}

	if externalToken == "" {
		log.Fatal("EXTERNAL_TOKEN must be set (get from external IdP)")
	}

	fmt.Printf("Host: %s\n", host)
	fmt.Printf("Token issuer: %s\n", getTokenIssuer(externalToken))
	fmt.Println()

	// Create static token provider
	baseProvider := tokenprovider.NewStaticTokenProvider(externalToken)

	// Wrap with federation provider
	federationProvider := tokenprovider.NewFederationProvider(baseProvider, host)
	cachedProvider := tokenprovider.NewCachedTokenProvider(federationProvider)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithTokenProvider(cachedProvider),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	fmt.Println("Testing connection with token passthrough...")
	if err := testConnection(db); err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}

	fmt.Println()
	fmt.Println("✓ Token Passthrough with Federation test PASSED")
}

// testExternalTokenWithFederation tests manual token exchange process
func testExternalTokenWithFederation() {
	fmt.Println("Test: Manual Token Exchange (for debugging)")
	fmt.Println("-------------------------------------------")
	fmt.Println()

	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	externalToken := os.Getenv("EXTERNAL_TOKEN")

	if host == "" || httpPath == "" || externalToken == "" {
		log.Fatal("DATABRICKS_HOST, DATABRICKS_HTTPPATH, and EXTERNAL_TOKEN must be set")
	}

	fmt.Printf("Host: %s\n", host)
	fmt.Printf("Token issuer: %s\n", getTokenIssuer(externalToken))
	fmt.Println()

	// Manual token exchange
	fmt.Println("Step 1: Manually exchanging token...")
	exchangedToken, err := manualTokenExchange(host, externalToken)
	if err != nil {
		log.Fatalf("Token exchange failed: %v", err)
	}

	fmt.Printf("✓ Token exchange successful\n")
	fmt.Printf("  Exchanged token length: %d chars\n", len(exchangedToken))
	fmt.Println()

	// Test connection with exchanged token
	fmt.Println("Step 2: Testing connection with exchanged token...")
	provider := tokenprovider.NewStaticTokenProvider(exchangedToken)
	cachedProvider := tokenprovider.NewCachedTokenProvider(provider)

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(host),
		dbsql.WithHTTPPath(httpPath),
		dbsql.WithTokenProvider(cachedProvider),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	if err := testConnection(db); err != nil {
		log.Fatalf("Connection test failed: %v", err)
	}

	fmt.Println()
	fmt.Println("✓ Manual Token Exchange test PASSED")
}

// Helper: Test database connection with queries
func testConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Test 1: Simple query
	var result int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("simple query failed: %w", err)
	}
	fmt.Printf("✓ SELECT 1 returned: %d\n", result)

	// Test 2: Range query
	rows, err := db.QueryContext(ctx, "SELECT * FROM RANGE(5)")
	if err != nil {
		return fmt.Errorf("range query failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		count++
	}
	fmt.Printf("✓ SELECT FROM RANGE(5) returned %d rows\n", count)

	// Test 3: Current user query
	var username string
	err = db.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&username)
	if err != nil {
		return fmt.Errorf("current user query failed: %w", err)
	}
	fmt.Printf("✓ Connected as user: %s\n", username)

	return nil
}

// Helper: Manual token exchange (for debugging/testing)
func manualTokenExchange(databricksHost, subjectToken string) (string, error) {
	exchangeURL := databricksHost
	if !strings.HasPrefix(exchangeURL, "http://") && !strings.HasPrefix(exchangeURL, "https://") {
		exchangeURL = "https://" + exchangeURL
	}
	if !strings.HasSuffix(exchangeURL, "/") {
		exchangeURL += "/"
	}
	exchangeURL += "oidc/v1/token"

	data := url.Values{}
	data.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
	data.Set("scope", "sql")
	data.Set("subject_token_type", "urn:ietf:params:oauth:token-type:jwt")
	data.Set("subject_token", subjectToken)

	req, err := http.NewRequest("POST", exchangeURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "*/*")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
		Scope       string `json:"scope"`
	}

	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", err
	}

	return tokenResp.AccessToken, nil
}

// Helper: Get token issuer from JWT (for logging)
func getTokenIssuer(tokenString string) string {
	parts := strings.Split(tokenString, ".")
	if len(parts) < 2 {
		return "not a JWT"
	}

	// Decode payload (second part)
	payload, err := decodeBase64(parts[1])
	if err != nil {
		return "invalid JWT"
	}

	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "invalid JWT"
	}

	if iss, ok := claims["iss"].(string); ok {
		return iss
	}

	return "unknown"
}

func decodeBase64(s string) ([]byte, error) {
	// Add padding if needed
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return io.ReadAll(strings.NewReader(s))
}

// U2MTokenProvider wraps U2M authenticator as a TokenProvider
type U2MTokenProvider struct {
	authenticator interface {
		Authenticate(*http.Request) error
	}
}

func (p *U2MTokenProvider) GetToken(ctx context.Context) (*tokenprovider.Token, error) {
	// Create a dummy request to trigger authentication
	req, err := http.NewRequestWithContext(ctx, "GET", "http://dummy", nil)
	if err != nil {
		return nil, err
	}

	// Authenticate will add Authorization header
	if err := p.authenticator.Authenticate(req); err != nil {
		return nil, err
	}

	// Extract token from Authorization header
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return nil, fmt.Errorf("no authorization header set")
	}

	// Parse "Bearer <token>"
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid authorization header format")
	}

	return &tokenprovider.Token{
		AccessToken: parts[1],
		TokenType:   parts[0],
	}, nil
}

func (p *U2MTokenProvider) Name() string {
	return "u2m-browser-oauth"
}
