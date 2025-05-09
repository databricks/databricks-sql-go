# OAuth Token Caching in Databricks SQL Go Driver

This document explains how to use the OAuth token caching feature in the Databricks SQL Go Driver.

## Overview

The OAuth token caching feature allows the driver to store OAuth tokens locally, so that users don't need to go through the browser authentication flow every time they connect to Databricks SQL. This is especially useful for:

- Command-line applications that are frequently run
- Applications that need to reconnect to Databricks SQL after a connection is closed
- Improving user experience by reducing the number of authentication prompts

## How It Works

1. When a user authenticates using OAuth U2M (User-to-Machine) authentication, the driver obtains an access token and a refresh token.
2. The tokens are securely stored in a local file in the user's home directory.
3. On subsequent connections, the driver checks if there's a valid cached token.
4. If a valid token exists, it's used directly without requiring browser authentication.
5. If the access token is expired but a refresh token is available, the driver automatically refreshes the access token.
6. If no valid token is found or the refresh fails, the driver falls back to the browser authentication flow.

## Configuration Options

### Using the Connector API

```go
connector, err := dbsql.NewConnector(
    // Basic connection parameters
    dbsql.WithServerHostname("your-workspace.cloud.databricks.com"),
    dbsql.WithPort(443),
    dbsql.WithHTTPPath("/sql/1.0/warehouses/your-warehouse-id"),
    
    // Token cache options
    dbsql.WithTokenCache(true), // Enable token caching (default is true)
    dbsql.WithTokenCachePath("/path/to/custom/cache"), // Optional: specify a custom path
    
    // OAuth U2M authentication with options
    dbsql.WithUserAuthenticationOptions(
        2*time.Minute, // Timeout for browser authentication
        u2m.WithScopes([]string{"offline_access", "sql"}), // Optional: specify scopes
    ),
)
```

### Using the DSN String

```go
dsn := "https://your-workspace.cloud.databricks.com:443/sql/1.0/warehouses/your-warehouse-id?authType=oauthU2M&useTokenCache=true&tokenCachePath=/path/to/custom/cache&oauthScopes=offline_access,sql"
db, err := sql.Open("databricks", dsn)
```

## Token Cache Location

By default, tokens are stored in:

```
$HOME/.config/databricks-sql-go/oauth/<hash>
```

Where `<hash>` is a SHA-256 hash of the host, client ID, and scopes, ensuring that different configurations use different cache files.

## Security Considerations

- Token cache files are created with permissions that restrict access to the current user only (0600).
- The cache directory is created with permissions that restrict access to the current user only (0700).
- Token cache files contain sensitive information and should be protected accordingly.
- If you're concerned about storing tokens on disk, you can disable token caching with `WithTokenCache(false)`.

## Troubleshooting

If you're experiencing issues with token caching:

1. Check if the token cache file exists in the expected location.
2. Ensure the user running the application has read/write permissions to the cache directory.
3. Try deleting the cache file to force a new authentication flow.
4. Enable debug logging to see more detailed information about token cache operations.

## Example

See the `examples/oauth_token_cache/main.go` file for a complete example of using token caching with the Databricks SQL Go Driver.