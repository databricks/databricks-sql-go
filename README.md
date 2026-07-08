# Databricks SQL Driver for Go


![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)

## Description

This repo contains a Databricks SQL Driver for Go's [database/sql](https://golang.org/pkg/database/sql) package. It can be used to connect and query Databricks clusters and SQL Warehouses.

## Documentation

See `doc.go` for full documentation or the Databrick's documentation for [SQL Driver for Go](https://docs.databricks.com/dev-tools/go-sql-driver.html).

## Usage

```go
import (
  "context"
  "database/sql"
  _ "github.com/databricks/databricks-sql-go"
)

db, err := sql.Open("databricks", "token:********@********.databricks.com:443/sql/1.0/endpoints/********")
if err != nil {
  panic(err)
}
defer db.Close()


rows, err := db.QueryContext(context.Background(), "SELECT 1")
defer rows.Close()
```

Additional usage examples are available [here](https://github.com/databricks/databricks-sql-go/tree/main/examples).

### Connecting with DSN (Data Source Name)

The DSN format is:

```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?param=value
```

The `token:[your token]@` prefix authenticates with a personal access token (PAT). For other authentication types, omit the prefix and use the `authType`, `clientID`/`clientSecret`, or `accessToken` parameters described below.

#### Supported connection parameters

Optional parameters can be appended to the DSN as `?param=value&param=value`:

| Parameter | Description | Default |
|---|---|---|
| `catalog` | Sets the initial catalog name in the session | |
| `schema` | Sets the initial schema name in the session | |
| `maxRows` | Max rows fetched per network request | `100000` |
| `timeout` | Server-side query execution timeout, in seconds | no timeout |
| `userAgentEntry` | Identifies your application (partners/ISVs). Format: `<isv-name+product-name>` | |
| `useCloudFetch` | Enables Cloud Fetch to fetch large results in parallel via cloud storage | `true` |
| `maxDownloadThreads` | Number of concurrent Cloud Fetch download goroutines | `10` |
| `authType` | Authentication type. One of `Pat`, `OauthM2M`, `OauthU2M` | inferred from params |
| `accessToken` | Personal access token. Used when `authType=Pat` | |
| `clientID` | Service principal client ID. Used with OAuth M2M | |
| `clientSecret` | Service principal client secret. Used with OAuth M2M | |

Any parameter not listed above (e.g. `ansi_mode`, `timezone`) is passed through as a session parameter.

For example, to set a query timeout and max rows per request:

```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?timeout=1000&maxRows=1000
```

#### Cloud Fetch

Cloud Fetch (enabled by default) increases the performance of extracting large query results by fetching data in parallel via cloud storage (more info [here](https://www.databricks.com/blog/2021/08/11/how-we-achieved-high-bandwidth-connectivity-with-bi-tools.html)). You can set the number of concurrently fetching goroutines with `maxDownloadThreads`:

```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?useCloudFetch=true&maxDownloadThreads=3
```
To disable Cloud Fetch (e.g., when handling smaller datasets or to avoid additional overhead), append `useCloudFetch=false`:
```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?useCloudFetch=false
```

#### Authenticating with OAuth (client ID and secret)

To authenticate with OAuth machine-to-machine (M2M) credentials instead of a personal access token, leave the `token:...@` prefix off the DSN and pass the service principal's `clientID` and `clientSecret` as query parameters:

```
[Workspace hostname]:[Port number][Endpoint HTTP Path]?authType=OauthM2M&clientID=[your client ID]&clientSecret=[your client secret]
```

The `authType=OauthM2M` parameter is optional — supplying `clientID` and `clientSecret` is enough to select OAuth M2M authentication.

To authenticate interactively with OAuth user-to-machine (U2M) credentials (opens a browser login flow), set `authType=OauthU2M` with no token or client credentials:

```
[Workspace hostname]:[Port number][Endpoint HTTP Path]?authType=OauthU2M
```

#### Setting the user agent

To identify your application (e.g. for partners/ISVs), append a `userAgentEntry` query parameter with the format `<isv-name+product-name>`:

```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?userAgentEntry=[your-isv-name+product-name]
```

### Telemetry Configuration (Optional)

The driver includes optional telemetry to help improve performance and reliability. Telemetry is **disabled by default** and requires explicit opt-in.

**Opt-in to telemetry** (respects server-side feature flags):
```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?enableTelemetry=true
```

**Opt-out of telemetry** (explicitly disable):
```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?enableTelemetry=false
```

**What data is collected:**
- ✅ Query latency and performance metrics
- ✅ Error codes (not error messages)
- ✅ Feature usage (CloudFetch, LZ4, etc.)
- ✅ Driver version and environment info

**What is NOT collected:**
- ❌ SQL query text
- ❌ Query results or data values
- ❌ Table/column names
- ❌ User identities or credentials

Telemetry has < 1% performance overhead and uses circuit breaker protection to ensure it never impacts your queries. For more details, see `telemetry/DESIGN.md` and `telemetry/TROUBLESHOOTING.md`.

### Connecting with a new Connector

You can also connect with a new connector object. For example:

```go
import (
"database/sql"
  _ "github.com/databricks/databricks-sql-go"
)

connector, err := dbsql.NewConnector(
  dbsql.WithServerHostname(<Workspace hostname>),
  dbsql.WithPort(<Port number>),
  dbsql.WithHTTPPath(<Endpoint HTTP Path>),
  dbsql.WithAccessToken(<your token>)
)
if err != nil {
  log.Fatal(err)
}
db := sql.OpenDB(connector)
defer db.Close()
```

View `doc.go` or `connector.go` to understand all the functional options available when creating a new connector object.

## Develop

### Lint
We use `golangci-lint` as the lint tool. If you use vs code, just add the following settings:
``` json
{
    "go.lintTool": "golangci-lint",
    "go.lintFlags": [
        "--fast"
    ]
}
```
### Unit Tests

```bash
go test
```

## Issues

If you find any issues, feel free to create an issue or send a pull request directly.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## License

[Apache 2.0](https://github.com/databricks/databricks-sql-go/blob/main/LICENSE)
