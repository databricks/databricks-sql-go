# Databricks SQL Driver for Go (Beta)


![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)

## Description

This repo contains a Databricks SQL Driver for Go's [database/sql](https://golang.org/pkg/database/sql) package. It can be used to connect and query Databricks clusters and SQL Warehouses.

**NOTE: This Driver is Beta.**

## Documentation

Full documentation is not yet available. See below for usage examples.

## Usage

```go
import (
	"database/sql"
	"time"

	_ "github.com/databricks/databricks-sql-go"
)

db, err := sql.Open("databricks", "token:********@********.databricks.com/sql/1.0/endpoints/********")
if err != nil {
	panic(err)
}


rows, err := db.Query("SELECT 1")
```

Additional usage examples are available [here](https://github.com/databricks/databricks-sql-go/tree/main/examples).

### DSN (Data Source Name)

The DSN format is:

```
token:[your token]@[Workspace hostname][Endpoint HTTP Path]?param=value
```

You can set query timeout value by appending a `timeout` query parameter (in seconds) and you can set max rows to retrieve per network request by setting the `maxRows` query parameter:

```
token:[your token]@[Workspace hostname][Endpoint HTTP Path]?timeout=1000&maxRows=1000
```

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
