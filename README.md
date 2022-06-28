# Databricks SQL Driver for Go (Beta)


![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)

## Description

This repo contains a Databricks SQL Driver for Go's [database/sql](https://golang.org/pkg/database/sql) package. It can be used to connect and query Databricks clusters and SQL Warehouses. This project is a fork of [go-impala](https://github.com/bippio/go-impala).

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

db, err := sql.Open("databricks", "databricks://:dapi********@********.databricks.com/sql/1.0/endpoints/********")
if err != nil {
	panic(err)
}


rows, err := db.Query("SELECT 1")
```

Additional usage examples are available [here](https://github.com/databricks/databricks-sql-go/tree/main/examples).

### DSN (Data Source Name)

The DSN format is:

```
databricks://:[your token]@[Workspace hostname][Endpoint HTTP Path]
```

You can set HTTP Timeout value by appending a `timeout` query parameter (in milliseconds) and you can set max rows to retrieve by setting the `maxRows` query parameter:

```
databricks://:[your token]@[Workspace hostname][Endpoint HTTP Path]?timeout=1000&maxRows=1000
```

## Testing

### Unit Tests

```bash
go test
```

### e2e Tests

To run tests against a SQL warehouse, you need to pass a DSN environment variable first:

```
$ DATABRICKS_DSN="databricks://:dapi-secret-token@example.cloud.databricks.com/sql/1.0/endpoints/12345a1b2c3d456f" go test
```

## Issues

If you find any issues, feel free to create an issue or send a pull request directly.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## License

[Apache 2.0](https://github.com/databricks/databricks-sql-nodejs/blob/master/LICENSE)
