# Go Databricks SQL Driver

Databricks SQL driver for Go's [database/sql](https://golang.org/pkg/database/sql) package.

## Usage

```go
import (
	"database/sql"
	"time"

	_ "github.com/databricks/databricks-sql-go"
)

db, err := sql.Open("databricks", "databricks://:dapi-secret-token@example.cloud.databricks.com/sql/1.0/endpoints/12345a1b2c3d456f")
if err != nil {
	panic(err)
}


rows, err := db.Query("SELECT 1")
```

(see additional usage examples in [examples](https://github.com/databricks/databricks-sql-go/tree/main/examples))

## DSN (Data Source Name)

The Data Source Name expected is of the following format:

```
databricks://:[your token]@[Workspace hostname][Endpoint HTTP Path]
```

You can set HTTP Timeout value by appending a `timeout` query parameter (in milliseconds) and you can set max rows to retrieve by setting the `maxRows` query parameter:

```
databricks://:[your token]@[Workspace hostname][Endpoint HTTP Path]?timeout=1000&maxRows=1000
```

## Testing

To run only unit tests you can use `go test`, but if you want to run tests against a SQL endpoint, youw will need to pass a DSN environment variable:

```
$ DATABRICKS_DSN="databricks://:dapi-secret-token@example.cloud.databricks.com/sql/1.0/endpoints/12345a1b2c3d456f" go test
```

## License

Apache 2.0.
