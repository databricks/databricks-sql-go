# Go Databricks SQL Driver

Databricks SQL driver for Go's [database/sql](https://golang.org/pkg/database/sql) package.

## Usage

```go
import (
	"database/sql"
	"time"

	_ "github.com/arikfr/go-dbsql"
)

db, err := sql.Open("databricks", "databricks://:dapi-secret-token@example.cloud.databricks.com/sql/1.0/endpoints/12345a1b2c3d456f")
if err != nil {
	panic(err)
}


rows, err := db.Query("SELECT 1")
```

(see additional usage examples in [examples](https://github.com/arikfr/go-dbsql/tree/main/examples))

## DSN (Data Source Name)

The Data Source Name expected is of the following format:

```
databricks://:[your token]@[Workspace hostname][Endpoint HTTP Path]
```

You can set HTTP Timeout value by appending a `timeout` query parameter and you can set max rows to retrieve by setting the `maxRows` query parameter:

```
databricks://:[your token]@[Workspace hostname][Endpoint HTTP Path]?timeout=1000&maxRows=1000
```

## Testing

(set DSN)

## License

TBD.
