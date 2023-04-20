package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/joho/godotenv"
)

func main() {
	// Opening a driver typically will not attempt to connect to the database.
	err := godotenv.Load()

	if err != nil {
		log.Fatal(err.Error())
	}

	port, err := strconv.Atoi(os.Getenv("DATABRICKS_PORT"))
	if err != nil {
		log.Fatal(err.Error())
	}

	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(os.Getenv("DATABRICKS_HOST")),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(os.Getenv("DATABRICKS_HTTPPATH")),
		dbsql.WithAccessToken(os.Getenv("DATABRICKS_ACCESSTOKEN")),
	)
	handleErr(err)

	db := sql.OpenDB(connector)
	defer db.Close()

	// test the connection
	err = db.Ping()
	handleErr(err)

	// create a cancellable context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// make sure that cancel will be called to free any resources
	// even if the function exits on a panic
	defer cancel()

	// Add a correlation Id to the context.  The correlation id will appear in any logged messages.
	// The correlation Id can also be retrieved as a property of any DBError (the base type for databricks errors).
	ctx = driverctx.NewContextWithCorrelationId(ctx, "my correlation Id")

	// Add a callback to get the connection Id. The connection id will appear in any logged messages associated
	// with the connection.
	// The connection Id can also be retrieved as a property of any DBError (the base type for databricks errors).
	var connId string
	connIdCallback := func(id string) {
		connId = id
	}
	ctx = driverctx.NewContextWithConnIdCallback(ctx, connIdCallback)

	// Add a callback to get the query Id. The query id will appear in any logged messages associated
	// with the query.  The query Id can also be retrieved as a property of DBExecutionError.
	var queryId string
	queryIdCallback := func(id string) {
		queryId = id
	}
	ctx = driverctx.NewContextWithQueryIdCallback(ctx, queryIdCallback)

	var rows *sql.Rows
	maxRetries := 3
	shouldTry := true

	// We want to retry running the query if an error is returned where IsRetryable() is true up
	// to the maximum number of retries.
	for i := 0; i < maxRetries && shouldTry; i++ {
		var err1 error
		var wait time.Duration

		rows, err1 = db.QueryContext(ctx, `select * from default.Intervals`)

		// Check if the error is retryable and if there is a wait before
		// trying again.
		if shouldTry, wait = isRetryable(err1); shouldTry {
			fmt.Printf("query failed, retrying after %f seconds", wait.Seconds())
			time.Sleep(wait)
		} else {
			// handle the error, which may be nil
			handleErr(err1)
		}
	}

	// At this point the query completed successfully
	defer rows.Close()

	fmt.Printf("conn Id: %s, query Id: %s\n", connId, queryId)

	colNames, _ := rows.Columns()
	for i := range colNames {
		fmt.Printf("%d: %s\n", i, colNames[i])
	}

	var res1, res2 string
	for rows.Next() {
		err := rows.Scan(&res1, &res2)
		handleErr(err)
		fmt.Printf("%v, %v\n", res1, res2)
	}

}

// If the error is not nil extract/ databricks specific error information and then
// terminate the program.
func handleErr(err error) {
	if err == nil {
		return
	}

	// If the error chain contains a databricks error get the correlation and connection Id
	// and display them.
	if errors.Is(err, dbsqlerr.DatabricksError) {
		correlationId, connectionId := getCorrelationAndConnectionId(err)
		fmt.Printf("correlationId: %s, connection Id: %s\n", correlationId, connectionId)
	}

	// If the error chain contains an ExecutionError get the query Id and SQL state
	// and display them.
	if errors.Is(err, dbsqlerr.ExecutionError) {
		queryId, sqlState := getQueryIdAndSQLState(err)
		fmt.Printf("queryId: %s, sqlState: %s\n", queryId, sqlState)
	}

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		fmt.Println("context deadline exceeded")
	case errors.Is(err, context.Canceled):
		fmt.Println("context cancelled")
	case errors.Is(err, dbsqlerr.DriverError):
		fmt.Println("databricks driver error")
	case errors.Is(err, dbsqlerr.RequestError):
		fmt.Println("databricks request error")
	case errors.Is(err, dbsqlerr.ExecutionError):
		fmt.Println("databricks SQL execution error")
	case errors.Is(err, sql.ErrNoRows):
		fmt.Println("query returned no rows")
		return
	}

	fmt.Println(err.Error())

	// Use panic so that any deferred calls will still be made.
	panic(err)
}

// Use errors.As to extract a DBError from the error chain and return the associated
// correlation and connection Ids
func getCorrelationAndConnectionId(err error) (correlationId, connectionId string) {
	var dbErr dbsqlerr.DBError
	if errors.As(err, &dbErr) {
		correlationId = dbErr.CorrelationId()
		connectionId = dbErr.ConnectionId()
	}

	return
}

// Use errors.As to extract a DBExecutionError from the error chain and return the associated
// query Id and sql state
func getQueryIdAndSQLState(err error) (queryId, sqlState string) {
	var dbExecErr dbsqlerr.DBExecutionError
	if errors.As(err, &dbExecErr) {
		queryId = dbExecErr.QueryId()
		sqlState = dbExecErr.SqlState()
	}

	return
}

// Use errors.As to extract a DBError from the error chain and return the associated
// values for isRetryable and retryAfter
func isRetryable(err error) (isRetryable bool, retryAfter time.Duration) {
	var dbErr dbsqlerr.DBError
	if errors.As(err, &dbErr) {
		isRetryable = dbErr.IsRetryable()
		retryAfter = dbErr.RetryAfter()
	}
	return
}
