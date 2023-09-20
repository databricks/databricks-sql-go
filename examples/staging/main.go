package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	dbsql "github.com/databricks/databricks-sql-go"
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
		dbsql.WithInitialNamespace("main", "staging_test"),
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()
	ctx := dbsql.StagingCtx{}.WithDefaults()
	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}

	localFile := "staging/file.csv"
	_, err1 := db.ExecContext(ctx, fmt.Sprintf(`PUT '%s' INTO '/Volumes/main/staging_test/e2etests/file1.csv' OVERWRITE`, localFile))
	if err1 != nil {
		fmt.Println(err1.Error())
		return
	}

}
