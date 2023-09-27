package main

import (
	"context"
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
	ctx := context.Background()
	var res string
	err1 := db.QueryRowContext(ctx, `SELECT
	:p_bool AS col_bool,
	:p_int AS col_int,
	:p_double AS col_double,
	:p_floatfloat AS col_float`,
		dbsql.DBSqlParam{Name: "p_bool", Value: true},
		dbsql.DBSqlParam{Name: "p_int", Value: int(1234)},
		dbsql.DBSqlParam{Name: "p_double", Type: dbsql.Double, Value: 3.14},
		dbsql.DBSqlParam{Name: "p_float", Type: dbsql.Float, Value: 3.14}).Scan(&res)

	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Printf("err: %v\n", err1)
		}
	}
	fmt.Println(res)

}
