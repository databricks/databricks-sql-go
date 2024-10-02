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

func queryWithNamedParameters(db *sql.DB) {
	var p_bool bool
	var p_int int
	var p_double float64
	var p_float float32
	var p_date string

	err := db.QueryRow(`
		SELECT
			:p_bool AS col_bool,
			:p_int AS col_int,
			:p_double AS col_double,
			:p_float AS col_float,
			:p_date AS col_date
		`,
		dbsql.Parameter{Name: "p_bool", Value: true},
		dbsql.Parameter{Name: "p_int", Value: int(1234)},
		dbsql.Parameter{Name: "p_double", Type: dbsql.SqlDouble, Value: "3.14"},
		dbsql.Parameter{Name: "p_float", Type: dbsql.SqlFloat, Value: "3.14"},
		dbsql.Parameter{Name: "p_date", Type: dbsql.SqlDate, Value: "2017-07-23 00:00:00"},
	).Scan(&p_bool, &p_int, &p_double, &p_float, &p_date)

	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Printf("err: %v\n", err)
		}
	} else {
		fmt.Println(p_bool, p_int, p_double, p_float, p_date)
	}
}

func queryWithPositionalParameters(db *sql.DB) {
	var p_bool bool
	var p_int int
	var p_double float64
	var p_float float32
	var p_date string

	err := db.QueryRow(`
		SELECT
			:p_bool AS col_bool,
			:p_int AS col_int,
			:p_double AS col_double,
			:p_float AS col_float,
			:p_date AS col_date
		`,
		true,
		int(1234),
		"3.14",
		dbsql.Parameter{Type: dbsql.SqlFloat, Value: "3.14"},
		dbsql.Parameter{Type: dbsql.SqlDate, Value: "2017-07-23 00:00:00"},
	).Scan(&p_bool, &p_int, &p_double, &p_float, &p_date)

	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Printf("err: %v\n", err)
		}
	} else {
		fmt.Println(p_bool, p_int, p_double, p_float, p_date)
	}
}

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

	queryWithNamedParameters(db)
	queryWithPositionalParameters(db)
}
