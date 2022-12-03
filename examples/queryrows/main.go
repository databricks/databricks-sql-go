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
	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}

	rows, err1 := db.QueryContext(ctx, `select * from default.intervals`)
	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Println(err1.Error())
			return
		}
	}

	colNames, _ := rows.Columns()
	for i := range colNames {
		fmt.Printf("%d: %s\n", i, colNames[i])
	}

	// var bigInt int64
	// var binary sql.RawBytes
	// var boolVal bool
	// var dateVal time.Time
	// var decimal sql.RawBytes
	// var doubleVal float64
	// var floatVal float32
	// var intVal int32
	// var smallInt int16
	// var stringVal string
	// var timeVal time.Time
	// var tinyInt int8

	// var array_val, map_val, struct_val sql.RawBytes

	var res1, res2 string
	for rows.Next() {
		err := rows.Scan(&res1, &res2)
		if err != nil {
			fmt.Println(err)
			rows.Close()
			return
		}
		fmt.Printf("%v, %v\n", res1, res2)
	}

}
