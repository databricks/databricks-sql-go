package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/joho/godotenv"
)

func main() {
	// Opening a driver typically will not attempt to connect to the database.
	err := godotenv.Load()

	if err != nil {
		log.Fatal(err.Error())
	}
	db, err := sql.Open("databricks", fmt.Sprintf("token:%s@%s:%s%s", os.Getenv("DATABRICKS_ACCESSTOKEN"), os.Getenv("DATABRICKS_HOST"), os.Getenv("DATABRICKS_PORT"), os.Getenv("DATABRICKS_HTTPPATH")))

	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}
	defer db.Close()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()
	var res int
	err1 := db.QueryRowContext(ctx1, `SELECT id FROM RANGE(100000000) ORDER BY RANDOM() + 2 asc`).Scan(&res)
	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Printf("err: %v\n", err1)
		}
	}
	fmt.Printf("result: %d\n", res)

}
