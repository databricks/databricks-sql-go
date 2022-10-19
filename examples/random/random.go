package main

import (
	"context"
	"database/sql"
	_ "github.com/databricks/databricks-sql-go"
	"log"
	"os"
)

func main() {
	dsn := os.Getenv("DATABRICKS_DSN")
	//dsn := "databricks://:dapibad4be4021a53249cda07bd152cd715a@e2-dogfood.staging.cloud.databricks.com/sql/1.0/endpoints/e5edb16633f87f9b?runAsync=false"
	if dsn == "" {
		log.Fatalf("Please provide a connection string by setting the DATABRICKS_DSN environment variable.")
	}

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatalf("Could not connect to %s: %s", dsn, err)
	}
	defer db.Close()

	ctx := context.Background()

	rows, err := db.QueryContext(ctx, "SELECT id FROM RANGE(1000000) ORDER BY RANDOM() + 2 asc")
	if err != nil {
		log.Fatal(err)
	}

	var id string

	ids := make([]string, 0)

	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			log.Println("Error getting id")
			log.Fatal(err)
		}
		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("Ids:")

	for i, id := range ids {
		log.Printf("%d. %s", i, id)
	}
}
