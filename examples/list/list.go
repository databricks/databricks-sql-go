package main

// Simple program to list databases and the tables

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/databricks/databricks-sql-go"
)

func main() {
	dsn := os.Getenv("DATABRICKS_DSN")
	if dsn == "" {
		log.Fatalf("Please provide a connection string by setting the DATABRICKS_DSN environment variable.")
	}

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatalf("Could not connect to %s: %s", dsn, err)
	}
	defer db.Close()

	ctx := context.Background()

	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		log.Fatal(err)
	}

	var name string

	databases := make([]string, 0)

	for rows.Next() {
		if err := rows.Scan(&name); err != nil {
			log.Println("Error scanning database name:")
			log.Fatal(err)
		}
		databases = append(databases, name)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("Databases:")

	for i, database := range databases {
		log.Printf("%d. %s", i, database)
	}

	stmt, err := db.PrepareContext(ctx, "SHOW TABLES IN @p1")
	if err != nil {
		log.Fatal(err)
	}

	tbl := struct {
		database    string
		name        string
		isTemporary bool
	}{}

	for _, d := range databases {
		rows, err := stmt.QueryContext(ctx, d)
		if err != nil {
			log.Printf("error in querying database %s: %s", d, err.Error())
			continue
		}

		log.Printf("Tables in %s:", d)

		for rows.Next() {
			if err := rows.Scan(&tbl.database, &tbl.name, &tbl.isTemporary); err != nil {
				log.Println(err)
				continue
			}
			log.Printf("  %s", tbl.name)
		}
	}
}
