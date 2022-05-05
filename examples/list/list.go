package main

// Simple program to list databases and the tables

import (
	"context"
	"database/sql"
	"log"

	dbsql "github.com/arikfr/go-dbsql"
)

func main() {

	opts := dbsql.DefaultOptions

	opts.Host = ""

	// enable LDAP authentication:
	opts.Token = ""
	opts.HTTPPath = "/sql/1.0/endpoints/5c89f447c476a5a8"

	connector := dbsql.NewConnector(&opts)

	db := sql.OpenDB(connector)
	db, err := sql.Open("databricks", dsn)
	defer db.Close()

	ctx := context.Background()

	log.Println("select 1...")
	rows, err := db.QueryContext(ctx, "select 1")
	if err != nil {
		log.Fatal(err)
	}

	var i int32
	for rows.Next() {
		log.Println("select 1 returned seomthing")

		if err := rows.Scan(&i); err != nil {
			log.Fatal(err)
		}

		log.Println(i)
	}

	rows, err = db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		log.Fatal(err)
	}

	r := struct {
		name    string
		comment string
	}{}

	databases := make([]string, 0) // databases will contain all the DBs to enumerate later
	for rows.Next() {
		log.Println("- rows.Next()")
		// if err := rows.Scan(&r.name, &r.comment); err != nil {
		if err := rows.Scan(&r.name); err != nil {
			log.Println("err with scan")
			log.Fatal(err)
		}
		databases = append(databases, r.name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println("List of Databases", databases)

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

		tables := make([]string, 0)
		// columns, err := rows.ColumnTypes()
		// for i, col := range columns {
		// 	log.Printf("%d %s: %s", i, col.Name(), col.DatabaseTypeName())
		// }

		for rows.Next() {
			if err := rows.Scan(&tbl.database, &tbl.name, &tbl.isTemporary); err != nil {
				log.Println(err)
				continue
			}
			tables = append(tables, tbl.name)
		}
		log.Printf("List of Tables in Database %s: %v\n", d, tables)
	}
}
