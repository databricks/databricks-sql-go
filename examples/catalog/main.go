package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type spec struct {
	Host        string `envconfig:"HOST"`
	Port        int    `default:"443" envconfig:"PORT"`
	HTTPPath    string `envconfig:"HTTPPATH"`
	AccessToken string `envconfig:"ACCESSTOKEN"`
}

func main() {
	// Opening a driver typically will not attempt to connect to the database.
	godotenv.Load()
	var s spec
	err := envconfig.Process("dbsql", &s)
	if err != nil {
		log.Fatal(err.Error())
	}
	connector, err := dbsql.NewConnector(
		dbsql.WithServerHostname(s.Host),
		dbsql.WithPort(s.Port),
		dbsql.WithHTTPPath(s.HTTPPath),
		dbsql.WithAccessToken(s.AccessToken),
		dbsql.WithInitialNamespace("quickstart_catalog1", "quickstart_schema1"),
	)
	// quickstart_catalog1
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Println(err)
	}

	rows, err1 := db.QueryContext(context.Background(), `select * from quickstart_table1`)
	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Println(err1.Error())
			return
		}
	}
	var res string
	for rows.Next() {
		err := rows.Scan(&res)
		if err != nil {
			fmt.Println(err)
			rows.Close()
			return
		}
		fmt.Println(res)
	}

}
