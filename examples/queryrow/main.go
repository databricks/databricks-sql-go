package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

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
	)
	if err != nil {
		// This will not be a connection error, but a DSN parse error or
		// another initialization error.
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)

	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()
	ctx := context.Background()
	var res float64
	err1 := db.QueryRowContext(ctx, `select max(carat) from default.diamonds`).Scan(res)

	if err1 != nil {
		if err1 == sql.ErrNoRows {
			fmt.Println("not found")
			return
		} else {
			fmt.Printf("db down: %v\n", err1)
		}
	}
	fmt.Println(res)

}