package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v12/arrow/ipc"
	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file if it exists
	// This will not override existing environment variables
	_ = godotenv.Load()

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
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, _ := db.Conn(ctx)
	defer conn.Close()

	query := `SELECT * FROM samples.nyctaxi.trips LIMIT 1000`

	var rows driver.Rows
	err = conn.Raw(func(d interface{}) error {
		var err error
		rows, err = d.(driver.QueryerContext).QueryContext(ctx, query, nil)
		return err
	})

	if err != nil {
		log.Fatal("Failed to execute query: ", err)
	}
	defer rows.Close()

	// Get the IPC stream iterator
	ipcStreams, err := rows.(dbsqlrows.Rows).GetArrowIPCStreams(ctx)
	if err != nil {
		log.Fatal("Failed to get IPC streams: ", err)
	}
	defer ipcStreams.Close()

	// Get the schema bytes
	schemaBytes, err := ipcStreams.SchemaBytes()
	if err != nil {
		log.Fatal("Failed to get schema bytes: ", err)
	}
	log.Printf("Schema bytes length: %d", len(schemaBytes))

	// Process IPC streams
	streamCount := 0
	recordCount := 0

	for ipcStreams.HasNext() {
		// Get the next IPC stream
		reader, err := ipcStreams.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal("Failed to get next IPC stream: ", err)
		}

		streamCount++

		// Create an IPC reader for this stream
		ipcReader, err := ipc.NewReader(reader)
		if err != nil {
			log.Fatal("Failed to create IPC reader: ", err)
		}

		// Process records in the stream
		for ipcReader.Next() {
			record := ipcReader.Record()
			recordCount++
			log.Printf("Stream %d, Record %d: %d rows, %d columns",
				streamCount, recordCount, record.NumRows(), record.NumCols())

			// Don't forget to release the record when done
			record.Release()
		}

		if err := ipcReader.Err(); err != nil {
			log.Fatal("IPC reader error: ", err)
		}

		ipcReader.Release()
	}

	log.Printf("Processed %d IPC streams with %d total records", streamCount, recordCount)
}
