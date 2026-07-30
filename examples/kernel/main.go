// Command kernel demonstrates the experimental SEA-via-kernel backend.
//
// The kernel backend is opt-in and compiled in only under the databricks_kernel
// build tag with CGO enabled, and it links the Rust kernel static library. Build
// the library first, then run this example with the tag:
//
//	make -C ../.. kernel-lib                       # build the pinned kernel .a + header
//	CGO_ENABLED=1 go run -tags databricks_kernel .
//
// Without the tag, the program still compiles (it uses only the stable public
// API), but WithUseKernel(true) returns an error wrapping
// dbsqlerr.ErrKernelNotCompiled at connect — which this example detects and
// reports rather than treating as a hard failure.
//
// Required environment (a .env file in this directory is loaded automatically):
//
//	DATABRICKS_HOST         workspace hostname (no scheme)
//	DATABRICKS_ACCESSTOKEN  personal access token
//	DATABRICKS_HTTPPATH     warehouse http path (or set DATABRICKS_WAREHOUSEID)
//	DATABRICKS_WAREHOUSEID  bare warehouse id (kernel routes by this when set)
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	dbsql "github.com/databricks/databricks-sql-go"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	host := os.Getenv("DATABRICKS_HOST")
	token := os.Getenv("DATABRICKS_ACCESSTOKEN")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	warehouseID := os.Getenv("DATABRICKS_WAREHOUSEID")
	if host == "" || token == "" || (httpPath == "" && warehouseID == "") {
		log.Fatal("set DATABRICKS_HOST, DATABRICKS_ACCESSTOKEN, and DATABRICKS_HTTPPATH (or DATABRICKS_WAREHOUSEID)")
	}

	opts := []dbsql.ConnOption{
		dbsql.WithServerHostname(host),
		dbsql.WithAccessToken(token),
		// Select the SEA-via-kernel backend (same as the useKernel=true DSN param).
		dbsql.WithUseKernel(true),
	}
	if httpPath != "" {
		opts = append(opts, dbsql.WithHTTPPath(httpPath))
	}
	// The kernel routes by bare warehouse id (preferred over the http path);
	// the Thrift backend ignores this option.
	if warehouseID != "" {
		opts = append(opts, dbsql.WithWarehouseID(warehouseID))
	}

	connector, err := dbsql.NewConnector(opts...)
	if err != nil {
		log.Fatalf("NewConnector: %v", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		// The kernel backend isn't linked into a build without the tag; detect
		// that case with errors.Is (a caller could fall back to Thrift here).
		if errors.Is(err, dbsqlerr.ErrKernelNotCompiled) {
			log.Fatal("kernel backend not compiled in — rebuild with `make kernel-lib` " +
				"then `CGO_ENABLED=1 go run -tags databricks_kernel .`")
		}
		log.Fatalf("ping: %v", err)
	}
	fmt.Println("connected via the SEA-via-kernel backend")

	var version string
	if err := db.QueryRowContext(ctx, "SELECT current_version()").Scan(&version); err != nil {
		log.Fatalf("query: %v", err)
	}
	fmt.Printf("server version: %s\n", version)

	// Options the kernel can't honor are rejected with an error wrapping
	// ErrNotSupportedByKernel, never silently ignored. Staging (PUT/GET/REMOVE on a
	// Unity Catalog volume) is one such feature, rejected at execute.
	_, err = db.ExecContext(ctx, "PUT '/tmp/local.csv' INTO '/Volumes/main/default/vol/f.csv' OVERWRITE")
	if errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
		fmt.Println("staging is not supported on the kernel backend (use Thrift for staging)")
	} else if err != nil {
		fmt.Printf("staging: %v\n", err)
	}
}
