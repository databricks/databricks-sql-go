//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// thriftTestDB opens the same warehouse over the default Thrift backend, for
// parity comparison against the kernel backend.
func thriftTestDB(t *testing.T) *sql.DB {
	t.Helper()
	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")
	token := os.Getenv("DATABRICKS_TOKEN")
	if host == "" || httpPath == "" || token == "" {
		t.Skip("set DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN for the parity test")
	}
	connector, err := NewConnector(
		WithServerHostname(host),
		WithHTTPPath(httpPath),
		WithAccessToken(token),
	)
	if err != nil {
		t.Fatalf("NewConnector (thrift): %v", err)
	}
	return sql.OpenDB(connector)
}

// TestKernelThriftParity runs the same scalar query through both backends and
// asserts identical row output. The two scanners are intentionally separate, so
// this golden comparison is the guarantee that they render the scalar types
// equivalently.
func TestKernelThriftParity(t *testing.T) {
	const query = "SELECT CAST(7 AS BIGINT), CAST(2.5 AS DOUBLE), 'parity', " +
		"CAST(NULL AS STRING), CAST(9.99 AS DECIMAL(5,2)), true"

	kernelDB := kernelTestDB(t)
	defer kernelDB.Close()
	thriftDB := thriftTestDB(t)
	defer thriftDB.Close()

	kernelRow := scanOneRowAsStrings(t, kernelDB, query)
	thriftRow := scanOneRowAsStrings(t, thriftDB, query)

	if len(kernelRow) != len(thriftRow) {
		t.Fatalf("column count differs: kernel=%d thrift=%d", len(kernelRow), len(thriftRow))
	}
	for i := range kernelRow {
		if kernelRow[i] != thriftRow[i] {
			t.Errorf("col %d differs: kernel=%q thrift=%q", i, kernelRow[i], thriftRow[i])
		}
	}
}

// scanOneRowAsStrings scans the first row into a []string via sql.RawBytes, so a
// NULL renders as "<nil>" and every value is compared in its wire form,
// independent of Go-type coercion differences between the backends.
func scanOneRowAsStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("no row: %v", rows.Err())
	}
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		t.Fatalf("scan: %v", err)
	}
	out := make([]string, len(cols))
	for i, b := range raw {
		if b == nil {
			out[i] = "<nil>"
		} else {
			out[i] = string(b)
		}
	}
	return out
}
