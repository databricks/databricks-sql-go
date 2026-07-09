//go:build cgo && databricks_kernel

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"testing"
	"time"
)

// kernelTestDB opens a kernel-backed *sql.DB from DATABRICKS_HOST /
// DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN, or skips when they are unset. It goes
// through the standard connector with WithUseKernel(true) — the same path a real
// consumer uses — not a kernel-only connector.
func kernelTestDB(t *testing.T) *sql.DB {
	t.Helper()
	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")
	token := os.Getenv("DATABRICKS_TOKEN")
	if host == "" || httpPath == "" || token == "" {
		t.Skip("set DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN for the kernel e2e")
	}
	connector, err := NewConnector(
		WithServerHostname(host),
		WithHTTPPath(httpPath),
		WithAccessToken(token),
		WithUseKernel(true),
	)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	return sql.OpenDB(connector)
}

// TestKernelE2ESelect1 is the smallest end-to-end proof: PAT session over the
// kernel, execute, scan one scalar row.
func TestKernelE2ESelect1(t *testing.T) {
	db := kernelTestDB(t)
	defer db.Close()

	var got int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 = %d, want 1", got)
	}
}

// TestKernelE2EDataTypes scans each supported scalar type in its own subtest, so
// a failure names the exact type rather than being masked by others in a shared
// row. Each case selects a single value and compares the scanned result. NULL is
// covered as its own case.
func TestKernelE2EDataTypes(t *testing.T) {
	db := kernelTestDB(t)
	defer db.Close()

	cases := []struct {
		name string
		expr string       // the single SELECT expression
		want driver.Value // expected scanned value (nil for SQL NULL)
	}{
		{"bigint", "CAST(42 AS BIGINT)", int64(42)},
		{"int", "CAST(7 AS INT)", int64(7)},
		{"smallint", "CAST(3 AS SMALLINT)", int64(3)},
		{"tinyint", "CAST(1 AS TINYINT)", int64(1)},
		{"double", "CAST(3.5 AS DOUBLE)", float64(3.5)},
		{"float", "CAST(1.5 AS FLOAT)", float64(1.5)},
		{"boolean", "true", true},
		{"string", "'hi'", "hi"},
		{"binary", "CAST('abc' AS BINARY)", []byte("abc")},
		{"decimal_exact", "CAST(1.25 AS DECIMAL(5,2))", "1.25"},
		{"date", "CAST('2026-07-09' AS DATE)", time.Date(2026, time.July, 9, 0, 0, 0, 0, time.UTC)},
		{"timestamp", "CAST('2026-07-09 12:34:56' AS TIMESTAMP)", time.Date(2026, time.July, 9, 12, 34, 56, 0, time.UTC)},
		{"null", "CAST(NULL AS STRING)", nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got any
			err := db.QueryRowContext(context.Background(), "SELECT "+c.expr).Scan(&got)
			if err != nil {
				t.Fatalf("scan %s: %v", c.expr, err)
			}
			if !dataTypeEqual(got, c.want) {
				t.Errorf("%s = %#v (%T), want %#v (%T)", c.expr, got, got, c.want, c.want)
			}
		})
	}
}

// dataTypeEqual compares scanned values, handling the two non-comparable cases:
// []byte (bytes.Equal) and time.Time (Equal, which is instant-based and ignores
// the location the value was materialized in).
func dataTypeEqual(got, want driver.Value) bool {
	switch w := want.(type) {
	case nil:
		return got == nil
	case []byte:
		g, ok := got.([]byte)
		return ok && bytes.Equal(g, w)
	case time.Time:
		g, ok := got.(time.Time)
		return ok && g.Equal(w)
	default:
		return got == want
	}
}

// TestKernelE2ECloudFetch streams a CloudFetch-sized result end to end. CloudFetch
// is internal to the kernel, so "it works" means many batches stream and scan
// correctly — which also exercises the per-batch release/lifetime path.
func TestKernelE2ECloudFetch(t *testing.T) {
	db := kernelTestDB(t)
	defer db.Close()

	const want = 1_000_000
	rows, err := db.QueryContext(context.Background(),
		"SELECT id FROM range(0, 1000000)")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var count, last int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan at row %d: %v", count, err)
		}
		count++
		last = id
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iteration: %v", err)
	}
	if count != want {
		t.Errorf("row count = %d, want %d", count, want)
	}
	if last != want-1 {
		t.Errorf("last id = %d, want %d", last, want-1)
	}
}

// TestKernelE2ECancellation cancels a long-running query via ctx and asserts it
// returns well before its uncancelled runtime.
func TestKernelE2ECancellation(t *testing.T) {
	db := kernelTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	// A query that would run far longer than the 3s deadline.
	_, err := db.QueryContext(ctx, "SELECT count(*) FROM range(0, 100000000000) WHERE id % 7 = 0")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a cancellation error, got nil")
	}
	if elapsed > 30*time.Second {
		t.Errorf("cancellation took %v; expected it to abandon well before the query's natural runtime", elapsed)
	}
	t.Logf("cancelled after %v with err=%v", elapsed, err)
}
