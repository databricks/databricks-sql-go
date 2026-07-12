//go:build cgo && databricks_kernel

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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
	return kernelTestDBWith(t)
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

// kernelTestDBWith opens a kernel-backed *sql.DB with extra connector options on
// top of the base host/path/PAT, or skips when creds are unset. It is the config
// counterpart to kernelTestDB.
func kernelTestDBWith(t *testing.T, extra ...ConnOption) *sql.DB {
	t.Helper()
	host := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")
	token := os.Getenv("DATABRICKS_TOKEN")
	if host == "" || httpPath == "" || token == "" {
		t.Skip("set DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN for the kernel e2e")
	}
	opts := append([]ConnOption{
		WithServerHostname(host),
		WithHTTPPath(httpPath),
		WithAccessToken(token),
		WithUseKernel(true),
	}, extra...)
	connector, err := NewConnector(opts...)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	return sql.OpenDB(connector)
}

// TestKernelE2EQueryTags proves session confs reach the server: WithQueryTags
// (the same option the Thrift path uses) is routed to the kernel and read back
// via SET, which echoes each tag by key.
func TestKernelE2EQueryTags(t *testing.T) {
	db := kernelTestDBWith(t, WithQueryTags(map[string]string{"team": "peco"}))
	defer db.Close()

	var key, val string
	if err := db.QueryRowContext(context.Background(), "SET query_tags").Scan(&key, &val); err != nil {
		t.Fatalf("SET query_tags: %v", err)
	}
	if key != "team" || val != "peco" {
		t.Errorf("query tag read back as %q=%q, want team=peco", key, val)
	}
}

// TestKernelE2EStatementTimeout proves a STATEMENT_TIMEOUT session param (via
// WithSessionParams) is applied on the kernel session and read back via SET.
func TestKernelE2EStatementTimeout(t *testing.T) {
	db := kernelTestDBWith(t, WithSessionParams(map[string]string{"STATEMENT_TIMEOUT": "300"}))
	defer db.Close()

	var key, val string
	if err := db.QueryRowContext(context.Background(), "SET statement_timeout").Scan(&key, &val); err != nil {
		t.Fatalf("SET statement_timeout: %v", err)
	}
	if val != "300" {
		t.Errorf("statement_timeout read back as %q, want 300", val)
	}
}

// TestKernelE2ETimeZone proves the session time zone (WithSessionParams
// timezone) is applied to scanned TIMESTAMP values, matching the Thrift path —
// the returned time.Time carries the configured location, not UTC.
func TestKernelE2ETimeZone(t *testing.T) {
	const tz = "America/New_York"
	db := kernelTestDBWith(t, WithSessionParams(map[string]string{"timezone": tz}))
	defer db.Close()

	var ts time.Time
	if err := db.QueryRowContext(context.Background(),
		"SELECT CAST('2026-07-09 12:00:00' AS TIMESTAMP)").Scan(&ts); err != nil {
		t.Fatalf("query: %v", err)
	}
	if ts.Location().String() != tz {
		t.Errorf("timestamp location = %q, want %q", ts.Location(), tz)
	}
}

// TestKernelE2ETLSSkipVerify checks that WithSkipTLSHostVerify (a relaxation
// knob) is accepted on the kernel path; the connection must still succeed
// against the warehouse's valid certificate.
func TestKernelE2ETLSSkipVerify(t *testing.T) {
	db := kernelTestDBWith(t, WithSkipTLSHostVerify())
	defer db.Close()

	var got int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("query with TLS skip-verify: %v", err)
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
		// Integer widths scan to their native Go type (int8/16/32/64), matching the
		// Thrift backend. (Both are technically off the driver.Value spec, which
		// names only int64; unifying both backends on int64 is a tracked follow-up.)
		{"bigint", "CAST(42 AS BIGINT)", int64(42)},
		{"int", "CAST(7 AS INT)", int32(7)},
		{"smallint", "CAST(3 AS SMALLINT)", int16(3)},
		{"tinyint", "CAST(1 AS TINYINT)", int8(1)},
		{"double", "CAST(3.5 AS DOUBLE)", float64(3.5)},
		// 0.1 is NOT exactly representable, so a float32-vs-float64 mismatch would
		// surface here (float64(float32(0.1)) != float32(0.1)); a bare FLOAT must
		// scan to a native float32, matching Thrift.
		{"float", "CAST(0.1 AS FLOAT)", float32(0.1)},
		{"boolean", "true", true},
		{"string", "'hi'", "hi"},
		{"binary", "CAST('abc' AS BINARY)", []byte("abc")},
		{"decimal_exact", "CAST(1.25 AS DECIMAL(5,2))", "1.25"},
		{"date", "CAST('2026-07-09' AS DATE)", time.Date(2026, time.July, 9, 0, 0, 0, 0, time.UTC)},
		{"timestamp", "CAST('2026-07-09 12:34:56' AS TIMESTAMP)", time.Date(2026, time.July, 9, 12, 34, 56, 0, time.UTC)},
		{"null", "CAST(NULL AS STRING)", nil},
		// Nested types render to a JSON string; VARIANT arrives nested, GEOMETRY
		// as a WKT/WKB string.
		{"array", "array(1, 2, 3)", "[1,2,3]"},
		{"map", "map('k', 9)", `{"k":9}`},
		{"struct", "named_struct('a', 1, 'b', 'x')", `{"a":1,"b":"x"}`},
		{"variant", `parse_json('{"a":1,"b":[2,3]}')`, `{"a":1,"b":[2,3]}`},
		{"geometry", "st_point(1, 2)", "POINT(1 2)"},
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
	// It must be the deadline firing, not an unrelated failure (syntax error,
	// transient network, server-side timeout) — otherwise the test would pass
	// without proving cancellation works.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
	// Well under the query's natural runtime and not far past the 3s deadline;
	// a no-op cancel that only returned when the query finished would blow past
	// this.
	if elapsed > 10*time.Second {
		t.Errorf("cancellation took %v; expected it to abandon close to the 3s deadline", elapsed)
	}
	t.Logf("cancelled after %v with err=%v", elapsed, err)
}

// TestKernelE2EInitialNamespace proves WithInitialNamespace selects the initial
// catalog/schema on the kernel session — applied post-connect via USE CATALOG /
// USE SCHEMA, since the kernel C ABI has no namespace setter. current_catalog() /
// current_schema() must return the configured values. Uses system.information_schema,
// which every workspace has, so the test is workspace-agnostic.
func TestKernelE2EInitialNamespace(t *testing.T) {
	const catalog, schema = "system", "information_schema"
	db := kernelTestDBWith(t, WithInitialNamespace(catalog, schema))
	defer db.Close()

	var gotCatalog, gotSchema string
	if err := db.QueryRowContext(context.Background(),
		"SELECT current_catalog(), current_schema()").Scan(&gotCatalog, &gotSchema); err != nil {
		t.Fatalf("query current namespace: %v", err)
	}
	if gotCatalog != catalog {
		t.Errorf("current_catalog() = %q, want %q", gotCatalog, catalog)
	}
	if gotSchema != schema {
		t.Errorf("current_schema() = %q, want %q", gotSchema, schema)
	}
}

// TestKernelE2EMetricViewMetadata proves WithEnableMetricViewMetadata is accepted
// by the kernel session — the option is routed as the same server session conf the
// Thrift path sends (spark.sql.thriftserver.metadata.metricview.enabled), folded in
// backend-neutrally by config.EffectiveSessionParams.
//
// The assertion is "session opens and queries succeed with the conf set", not a SET
// read-back: this conf is not SET-introspectable on the warehouse — `SET <conf>`
// errors with CONFIG_NOT_AVAILABLE on BOTH backends (verified against Thrift), so a
// read-back would be testing the warehouse's SET behavior, not our routing. A
// successful connect+query with the flag on is the parity bar (the Thrift path sends
// the identical conf at OpenSession and likewise never reads it back via SET).
func TestKernelE2EMetricViewMetadata(t *testing.T) {
	db := kernelTestDBWith(t, WithEnableMetricViewMetadata(true))
	defer db.Close()

	var got int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("kernel session with metric-view metadata enabled failed to query: %v", err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 = %d, want 1", got)
	}
}
