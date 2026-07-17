//go:build cgo && databricks_kernel

package dbsql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
)

// kernelTestDB opens a kernel-backed *sql.DB from the DATABRICKS_PECOTESTING_*
// warehouse credentials, or skips when they are unset. It goes through the standard
// connector with WithUseKernel(true) — the same path a real consumer uses — not a
// kernel-only connector.
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
	// Same DATABRICKS_PECOTESTING_* warehouse credentials as the Thrift E2E suite, so
	// both backends run against one test warehouse from one secret set.
	host, httpPath, token := pecoTestingCreds(t)
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

// TestKernelE2ETimestampNTZ proves TIMESTAMP and TIMESTAMP_NTZ round-trip
// byte-identically to the Thrift path at a non-UTC session tz. The kernel delivers
// TIMESTAMP with an Arrow tz and TIMESTAMP_NTZ without one, but — like the Thrift
// path — the driver ignores that field and renders via .In(loc). So the naive
// wall-clock of an NTZ literal is treated as a UTC instant and shifted into the
// session tz (12:00 NTZ in America/New_York → the -04:00 local of 12:00Z = 08:00),
// exactly as Thrift does. Verified live on both backends; this pins the round-trip
// so a one-sided "don't shift NTZ" change is caught.
func TestKernelE2ETimestampNTZ(t *testing.T) {
	const tz = "America/New_York"
	db := kernelTestDBWith(t, WithSessionParams(map[string]string{"timezone": tz}))
	defer db.Close()

	loc, err := time.LoadLocation(tz)
	if err != nil {
		t.Skipf("tz database unavailable: %v", err)
	}
	ctx := context.Background()

	var ltz time.Time
	if err := db.QueryRowContext(ctx,
		"SELECT CAST('2026-07-09 12:00:00' AS TIMESTAMP)").Scan(&ltz); err != nil {
		t.Fatalf("TIMESTAMP query: %v", err)
	}
	// A zoned TIMESTAMP literal is the wall-clock in the session tz.
	if want := time.Date(2026, 7, 9, 12, 0, 0, 0, loc); !ltz.Equal(want) {
		t.Errorf("TIMESTAMP = %s, want %s", ltz, want)
	}

	var ntz time.Time
	if err := db.QueryRowContext(ctx,
		"SELECT CAST('2026-07-09 12:00:00' AS TIMESTAMP_NTZ)").Scan(&ntz); err != nil {
		t.Fatalf("TIMESTAMP_NTZ query: %v", err)
	}
	// The NTZ wall-clock is treated as a UTC instant then shifted into the session
	// tz: 12:00Z == 08:00 in America/New_York (-04:00 in July).
	if want := time.Date(2026, 7, 9, 8, 0, 0, 0, loc); !ntz.Equal(want) {
		t.Errorf("TIMESTAMP_NTZ = %s, want %s (UTC instant %s)", ntz, want, ntz.UTC())
	}
	if ntz.Location().String() != tz {
		t.Errorf("TIMESTAMP_NTZ location = %q, want %q", ntz.Location(), tz)
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

// TestKernelE2EErrorSurface proves a failed query surfaces as a DBExecutionError
// carrying a SqlState, the same error shape as the Thrift path (consumers rely on
// errors.As(err, &DBExecutionError) + SqlState()). Verified live: unknown table →
// 42P01, unknown column → 42703 — byte-identical sqlstate to Thrift, though the
// kernel's message is richer (it includes the SQL error class + suggestions).
func TestKernelE2EErrorSurface(t *testing.T) {
	db := kernelTestDB(t)
	defer db.Close()

	cases := []struct {
		name, query, wantState string
	}{
		{"unknown_table", "SELECT * FROM this_table_does_not_exist_xyz", "42P01"},
		{"unknown_column", "SELECT no_such_col FROM range(1)", "42703"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := db.QueryContext(context.Background(), c.query)
			if err == nil {
				t.Fatalf("expected an error for %q, got none", c.query)
			}
			var de dbsqlerr.DBExecutionError
			if !errors.As(err, &de) {
				t.Fatalf("error is not a DBExecutionError: %v", err)
			}
			if de.SqlState() != c.wantState {
				t.Errorf("sqlState = %q, want %q (err: %v)", de.SqlState(), c.wantState, err)
			}
		})
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

// TestKernelE2EStatementID proves the kernel backend surfaces the server query id
// on the success path: a registered QueryIdCallback fires with a non-empty id
// (driven by kernelOp.StatementID() → kernel_executed_statement_query_id). This is
// the observable end of the EXECUTE_STATEMENT telemetry / query-history path that
// was previously dark on the kernel backend (StatementID() returned "").
func TestKernelE2EStatementID(t *testing.T) {
	db := kernelTestDB(t)
	defer db.Close()

	var gotID string
	var fired bool
	ctx := driverctx.NewContextWithQueryIdCallback(context.Background(), func(id string) {
		fired = true
		gotID = id
	})

	var v int64
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("query: %v", err)
	}
	if !fired {
		t.Fatal("QueryIdCallback did not fire")
	}
	if gotID == "" {
		t.Error("kernel backend surfaced an empty query id; want the server statement id")
	}
}

// TestKernelE2EConnectionPool proves the database/sql connection pool works with
// the kernel backend: the pool lives above the backend seam (each connection wraps
// one kernel session), so many concurrent queries over a capped pool must all
// succeed with checkout/return and per-conn single-session isolation intact.
func TestKernelE2EConnectionPool(t *testing.T) {
	db := kernelTestDB(t)
	defer db.Close()
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	const n = 40
	var errs, ok int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			var v int
			if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&v); err != nil || v != 1 {
				atomic.AddInt64(&errs, 1)
				return
			}
			atomic.AddInt64(&ok, 1)
		}()
	}
	wg.Wait()
	if errs != 0 {
		t.Fatalf("connection pool: %d/%d queries failed", errs, n)
	}
	t.Logf("connection pool OK: %d/%d queries succeeded over pool cap 8", ok, n)
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
//
// Scope note: this is intentionally a thin live smoke test — it proves the flag is
// accepted end to end, not that the conf value is correct. The actual routing (the
// flag → the metricview.enabled=true session conf) is asserted in the default-build
// unit test TestEffectiveSessionParams; the SELECT here overlaps TestKernelE2ESelect1
// by design, since the conf's live effect can't be observed (see above). Kept as a
// connect-with-flag smoke rather than deleted so the flag has at least one live path.
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

// TestKernelE2EM2M authenticates to a real warehouse via OAuth M2M over the kernel
// (WithClientCredentials → the kernel's own client-credentials flow), then runs
// SELECT 1. It self-skips unless DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET are
// set (alongside the host/http-path base), mirroring the PAT tests' skip pattern —
// so CI without a service principal stays green while a credentialed run proves the
// M2M setter path actually authenticates end to end (not just that set_auth_m2m
// returns OK, which TestSetAuthByMode already covers).
func TestKernelE2EM2M(t *testing.T) {
	host := os.Getenv("DATABRICKS_PECOTESTING_SERVER_HOSTNAME")
	httpPath := os.Getenv("DATABRICKS_PECOTESTING_HTTP_PATH2")
	clientID := os.Getenv("DATABRICKS_CLIENT_ID")
	clientSecret := os.Getenv("DATABRICKS_CLIENT_SECRET")
	if host == "" || httpPath == "" || clientID == "" || clientSecret == "" {
		t.Skip("set DATABRICKS_PECOTESTING_SERVER_HOSTNAME, DATABRICKS_PECOTESTING_HTTP_PATH2, DATABRICKS_CLIENT_ID, and DATABRICKS_CLIENT_SECRET for the M2M e2e")
	}

	connector, err := NewConnector(
		WithServerHostname(host),
		WithHTTPPath(httpPath),
		WithClientCredentials(clientID, clientSecret),
		WithUseKernel(true),
	)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	var got int64
	if err := db.QueryRowContext(context.Background(), "SELECT 1").Scan(&got); err != nil {
		t.Fatalf("M2M-authenticated query failed: %v", err)
	}
	if got != 1 {
		t.Errorf("SELECT 1 = %d, want 1", got)
	}
}
