//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// thriftTestDB opens the same warehouse over the default Thrift backend, for
// parity comparison against the kernel backend.
func thriftTestDB(t *testing.T) *sql.DB {
	t.Helper()
	host, httpPath, token := pecoTestingCreds(t)
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
		"CAST(NULL AS STRING), CAST(9.99 AS DECIMAL(5,2)), true, " +
		// A bare FLOAT at a non-exactly-representable value: catches the
		// float32-vs-widened-float64 divergence (0.1 vs 0.10000000149011612).
		"CAST(0.1 AS FLOAT), " +
		"array(1, 2, 3), map('k', 9), named_struct('a', 1, 'b', 'x'), " +
		`parse_json('{"a":1,"b":[2,3]}'), st_point(1, 2), ` +
		// A decimal inside a struct: exercises exact-string nested-decimal
		// rendering (19.99, not a lossy 19.990000000000002).
		"named_struct('d', CAST(19.99 AS DECIMAL(5,2)))"

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

// TestKernelE2EInterval gives the INTERVAL rendering the same live ground-truth
// check the other types have. The kernel scans a native arrow duration/month-interval
// and formats it Go-side; the Thrift path receives the server's pre-formatted string
// (native-interval is off in prod). Comparing the two scanned strings proves the
// Go-side formatter reproduces the server form byte-for-byte — including the sign
// placement, day separator, and fractional-digit edges the arrow-level parity suite
// cannot reach (it never scans a duration array). Named TestKernelE2E* so the nightly
// -run picks it up.
func TestKernelE2EInterval(t *testing.T) {
	// Day-time and year-month, each with a positive and a negative case (the sign is
	// the highest-risk format edge — see formatDayTimeInterval / formatYearMonthInterval).
	const query = "SELECT INTERVAL '1 02:03:04.5' DAY TO SECOND, " +
		"INTERVAL '-1 02:03:04.5' DAY TO SECOND, " +
		"INTERVAL '3-4' YEAR TO MONTH, " +
		"INTERVAL '-3-4' YEAR TO MONTH"

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

// TestKernelThriftColumnTypeParity is the live guard for PECOBLR-3692: the kernel
// backend must report the SAME sql.ColumnType metadata (DatabaseTypeName,
// ScanType, Nullable, Length) as the Thrift backend for every type. The
// value-parity suites above compare scanned VALUES and so are blind to this — the
// gap was a Rows.ColumnType* omission (kernelRows implemented only driver.Rows),
// which surfaces as "" / interface{} metadata, not a wrong value. Named
// TestKernelThrift* / TestKernel* so the nightly kernel -run picks it up.
func TestKernelThriftColumnTypeParity(t *testing.T) {
	// One column per Databricks type the driver scans, mirroring the mixed-type
	// query used to capture the ground truth. VARCHAR/CHAR/VARIANT/GEOMETRY all
	// collapse to STRING on both backends (they arrive as Arrow Utf8), and both
	// interval types are server/Go-stringified to STRING — included so the parity
	// covers those collapses too.
	const query = "SELECT " +
		"CAST(1 AS TINYINT) a_tinyint, CAST(1 AS SMALLINT) a_smallint, " +
		"CAST(1 AS INT) a_int, CAST(1 AS BIGINT) a_bigint, " +
		"CAST(1 AS FLOAT) a_float, CAST(1 AS DOUBLE) a_double, " +
		"CAST(1 AS BOOLEAN) a_bool, CAST('x' AS STRING) a_string, " +
		"CAST('x' AS VARCHAR(10)) a_varchar, CAST('x' AS CHAR(3)) a_char, " +
		"CAST(1.5 AS DECIMAL(10,2)) a_decimal, CAST('2020-01-01' AS DATE) a_date, " +
		"CAST('2020-01-01 00:00:00' AS TIMESTAMP) a_ts, CAST('abc' AS BINARY) a_binary, " +
		"array(1,2,3) a_array, map('k',1) a_map, named_struct('x',1) a_struct, " +
		`parse_json('{"a":1}') a_variant, INTERVAL '1' DAY a_iv_dt, ` +
		// VOID: the server declares it STRING_TYPE over Thrift; asserts the kernel
		// arrow.NULL→STRING mapping matches live, not just in the pure-Go tests.
		"INTERVAL '1' MONTH a_iv_ym, st_point(1,2) a_geom, CAST(NULL AS VOID) a_void"

	kernelDB := kernelTestDB(t)
	defer kernelDB.Close()
	thriftDB := thriftTestDB(t)
	defer thriftDB.Close()

	kernelCT := columnTypeStrings(t, kernelDB, query)
	thriftCT := columnTypeStrings(t, thriftDB, query)

	if len(kernelCT) != len(thriftCT) {
		t.Fatalf("column count differs: kernel=%d thrift=%d", len(kernelCT), len(thriftCT))
	}
	for i := range kernelCT {
		if kernelCT[i] != thriftCT[i] {
			t.Errorf("col %d metadata differs:\n  kernel=%s\n  thrift=%s", i, kernelCT[i], thriftCT[i])
		}
	}
}

// columnTypeStrings renders each column's sql.ColumnType metadata to a stable
// string (name, DatabaseTypeName, ScanType, Nullable, Length), so the kernel and
// Thrift backends can be compared field-for-field. A nil ScanType renders as
// "<nil>" so the pre-fix interface{} fallback would differ visibly.
func columnTypeStrings(t *testing.T, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}
	out := make([]string, len(cts))
	for i, ct := range cts {
		scan := "<nil>"
		if ct.ScanType() != nil {
			scan = ct.ScanType().String()
		}
		nl, nlok := ct.Nullable()
		ln, lnok := ct.Length()
		out[i] = fmt.Sprintf("name=%s db=%s scan=%s nullable=%v/%v len=%d/%v",
			ct.Name(), ct.DatabaseTypeName(), scan, nl, nlok, ln, lnok)
	}
	return out
}

// TestKernelThriftDecimalScaleParity exercises the shared exact-decimal renderer
// (decimalfmt, PECOBLR-3691) at scale against a live warehouse: it generates many
// decimal rows so the kernel result spans multiple Arrow batches, and asserts
// every rendered value matches the Thrift backend byte-for-byte — the guarantee
// that the alloc-free ExactString rewrite changed only cost, not output, over a
// real multi-batch stream and including magnitudes past float64 precision. The
// pure-Go TestExactStringOracleParity pins the renderer against its own
// pre-rewrite implementation; this pins the integrated kernel scan path against
// Thrift. Named TestKernelThrift* for the nightly -run.
func TestKernelThriftDecimalScaleParity(t *testing.T) {
	// 50k rows × several DECIMALs, with a value beyond float64's exact range
	// (DECIMAL(38,4)) so a lossy path would diverge. range() drives enough rows to
	// cross batch boundaries on the kernel stream.
	const rowCount = 50000
	query := "SELECT " +
		"CAST(id AS DECIMAL(10,2)) d1, " +
		"CAST(id * -1.25 AS DECIMAL(20,4)) d2, " +
		"CAST(id AS DECIMAL(38,4)) + 123456789012345678901234.5678 d3, " +
		"CAST(id % 7 AS DECIMAL(5,3)) d4 " +
		"FROM range(" + fmt.Sprint(rowCount) + ") ORDER BY id"

	kernelDB := kernelTestDB(t)
	defer kernelDB.Close()
	thriftDB := thriftTestDB(t)
	defer thriftDB.Close()

	kernelRows := scanAllRowsAsStrings(t, kernelDB, query)
	thriftRows := scanAllRowsAsStrings(t, thriftDB, query)

	if len(kernelRows) != len(thriftRows) {
		t.Fatalf("row count differs: kernel=%d thrift=%d", len(kernelRows), len(thriftRows))
	}
	if len(kernelRows) != rowCount {
		t.Fatalf("expected %d rows, got %d", rowCount, len(kernelRows))
	}
	for i := range kernelRows {
		if kernelRows[i] != thriftRows[i] {
			t.Fatalf("row %d differs:\n  kernel=%s\n  thrift=%s", i, kernelRows[i], thriftRows[i])
		}
	}
	t.Logf("verified %d decimal rows byte-identical across backends (arena spans multiple batches)", len(kernelRows))
}

// scanAllRowsAsStrings scans every row into a "|"-joined string of column wire
// forms (via sql.RawBytes), so two backends can be compared row-by-row
// independent of Go-type coercion.
func scanAllRowsAsStrings(t *testing.T, db *sql.DB, query string) []string {
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
	var out []string
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		line := ""
		for i, b := range raw {
			if i > 0 {
				line += "|"
			}
			if b == nil {
				line += "<nil>"
			} else {
				line += string(b)
			}
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return out
}

// paramCase is one parameterized-query parity case: the same SQL + args run on
// both backends must yield byte-identical output.
type paramCase struct {
	name string
	sql  string
	args []any
}

// The kernel binds these via kernel_statement_bind_parameter (the driver's
// backend.Param{Name, Type, Value}); Thrift binds via toSparkParameters. Covers
// positional (?) and named (:n) markers, each scalar type, SQL NULL, multi-param,
// and a predicate.
var paramCases = []paramCase{
	{"pos_int", "SELECT ? AS v", []any{int64(42)}},
	{"pos_string", "SELECT ? AS v", []any{"hello"}},
	{"pos_double", "SELECT ? AS v", []any{3.5}},
	{"pos_bool", "SELECT ? AS v", []any{true}},
	{"pos_null", "SELECT ? AS v", []any{nil}},
	{"pos_two", "SELECT ? + ? AS v", []any{int64(2), int64(40)}},
	{"pos_in_predicate", "SELECT count(*) AS v FROM range(100) WHERE id < ?", []any{int64(10)}},
	{"named_int", "SELECT :n AS v", []any{sql.Named("n", int64(7))}},
	{"named_string", "SELECT :s AS v", []any{sql.Named("s", "world")}},
	{"named_two", "SELECT :a AS a, :b AS b", []any{sql.Named("a", int64(1)), sql.Named("b", "x")}},
}

// TestKernelParamsVsThrift asserts parameterized queries produce byte-identical
// output on the kernel and Thrift backends — the bound-parameter acceptance gate.
func TestKernelParamsVsThrift(t *testing.T) {
	kernelDB := kernelTestDB(t)
	defer kernelDB.Close()
	thriftDB := thriftTestDB(t)
	defer thriftDB.Close()

	for _, c := range paramCases {
		t.Run(c.name, func(t *testing.T) {
			kernelRow := scanOneRowAsStringsArgs(t, kernelDB, c.sql, c.args...)
			thriftRow := scanOneRowAsStringsArgs(t, thriftDB, c.sql, c.args...)
			if len(kernelRow) != len(thriftRow) {
				t.Fatalf("column count differs: kernel=%d thrift=%d", len(kernelRow), len(thriftRow))
			}
			for i := range kernelRow {
				if kernelRow[i] != thriftRow[i] {
					t.Errorf("col %d differs: kernel=%q thrift=%q (sql=%q args=%v)", i, kernelRow[i], thriftRow[i], c.sql, c.args)
				}
			}
		})
	}
}

// scanOneRowAsStrings scans the first row into a []string via sql.RawBytes, so a
// NULL renders as "<nil>" and every value is compared in its wire form,
// independent of Go-type coercion differences between the backends.
func scanOneRowAsStrings(t *testing.T, db *sql.DB, query string) []string {
	return scanOneRowAsStringsArgs(t, db, query)
}

// scanOneRowAsStringsArgs is scanOneRowAsStrings with query arguments (bound
// parameters). Same wire-form comparison contract.
func scanOneRowAsStringsArgs(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
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
