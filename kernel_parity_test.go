//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"database/sql"
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
