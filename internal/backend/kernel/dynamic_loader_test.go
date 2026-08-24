//go:build databricks_kernel_dynamic

package kernel

import (
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

// dynSession is a tiny test harness around the pure-Go loader: it holds an open
// kernel session and runs queries through the pure-Go data plane (dynRows).
type dynSession struct {
	t       *testing.T
	l       *dynLib
	session uintptr
}

func openDynSession(t *testing.T, dylib, host, httpPath, token string) *dynSession {
	t.Helper()
	l, err := openDynLib(dylib)
	if err != nil {
		t.Fatalf("openDynLib: %v", err)
	}
	_ = l.callDyn(func() int32 { return l.initLogging("warn", 0) })

	var cfg uintptr
	if err := l.callDyn(func() int32 { return l.configNew(&cfg) }); err != nil {
		t.Fatalf("configNew: %v", err)
	}
	if err := l.callDyn(func() int32 { return l.configSetHTTPath(cfg, host, httpPath) }); err != nil {
		l.configFree(cfg)
		t.Fatalf("set_http_path: %v", err)
	}
	if err := l.callDyn(func() int32 { return l.configSetAuthPAT(cfg, token) }); err != nil {
		l.configFree(cfg)
		t.Fatalf("set_auth_pat: %v", err)
	}
	var session uintptr
	if err := l.callDyn(func() int32 { return l.sessionOpen(cfg, &session) }); err != nil {
		l.configFree(cfg)
		t.Fatalf("session_open: %v", err)
	}
	return &dynSession{t: t, l: l, session: session}
}

func (s *dynSession) close() {
	if s.session != 0 {
		_ = s.l.callDyn(func() int32 { return s.l.sessionClose(s.session) })
		s.session = 0
	}
}

// queryAll runs sql and returns every row scanned through the pure-Go data
// plane. Fails the test on any error.
func (s *dynSession) queryAll(t *testing.T, sql string) [][]driver.Value {
	t.Helper()
	l := s.l

	var stmt uintptr
	if err := l.callDyn(func() int32 { return l.newStatement(s.session, &stmt) }); err != nil {
		t.Fatalf("new_statement: %v", err)
	}
	defer func() { _ = l.callDyn(func() int32 { return l.statementClose(stmt) }) }()

	if err := l.callDyn(func() int32 { return l.setSQL(stmt, sql) }); err != nil {
		t.Fatalf("set_sql: %v", err)
	}
	var executed uintptr
	if err := l.callDyn(func() int32 { return l.execute(stmt, &executed) }); err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer func() { _ = l.callDyn(func() int32 { return l.execClose(executed) }) }()

	var stream uintptr
	if err := l.callDyn(func() int32 { return l.getResultStream(executed, &stream) }); err != nil {
		t.Fatalf("get_result_stream: %v", err)
	}

	rows, err := newDynRows(l, stream, time.UTC)
	if err != nil {
		t.Fatalf("newDynRows: %v", err)
	}
	defer rows.Close()

	ncols := len(rows.Columns())
	var out [][]driver.Value
	for {
		dest := make([]driver.Value, ncols)
		err := rows.Next(dest)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("rows.Next: %v", err)
		}
		out = append(out, dest)
	}
	return out
}

// TestDynamicLoaderControlPlane proves the whole thesis of the dynamic-loading
// approach: a PURE-GO (CGO_ENABLED=0) binary can drive the closed-source kernel
// end-to-end for the control plane — dlopen the shared library, build a config,
// open a session against a real warehouse, execute a statement, read its server
// query id, and tear everything down — with no cgo, no C compiler, no static
// linking.
//
// It is skipped unless BOTH are set:
//
//	DBX_KERNEL_DYLIB   absolute path to libdatabricks_sql_kernel.{so,dylib,dll}
//	DBX_KERNEL_HOST    warehouse hostname (no scheme)
//	DBX_KERNEL_HTTPATH /sql/1.0/warehouses/<id>
//	DBX_KERNEL_TOKEN   PAT
//
// Run:
//
//	DBX_KERNEL_DYLIB=$HOME/Desktop/databricks-sql-kernel/target/release/libdatabricks_sql_kernel.dylib \
//	DBX_KERNEL_HOST=$DATABRICKS_PECOTESTING_SERVER_HOSTNAME \
//	DBX_KERNEL_HTTPATH=/sql/1.0/warehouses/00adc7b6c00429b8 \
//	DBX_KERNEL_TOKEN=$DATABRICKS_PECOTESTING_TOKEN_PERSONAL \
//	CGO_ENABLED=0 go test -tags databricks_kernel_dynamic \
//	  -run TestDynamicLoaderControlPlane ./internal/backend/kernel/ -v
func TestDynamicLoaderControlPlane(t *testing.T) {
	dylib := os.Getenv("DBX_KERNEL_DYLIB")
	host := os.Getenv("DBX_KERNEL_HOST")
	httpPath := os.Getenv("DBX_KERNEL_HTTPATH")
	token := os.Getenv("DBX_KERNEL_TOKEN")
	if dylib == "" || host == "" || httpPath == "" || token == "" {
		t.Skip("set DBX_KERNEL_DYLIB, DBX_KERNEL_HOST, DBX_KERNEL_HTTPATH, DBX_KERNEL_TOKEN to run")
	}

	l, err := openDynLib(dylib)
	if err != nil {
		t.Fatalf("openDynLib: %v", err)
	}
	t.Logf("OK dlopen: bound kernel C ABI from %s (CGO_ENABLED=0)", dylib)

	// Best-effort logging init (benign if the host already installed one).
	// file_path = 0 is a real C NULL → kernel logs to stderr.
	_ = l.callDyn(func() int32 { return l.initLogging("warn", 0) })

	// Build the session config.
	var cfg uintptr
	if err := l.callDyn(func() int32 { return l.configNew(&cfg) }); err != nil {
		t.Fatalf("configNew: %v", err)
	}
	if cfg == 0 {
		t.Fatal("configNew returned success but null config")
	}
	// From here, on any early failure the config must be freed unless it was
	// consumed by a successful session_open.
	consumed := false
	defer func() {
		if !consumed {
			l.configFree(cfg)
		}
	}()

	if err := l.callDyn(func() int32 { return l.configSetHTTPath(cfg, host, httpPath) }); err != nil {
		t.Fatalf("set_http_path: %v", err)
	}
	if err := l.callDyn(func() int32 { return l.configSetAuthPAT(cfg, token) }); err != nil {
		t.Fatalf("set_auth_pat: %v", err)
	}
	t.Log("OK config: http_path + PAT set")

	// Open the session (consumes the config on success).
	var session uintptr
	if err := l.callDyn(func() int32 { return l.sessionOpen(cfg, &session) }); err != nil {
		t.Fatalf("session_open: %v", err)
	}
	consumed = true
	if session == 0 {
		t.Fatal("session_open returned success but null session")
	}
	defer func() {
		if err := l.callDyn(func() int32 { return l.sessionClose(session) }); err != nil {
			t.Errorf("session_close: %v", err)
		}
	}()
	t.Log("OK session_open: connected to warehouse over SEA via the kernel")

	// Prepare + execute a statement.
	var stmt uintptr
	if err := l.callDyn(func() int32 { return l.newStatement(session, &stmt) }); err != nil {
		t.Fatalf("new_statement: %v", err)
	}
	defer func() { _ = l.callDyn(func() int32 { return l.statementClose(stmt) }) }()

	if err := l.callDyn(func() int32 { return l.setSQL(stmt, "SELECT 1 AS one") }); err != nil {
		t.Fatalf("set_sql: %v", err)
	}

	var executed uintptr
	if err := l.callDyn(func() int32 { return l.execute(stmt, &executed) }); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if executed == 0 {
		t.Fatal("execute returned success but null executed-statement")
	}
	defer func() { _ = l.callDyn(func() int32 { return l.execClose(executed) }) }()

	// Read control-plane result metadata.
	queryID := goStringFromC(l.execQueryID(executed))
	rows := l.execNumRows(executed)
	t.Logf("OK execute: server queryId=%q numModifiedRows=%d", queryID, rows)

	if queryID == "" {
		t.Error("expected a non-empty server query id from the executed statement")
	}

	t.Log("PROOF: pure-Go (CGO_ENABLED=0) drove the closed-source kernel control plane end-to-end.")
}

// TestDynamicLoaderDataPlane proves the DATA plane: fetch actual result rows
// through the pure-Go Arrow C-Data importer (no cgo). It runs a query with a
// mix of types and asserts the scanned values, so a layout/import bug would
// surface as a wrong value, not just a non-crash.
//
// Same env gating as TestDynamicLoaderControlPlane.
func TestDynamicLoaderDataPlane(t *testing.T) {
	dylib := os.Getenv("DBX_KERNEL_DYLIB")
	host := os.Getenv("DBX_KERNEL_HOST")
	httpPath := os.Getenv("DBX_KERNEL_HTTPATH")
	token := os.Getenv("DBX_KERNEL_TOKEN")
	if dylib == "" || host == "" || httpPath == "" || token == "" {
		t.Skip("set DBX_KERNEL_DYLIB, DBX_KERNEL_HOST, DBX_KERNEL_HTTPATH, DBX_KERNEL_TOKEN to run")
	}

	sess := openDynSession(t, dylib, host, httpPath, token)
	defer sess.close()

	t.Run("scalars + null + decimal + string", func(t *testing.T) {
		got := sess.queryAll(t, `SELECT
			CAST(1 AS INT)            AS i,
			CAST(2 AS BIGINT)         AS b,
			CAST(3.5 AS DOUBLE)       AS d,
			CAST('hello' AS STRING)   AS s,
			CAST(true AS BOOLEAN)     AS bo,
			CAST(19.99 AS DECIMAL(10,2)) AS dec,
			CAST(NULL AS STRING)      AS n`)
		if len(got) != 1 {
			t.Fatalf("expected 1 row, got %d", len(got))
		}
		row := got[0]
		checkEq(t, "i", row[0], int32(1))
		checkEq(t, "b", row[1], int64(2))
		checkEq(t, "d", row[2], float64(3.5))
		checkEq(t, "s", row[3], "hello")
		checkEq(t, "bo", row[4], true)
		checkEq(t, "dec", row[5], "19.99") // decimal renders as exact string
		checkEq(t, "n", row[6], nil)
		t.Logf("OK scalars row = %v", row)
	})

	t.Run("multi-row range", func(t *testing.T) {
		got := sess.queryAll(t, `SELECT id FROM range(0, 1000) ORDER BY id`)
		if len(got) != 1000 {
			t.Fatalf("expected 1000 rows, got %d", len(got))
		}
		for i, row := range got {
			if row[0].(int64) != int64(i) {
				t.Fatalf("row %d: got %v", i, row[0])
			}
		}
		t.Logf("OK fetched %d rows in order across batches", len(got))
	})

	t.Run("nested array/map/struct", func(t *testing.T) {
		got := sess.queryAll(t, `SELECT
			array(1,2,3)                          AS arr,
			map('k', 1)                           AS m,
			named_struct('a', 1, 'b', 'x')        AS st`)
		if len(got) != 1 {
			t.Fatalf("expected 1 row, got %d", len(got))
		}
		// Nested types render as JSON strings via the shared scanner.
		checkEq(t, "arr", got[0][0], "[1,2,3]")
		checkEq(t, "map", got[0][1], `{"k":1}`)
		checkEq(t, "struct", got[0][2], `{"a":1,"b":"x"}`)
		t.Logf("OK nested row = %v", got[0])
	})

	t.Run("temporal + binary + float edge", func(t *testing.T) {
		got := sess.queryAll(t, `SELECT
			CAST('2021-07-01' AS DATE)                    AS d,
			CAST('2021-07-01 05:43:28' AS TIMESTAMP)      AS ts,
			CAST(X'1abf' AS BINARY)                       AS bin,
			CAST(3.3 AS FLOAT)                            AS f,
			CAST(-0.01 AS DECIMAL(5,2))                   AS negdec,
			CAST(9999999999999999999999999999.99 AS DECIMAL(38,2)) AS bigdec`)
		if len(got) != 1 {
			t.Fatalf("expected 1 row, got %d", len(got))
		}
		row := got[0]
		// date/timestamp scan to time.Time; check via string form.
		checkEq(t, "date", fmt.Sprintf("%v", row[0]), "2021-07-01 00:00:00 +0000 UTC")
		checkEq(t, "ts", fmt.Sprintf("%v", row[1]), "2021-07-01 05:43:28 +0000 UTC")
		if b, ok := row[2].([]byte); !ok || len(b) != 2 || b[0] != 0x1a || b[1] != 0xbf {
			t.Errorf("binary: got %v (%T)", row[2], row[2])
		}
		checkEq(t, "negdec", row[4], "-0.01")
		// high-precision decimal must be exact (no float corruption).
		checkEq(t, "bigdec", row[5], "9999999999999999999999999999.99")
		t.Logf("OK temporal/binary/float row = %v", row)
	})

	t.Run("empty result set", func(t *testing.T) {
		got := sess.queryAll(t, `SELECT 1 AS x WHERE 1=0`)
		if len(got) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(got))
		}
		t.Log("OK empty result set drained cleanly")
	})

	t.Run("large result 100k rows", func(t *testing.T) {
		got := sess.queryAll(t, `SELECT id, id*2 AS doubled, CAST(id AS STRING) AS s FROM range(0, 100000)`)
		if len(got) != 100000 {
			t.Fatalf("expected 100000 rows, got %d", len(got))
		}
		// spot-check a few rows
		for _, i := range []int{0, 1, 50000, 99999} {
			if got[i][0].(int64) != int64(i) || got[i][1].(int64) != int64(i*2) {
				t.Fatalf("row %d mismatch: %v", i, got[i])
			}
		}
		t.Logf("OK fetched %d rows (multi-batch, likely CloudFetch)", len(got))
	})

	t.Log("PROOF: pure-Go (CGO_ENABLED=0) fetched + scanned result rows end-to-end via the C-Data importer.")
}

func checkEq(t *testing.T, name string, got, want any) {
	t.Helper()
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Errorf("%s: got %v (%T), want %v (%T)", name, got, got, want, want)
	}
}
