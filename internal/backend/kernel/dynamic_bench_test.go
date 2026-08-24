//go:build databricks_kernel_dynamic

package kernel

import (
	"database/sql/driver"
	"io"
	"os"
	"testing"
	"time"
)

// BenchmarkDynLowLatency and BenchmarkDynLargeResult measure the pure-Go
// (purego dynamic) path so its numbers can be compared head-to-head with the
// cgo static path (BenchmarkCgo* in cgo_bench_test.go, built with the
// databricks_kernel tag). Both benchmarks drive the identical query through the
// same loader-level path, isolating the FFI + Arrow-import cost that differs
// between the two linking models.
//
// Run:
//
//	DBX_KERNEL_DYLIB=... DBX_KERNEL_HOST=... DBX_KERNEL_HTTPATH=... DBX_KERNEL_TOKEN=... \
//	CGO_ENABLED=0 go test -tags databricks_kernel_dynamic -run x \
//	  -bench 'BenchmarkDyn' -benchtime 20x ./internal/backend/kernel/
func benchEnv(b *testing.B) (dylib, host, httpPath, token string) {
	dylib = os.Getenv("DBX_KERNEL_DYLIB")
	host = os.Getenv("DBX_KERNEL_HOST")
	httpPath = os.Getenv("DBX_KERNEL_HTTPATH")
	token = os.Getenv("DBX_KERNEL_TOKEN")
	if dylib == "" || host == "" || httpPath == "" || token == "" {
		b.Skip("set DBX_KERNEL_DYLIB, DBX_KERNEL_HOST, DBX_KERNEL_HTTPATH, DBX_KERNEL_TOKEN")
	}
	return
}

func benchOpen(b *testing.B, dylib, host, httpPath, token string) *dynSession {
	b.Helper()
	l, err := openDynLib(dylib)
	if err != nil {
		b.Fatalf("openDynLib: %v", err)
	}
	_ = l.callDyn(func() int32 { return l.initLogging("warn", 0) })
	var cfg uintptr
	if err := l.callDyn(func() int32 { return l.configNew(&cfg) }); err != nil {
		b.Fatalf("configNew: %v", err)
	}
	if err := l.callDyn(func() int32 { return l.configSetHTTPath(cfg, host, httpPath) }); err != nil {
		b.Fatalf("set_http_path: %v", err)
	}
	if err := l.callDyn(func() int32 { return l.configSetAuthPAT(cfg, token) }); err != nil {
		b.Fatalf("set_auth_pat: %v", err)
	}
	var session uintptr
	if err := l.callDyn(func() int32 { return l.sessionOpen(cfg, &session) }); err != nil {
		b.Fatalf("session_open: %v", err)
	}
	return &dynSession{l: l, session: session}
}

// run one query and drain all rows through the pure-Go data plane.
func (s *dynSession) drain(b *testing.B, sql string) int {
	l := s.l
	var stmt uintptr
	if err := l.callDyn(func() int32 { return l.newStatement(s.session, &stmt) }); err != nil {
		b.Fatalf("new_statement: %v", err)
	}
	defer func() { _ = l.callDyn(func() int32 { return l.statementClose(stmt) }) }()
	if err := l.callDyn(func() int32 { return l.setSQL(stmt, sql) }); err != nil {
		b.Fatalf("set_sql: %v", err)
	}
	var executed uintptr
	if err := l.callDyn(func() int32 { return l.execute(stmt, &executed) }); err != nil {
		b.Fatalf("execute: %v", err)
	}
	defer func() { _ = l.callDyn(func() int32 { return l.execClose(executed) }) }()
	var stream uintptr
	if err := l.callDyn(func() int32 { return l.getResultStream(executed, &stream) }); err != nil {
		b.Fatalf("get_result_stream: %v", err)
	}
	rows, err := newDynRows(l, stream, time.UTC)
	if err != nil {
		b.Fatalf("newDynRows: %v", err)
	}
	defer rows.Close()
	ncols := len(rows.Columns())
	dest := make([]driver.Value, ncols)
	n := 0
	for {
		if err := rows.Next(dest); err == io.EOF {
			break
		} else if err != nil {
			b.Fatalf("Next: %v", err)
		}
		n++
	}
	return n
}

func BenchmarkDynLowLatency(b *testing.B) {
	dylib, host, httpPath, token := benchEnv(b)
	s := benchOpen(b, dylib, host, httpPath, token)
	defer s.close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.drain(b, "SELECT 1 AS one")
	}
}

func BenchmarkDynLargeResult(b *testing.B) {
	dylib, host, httpPath, token := benchEnv(b)
	s := benchOpen(b, dylib, host, httpPath, token)
	defer s.close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := s.drain(b, "SELECT id, id*2 AS doubled, CAST(id AS STRING) AS s FROM range(0, 500000)")
		if n != 500000 {
			b.Fatalf("got %d rows", n)
		}
	}
}
