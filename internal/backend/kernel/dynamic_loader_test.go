//go:build databricks_kernel_dynamic

package kernel

import (
	"os"
	"testing"
)

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
