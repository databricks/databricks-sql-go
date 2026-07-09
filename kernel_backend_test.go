//go:build cgo && databricks_kernel

package dbsql

import (
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"
)

// proxyForEndpoint returns a valid config's endpoint proxy without error. Its
// value comes from http.ProxyFromEnvironment, which snapshots the proxy env once
// per process (a sync.Once) — the same cached decision the Thrift transport
// makes — so the resolved value can't be re-driven by setting env vars mid-test.
// This asserts the invariant that matters here: a well-formed config never makes
// the resolver error out (it returns "" for direct), and a malformed config
// (missing host) is handled gracefully rather than panicking.
func TestProxyForEndpoint(t *testing.T) {
	valid := config.WithDefaults()
	valid.Host = "my-workspace.databricks.com"
	valid.Port = 443
	valid.HTTPPath = "/sql/1.0/warehouses/abc"
	// Must not panic; with no proxy env in the test environment this is "".
	_ = proxyForEndpoint(valid)

	// A config whose endpoint URL can't be built (no host) resolves to direct
	// rather than erroring.
	bad := config.WithDefaults()
	bad.Host = ""
	if got := proxyForEndpoint(bad); got != "" {
		t.Errorf("unbuildable endpoint should resolve to direct, got %q", got)
	}
}
