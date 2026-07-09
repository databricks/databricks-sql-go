//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/config"
)

// newKernelBackend rejects options it can't yet honor (initial namespace,
// metric-view metadata) loudly, rather than silently ignoring them — which would
// behave differently than the Thrift backend.
func TestNewKernelBackendRejectsUnsupportedOptions(t *testing.T) {
	base := func() *config.Config {
		c := config.WithDefaults()
		c.Host = "h.databricks.com"
		c.Port = 443
		c.HTTPPath = "/sql/1.0/warehouses/abc"
		c.AccessToken = "dapi-x"
		return c
	}

	t.Run("catalog rejected", func(t *testing.T) {
		c := base()
		c.Catalog = "main"
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("expected an error when a catalog is set on the kernel backend")
		}
	})

	t.Run("schema rejected", func(t *testing.T) {
		c := base()
		c.Schema = "default"
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("expected an error when a schema is set on the kernel backend")
		}
	})

	t.Run("metric view rejected", func(t *testing.T) {
		c := base()
		c.EnableMetricViewMetadata = true
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("expected an error when metric-view metadata is enabled on the kernel backend")
		}
	})

	t.Run("supported options ok", func(t *testing.T) {
		c := base()
		c.SessionParams = map[string]string{"QUERY_TAGS": "a:1"}
		if _, err := newKernelBackend(context.Background(), c); err != nil {
			t.Errorf("a supported config should build cleanly, got %v", err)
		}
	})
}

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
