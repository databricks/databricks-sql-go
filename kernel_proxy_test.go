package dbsql

import (
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// proxyForEndpointFunc maps the injected resolver's decision to a proxy URL
// string (or "" for direct), and returns "" for an unbuildable endpoint. The
// production path uses http.ProxyFromEnvironment, whose env is snapshotted once
// per process (sync.Once) and so can't be re-driven mid-test; the resolver seam
// lets us assert every branch deterministically. Each case mirrors an
// httpproxy-style outcome: proxy set for this host, host excluded by NO_PROXY,
// no proxy configured, and an unbuildable endpoint. Pure Go, so it runs in the
// default CGO_ENABLED=0 build (no kernel lib required).
func TestProxyForEndpoint(t *testing.T) {
	validCfg := func() *config.Config {
		c := config.WithDefaults()
		c.Host = "my-workspace.databricks.com"
		c.Port = 443
		c.HTTPPath = "/sql/1.0/warehouses/abc"
		return c
	}
	proxyURL, _ := url.Parse("http://corp-proxy:3128")

	cases := []struct {
		name    string
		cfg     *config.Config
		resolve func(*http.Request) (*url.URL, error)
		want    string
	}{
		{
			name:    "proxy set for host",
			cfg:     validCfg(),
			resolve: func(*http.Request) (*url.URL, error) { return proxyURL, nil },
			want:    "http://corp-proxy:3128",
		},
		{
			// Warehouse-id addressing mode (HTTPPath == "") is what the kernel
			// prefers; the proxy must still resolve. Previously ToEndpointURL errored
			// on the empty path and the proxy was silently dropped to "" (direct).
			name: "warehouse-id mode (no HTTPPath) still resolves proxy",
			cfg: func() *config.Config {
				c := config.WithDefaults()
				c.Host = "my-workspace.databricks.com"
				c.Port = 443
				c.WarehouseID = "abc" // no HTTPPath
				return c
			}(),
			resolve: func(*http.Request) (*url.URL, error) { return proxyURL, nil },
			want:    "http://corp-proxy:3128",
		},
		{
			name:    "host excluded by NO_PROXY -> direct",
			cfg:     validCfg(),
			resolve: func(*http.Request) (*url.URL, error) { return nil, nil },
			want:    "",
		},
		{
			name:    "resolver error -> direct",
			cfg:     validCfg(),
			resolve: func(*http.Request) (*url.URL, error) { return nil, errors.New("bad proxy url") },
			want:    "",
		},
		{
			name: "unbuildable endpoint -> direct (resolver never consulted)",
			cfg: func() *config.Config {
				c := config.WithDefaults()
				c.Host = ""
				return c
			}(),
			resolve: func(*http.Request) (*url.URL, error) {
				t.Error("resolver must not be called for an unbuildable endpoint")
				return proxyURL, nil
			},
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := proxyForEndpointFunc(tc.cfg, tc.resolve); got != tc.want {
				t.Errorf("proxyForEndpointFunc = %q, want %q", got, tc.want)
			}
		})
	}

	// The production wrapper wires http.ProxyFromEnvironment and must not panic.
	_ = proxyForEndpoint(validCfg())
}

// resolveKernelProxy: an explicit WithKernelProxy (KernelExperimental.ProxyURL
// set) wins verbatim — url + credentials + bypass list — over the environment;
// with no explicit proxy it falls back to the endpoint's env-derived URL and
// leaves credentials / bypass empty. Pure Go, default CGO_ENABLED=0 build.
func TestResolveKernelProxy(t *testing.T) {
	t.Run("explicit WithKernelProxy wins verbatim over env", func(t *testing.T) {
		c := config.WithDefaults()
		c.Host = "my-workspace.databricks.com"
		c.Port = 443
		c.WarehouseID = "abc"
		c.KernelExperimental = &config.KernelExperimentalConfig{ //nolint:gosec // G101: test literals (ProxyPassword), not real credentials
			ProxyURL:         "http://explicit-proxy:8080",
			ProxyUsername:    "user",
			ProxyPassword:    "pass",
			ProxyBypassHosts: "localhost,*.internal",
		}
		var kc kernel.Config
		resolveKernelProxy(c, &kc)
		if kc.ProxyURL != "http://explicit-proxy:8080" {
			t.Errorf("ProxyURL = %q, want the explicit WithKernelProxy URL", kc.ProxyURL)
		}
		if kc.ProxyUsername != "user" || kc.ProxyPassword != "pass" || kc.ProxyBypassHosts != "localhost,*.internal" {
			t.Errorf("explicit proxy credentials/bypass not forwarded: %+v", kc)
		}
	})

	t.Run("no explicit proxy falls back to env (no creds/bypass)", func(t *testing.T) {
		c := config.WithDefaults()
		c.Host = "my-workspace.databricks.com"
		c.Port = 443
		c.WarehouseID = "abc"
		// No KernelExperimental proxy → env resolution. With no proxy env set in
		// the test process this resolves to direct (""), and the credential /
		// bypass fields must stay empty (the env path can't carry them).
		var kc kernel.Config
		resolveKernelProxy(c, &kc)
		if kc.ProxyUsername != "" || kc.ProxyPassword != "" || kc.ProxyBypassHosts != "" {
			t.Errorf("env path must not populate credentials/bypass: %+v", kc)
		}
	})
}
