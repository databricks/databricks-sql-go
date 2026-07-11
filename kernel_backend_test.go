//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// nonPATAuth is a stand-in for any non-PAT authenticator (OAuth / token-provider /
// external / federated) — the kernel backend must reject it at connect.
type nonPATAuth struct{}

func (nonPATAuth) Authenticate(*http.Request) error { return nil }

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

	t.Run("PAT via WithAuthenticator ok", func(t *testing.T) {
		// WithAuthenticator(&pat.PATAuth{...}) sets only cfg.Authenticator, leaving
		// cfg.AccessToken empty. The backend must still build (token sourced from
		// the authenticator), not fail with an empty PAT at connect.
		c := base()
		c.AccessToken = ""
		c.Authenticator = &pat.PATAuth{AccessToken: "dapi-y"}
		if _, err := newKernelBackend(context.Background(), c); err != nil {
			t.Errorf("PAT supplied via WithAuthenticator should build cleanly, got %v", err)
		}
	})

	t.Run("empty token rejected", func(t *testing.T) {
		c := base()
		c.AccessToken = ""
		c.Authenticator = &pat.PATAuth{AccessToken: ""}
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("expected an error when the resolved PAT is empty")
		}
	})

	t.Run("non-PAT authenticator rejected", func(t *testing.T) {
		c := base()
		c.Authenticator = nonPATAuth{}
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("expected an error for a non-PAT authenticator")
		}
	})

	t.Run("query timeout rejected", func(t *testing.T) {
		c := base()
		c.QueryTimeout = 30 * time.Second
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("expected an error when WithTimeout (query timeout) is set on the kernel backend")
		}
	})

	t.Run("disabling retries rejected", func(t *testing.T) {
		c := base()
		c.RetryMax = -1
		if _, err := newKernelBackend(context.Background(), c); err == nil {
			t.Error("expected an error when retries are disabled (WithRetries(-1)) on the kernel backend")
		}
	})

	t.Run("positive retry tuning + maxrows accepted", func(t *testing.T) {
		// These are accepted (not applied) per doc.go — the kernel manages fetch /
		// retries internally, so they must not error.
		c := base()
		c.RetryMax = 8
		c.MaxRows = 5000
		if _, err := newKernelBackend(context.Background(), c); err != nil {
			t.Errorf("positive retry/maxrows tuning should build cleanly, got %v", err)
		}
	})
}

// proxyForEndpointFunc maps the injected resolver's decision to a proxy URL
// string (or "" for direct), and returns "" for an unbuildable endpoint. The
// production path uses http.ProxyFromEnvironment, whose env is snapshotted once
// per process (sync.Once) and so can't be re-driven mid-test; the resolver seam
// lets us assert every branch deterministically. Each case mirrors an
// httpproxy-style outcome: proxy set for this host, host excluded by NO_PROXY,
// no proxy configured, and an unbuildable endpoint.
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
