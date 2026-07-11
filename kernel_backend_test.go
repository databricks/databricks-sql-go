//go:build cgo && databricks_kernel

package dbsql

import (
	"context"
	"net/http"
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
