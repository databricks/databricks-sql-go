package dbsql

import (
	"testing"

	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/internal/config"
)

// These tests run in the default CGO_ENABLED=0 build (kernel_telemetry.go is
// untagged): the connection-config payload is assembled from pure Go config.

// kernelConnectionTelemetry must emit the closed-enum values the ingestion schema
// accepts — mode="SEA" (not "kernel"), auth_mech ∈ {PAT, OAUTH}, auth_flow ∈
// {CLIENT_CREDENTIALS, BROWSER_BASED_AUTHENTICATION} — and reflect the resolved
// connection config (arrow, http path, query tags, metric-view, proxy usage).
func TestKernelConnectionTelemetry(t *testing.T) {
	t.Run("PAT connection reports SEA + PAT with no auth_flow", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.Host = "example.cloud.databricks.com"
		cfg.Port = 443
		cfg.HTTPPath = "/sql/1.0/warehouses/abc"
		cfg.AccessToken = "dapi-x"
		cfg.Authenticator = &pat.PATAuth{AccessToken: "dapi-x"}

		p := kernelConnectionTelemetry(cfg)
		if p.Mode != "SEA" {
			t.Errorf("Mode = %q, want SEA (the kernel speaks the Statement Execution API; 'kernel' would NULL the field)", p.Mode)
		}
		if p.AuthMech != "PAT" {
			t.Errorf("AuthMech = %q, want PAT", p.AuthMech)
		}
		if p.AuthFlow != "" {
			t.Errorf("AuthFlow = %q, want empty for PAT (not an OAuth flow)", p.AuthFlow)
		}
		if !p.EnableArrow {
			t.Error("EnableArrow = false, want true (the kernel returns Arrow results)")
		}
		if p.HTTPPath != "/sql/1.0/warehouses/abc" {
			t.Errorf("HTTPPath = %q, want the configured path", p.HTTPPath)
		}
		if p.HostInfo == nil || p.HostInfo.HostURL != "example.cloud.databricks.com" || p.HostInfo.Port != 443 {
			t.Errorf("HostInfo = %+v, want host/port populated", p.HostInfo)
		}
	})

	t.Run("query tags + metric-view are reflected", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.AccessToken = "dapi-x"
		cfg.SessionParams = map[string]string{"QUERY_TAGS": "team=peco"}
		cfg.EnableMetricViewMetadata = true

		p := kernelConnectionTelemetry(cfg)
		if p.QueryTags != "team=peco" {
			t.Errorf("QueryTags = %q, want team=peco", p.QueryTags)
		}
		if !p.EnableMetricViewMeta {
			t.Error("EnableMetricViewMeta = false, want true")
		}
	})

	t.Run("explicit WithKernelProxy marks UseProxy", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.AccessToken = "dapi-x"
		WithKernelProxy("http://proxy:3128", "", "", "")(cfg)

		if p := kernelConnectionTelemetry(cfg); !p.UseProxy {
			t.Error("UseProxy = false, want true when WithKernelProxy is set")
		}
	})
}

// kernelAuthMech maps each resolvable auth form to the closed AuthMech/AuthFlow
// enums. M2M and U2M are both OAUTH but differ in flow; PAT is PAT with no flow.
func TestKernelAuthMech(t *testing.T) {
	t.Run("PAT", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.AccessToken = "dapi-x"
		mech, flow := kernelAuthMech(cfg)
		if mech != "PAT" || flow != "" {
			t.Errorf("PAT -> (%q, %q), want (PAT, \"\")", mech, flow)
		}
	})
}
