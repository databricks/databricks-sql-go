package dbsql

import (
	"testing"
	"time"

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
		if p.SocketTimeout != 900_000 {
			t.Errorf("SocketTimeout = %d, want 900000", p.SocketTimeout)
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

	t.Run("request timeout reports the C ABI value", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.ClientTimeout = 0
		if got := kernelConnectionTelemetry(cfg).SocketTimeout; got != 0 {
			t.Errorf("SocketTimeout = %d, want 0 (use kernel default)", got)
		}

		cfg.ClientTimeout = time.Nanosecond
		if got := kernelConnectionTelemetry(cfg).SocketTimeout; got != 1 {
			t.Errorf("SocketTimeout = %d, want rounded-up 1", got)
		}
	})

	t.Run("explicit WithKernelProxy marks UseProxy", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.AccessToken = "dapi-x"
		WithKernelProxy(KernelProxy{URL: "http://proxy:3128"})(cfg)

		if p := kernelConnectionTelemetry(cfg); !p.UseProxy {
			t.Error("UseProxy = false, want true when WithKernelProxy is set")
		}
	})

	t.Run("resolved retry policy + chunk cap are reflected", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.AccessToken = "dapi-x"
		WithRetries(7, 0, 0)(cfg)
		WithKernelRetryOverallTimeout(90 * time.Second)(cfg)
		WithKernelMaxChunksInMemory(4)(cfg)

		p := kernelConnectionTelemetry(cfg)
		if p.RetryMaxAttempts != 7 {
			t.Errorf("RetryMaxAttempts = %d, want 7", p.RetryMaxAttempts)
		}
		if p.RetryOverallTimeoutMs != 90_000 {
			t.Errorf("RetryOverallTimeoutMs = %d, want 90000", p.RetryOverallTimeoutMs)
		}
		if p.MaxChunksInMemory != 4 {
			t.Errorf("MaxChunksInMemory = %d, want 4", p.MaxChunksInMemory)
		}
	})

	t.Run("retry/chunk fields stay zero (omitted) at defaults", func(t *testing.T) {
		// WithDefaults sets RetryMax=4, but the disable/default form and the unset
		// kernel-only knobs must not fabricate telemetry: only an explicit positive
		// RetryMax is emitted, so document that the default RetryMax IS surfaced while
		// the kernel-only knobs stay zero when unset.
		cfg := config.WithDefaults()
		cfg.AccessToken = "dapi-x"
		p := kernelConnectionTelemetry(cfg)
		if int(p.RetryMaxAttempts) != cfg.RetryMax {
			t.Errorf("RetryMaxAttempts = %d, want %d (the resolved RetryMax)", p.RetryMaxAttempts, cfg.RetryMax)
		}
		if p.RetryOverallTimeoutMs != 0 || p.MaxChunksInMemory != 0 {
			t.Errorf("kernel-only knobs should be zero when unset: overall=%d chunks=%d",
				p.RetryOverallTimeoutMs, p.MaxChunksInMemory)
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

	// M2M and U2M are the values that silently NULL server-side if the (mech, flow)
	// enums are wrong, so assert both arms explicitly — not just PAT. resolveKernelAuth
	// selects the arm by asserting the M2M/U2M provider interfaces on cfg.Authenticator,
	// which the fake authenticators satisfy (see kernel_config_test.go).
	t.Run("M2M -> OAUTH / CLIENT_CREDENTIALS", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.AccessToken = ""
		cfg.Authenticator = fakeM2MAuth{id: "cid", secret: "sec", scopes: []string{"all-apis"}}
		mech, flow := kernelAuthMech(cfg)
		if mech != "OAUTH" || flow != "CLIENT_CREDENTIALS" {
			t.Errorf("M2M -> (%q, %q), want (OAUTH, CLIENT_CREDENTIALS)", mech, flow)
		}
	})

	t.Run("U2M -> OAUTH / BROWSER_BASED_AUTHENTICATION", func(t *testing.T) {
		cfg := config.WithDefaults()
		cfg.AccessToken = ""
		cfg.Authenticator = fakeU2MAuth{id: "databricks-sql-connector"}
		mech, flow := kernelAuthMech(cfg)
		if mech != "OAUTH" || flow != "BROWSER_BASED_AUTHENTICATION" {
			t.Errorf("U2M -> (%q, %q), want (OAUTH, BROWSER_BASED_AUTHENTICATION)", mech, flow)
		}
	})
}
