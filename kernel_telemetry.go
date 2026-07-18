package dbsql

import (
	"github.com/databricks/databricks-sql-go/internal/backend/kernel"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/telemetry"
)

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag:
// the connection-config telemetry payload is assembled from pure Go config (no
// kernel C symbol), so keeping it untagged lets its test run under CGO_ENABLED=0.
// The connector (also untagged) emits it on the kernel path right after
// CREATE_SESSION.

// kernelConnectionTelemetry builds the connection-configuration telemetry payload
// for a kernel-backed connection: the resolved connection parameters (http path,
// proxy usage, arrow, query tags, metric-view metadata) and the auth mechanism.
// It populates the DriverConnectionParameters shape the proto defines (which the
// Thrift path never populates on this driver either); the kernel path is the first
// to actually emit it. Pure Go so it is unit-testable in the default build.
//
// It reports only what the kernel path genuinely applies: EnableArrow is always
// true (the kernel returns Arrow results), UseProxy reflects whether a proxy was
// resolved (env-var or WithKernelProxy), and it does not claim direct-results or a
// socket timeout the kernel C ABI has no knob for.
func kernelConnectionTelemetry(cfg *config.Config) *telemetry.DriverConnectionParameters {
	params := &telemetry.DriverConnectionParameters{
		HTTPPath: cfg.HTTPPath,
		// The `mode` telemetry field is a closed server-side enum
		// (DatabricksClientType = SEA | THRIFT | TYPE_UNSPECIFIED); it has no
		// "kernel" member. The kernel backend speaks the Statement Execution API,
		// so it reports "SEA" — the same value the JDBC and Python SEA paths land.
		// Emitting "kernel" here would drop the field to NULL on ingestion (the
		// enum rejects unknown members).
		Mode:        "SEA",
		EnableArrow: true,
		HostInfo: &telemetry.HostDetails{
			HostURL: cfg.Host,
			Port:    int32(cfg.Port), //nolint:gosec // port is a small positive int
		},
		UseProxy:             kernelUsesProxy(cfg),
		EnableMetricViewMeta: cfg.EnableMetricViewMetadata,
	}
	if qt := cfg.SessionParams["QUERY_TAGS"]; qt != "" {
		params.QueryTags = qt
	}
	mech, flow := kernelAuthMech(cfg)
	params.AuthMech = mech
	params.AuthFlow = flow
	return params
}

// kernelUsesProxy reports whether the kernel connection will route through a
// proxy: either an explicit WithKernelProxy or an environment-resolved one for
// this endpoint. Mirrors resolveKernelProxy's proxy-source precedence.
func kernelUsesProxy(cfg *config.Config) bool {
	if ke := cfg.KernelExperimental; ke != nil && ke.ProxyURL != "" {
		return true
	}
	return proxyForEndpoint(cfg) != ""
}

// kernelAuthMech maps the resolved kernel auth form to the telemetry auth_mech and
// auth_flow fields. Both are closed server-side enums, NOT the driver's own auth
// names — emitting a non-enum string (e.g. "Pat", "OauthM2M") drops the field to
// NULL on ingestion. The mapping mirrors the Python driver's
// TelemetryHelper.get_auth_mechanism / get_auth_flow so all drivers land the same
// values:
//   - auth_mech (AuthMech enum): PAT | OAUTH
//   - auth_flow (AuthFlow enum): CLIENT_CREDENTIALS (M2M) | BROWSER_BASED_AUTHENTICATION (U2M)
//
// PAT has no auth_flow (it is not an OAuth flow), so auth_flow is "" there. An
// unresolvable auth (rejected at connect anyway) reports empty (unspecified).
func kernelAuthMech(cfg *config.Config) (mech, flow string) {
	const (
		authMechPAT   = "PAT"
		authMechOAuth = "OAUTH"

		authFlowClientCreds = "CLIENT_CREDENTIALS"
		authFlowBrowser     = "BROWSER_BASED_AUTHENTICATION"
	)
	ka, err := resolveKernelAuth(cfg)
	if err != nil {
		return "", ""
	}
	switch ka.Mode {
	case kernel.AuthM2M:
		return authMechOAuth, authFlowClientCreds
	case kernel.AuthU2M:
		return authMechOAuth, authFlowBrowser
	default:
		return authMechPAT, ""
	}
}
