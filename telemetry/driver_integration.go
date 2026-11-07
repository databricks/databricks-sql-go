package telemetry

import (
	"context"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/internal/featureflags"
)

// TelemetryInitOptions bundles the parameters for InitializeForConnection.
type TelemetryInitOptions struct {
	FeatureFlags featureflags.Request

	// EnableTelemetry is a tristate from the client DSN:
	//   unset  — server feature flag controls enablement
	//   true   — client explicitly opted in
	//   false  — client explicitly opted out
	EnableTelemetry config.ConfigValue[bool]

	// BatchSize is the number of metrics per batch (0 = use default).
	BatchSize int

	// FlushInterval is the flush interval (0 = use default).
	FlushInterval time.Duration
}

// InitializeForConnection initializes telemetry for a database connection.
// Returns an Interceptor if telemetry is enabled, nil otherwise.
// This function handles all the logic for checking feature flags and creating the interceptor.
func InitializeForConnection(ctx context.Context, opts TelemetryInitOptions) *Interceptor {
	// Create telemetry config and apply client overlay.
	// Priority: client DSN > server feature flag > default (disabled).
	cfg := DefaultConfig()
	if val, isSet := opts.EnableTelemetry.Get(); isSet {
		cfg.EnableTelemetry = &val // non-nil: client explicitly set via DSN
	}
	// When unset: cfg.EnableTelemetry remains nil, server feature flag controls enablement.
	if opts.BatchSize > 0 {
		cfg.BatchSize = opts.BatchSize
	}
	if opts.FlushInterval > 0 {
		cfg.FlushInterval = opts.FlushInterval
	}

	// Check if telemetry should be enabled
	request := opts.FeatureFlags
	enabled := isTelemetryEnabled(ctx, cfg, request)
	if !enabled {
		return nil
	}

	// Get or create telemetry client for this host
	clientMgr := getClientManager()
	telemetryClient := clientMgr.getOrCreateClient(request.Host, request.DriverVersion, request.UserAgent, request.HTTPClient, cfg)
	if telemetryClient == nil {
		return nil
	}

	return telemetryClient.GetInterceptor(true)
}

// ReleaseForConnection releases telemetry resources for a connection.
// Should be called when the connection is closed.
//
// Parameters:
//   - host: Databricks host
func ReleaseForConnection(host string) {
	// Release client manager reference
	clientMgr := getClientManager()
	_ = clientMgr.releaseClient(host)
}
