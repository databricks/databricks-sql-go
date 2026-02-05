package telemetry

import (
	"context"
	"net/http"

	"github.com/databricks/databricks-sql-go/internal/config"
)

// InitializeForConnection initializes telemetry for a database connection.
// Returns an Interceptor if telemetry is enabled, nil otherwise.
// This function handles all the logic for checking feature flags and creating the interceptor.
//
// Parameters:
//   - ctx: Context for the initialization
//   - host: Databricks host
//   - driverVersion: Driver version string
//   - httpClient: HTTP client for making requests
//   - enableTelemetry: User opt-in flag (nil = unset, true = enable, false = disable)
//
// Returns:
//   - *Interceptor: Telemetry interceptor if enabled, nil otherwise
func InitializeForConnection(
	ctx context.Context,
	host string,
	driverVersion string,
	httpClient *http.Client,
	enableTelemetry *bool,
) *Interceptor {
	// Create telemetry config
	cfg := DefaultConfig()

	// Set EnableTelemetry based on user preference
	if enableTelemetry != nil {
		cfg.EnableTelemetry = config.NewConfigValue(*enableTelemetry)
	}
	// else: leave unset (will check server feature flag)

	// Check if telemetry should be enabled
	if !isTelemetryEnabled(ctx, cfg, host, httpClient) {
		return nil
	}

	// Get or create telemetry client for this host
	clientMgr := getClientManager()
	telemetryClient := clientMgr.getOrCreateClient(host, driverVersion, httpClient, cfg)
	if telemetryClient == nil {
		return nil
	}

	// Get feature flag cache context (for reference counting)
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(host)

	// Return interceptor
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

	// Release feature flag cache reference
	flagCache := getFeatureFlagCache()
	flagCache.releaseContext(host)
}
