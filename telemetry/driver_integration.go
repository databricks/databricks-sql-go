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
//   - enableTelemetry: Client config overlay (unset = check server flag, true/false = override server)
//
// Returns:
//   - *Interceptor: Telemetry interceptor if enabled, nil otherwise
func InitializeForConnection(
	ctx context.Context,
	host string,
	driverVersion string,
	httpClient *http.Client,
	enableTelemetry config.ConfigValue[bool],
) *Interceptor {
	// Create telemetry config and apply client overlay.
	// ConfigValue[bool] semantics:
	//   - unset  → true (let server feature flag decide)
	//   - true   → true (server feature flag still consulted)
	//   - false  → false (explicitly disabled, skip server flag check)
	cfg := DefaultConfig()
	if val, isSet := enableTelemetry.Get(); isSet {
		cfg.EnableTelemetry = val
	} else {
		cfg.EnableTelemetry = true // Unset: default to enabled, server flag decides
	}

	// Get feature flag cache context FIRST (for reference counting)
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(host)

	// Check if telemetry should be enabled
	enabled := isTelemetryEnabled(ctx, cfg, host, driverVersion, httpClient)
	if !enabled {
		flagCache.releaseContext(host)
		return nil
	}

	// Get or create telemetry client for this host
	clientMgr := getClientManager()
	telemetryClient := clientMgr.getOrCreateClient(host, driverVersion, httpClient, cfg)
	if telemetryClient == nil {
		// Client failed to start; release the flag cache ref we incremented above
		flagCache.releaseContext(host)
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

	// Release feature flag cache reference
	flagCache := getFeatureFlagCache()
	flagCache.releaseContext(host)
}
