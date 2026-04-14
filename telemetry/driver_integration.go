package telemetry

import (
	"context"
	"net/http"
	"time"

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
//   - batchSize: Number of metrics per batch (0 = use default)
//   - flushInterval: Flush interval (0 = use default)
//   - retryCount: Max retry attempts (-1 = use default (3); 0 = disable retries)
//   - retryDelay: Base delay between retries (0 = use default)
//
// Returns:
//   - *Interceptor: Telemetry interceptor if enabled, nil otherwise
func InitializeForConnection(
	ctx context.Context,
	host string,
	driverVersion string,
	httpClient *http.Client,
	enableTelemetry config.ConfigValue[bool],
	batchSize int,
	flushInterval time.Duration,
	retryCount int,
	retryDelay time.Duration,
) *Interceptor {
	// Create telemetry config and apply client overlay.
	// Priority: client DSN > server feature flag > default (disabled).
	cfg := DefaultConfig()
	if val, isSet := enableTelemetry.Get(); isSet {
		cfg.EnableTelemetry = &val // non-nil: client explicitly set via DSN
	}
	// When unset: cfg.EnableTelemetry remains nil, server feature flag controls enablement.
	if batchSize > 0 {
		cfg.BatchSize = batchSize
	}
	if flushInterval > 0 {
		cfg.FlushInterval = flushInterval
	}
	if retryCount >= 0 {
		cfg.MaxRetries = retryCount
	}
	if retryDelay > 0 {
		cfg.RetryDelay = retryDelay
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
