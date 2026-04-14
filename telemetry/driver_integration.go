package telemetry

import (
	"context"
	"net/http"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
)

// TelemetryInitOptions bundles the parameters for InitializeForConnection.
type TelemetryInitOptions struct {
	// Host is the Databricks host.
	Host string

	// DriverVersion is the driver version string.
	DriverVersion string

	// HTTPClient is the HTTP client used for both feature-flag checks and
	// telemetry export. The /telemetry-ext endpoint requires authentication,
	// so this should be the authenticated driver client.
	HTTPClient *http.Client

	// EnableTelemetry is a tristate from the client DSN:
	//   unset  — server feature flag controls enablement
	//   true   — client explicitly opted in
	//   false  — client explicitly opted out
	EnableTelemetry config.ConfigValue[bool]

	// BatchSize is the number of metrics per batch (0 = use default 100).
	BatchSize int

	// FlushInterval is the flush interval (0 = use default 5s).
	FlushInterval time.Duration

	// RetryCount is max retry attempts (-1 = use default 3; 0 = disable retries).
	RetryCount int

	// RetryDelay is the base delay between retries (0 = use default 100ms).
	RetryDelay time.Duration
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
	if opts.RetryCount >= 0 {
		cfg.MaxRetries = opts.RetryCount
	}
	if opts.RetryDelay > 0 {
		cfg.RetryDelay = opts.RetryDelay
	}

	// Get feature flag cache context FIRST (for reference counting)
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(opts.Host)

	// Check if telemetry should be enabled
	enabled := isTelemetryEnabled(ctx, cfg, opts.Host, opts.DriverVersion, opts.HTTPClient)
	if !enabled {
		flagCache.releaseContext(opts.Host)
		return nil
	}

	// Get or create telemetry client for this host
	clientMgr := getClientManager()
	telemetryClient := clientMgr.getOrCreateClient(opts.Host, opts.DriverVersion, opts.HTTPClient, cfg)
	if telemetryClient == nil {
		// Client failed to start; release the flag cache ref we incremented above
		flagCache.releaseContext(opts.Host)
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
