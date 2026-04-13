package telemetry

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

// Config holds telemetry configuration.
type Config struct {
	// Enabled controls whether telemetry is active
	Enabled bool

	// EnableTelemetry indicates user wants telemetry enabled.
	// Follows client > server > default priority.
	EnableTelemetry bool

	// ClientExplicit is true when the client explicitly set EnableTelemetry via DSN.
	// When true, EnableTelemetry is used directly (overrides the server feature flag).
	// When false (default), the server feature flag controls enablement.
	ClientExplicit bool

	// BatchSize is the number of metrics to batch before flushing
	BatchSize int

	// FlushInterval is how often to flush metrics
	FlushInterval time.Duration

	// MaxRetries is the maximum number of retry attempts
	MaxRetries int

	// RetryDelay is the base delay between retries
	RetryDelay time.Duration

	// CircuitBreakerEnabled enables circuit breaker protection
	CircuitBreakerEnabled bool

	// CircuitBreakerThreshold is failures before opening circuit
	CircuitBreakerThreshold int

	// CircuitBreakerTimeout is time before retrying after open
	CircuitBreakerTimeout time.Duration
}

// DefaultConfig returns default telemetry configuration.
// Note: Telemetry is disabled by default. The default will remain false until
// server-side feature flags are wired in to control the rollout.
func DefaultConfig() *Config {
	return &Config{
		Enabled:                 false,
		EnableTelemetry:         false,
		BatchSize:               100,
		FlushInterval:           5 * time.Second,
		MaxRetries:              3,
		RetryDelay:              100 * time.Millisecond,
		CircuitBreakerEnabled:   true,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   1 * time.Minute,
	}
}

// ParseTelemetryConfig extracts telemetry config from connection parameters.
func ParseTelemetryConfig(params map[string]string) *Config {
	cfg := DefaultConfig()

	if v, ok := params["enableTelemetry"]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.EnableTelemetry = b
			cfg.ClientExplicit = true // client explicitly set via DSN
		}
	}

	if v, ok := params["telemetry_batch_size"]; ok {
		if size, err := strconv.Atoi(v); err == nil && size > 0 {
			cfg.BatchSize = size
		}
	}

	if v, ok := params["telemetry_flush_interval"]; ok {
		if duration, err := time.ParseDuration(v); err == nil && duration > 0 {
			cfg.FlushInterval = duration
		}
	}

	if v, ok := params["telemetry_retry_count"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.MaxRetries = n
		}
	}

	if v, ok := params["telemetry_retry_delay"]; ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.RetryDelay = d
		}
	}

	return cfg
}

// isTelemetryEnabled checks if telemetry should be enabled for this connection.
// Implements the priority-based decision tree for telemetry enablement.
//
// Priority (highest to lowest):
//  1. Client DSN setting (enableTelemetry=true|false) — overrides server when ClientExplicit=true
//  2. Server feature flag (databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver)
//  3. Default: disabled
//
// Parameters:
//   - ctx: Context for the request
//   - cfg: Telemetry configuration
//   - host: Databricks host to check feature flags against
//   - driverVersion: Driver version string
//   - httpClient: HTTP client for making feature flag requests
//
// Returns:
//   - bool: true if telemetry should be enabled, false otherwise
func isTelemetryEnabled(ctx context.Context, cfg *Config, host string, driverVersion string, httpClient *http.Client) bool {
	// Priority 1: Client DSN setting overrides server when explicitly set
	// enableTelemetry=true  → enabled (no server check)
	// enableTelemetry=false → disabled (no server check)
	if cfg.ClientExplicit {
		return cfg.EnableTelemetry
	}

	// Priority 2: Check server-side feature flag (no explicit client setting)
	flagCache := getFeatureFlagCache()
	serverEnabled, err := flagCache.isTelemetryEnabled(ctx, host, driverVersion, httpClient)
	if err != nil {
		// Priority 3: Default disabled on server error
		return false
	}

	return serverEnabled
}
