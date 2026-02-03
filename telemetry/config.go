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

	// EnableTelemetry is the client-side telemetry preference
	// - nil: not set by client, use server feature flag (default behavior)
	// - true: client wants telemetry enabled (overrides server)
	// - false: client wants telemetry disabled (overrides server)
	// This implements config overlay: client > server > default
	EnableTelemetry *bool

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
// Note: Telemetry uses config overlay - enabled by default, controlled by server feature flags.
// Clients can override by explicitly setting enableTelemetry=true/false.
func DefaultConfig() *Config {
	return &Config{
		Enabled:                 false, // Will be set based on overlay logic
		EnableTelemetry:         nil,   // nil = use server feature flag (config overlay)
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

	// Config overlay approach: client setting overrides server feature flag
	// Priority:
	//   1. Client explicit setting (enableTelemetry=true/false) - overrides server
	//   2. Server feature flag (when client doesn't set) - server controls
	//   3. Default disabled (when server flag unavailable) - fail-safe
	if v, ok := params["enableTelemetry"]; ok {
		enabled := (v == "true" || v == "1")
		cfg.EnableTelemetry = &enabled
	}
	// Note: If not present in params, stays nil (use server feature flag)

	if v, ok := params["telemetry_batch_size"]; ok {
		if size, err := strconv.Atoi(v); err == nil && size > 0 {
			cfg.BatchSize = size
		}
	}

	if v, ok := params["telemetry_flush_interval"]; ok {
		if duration, err := time.ParseDuration(v); err == nil {
			cfg.FlushInterval = duration
		}
	}

	return cfg
}

// isTelemetryEnabled checks if telemetry should be enabled for this connection.
// Implements config overlay approach with clear priority order.
//
// Config Overlay Priority (highest to lowest):
// 1. Client Config - enableTelemetry explicitly set (true/false) - overrides server
// 2. Server Config - feature flag controls when client doesn't specify
// 3. Fail-Safe Default - disabled when server flag unavailable/errors
//
// Parameters:
//   - ctx: Context for the request
//   - cfg: Telemetry configuration
//   - host: Databricks host to check feature flags against
//   - httpClient: HTTP client for making feature flag requests
//
// Returns:
//   - bool: true if telemetry should be enabled, false otherwise
func isTelemetryEnabled(ctx context.Context, cfg *Config, host string, httpClient *http.Client) bool {
	// Priority 1: Client explicitly set enableTelemetry (overrides server)
	if cfg.EnableTelemetry != nil {
		return *cfg.EnableTelemetry
	}

	// Priority 2: Check server-side feature flag
	flagCache := getFeatureFlagCache()
	serverEnabled, err := flagCache.isTelemetryEnabled(ctx, host, httpClient)
	if err != nil {
		// Priority 3: Fail-safe default (disabled)
		// On error, default to disabled to ensure telemetry failures don't impact driver
		return false
	}

	return serverEnabled
}

