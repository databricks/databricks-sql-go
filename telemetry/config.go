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
// Note: Telemetry is disabled by default and requires explicit opt-in.
func DefaultConfig() *Config {
	return &Config{
		Enabled:                 false, // Disabled by default, requires explicit opt-in
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

	// Check for enableTelemetry flag (follows client > server > default priority)
	if v, ok := params["enableTelemetry"]; ok {
		if v == "true" || v == "1" {
			cfg.EnableTelemetry = true
		} else if v == "false" || v == "0" {
			cfg.EnableTelemetry = false
		}
	}

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
// Implements the priority-based decision tree for telemetry enablement.
//
// Priority (highest to lowest):
// 1. enableTelemetry=true - Client opt-in (server feature flag still consulted)
// 2. enableTelemetry=false - Explicit opt-out (always disabled)
// 3. Server Feature Flag Only - Default behavior (Databricks-controlled)
// 4. Default - Disabled (false)
//
// Parameters:
//   - ctx: Context for the request
//   - cfg: Telemetry configuration
//   - host: Databricks host to check feature flags against
//   - httpClient: HTTP client for making feature flag requests
//
// Returns:
//   - bool: true if telemetry should be enabled, false otherwise
func isTelemetryEnabled(ctx context.Context, cfg *Config, host string, driverVersion string, httpClient *http.Client) bool {
	// Priority 1 & 2: Respect client preference when explicitly set
	// enableTelemetry=false → always disabled; enableTelemetry=true → check server flag
	// When enableTelemetry is explicitly set to false, respect that
	if !cfg.EnableTelemetry {
		return false
	}

	// Priority 3 & 4: Check server-side feature flag
	// This handles both:
	// - User explicitly opted in (enableTelemetry=true) - respect server decision
	// - Default behavior (no explicit setting) - server controls enablement
	flagCache := getFeatureFlagCache()
	serverEnabled, err := flagCache.isTelemetryEnabled(ctx, host, driverVersion, httpClient)
	if err != nil {
		// On error, respect default (disabled)
		// This ensures telemetry failures don't impact driver operation
		return false
	}

	return serverEnabled
}
