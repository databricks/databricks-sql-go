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

	// ForceEnableTelemetry bypasses server-side feature flag checks
	// When true, telemetry is always enabled regardless of server flags
	ForceEnableTelemetry bool

	// EnableTelemetry indicates user wants telemetry enabled if server allows
	// Respects server-side feature flags and rollout percentage
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
		ForceEnableTelemetry:    false,
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

	// Check for forceEnableTelemetry flag (bypasses server feature flags)
	if v, ok := params["forceEnableTelemetry"]; ok {
		if v == "true" || v == "1" {
			cfg.ForceEnableTelemetry = true
			cfg.Enabled = true // Also set Enabled for backward compatibility
		}
	}

	// Check for enableTelemetry flag (respects server feature flags)
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
// 1. forceEnableTelemetry=true - Bypasses all server checks (testing/internal)
// 2. enableTelemetry=false - Explicit opt-out (always disabled)
// 3. enableTelemetry=true + Server Feature Flag - User opt-in with server control
// 4. Server Feature Flag Only - Default behavior (Databricks-controlled)
// 5. Default - Disabled (false)
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
	// Priority 1: Force enable bypasses all server checks
	if cfg.ForceEnableTelemetry {
		return true
	}

	// Priority 2: Explicit opt-out always disables
	// When enableTelemetry is explicitly set to false, respect that
	if !cfg.EnableTelemetry {
		return false
	}

	// Priority 3 & 4: Check server-side feature flag
	// This handles both:
	// - User explicitly opted in (enableTelemetry=true) - respect server decision
	// - Default behavior (no explicit setting) - server controls enablement
	flagCache := getFeatureFlagCache()
	serverEnabled, err := flagCache.isTelemetryEnabled(ctx, host, httpClient)
	if err != nil {
		// On error, respect default (disabled)
		// This ensures telemetry failures don't impact driver operation
		return false
	}

	return serverEnabled
}

