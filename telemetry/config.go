package telemetry

import (
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
		Enabled:              false, // Disabled by default, requires explicit opt-in
		ForceEnableTelemetry: false,
		EnableTelemetry:      false,
		BatchSize:            100,
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
