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

	// EnableTelemetry is a tristate for the client DSN setting:
	//   nil   — not set by the client; server feature flag controls enablement
	//   &true — client explicitly opted in (overrides server flag)
	//   &false— client explicitly opted out (overrides server flag)
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
// EnableTelemetry is nil (unset): the server feature flag controls enablement.
func DefaultConfig() *Config {
	return &Config{
		Enabled:                 false,
		EnableTelemetry:         nil, // unset — server feature flag decides
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
			cfg.EnableTelemetry = &b // non-nil: client explicitly set via DSN
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

// isTelemetryEnabled returns true in exactly two cases:
//  1. The client explicitly set enableTelemetry=true in the DSN.
//  2. The client did not set enableTelemetry and the server feature flag is enabled
//     (databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver).
//
// In all other cases — explicit opt-out or server flag absent/unreachable — returns false.
func isTelemetryEnabled(ctx context.Context, cfg *Config, host string, driverVersion string, httpClient *http.Client) bool {
	if cfg.EnableTelemetry != nil {
		return *cfg.EnableTelemetry
	}

	serverEnabled, err := getFeatureFlagCache().isTelemetryEnabled(ctx, host, driverVersion, httpClient)
	if err != nil {
		return false
	}
	return serverEnabled
}
