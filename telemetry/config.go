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

	// CircuitBreakerEnabled enables circuit breaker protection
	CircuitBreakerEnabled bool

	// CircuitBreakerThreshold is failures before opening circuit
	CircuitBreakerThreshold int

	// CircuitBreakerTimeout is time before retrying after open
	CircuitBreakerTimeout time.Duration
}

// DefaultConfig returns default telemetry configuration.
//
// BEHAVIORAL NOTE (SDR-approved): When EnableTelemetry is nil (the default),
// telemetry enablement is controlled by the server-side feature flag
// (databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver).
// This means telemetry may be active without the user explicitly opting in.
// The user can always override by setting enableTelemetry=true or enableTelemetry=false
// in the DSN or via WithEnableTelemetry(). No PII is collected; only aggregate
// driver performance metrics are sent to the Databricks telemetry endpoint.
func DefaultConfig() *Config {
	return &Config{
		Enabled:                 false,
		EnableTelemetry:         nil, // unset — server feature flag decides
		BatchSize:               200,
		FlushInterval:           30 * time.Second,
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

	// Note: telemetry_retry_count and telemetry_retry_delay DSN parameters
	// are accepted for backwards compatibility but are no longer applied
	// here. Retries are owned by the underlying retryablehttp-wrapped client
	// (see internal/client.RetryableClient), which honors Retry-After.
	return cfg
}

// isTelemetryEnabled returns true in exactly two cases:
//  1. The client explicitly set enableTelemetry=true in the DSN.
//  2. The client did not set enableTelemetry and the server feature flag is enabled
//     (databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver).
//
// In all other cases — explicit opt-out or server flag absent/unreachable — returns false.
func isTelemetryEnabled(ctx context.Context, cfg *Config, host string, driverVersion string, userAgent string, httpClient *http.Client) bool {
	if cfg.EnableTelemetry != nil {
		return *cfg.EnableTelemetry
	}

	serverEnabled, err := getFeatureFlagCache().isTelemetryEnabled(ctx, host, driverVersion, userAgent, httpClient)
	if err != nil {
		return false
	}
	return serverEnabled
}
