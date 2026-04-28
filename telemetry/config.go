package telemetry

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
)

const (
	// maxTelemetryRetryCount caps DSN-provided retry count to prevent
	// excessive retries from misconfiguration.
	maxTelemetryRetryCount = 10

	// maxTelemetryRetryDelay caps DSN-provided retry delay to prevent
	// excessively long backoff from misconfiguration.
	maxTelemetryRetryDelay = 30 * time.Second
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
			if n > maxTelemetryRetryCount {
				logger.Debug().Msgf("telemetry: retry_count %d exceeds max %d, clamping", n, maxTelemetryRetryCount)
				n = maxTelemetryRetryCount
			}
			cfg.MaxRetries = n
		}
	}

	if v, ok := params["telemetry_retry_delay"]; ok {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			if d > maxTelemetryRetryDelay {
				logger.Debug().Msgf("telemetry: retry_delay %v exceeds max %v, clamping", d, maxTelemetryRetryDelay)
				d = maxTelemetryRetryDelay
			}
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
