package telemetry

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Verify telemetry is disabled by default
	if cfg.Enabled {
		t.Error("Expected telemetry to be disabled by default, got enabled")
	}

	// Verify other defaults
	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize 100, got %d", cfg.BatchSize)
	}

	if cfg.FlushInterval != 5*time.Second {
		t.Errorf("Expected FlushInterval 5s, got %v", cfg.FlushInterval)
	}

	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries 3, got %d", cfg.MaxRetries)
	}

	if cfg.RetryDelay != 100*time.Millisecond {
		t.Errorf("Expected RetryDelay 100ms, got %v", cfg.RetryDelay)
	}

	if !cfg.CircuitBreakerEnabled {
		t.Error("Expected CircuitBreakerEnabled true, got false")
	}

	if cfg.CircuitBreakerThreshold != 5 {
		t.Errorf("Expected CircuitBreakerThreshold 5, got %d", cfg.CircuitBreakerThreshold)
	}

	if cfg.CircuitBreakerTimeout != 1*time.Minute {
		t.Errorf("Expected CircuitBreakerTimeout 1m, got %v", cfg.CircuitBreakerTimeout)
	}
}

func TestParseTelemetryConfig_EmptyParams(t *testing.T) {
	params := map[string]string{}
	cfg := ParseTelemetryConfig(params)

	// Should return defaults
	if cfg.Enabled {
		t.Error("Expected telemetry to be disabled by default")
	}

	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize 100, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_EnabledTrue(t *testing.T) {
	params := map[string]string{
		"telemetry": "true",
	}
	cfg := ParseTelemetryConfig(params)

	if !cfg.Enabled {
		t.Error("Expected telemetry to be enabled when set to 'true'")
	}
}

func TestParseTelemetryConfig_Enabled1(t *testing.T) {
	params := map[string]string{
		"telemetry": "1",
	}
	cfg := ParseTelemetryConfig(params)

	if !cfg.Enabled {
		t.Error("Expected telemetry to be enabled when set to '1'")
	}
}

func TestParseTelemetryConfig_EnabledFalse(t *testing.T) {
	params := map[string]string{
		"telemetry": "false",
	}
	cfg := ParseTelemetryConfig(params)

	if cfg.Enabled {
		t.Error("Expected telemetry to be disabled when set to 'false'")
	}
}

func TestParseTelemetryConfig_BatchSize(t *testing.T) {
	params := map[string]string{
		"telemetry_batch_size": "50",
	}
	cfg := ParseTelemetryConfig(params)

	if cfg.BatchSize != 50 {
		t.Errorf("Expected BatchSize 50, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_BatchSizeInvalid(t *testing.T) {
	params := map[string]string{
		"telemetry_batch_size": "invalid",
	}
	cfg := ParseTelemetryConfig(params)

	// Should fall back to default
	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize to fallback to 100, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_BatchSizeZero(t *testing.T) {
	params := map[string]string{
		"telemetry_batch_size": "0",
	}
	cfg := ParseTelemetryConfig(params)

	// Should ignore zero and use default
	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize to fallback to 100 when zero, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_BatchSizeNegative(t *testing.T) {
	params := map[string]string{
		"telemetry_batch_size": "-10",
	}
	cfg := ParseTelemetryConfig(params)

	// Should ignore negative and use default
	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize to fallback to 100 when negative, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_FlushInterval(t *testing.T) {
	params := map[string]string{
		"telemetry_flush_interval": "10s",
	}
	cfg := ParseTelemetryConfig(params)

	if cfg.FlushInterval != 10*time.Second {
		t.Errorf("Expected FlushInterval 10s, got %v", cfg.FlushInterval)
	}
}

func TestParseTelemetryConfig_FlushIntervalInvalid(t *testing.T) {
	params := map[string]string{
		"telemetry_flush_interval": "invalid",
	}
	cfg := ParseTelemetryConfig(params)

	// Should fall back to default
	if cfg.FlushInterval != 5*time.Second {
		t.Errorf("Expected FlushInterval to fallback to 5s, got %v", cfg.FlushInterval)
	}
}

func TestParseTelemetryConfig_MultipleParams(t *testing.T) {
	params := map[string]string{
		"telemetry":                "true",
		"telemetry_batch_size":     "200",
		"telemetry_flush_interval": "30s",
	}
	cfg := ParseTelemetryConfig(params)

	if !cfg.Enabled {
		t.Error("Expected telemetry to be enabled")
	}

	if cfg.BatchSize != 200 {
		t.Errorf("Expected BatchSize 200, got %d", cfg.BatchSize)
	}

	if cfg.FlushInterval != 30*time.Second {
		t.Errorf("Expected FlushInterval 30s, got %v", cfg.FlushInterval)
	}

	// Other fields should still have defaults
	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries to remain default 3, got %d", cfg.MaxRetries)
	}
}
