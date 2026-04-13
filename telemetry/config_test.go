package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Enabled {
		t.Error("Expected telemetry to be disabled by default, got enabled")
	}
	if cfg.EnableTelemetry != nil {
		t.Error("Expected EnableTelemetry to be nil (unset) by default")
	}
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
	cfg := ParseTelemetryConfig(map[string]string{})

	if cfg.Enabled {
		t.Error("Expected telemetry to be disabled by default")
	}
	if cfg.EnableTelemetry != nil {
		t.Error("Expected EnableTelemetry to be nil when DSN param absent")
	}
	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize 100, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_EnabledTrue(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"enableTelemetry": "true"})

	if cfg.EnableTelemetry == nil || !*cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be &true when set to 'true'")
	}
}

func TestParseTelemetryConfig_Enabled1(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"enableTelemetry": "1"})

	if cfg.EnableTelemetry == nil || !*cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be &true when set to '1'")
	}
}

func TestParseTelemetryConfig_EnabledFalse(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"enableTelemetry": "false"})

	if cfg.EnableTelemetry == nil || *cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be &false when set to 'false'")
	}
}

func TestParseTelemetryConfig_EnableTelemetry_IsSetWhenDSNProvided(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"enableTelemetry": "true"})

	if cfg.EnableTelemetry == nil {
		t.Error("Expected EnableTelemetry to be non-nil when enableTelemetry DSN param is present")
	}
}

func TestParseTelemetryConfig_EnableTelemetry_IsNilWhenDSNAbsent(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{})

	if cfg.EnableTelemetry != nil {
		t.Error("Expected EnableTelemetry to be nil when enableTelemetry DSN param is absent")
	}
}

func TestParseTelemetryConfig_BatchSize(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_batch_size": "50"})

	if cfg.BatchSize != 50 {
		t.Errorf("Expected BatchSize 50, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_BatchSizeInvalid(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_batch_size": "invalid"})

	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize to fallback to 100, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_BatchSizeZero(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_batch_size": "0"})

	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize to fallback to 100 when zero, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_BatchSizeNegative(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_batch_size": "-10"})

	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize to fallback to 100 when negative, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_FlushInterval(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_flush_interval": "10s"})

	if cfg.FlushInterval != 10*time.Second {
		t.Errorf("Expected FlushInterval 10s, got %v", cfg.FlushInterval)
	}
}

func TestParseTelemetryConfig_FlushIntervalInvalid(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_flush_interval": "invalid"})

	if cfg.FlushInterval != 5*time.Second {
		t.Errorf("Expected FlushInterval to fallback to 5s, got %v", cfg.FlushInterval)
	}
}

func TestParseTelemetryConfig_RetryCount(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_retry_count": "5"})

	if cfg.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", cfg.MaxRetries)
	}
}

func TestParseTelemetryConfig_RetryCountZero(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_retry_count": "0"})

	if cfg.MaxRetries != 0 {
		t.Errorf("Expected MaxRetries 0 (disable retries), got %d", cfg.MaxRetries)
	}
}

func TestParseTelemetryConfig_RetryCountInvalid(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_retry_count": "invalid"})

	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries to fallback to 3, got %d", cfg.MaxRetries)
	}
}

func TestParseTelemetryConfig_RetryDelay(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_retry_delay": "500ms"})

	if cfg.RetryDelay != 500*time.Millisecond {
		t.Errorf("Expected RetryDelay 500ms, got %v", cfg.RetryDelay)
	}
}

func TestParseTelemetryConfig_RetryDelayInvalid(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{"telemetry_retry_delay": "invalid"})

	if cfg.RetryDelay != 100*time.Millisecond {
		t.Errorf("Expected RetryDelay to fallback to 100ms, got %v", cfg.RetryDelay)
	}
}

func TestParseTelemetryConfig_MultipleParams(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{
		"enableTelemetry":          "true",
		"telemetry_batch_size":     "200",
		"telemetry_flush_interval": "30s",
	})

	if cfg.EnableTelemetry == nil || !*cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be &true")
	}
	if cfg.BatchSize != 200 {
		t.Errorf("Expected BatchSize 200, got %d", cfg.BatchSize)
	}
	if cfg.FlushInterval != 30*time.Second {
		t.Errorf("Expected FlushInterval 30s, got %v", cfg.FlushInterval)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries to remain default 3, got %d", cfg.MaxRetries)
	}
}

func TestParseTelemetryConfig_AllParams(t *testing.T) {
	cfg := ParseTelemetryConfig(map[string]string{
		"enableTelemetry":          "true",
		"telemetry_batch_size":     "200",
		"telemetry_flush_interval": "30s",
		"telemetry_retry_count":    "5",
		"telemetry_retry_delay":    "250ms",
	})

	if cfg.EnableTelemetry == nil || !*cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be &true")
	}
	if cfg.BatchSize != 200 {
		t.Errorf("Expected BatchSize 200, got %d", cfg.BatchSize)
	}
	if cfg.FlushInterval != 30*time.Second {
		t.Errorf("Expected FlushInterval 30s, got %v", cfg.FlushInterval)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", cfg.MaxRetries)
	}
	if cfg.RetryDelay != 250*time.Millisecond {
		t.Errorf("Expected RetryDelay 250ms, got %v", cfg.RetryDelay)
	}
}

// TestIsTelemetryEnabled_ExplicitOptOut: client DSN sets enableTelemetry=false → disabled,
// server flag is NOT consulted.
func TestIsTelemetryEnabled_ExplicitOptOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	cfg := &Config{EnableTelemetry: boolPtr(false)}

	result := isTelemetryEnabled(context.Background(), cfg, server.URL, "test-version", &http.Client{Timeout: 5 * time.Second})

	if result {
		t.Error("Expected telemetry to be disabled when client DSN sets enableTelemetry=false, got enabled")
	}
}

// TestIsTelemetryEnabled_UserOptInServerEnabled: client DSN sets enableTelemetry=true → enabled,
// server flag is NOT consulted (no network call).
func TestIsTelemetryEnabled_UserOptInServerEnabled(t *testing.T) {
	cfg := &Config{EnableTelemetry: boolPtr(true)}

	result := isTelemetryEnabled(context.Background(), cfg, "http://unreachable-host", "test-version", &http.Client{Timeout: 5 * time.Second})

	if !result {
		t.Error("Expected telemetry to be enabled when client DSN sets enableTelemetry=true, got disabled")
	}
}

// TestIsTelemetryEnabled_UserOptInServerDisabled: client DSN=true wins over server disabled.
func TestIsTelemetryEnabled_UserOptInServerDisabled(t *testing.T) {
	cfg := &Config{EnableTelemetry: boolPtr(true)}

	result := isTelemetryEnabled(context.Background(), cfg, "http://unreachable-host", "test-version", &http.Client{Timeout: 5 * time.Second})

	if !result {
		t.Error("Expected telemetry to be enabled when client explicitly opts in, got disabled")
	}
}

// TestIsTelemetryEnabled_DefaultChecksServerFlag: EnableTelemetry=nil → server flag controls.
// Server returns true → enabled.
func TestIsTelemetryEnabled_DefaultChecksServerFlag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(context.Background(), &Config{}, server.URL, "test-version", &http.Client{Timeout: 5 * time.Second})

	if !result {
		t.Error("Expected telemetry to be enabled when server flag is true and EnableTelemetry is nil, got disabled")
	}
}

// TestIsTelemetryEnabled_DefaultServerDisabled: EnableTelemetry=nil, server returns false → disabled.
func TestIsTelemetryEnabled_DefaultServerDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "false"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(context.Background(), &Config{}, server.URL, "test-version", &http.Client{Timeout: 5 * time.Second})

	if result {
		t.Error("Expected telemetry to be disabled when server flag is false and EnableTelemetry is nil, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerFlagOnly: EnableTelemetry=nil, server returns true → enabled.
func TestIsTelemetryEnabled_ServerFlagOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(context.Background(), &Config{}, server.URL, "test-version", &http.Client{Timeout: 5 * time.Second})

	if !result {
		t.Error("Expected telemetry to be enabled when server flag is true and no DSN override, got disabled")
	}
}

// TestIsTelemetryEnabled_Default: EnableTelemetry=nil (default), server unreachable → disabled.
// Neither priority 1 nor priority 2 fires, so telemetry is off.
func TestIsTelemetryEnabled_Default(t *testing.T) {
	result := isTelemetryEnabled(context.Background(), DefaultConfig(), "test-host", "test-version", &http.Client{Timeout: 5 * time.Second})

	if result {
		t.Error("Expected telemetry to be disabled when EnableTelemetry is nil and server is unreachable, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerError: EnableTelemetry=nil, server errors → disabled.
// Priority 2 fails, so telemetry stays off.
func TestIsTelemetryEnabled_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(context.Background(), &Config{}, server.URL, "test-version", &http.Client{Timeout: 5 * time.Second})

	if result {
		t.Error("Expected telemetry to be disabled when server errors and EnableTelemetry is nil, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerUnreachable: EnableTelemetry=nil, server unreachable → disabled.
// Priority 2 fails, so telemetry stays off.
func TestIsTelemetryEnabled_ServerUnreachable(t *testing.T) {
	flagCache := getFeatureFlagCache()
	unreachableHost := "http://localhost:9999"
	flagCache.getOrCreateContext(unreachableHost)
	defer flagCache.releaseContext(unreachableHost)

	result := isTelemetryEnabled(context.Background(), &Config{}, unreachableHost, "test-version", &http.Client{Timeout: 1 * time.Second})

	if result {
		t.Error("Expected telemetry to be disabled when server is unreachable and EnableTelemetry is nil, got enabled")
	}
}
