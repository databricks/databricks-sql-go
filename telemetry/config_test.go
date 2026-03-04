package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/databricks/databricks-sql-go/internal/config"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Verify telemetry uses config overlay (nil = use server flag)
	if cfg.Enabled {
		t.Error("Expected Enabled to be false by default")
	}

	// Verify EnableTelemetry is unset (config overlay - use server flag)
	if cfg.EnableTelemetry.IsSet() {
		t.Error("Expected EnableTelemetry to be unset (use server flag), got set")
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

	// Should return defaults - EnableTelemetry unset means use server flag
	if cfg.EnableTelemetry.IsSet() {
		t.Error("Expected EnableTelemetry to be unset (use server flag) when no params provided")
	}

	if cfg.BatchSize != 100 {
		t.Errorf("Expected BatchSize 100, got %d", cfg.BatchSize)
	}
}

func TestParseTelemetryConfig_EnabledTrue(t *testing.T) {
	params := map[string]string{
		"enableTelemetry": "true",
	}
	cfg := ParseTelemetryConfig(params)

	val, ok := cfg.EnableTelemetry.Get()
	if !ok || !val {
		t.Error("Expected EnableTelemetry to be true when set to 'true'")
	}
}

func TestParseTelemetryConfig_Enabled1(t *testing.T) {
	params := map[string]string{
		"enableTelemetry": "1",
	}
	cfg := ParseTelemetryConfig(params)

	val, ok := cfg.EnableTelemetry.Get()
	if !ok || !val {
		t.Error("Expected EnableTelemetry to be true when set to '1'")
	}
}

func TestParseTelemetryConfig_EnabledFalse(t *testing.T) {
	params := map[string]string{
		"enableTelemetry": "false",
	}
	cfg := ParseTelemetryConfig(params)

	val, ok := cfg.EnableTelemetry.Get()
	if !ok || val {
		t.Error("Expected EnableTelemetry to be false when set to 'false'")
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
		"enableTelemetry":          "true",
		"telemetry_batch_size":     "200",
		"telemetry_flush_interval": "30s",
	}
	cfg := ParseTelemetryConfig(params)

	val, ok := cfg.EnableTelemetry.Get()
	if !ok || !val {
		t.Error("Expected EnableTelemetry to be true")
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

// TestIsTelemetryEnabled_ClientOverrideEnabled tests Priority 1: client explicitly enables (overrides server)
func TestIsTelemetryEnabled_ClientOverrideEnabled(t *testing.T) {
	// Setup: Create a server that returns disabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Server says disabled, but client override should win
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": false,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		EnableTelemetry: config.NewConfigValue(true), // Priority 1: Client explicitly enables
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	// Client override should bypass server check
	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if !result {
		t.Error("Expected telemetry to be enabled when client explicitly sets enableTelemetry=true, got disabled")
	}
}

// TestIsTelemetryEnabled_ClientOverrideDisabled tests Priority 1: client explicitly disables (overrides server)
func TestIsTelemetryEnabled_ClientOverrideDisabled(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Server says enabled, but client override should win
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": true,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		EnableTelemetry: config.NewConfigValue(false), // Priority 1: Client explicitly disables
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if result {
		t.Error("Expected telemetry to be disabled when client explicitly sets enableTelemetry=false, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerEnabled tests Priority 2: server flag enables (client didn't set)
func TestIsTelemetryEnabled_ServerEnabled(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": true,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		EnableTelemetry: config.ConfigValue[bool]{}, // Client didn't set - use server flag
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if !result {
		t.Error("Expected telemetry to be enabled when server flag is true, got disabled")
	}
}

// TestIsTelemetryEnabled_ServerDisabled tests Priority 2: server flag disables (client didn't set)
func TestIsTelemetryEnabled_ServerDisabled(t *testing.T) {
	// Setup: Create a server that returns disabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": false,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		EnableTelemetry: config.ConfigValue[bool]{}, // Client didn't set - use server flag
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if result {
		t.Error("Expected telemetry to be disabled when server flag is false, got enabled")
	}
}

// TestIsTelemetryEnabled_FailSafeDefault tests Priority 3: default disabled when server unavailable
func TestIsTelemetryEnabled_FailSafeDefault(t *testing.T) {
	cfg := DefaultConfig()

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// No server available, should default to disabled (fail-safe)
	result := isTelemetryEnabled(ctx, cfg, "nonexistent-host", httpClient)

	if result {
		t.Error("Expected telemetry to be disabled when server unavailable (fail-safe), got enabled")
	}
}

// TestIsTelemetryEnabled_ServerError tests Priority 3: fail-safe default on server error
func TestIsTelemetryEnabled_ServerError(t *testing.T) {
	// Setup: Create a server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &Config{
		EnableTelemetry: config.ConfigValue[bool]{}, // Client didn't set - should use server, but server errors
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	// On error, should default to disabled (fail-safe)
	if result {
		t.Error("Expected telemetry to be disabled on server error (fail-safe), got enabled")
	}
}

// TestIsTelemetryEnabled_ServerUnreachable tests Priority 3: fail-safe default on unreachable server
func TestIsTelemetryEnabled_ServerUnreachable(t *testing.T) {
	cfg := &Config{
		EnableTelemetry: config.ConfigValue[bool]{}, // Client didn't set - should use server, but server unreachable
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 1 * time.Second}

	// Setup feature flag cache context with unreachable host
	flagCache := getFeatureFlagCache()
	unreachableHost := "http://localhost:9999"
	flagCache.getOrCreateContext(unreachableHost)
	defer flagCache.releaseContext(unreachableHost)

	result := isTelemetryEnabled(ctx, cfg, unreachableHost, httpClient)

	// On error, should default to disabled (fail-safe)
	if result {
		t.Error("Expected telemetry to be disabled when server unreachable (fail-safe), got enabled")
	}
}

// TestIsTelemetryEnabled_ClientOverridesServerError tests Priority 1 > Priority 3
func TestIsTelemetryEnabled_ClientOverridesServerError(t *testing.T) {
	// Setup: Create a server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &Config{
		EnableTelemetry: config.NewConfigValue(true), // Client explicitly enables - should override server error
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	// Client override should work even when server errors
	if !result {
		t.Error("Expected telemetry to be enabled when client explicitly sets true, even with server error, got disabled")
	}
}
