package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		"enableTelemetry": "true",
	}
	cfg := ParseTelemetryConfig(params)

	if !cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be true when set to 'true'")
	}
}

func TestParseTelemetryConfig_Enabled1(t *testing.T) {
	params := map[string]string{
		"enableTelemetry": "1",
	}
	cfg := ParseTelemetryConfig(params)

	if !cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be true when set to '1'")
	}
}

func TestParseTelemetryConfig_EnabledFalse(t *testing.T) {
	params := map[string]string{
		"enableTelemetry": "false",
	}
	cfg := ParseTelemetryConfig(params)

	if cfg.EnableTelemetry {
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

	if !cfg.EnableTelemetry {
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

// TestIsTelemetryEnabled_ForceEnable tests Priority 1: forceEnableTelemetry=true
func TestIsTelemetryEnabled_ForceEnable(t *testing.T) {
	// Setup: Create a server that returns disabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Even if server says disabled, force enable should bypass
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": false,
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		ForceEnableTelemetry: true, // Priority 1: Force enable
		EnableTelemetry:      false,
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Force enable should bypass server check
	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if !result {
		t.Error("Expected telemetry to be enabled with ForceEnableTelemetry=true, got disabled")
	}
}

// TestIsTelemetryEnabled_ExplicitOptOut tests Priority 2: enableTelemetry=false
func TestIsTelemetryEnabled_ExplicitOptOut(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Even if server says enabled, explicit opt-out should disable
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": true,
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		ForceEnableTelemetry: false,
		EnableTelemetry:      false, // Priority 2: Explicit opt-out
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if result {
		t.Error("Expected telemetry to be disabled with EnableTelemetry=false, got enabled")
	}
}

// TestIsTelemetryEnabled_UserOptInServerEnabled tests Priority 3: user opts in + server enabled
func TestIsTelemetryEnabled_UserOptInServerEnabled(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": true,
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		ForceEnableTelemetry: false,
		EnableTelemetry:      true, // User wants telemetry
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if !result {
		t.Error("Expected telemetry to be enabled when user opts in and server allows, got disabled")
	}
}

// TestIsTelemetryEnabled_UserOptInServerDisabled tests Priority 3: user opts in but server disabled
func TestIsTelemetryEnabled_UserOptInServerDisabled(t *testing.T) {
	// Setup: Create a server that returns disabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": false,
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		ForceEnableTelemetry: false,
		EnableTelemetry:      true, // User wants telemetry
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	if result {
		t.Error("Expected telemetry to be disabled when server disables it, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerFlagOnly tests Priority 4: server flag controls (default behavior)
func TestIsTelemetryEnabled_ServerFlagOnly(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": true,
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := &Config{
		ForceEnableTelemetry: false,
		EnableTelemetry:      false, // Default: no explicit user preference
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	// When enableTelemetry is false (default), should return false (Priority 2)
	if result {
		t.Error("Expected telemetry to be disabled with default EnableTelemetry=false, got enabled")
	}
}

// TestIsTelemetryEnabled_Default tests Priority 5: default disabled
func TestIsTelemetryEnabled_Default(t *testing.T) {
	cfg := DefaultConfig()

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	result := isTelemetryEnabled(ctx, cfg, "test-host", httpClient)

	if result {
		t.Error("Expected telemetry to be disabled by default, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerError tests error handling
func TestIsTelemetryEnabled_ServerError(t *testing.T) {
	// Setup: Create a server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &Config{
		ForceEnableTelemetry: false,
		EnableTelemetry:      true, // User wants telemetry
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, httpClient)

	// On error, should default to disabled
	if result {
		t.Error("Expected telemetry to be disabled on server error, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerUnreachable tests unreachable server
func TestIsTelemetryEnabled_ServerUnreachable(t *testing.T) {
	cfg := &Config{
		ForceEnableTelemetry: false,
		EnableTelemetry:      true, // User wants telemetry
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 1 * time.Second}

	// Setup feature flag cache context with unreachable host
	flagCache := getFeatureFlagCache()
	unreachableHost := "http://localhost:9999"
	flagCache.getOrCreateContext(unreachableHost)
	defer flagCache.releaseContext(unreachableHost)

	result := isTelemetryEnabled(ctx, cfg, unreachableHost, httpClient)

	// On error, should default to disabled
	if result {
		t.Error("Expected telemetry to be disabled when server unreachable, got enabled")
	}
}
