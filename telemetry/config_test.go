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

func TestParseTelemetryConfig_RetryCount(t *testing.T) {
	params := map[string]string{
		"telemetry_retry_count": "5",
	}
	cfg := ParseTelemetryConfig(params)

	if cfg.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries 5, got %d", cfg.MaxRetries)
	}
}

func TestParseTelemetryConfig_RetryCountZero(t *testing.T) {
	params := map[string]string{
		"telemetry_retry_count": "0",
	}
	cfg := ParseTelemetryConfig(params)

	if cfg.MaxRetries != 0 {
		t.Errorf("Expected MaxRetries 0 (disable retries), got %d", cfg.MaxRetries)
	}
}

func TestParseTelemetryConfig_RetryCountInvalid(t *testing.T) {
	params := map[string]string{
		"telemetry_retry_count": "invalid",
	}
	cfg := ParseTelemetryConfig(params)

	// Should fall back to default
	if cfg.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries to fallback to 3, got %d", cfg.MaxRetries)
	}
}

func TestParseTelemetryConfig_RetryDelay(t *testing.T) {
	params := map[string]string{
		"telemetry_retry_delay": "500ms",
	}
	cfg := ParseTelemetryConfig(params)

	if cfg.RetryDelay != 500*time.Millisecond {
		t.Errorf("Expected RetryDelay 500ms, got %v", cfg.RetryDelay)
	}
}

func TestParseTelemetryConfig_RetryDelayInvalid(t *testing.T) {
	params := map[string]string{
		"telemetry_retry_delay": "invalid",
	}
	cfg := ParseTelemetryConfig(params)

	// Should fall back to default
	if cfg.RetryDelay != 100*time.Millisecond {
		t.Errorf("Expected RetryDelay to fallback to 100ms, got %v", cfg.RetryDelay)
	}
}

func TestParseTelemetryConfig_ClientExplicit_Set(t *testing.T) {
	params := map[string]string{
		"enableTelemetry": "true",
	}
	cfg := ParseTelemetryConfig(params)

	if !cfg.ClientExplicit {
		t.Error("Expected ClientExplicit=true when enableTelemetry DSN param is present")
	}
}

func TestParseTelemetryConfig_ClientExplicit_NotSet(t *testing.T) {
	params := map[string]string{}
	cfg := ParseTelemetryConfig(params)

	if cfg.ClientExplicit {
		t.Error("Expected ClientExplicit=false when enableTelemetry DSN param is absent")
	}
}

func TestParseTelemetryConfig_AllParams(t *testing.T) {
	params := map[string]string{
		"enableTelemetry":          "true",
		"telemetry_batch_size":     "200",
		"telemetry_flush_interval": "30s",
		"telemetry_retry_count":    "5",
		"telemetry_retry_delay":    "250ms",
	}
	cfg := ParseTelemetryConfig(params)

	if !cfg.EnableTelemetry {
		t.Error("Expected EnableTelemetry to be true")
	}
	if !cfg.ClientExplicit {
		t.Error("Expected ClientExplicit to be true")
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

// TestIsTelemetryEnabled_ExplicitOptOut tests Priority 1 (client opt-out): enableTelemetry=false
// Client explicitly opts out via DSN — server flag is NOT consulted.
func TestIsTelemetryEnabled_ExplicitOptOut(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Even if server says enabled, explicit client opt-out must win
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	cfg := &Config{
		EnableTelemetry: false, // Client explicitly disabled via DSN
		ClientExplicit:  true,  // Marks that client set this, overriding server
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	result := isTelemetryEnabled(ctx, cfg, server.URL, "test-version", httpClient)

	if result {
		t.Error("Expected telemetry to be disabled with ClientExplicit=true, EnableTelemetry=false, got enabled")
	}
}

// TestIsTelemetryEnabled_UserOptInServerEnabled tests Priority 1 (client opt-in): client DSN=true → enabled,
// server flag is NOT consulted when client explicitly sets enableTelemetry=true.
func TestIsTelemetryEnabled_UserOptInServerEnabled(t *testing.T) {
	cfg := &Config{
		EnableTelemetry: true, // Client explicitly enabled via DSN
		ClientExplicit:  true, // Marks that client set this, overriding server
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// No server setup needed — client override skips server flag check entirely.
	result := isTelemetryEnabled(ctx, cfg, "http://unreachable-host", "test-version", httpClient)

	if !result {
		t.Error("Expected telemetry to be enabled with ClientExplicit=true, EnableTelemetry=true, got disabled")
	}
}

// TestIsTelemetryEnabled_DefaultChecksServerFlag tests Priority 2: when no DSN param is set,
// the server feature flag controls enablement.
func TestIsTelemetryEnabled_DefaultChecksServerFlag(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	cfg := &Config{
		// ClientExplicit=false (zero value): no DSN setting, server flag controls
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, "test-version", httpClient)

	if !result {
		t.Error("Expected telemetry to be enabled when server flag is true and no client DSN override, got disabled")
	}
}

// TestIsTelemetryEnabled_UserOptInServerDisabled tests Priority 1 (client wins over server):
// client explicitly sets enableTelemetry=true, server returns disabled → client wins → enabled.
func TestIsTelemetryEnabled_UserOptInServerDisabled(t *testing.T) {
	cfg := &Config{
		EnableTelemetry: true, // Client explicitly enabled via DSN
		ClientExplicit:  true, // Client overrides server
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// No real server needed — client explicit setting skips server flag check.
	result := isTelemetryEnabled(ctx, cfg, "http://unreachable-host", "test-version", httpClient)

	// Client explicitly opted in — result must be true regardless of server.
	if !result {
		t.Error("Expected telemetry to be enabled when client explicitly opts in (client overrides server), got disabled")
	}
}

// TestIsTelemetryEnabled_DefaultServerDisabled tests Priority 2: when no DSN param is set
// and server flag is disabled, telemetry is disabled.
func TestIsTelemetryEnabled_DefaultServerDisabled(t *testing.T) {
	// Setup: Create a server that returns disabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "false"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	cfg := &Config{
		// ClientExplicit=false (zero value): no DSN setting, server flag controls
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, "test-version", httpClient)

	if result {
		t.Error("Expected telemetry to be disabled when server flag is false and no client DSN override, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerFlagOnly tests Priority 2 (server flag controls):
// when no DSN setting is provided, the server feature flag alone decides.
// Server returns true → telemetry enabled.
func TestIsTelemetryEnabled_ServerFlagOnly(t *testing.T) {
	// Setup: Create a server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"flags": [{"name": "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver", "value": "true"}], "ttl_seconds": 300}`))
	}))
	defer server.Close()

	cfg := &Config{
		// ClientExplicit=false, EnableTelemetry=false (zero values): no DSN override, server decides
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, "test-version", httpClient)

	// No client DSN override → server controls → server says true → enabled
	if !result {
		t.Error("Expected telemetry to be enabled when server flag is true and no client DSN override, got disabled")
	}
}

// TestIsTelemetryEnabled_Default tests Priority 5: default disabled
func TestIsTelemetryEnabled_Default(t *testing.T) {
	cfg := DefaultConfig()

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	result := isTelemetryEnabled(ctx, cfg, "test-host", "test-version", httpClient)

	if result {
		t.Error("Expected telemetry to be disabled by default, got enabled")
	}
}

// TestIsTelemetryEnabled_ServerError tests Priority 3 (default disabled):
// when no client DSN override and server flag check errors, telemetry is disabled.
func TestIsTelemetryEnabled_ServerError(t *testing.T) {
	// Setup: Create a server that returns error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := &Config{
		// ClientExplicit=false: no DSN override, server flag controls
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Setup feature flag cache context
	flagCache := getFeatureFlagCache()
	flagCache.getOrCreateContext(server.URL)
	defer flagCache.releaseContext(server.URL)

	result := isTelemetryEnabled(ctx, cfg, server.URL, "test-version", httpClient)

	// Server error with no client override → default disabled
	if result {
		t.Error("Expected telemetry to be disabled on server error (no client DSN override), got enabled")
	}
}

// TestIsTelemetryEnabled_ServerUnreachable tests Priority 3 (default disabled):
// when no client DSN override and server is unreachable, telemetry is disabled.
func TestIsTelemetryEnabled_ServerUnreachable(t *testing.T) {
	cfg := &Config{
		// ClientExplicit=false: no DSN override, server flag controls
	}

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 1 * time.Second}

	// Setup feature flag cache context with unreachable host
	flagCache := getFeatureFlagCache()
	unreachableHost := "http://localhost:9999"
	flagCache.getOrCreateContext(unreachableHost)
	defer flagCache.releaseContext(unreachableHost)

	result := isTelemetryEnabled(ctx, cfg, unreachableHost, "test-version", httpClient)

	// Unreachable server with no client override → default disabled
	if result {
		t.Error("Expected telemetry to be disabled when server unreachable (no client DSN override), got enabled")
	}
}
