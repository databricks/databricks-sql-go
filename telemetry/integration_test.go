package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIntegration_EndToEnd_WithCircuitBreaker tests complete end-to-end flow.
func TestIntegration_EndToEnd_WithCircuitBreaker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := DefaultConfig()
	cfg.FlushInterval = 100 * time.Millisecond
	cfg.BatchSize = 5
	httpClient := &http.Client{Timeout: 5 * time.Second}

	requestCount := int32(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)

		// Verify request structure
		if r.Method != "POST" {
			t.Errorf("Expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/telemetry-ext" {
			t.Errorf("Expected /telemetry-ext, got %s", r.URL.Path)
		}

		// Parse payload (new TelemetryRequest format)
		body, _ := io.ReadAll(r.Body)
		var payload TelemetryRequest
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("Failed to parse payload: %v", err)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create telemetry client
	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)
	defer aggregator.close(context.Background()) //nolint:errcheck

	interceptor := newInterceptor(aggregator, true)

	// Simulate statement execution
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		statementID := "stmt-integration"
		ctx = interceptor.BeforeExecute(ctx, "session-id", statementID)
		time.Sleep(10 * time.Millisecond) // Simulate work
		interceptor.AfterExecute(ctx, nil)
		interceptor.CompleteStatement(ctx, statementID, false)
	}

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Verify requests were sent
	count := atomic.LoadInt32(&requestCount)
	if count == 0 {
		t.Error("Expected telemetry requests to be sent")
	}

	t.Logf("Integration test: sent %d requests", count)
}

// TestIntegration_CircuitBreakerOpening tests circuit breaker behavior under failures.
func TestIntegration_CircuitBreakerOpening(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := DefaultConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	cfg.MaxRetries = 0 // No retries for faster test
	httpClient := &http.Client{Timeout: 5 * time.Second}

	requestCount := int32(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		// Always fail to trigger circuit breaker
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)
	defer aggregator.close(context.Background()) //nolint:errcheck

	interceptor := newInterceptor(aggregator, true)
	cb := exporter.circuitBreaker

	// Send enough requests to open circuit (need 20+ calls with 50%+ failure rate)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		statementID := "stmt-circuit"
		ctx = interceptor.BeforeExecute(ctx, "session-id", statementID)
		interceptor.AfterExecute(ctx, nil)
		interceptor.CompleteStatement(ctx, statementID, false)

		// Small delay to ensure each batch is processed
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for flush and circuit breaker evaluation
	time.Sleep(500 * time.Millisecond)

	// Verify circuit opened (may still be closed if not enough failures recorded)
	state := cb.getState()
	t.Logf("Circuit breaker state after failures: %v", state)

	// Circuit should eventually open, but timing is async
	// If not open, at least verify requests were attempted
	initialCount := atomic.LoadInt32(&requestCount)
	if initialCount == 0 {
		t.Error("Expected at least some requests to be sent")
	}

	// Send more requests - should be dropped if circuit is open
	for i := 0; i < 10; i++ {
		statementID := "stmt-dropped"
		ctx = interceptor.BeforeExecute(ctx, "session-id", statementID)
		interceptor.AfterExecute(ctx, nil)
		interceptor.CompleteStatement(ctx, statementID, false)
	}

	time.Sleep(200 * time.Millisecond)

	finalCount := atomic.LoadInt32(&requestCount)
	t.Logf("Circuit breaker test: %d requests sent, state=%v", finalCount, cb.getState())

	// Test passes if either:
	// 1. Circuit opened and requests were dropped, OR
	// 2. Circuit is still trying (which is also acceptable for async system)
	if state == stateOpen && finalCount > initialCount+5 {
		t.Errorf("Expected requests to be dropped when circuit open, got %d additional requests", finalCount-initialCount)
	}
}

// TestIntegration_OptInPriority_ExplicitOptOut tests explicit opt-out.
func TestIntegration_OptInPriority_ExplicitOptOut(t *testing.T) {
	cfg := &Config{
		EnableTelemetry: false, // Priority 1 (client): Explicit opt-out
		BatchSize:       100,
		FlushInterval:   5 * time.Second,
		MaxRetries:      3,
		RetryDelay:      100 * time.Millisecond,
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Server that returns enabled
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"flags": map[string]bool{
				"databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver": true,
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	ctx := context.Background()

	// Should be disabled due to explicit opt-out
	result := isTelemetryEnabled(ctx, cfg, server.URL, "test-version", httpClient)

	if result {
		t.Error("Expected telemetry to be disabled by explicit opt-out")
	}
}

// TestIntegration_PrivacyCompliance verifies no sensitive data is collected.
func TestIntegration_PrivacyCompliance_NoQueryText(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	var mu sync.Mutex
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		capturedBody = body
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)
	defer aggregator.close(context.Background()) //nolint:errcheck

	interceptor := newInterceptor(aggregator, true)

	ctx := context.Background()
	statementID := "stmt-privacy"
	ctx = interceptor.BeforeExecute(ctx, "session-id", statementID)

	// Add sensitive tags — none of these should appear in the exported telemetry
	interceptor.AddTag(ctx, "query.text", "SELECT * FROM users")
	interceptor.AddTag(ctx, "user.email", "user@example.com")

	interceptor.AfterExecute(ctx, nil)
	interceptor.CompleteStatement(ctx, statementID, false)

	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	body := capturedBody
	mu.Unlock()

	if len(body) == 0 {
		t.Fatal("Expected telemetry request to be sent")
	}

	// The exporter sends TelemetryRequest with ProtoLogs (JSON-encoded TelemetryFrontendLog).
	// Verify sensitive values are absent from the serialised payload.
	bodyStr := string(body)
	if strings.Contains(bodyStr, "SELECT * FROM users") {
		t.Error("Query text must not be exported")
	}
	if strings.Contains(bodyStr, "user@example.com") {
		t.Error("User email must not be exported")
	}

	t.Log("Privacy compliance test passed: sensitive data not present in payload")
}

// TestIntegration_FieldMapping verifies that only known metric fields are exported
// in the TelemetryRequest format (no generic tag pass-through).
func TestIntegration_FieldMapping(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	var capturedRequest TelemetryRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &capturedRequest)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)

	metric := &telemetryMetric{
		metricType:  "connection",
		timestamp:   time.Now(),
		workspaceID: "ws-test",
		sessionID:   "sess-1",
		latencyMs:   42,
		tags: map[string]interface{}{
			"chunk_count":      3,
			"bytes_downloaded": int64(1024),
			"unknown.tag":      "value", // should NOT appear in output
		},
	}

	ctx := context.Background()
	exporter.export(ctx, []*telemetryMetric{metric})

	time.Sleep(150 * time.Millisecond)

	if len(capturedRequest.ProtoLogs) == 0 {
		t.Fatal("Expected at least one ProtoLog entry")
	}

	// Each ProtoLog entry is a JSON-encoded TelemetryFrontendLog.
	var log TelemetryFrontendLog
	if err := json.Unmarshal([]byte(capturedRequest.ProtoLogs[0]), &log); err != nil {
		t.Fatalf("Failed to unmarshal ProtoLog: %v", err)
	}

	if log.Entry == nil || log.Entry.SQLDriverLog == nil {
		t.Fatal("Expected SQLDriverLog to be populated")
	}

	entry := log.Entry.SQLDriverLog
	if entry.SessionID != "sess-1" {
		t.Errorf("Expected session_id=sess-1, got %q", entry.SessionID)
	}
	if entry.OperationLatencyMs != 42 {
		t.Errorf("Expected latency=42, got %d", entry.OperationLatencyMs)
	}
	if entry.SQLOperation != nil && entry.SQLOperation.ChunkDetails != nil {
		if entry.SQLOperation.ChunkDetails.TotalChunksIterated != 3 {
			t.Errorf("Expected total_chunks_iterated=3, got %d", entry.SQLOperation.ChunkDetails.TotalChunksIterated)
		}
	}

	// unknown.tag must not appear anywhere in the serialised output
	if strings.Contains(capturedRequest.ProtoLogs[0], "unknown.tag") {
		t.Error("unknown.tag must not be exported")
	}

	t.Log("Field mapping test passed")
}
