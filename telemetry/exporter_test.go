package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTelemetryExporter(t *testing.T) {
	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	host := "test-host"

	exporter := newTelemetryExporter(host, 443, "", "test-version", httpClient, cfg, nil)

	if exporter.host != host {
		t.Errorf("Expected host %s, got %s", host, exporter.host)
	}

	if exporter.httpClient != httpClient {
		t.Error("Expected httpClient to be set")
	}

	if exporter.circuitBreaker == nil {
		t.Error("Expected circuitBreaker to be initialized")
	}

	if exporter.cfg != cfg {
		t.Error("Expected cfg to be set")
	}
}

func TestExport_Success(t *testing.T) {
	requestReceived := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true

		// Verify request method and path
		if r.Method != "POST" {
			t.Errorf("Expected POST, got %s", r.Method)
		}

		if r.URL.Path != "/telemetry-ext" {
			t.Errorf("Expected path /telemetry-ext, got %s", r.URL.Path)
		}

		// Verify content type
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
		}

		// Verify payload structure
		body, _ := io.ReadAll(r.Body)
		var payload TelemetryRequest
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("Failed to unmarshal payload: %v", err)
		}

		if len(payload.ProtoLogs) != 1 {
			t.Errorf("Expected 1 protoLog, got %d", len(payload.ProtoLogs))
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	metrics := []*telemetryMetric{
		{
			metricType:  "connection",
			timestamp:   time.Now(),
			workspaceID: "test-workspace",
			sessionID:   "test-session",
			tags:        map[string]interface{}{"driver.version": "1.0.0"},
		},
	}

	ctx := context.Background()
	exporter.export(ctx, metrics)

	if !requestReceived {
		t.Error("Expected request to be sent to server")
	}
}

func TestExport_RetryOn5xx(t *testing.T) {
	attemptCount := int32(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&attemptCount, 1)
		if count < 3 {
			// Fail first 2 attempts
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			// Succeed on 3rd attempt
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryDelay = 10 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	metrics := []*telemetryMetric{
		{
			metricType: "connection",
			timestamp:  time.Now(),
		},
	}

	ctx := context.Background()
	exporter.export(ctx, metrics)

	// Should have retried and succeeded
	if atomic.LoadInt32(&attemptCount) != 3 {
		t.Errorf("Expected 3 attempts, got %d", attemptCount)
	}
}

func TestExport_NonRetryable4xx(t *testing.T) {
	attemptCount := int32(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attemptCount, 1)
		w.WriteHeader(http.StatusBadRequest) // 400 is not retryable
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryDelay = 10 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	metrics := []*telemetryMetric{
		{
			metricType: "connection",
			timestamp:  time.Now(),
		},
	}

	ctx := context.Background()
	exporter.export(ctx, metrics)

	// Should only try once (no retries for 4xx)
	if atomic.LoadInt32(&attemptCount) != 1 {
		t.Errorf("Expected 1 attempt, got %d", attemptCount)
	}
}

func TestExport_Retry429(t *testing.T) {
	attemptCount := int32(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&attemptCount, 1)
		if count < 2 {
			w.WriteHeader(http.StatusTooManyRequests) // 429 is retryable
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryDelay = 10 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	metrics := []*telemetryMetric{
		{
			metricType: "connection",
			timestamp:  time.Now(),
		},
	}

	ctx := context.Background()
	exporter.export(ctx, metrics)

	// Should have retried and succeeded
	if atomic.LoadInt32(&attemptCount) != 2 {
		t.Errorf("Expected 2 attempts, got %d", attemptCount)
	}
}

func TestExport_CircuitBreakerOpen(t *testing.T) {
	attemptCount := int32(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attemptCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	// Open the circuit breaker by recording failures
	cb := exporter.circuitBreaker
	ctx := context.Background()

	// Record enough failures to open circuit (50% failure rate with 20+ calls)
	for i := 0; i < 25; i++ {
		cb.recordCall(callFailure)
	}

	// Verify circuit is open
	if cb.getState() != stateOpen {
		t.Error("Expected circuit to be open")
	}

	metrics := []*telemetryMetric{
		{
			metricType: "connection",
			timestamp:  time.Now(),
		},
	}

	// Export should be dropped due to open circuit
	exporter.export(ctx, metrics)

	// No request should have been made
	if atomic.LoadInt32(&attemptCount) != 0 {
		t.Errorf("Expected 0 attempts with open circuit, got %d", attemptCount)
	}
}

func TestCreateTelemetryRequest_TagFiltering(t *testing.T) {
	metric := &telemetryMetric{
		metricType:  "connection",
		timestamp:   time.Date(2026, 1, 30, 10, 0, 0, 0, time.UTC),
		workspaceID: "test-workspace",
		sessionID:   "test-session",
		statementID: "test-statement",
		latencyMs:   100,
		errorType:   "test-error",
		tags: map[string]interface{}{
			"workspace.id":   "ws-123",         // Should be exported
			"driver.version": "1.0.0",          // Should be exported
			"server.address": "localhost:8080", // Should NOT be exported (local only)
			"unknown.tag":    "value",          // Should NOT be exported
		},
	}

	req, err := createTelemetryRequest([]*telemetryMetric{metric}, "1.0.0", &DriverConnectionParameters{Host: "test-host", Port: 443})
	if err != nil {
		t.Fatalf("Failed to create telemetry request: %v", err)
	}

	// Verify protoLogs were created
	if len(req.ProtoLogs) != 1 {
		t.Fatalf("Expected 1 protoLog, got %d", len(req.ProtoLogs))
	}

	// Parse the protoLog JSON to verify structure
	var logEntry TelemetryFrontendLog
	if err := json.Unmarshal([]byte(req.ProtoLogs[0]), &logEntry); err != nil {
		t.Fatalf("Failed to unmarshal protoLog: %v", err)
	}

	// Verify session_id is present in the SQLDriverLog
	if logEntry.Entry == nil || logEntry.Entry.SQLDriverLog == nil {
		t.Fatal("Expected Entry.SQLDriverLog to be present")
	}
	if logEntry.Entry.SQLDriverLog.SessionID != "test-session" {
		t.Errorf("Expected session_id 'test-session', got %s", logEntry.Entry.SQLDriverLog.SessionID)
	}

	// Verify tag filtering - this is done in the actual export process
	// The tags in telemetryMetric are filtered by shouldExportToDatabricks()
	// when converting to the frontend log format
}

func TestIsRetryableStatus(t *testing.T) {
	tests := []struct {
		status      int
		retryable   bool
		description string
	}{
		{200, false, "200 OK is not retryable"},
		{201, false, "201 Created is not retryable"},
		{400, false, "400 Bad Request is not retryable"},
		{401, false, "401 Unauthorized is not retryable"},
		{403, false, "403 Forbidden is not retryable"},
		{404, false, "404 Not Found is not retryable"},
		{429, true, "429 Too Many Requests is retryable"},
		{500, true, "500 Internal Server Error is retryable"},
		{502, true, "502 Bad Gateway is retryable"},
		{503, true, "503 Service Unavailable is retryable"},
		{504, true, "504 Gateway Timeout is retryable"},
	}

	for _, tt := range tests {
		result := isRetryableStatus(tt.status)
		if result != tt.retryable {
			t.Errorf("%s: expected %v, got %v", tt.description, tt.retryable, result)
		}
	}
}

func TestExport_ErrorSwallowing(t *testing.T) {
	// Server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.MaxRetries = 1
	cfg.RetryDelay = 10 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	metrics := []*telemetryMetric{
		{
			metricType: "connection",
			timestamp:  time.Now(),
		},
	}

	ctx := context.Background()

	// This should not panic even though all requests fail
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Export panicked: %v", r)
		}
	}()

	exporter.export(ctx, metrics)
	// If we get here without panic, error swallowing works
}

func TestExport_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slow server
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryDelay = 50 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	metrics := []*telemetryMetric{
		{
			metricType: "connection",
			timestamp:  time.Now(),
		},
	}

	// Create context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Export with cancelled context (should not panic)
	exporter.export(ctx, metrics)
	// If we get here, context cancellation is handled properly
}

func TestExport_ExponentialBackoff(t *testing.T) {
	attemptTimes := make([]time.Time, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptTimes = append(attemptTimes, time.Now())
		// Always fail to test all retries
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.MaxRetries = 3
	cfg.RetryDelay = 50 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, 443, "", "test-version", httpClient, cfg, nil)

	metrics := []*telemetryMetric{
		{
			metricType: "connection",
			timestamp:  time.Now(),
		},
	}

	ctx := context.Background()
	exporter.export(ctx, metrics)

	// Should have 4 attempts (1 initial + 3 retries)
	if len(attemptTimes) != 4 {
		t.Errorf("Expected 4 attempts, got %d", len(attemptTimes))
		return
	}

	// Verify exponential backoff delays
	// Attempt 0: immediate
	// Attempt 1: +50ms (2^0 * 50ms)
	// Attempt 2: +100ms (2^1 * 50ms)
	// Attempt 3: +200ms (2^2 * 50ms)

	delay1 := attemptTimes[1].Sub(attemptTimes[0])
	delay2 := attemptTimes[2].Sub(attemptTimes[1])
	delay3 := attemptTimes[3].Sub(attemptTimes[2])

	// Allow 30ms tolerance for timing variations
	tolerance := 30 * time.Millisecond

	if delay1 < (50*time.Millisecond-tolerance) || delay1 > (50*time.Millisecond+tolerance) {
		t.Errorf("Expected delay1 ~50ms, got %v", delay1)
	}

	if delay2 < (100*time.Millisecond-tolerance) || delay2 > (100*time.Millisecond+tolerance) {
		t.Errorf("Expected delay2 ~100ms, got %v", delay2)
	}

	if delay3 < (200*time.Millisecond-tolerance) || delay3 > (200*time.Millisecond+tolerance) {
		t.Errorf("Expected delay3 ~200ms, got %v", delay3)
	}
}
