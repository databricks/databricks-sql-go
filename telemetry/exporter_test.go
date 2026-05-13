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

	exporter := newTelemetryExporter(host, "test-version", "test-ua", httpClient, cfg)

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

		// Verify payload structure (new TelemetryRequest format)
		body, _ := io.ReadAll(r.Body)
		var payload TelemetryRequest
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("Failed to unmarshal payload: %v", err)
		}

		if len(payload.ProtoLogs) != 1 {
			t.Errorf("Expected 1 proto log, got %d", len(payload.ProtoLogs))
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, "test-version", "test-ua", httpClient, cfg)

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

// TestExport_SetsUserAgent verifies the configured User-Agent is sent on the
// telemetry POST so traffic is attributable in access logs.
func TestExport_SetsUserAgent(t *testing.T) {
	const wantUA = "godatabrickssqlconnector/9.9.9"
	gotUA := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	exporter := newTelemetryExporter(server.URL, "9.9.9", wantUA, httpClient, cfg)

	exporter.export(context.Background(), []*telemetryMetric{{
		metricType: "connection", timestamp: time.Now(),
	}})

	if gotUA != wantUA {
		t.Errorf("User-Agent: got %q, want %q", gotUA, wantUA)
	}
}

// TestExport_SingleAttemptPerExport asserts that doExport itself never
// retries — a single export is exactly one HTTP transaction. Retries are
// owned by the underlying retryablehttp-wrapped client (not exercised here
// because the test uses a plain *http.Client). Each export → one breaker
// outcome.
func TestExport_SingleAttemptPerExport(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{"400", http.StatusBadRequest},
		{"429", http.StatusTooManyRequests},
		{"500", http.StatusInternalServerError},
		{"503", http.StatusServiceUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attemptCount := int32(0)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&attemptCount, 1)
				w.WriteHeader(tc.status)
			}))
			defer server.Close()

			exporter := newTelemetryExporter(server.URL, "test-version", "test-ua", &http.Client{Timeout: 5 * time.Second}, DefaultConfig())
			exporter.export(context.Background(), []*telemetryMetric{{
				metricType: "connection", timestamp: time.Now(),
			}})

			if got := atomic.LoadInt32(&attemptCount); got != 1 {
				t.Errorf("status %d: expected 1 attempt at exporter layer, got %d", tc.status, got)
			}
		})
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
	exporter := newTelemetryExporter(server.URL, "test-version", "test-ua", httpClient, cfg)

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

func TestExport_ErrorSwallowing(t *testing.T) {
	// Server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, "test-version", "test-ua", httpClient, cfg)

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

// TestExport_429RecordsRetryAfter verifies that a 429 with a Retry-After
// header pushes its delta into the per-host circuit breaker so subsequent
// open-state checks respect the server hint instead of the default
// waitDurationInOpenState.
func TestExport_429RecordsRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "42")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, "test-version", "test-ua",
		&http.Client{Timeout: 5 * time.Second}, DefaultConfig())

	// Sanity: a fresh breaker has no hint.
	exporter.circuitBreaker.mu.RLock()
	if hint := exporter.circuitBreaker.retryAfterHint; hint != 0 {
		exporter.circuitBreaker.mu.RUnlock()
		t.Fatalf("expected fresh breaker to have no hint, got %v", hint)
	}
	exporter.circuitBreaker.mu.RUnlock()

	exporter.export(context.Background(), []*telemetryMetric{{
		metricType: "connection", timestamp: time.Now(),
	}})

	exporter.circuitBreaker.mu.RLock()
	hint := exporter.circuitBreaker.retryAfterHint
	exporter.circuitBreaker.mu.RUnlock()

	if hint != 42*time.Second {
		t.Errorf("breaker retryAfterHint after 429 with Retry-After:42: got %v, want %v",
			hint, 42*time.Second)
	}
}

func TestExport_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Slow server
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	// Use full server URL for testing
	exporter := newTelemetryExporter(server.URL, "test-version", "test-ua", httpClient, cfg)

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
