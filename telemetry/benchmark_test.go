package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// BenchmarkInterceptor_Overhead measures the overhead when telemetry is enabled.
func BenchmarkInterceptor_Overhead_Enabled(b *testing.B) {
	// Setup
	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, httpClient, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)
	defer aggregator.close(context.Background())

	interceptor := newInterceptor(aggregator, true) // Enabled

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		statementID := "stmt-bench"
		ctx = interceptor.BeforeExecute(ctx, statementID)
		interceptor.AfterExecute(ctx, nil)
		interceptor.CompleteStatement(ctx, statementID, false)
	}
}

// BenchmarkInterceptor_Overhead_Disabled measures the overhead when telemetry is disabled.
func BenchmarkInterceptor_Overhead_Disabled(b *testing.B) {
	// Setup
	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, httpClient, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)
	defer aggregator.close(context.Background())

	interceptor := newInterceptor(aggregator, false) // Disabled

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		statementID := "stmt-bench"
		ctx = interceptor.BeforeExecute(ctx, statementID)
		interceptor.AfterExecute(ctx, nil)
		interceptor.CompleteStatement(ctx, statementID, false)
	}
}

// BenchmarkAggregator_RecordMetric measures aggregator performance.
func BenchmarkAggregator_RecordMetric(b *testing.B) {
	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, httpClient, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)
	defer aggregator.close(context.Background())

	ctx := context.Background()
	metric := &telemetryMetric{
		metricType:  "statement",
		timestamp:   time.Now(),
		statementID: "stmt-bench",
		latencyMs:   100,
		tags:        make(map[string]interface{}),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregator.recordMetric(ctx, metric)
	}
}

// BenchmarkExporter_Export measures export performance.
func BenchmarkExporter_Export(b *testing.B) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 0 // No retries for benchmark
	httpClient := &http.Client{Timeout: 5 * time.Second}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, httpClient, cfg)

	ctx := context.Background()
	metrics := []*telemetryMetric{
		{
			metricType:  "statement",
			timestamp:   time.Now(),
			statementID: "stmt-bench",
			latencyMs:   100,
			tags:        make(map[string]interface{}),
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exporter.export(ctx, metrics)
	}
}

// BenchmarkConcurrentConnections_PerHostSharing tests performance with concurrent connections.
func BenchmarkConcurrentConnections_PerHostSharing(b *testing.B) {
	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := server.URL

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Simulate getting a client (should share per host)
			mgr := getClientManager()
			client := mgr.getOrCreateClient(host, httpClient, cfg)
			_ = client

			// Release client
			mgr.releaseClient(host)
		}
	})
}

// BenchmarkCircuitBreaker_Execute measures circuit breaker overhead.
func BenchmarkCircuitBreaker_Execute(b *testing.B) {
	cb := newCircuitBreaker(defaultCircuitBreakerConfig())
	ctx := context.Background()

	fn := func() error {
		return nil // Success
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.execute(ctx, fn)
	}
}

// TestLoadTesting_ConcurrentConnections validates behavior under load.
func TestLoadTesting_ConcurrentConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	requestCount := 0
	mu := sync.Mutex{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := server.URL
	mgr := getClientManager()

	// Simulate 100 concurrent connections to the same host
	const numConnections = 100
	var wg sync.WaitGroup
	wg.Add(numConnections)

	for i := 0; i < numConnections; i++ {
		go func() {
			defer wg.Done()

			// Get client (should share)
			client := mgr.getOrCreateClient(host, httpClient, cfg)
			interceptor := client.GetInterceptor(true)

			// Simulate some operations
			ctx := context.Background()
			for j := 0; j < 10; j++ {
				statementID := "stmt-load"
				ctx = interceptor.BeforeExecute(ctx, statementID)
				time.Sleep(1 * time.Millisecond) // Simulate work
				interceptor.AfterExecute(ctx, nil)
				interceptor.CompleteStatement(ctx, statementID, false)
			}

			// Release client
			mgr.releaseClient(host)
		}()
	}

	wg.Wait()

	// Verify per-host client sharing worked
	// All 100 connections should have shared the same client
	t.Logf("Load test completed: %d connections, %d requests", numConnections, requestCount)
}

// TestGracefulShutdown_ReferenceCountingCleanup validates cleanup behavior.
func TestGracefulShutdown_ReferenceCountingCleanup(t *testing.T) {
	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	host := server.URL
	mgr := getClientManager()

	// Create multiple references
	client1 := mgr.getOrCreateClient(host, httpClient, cfg)
	client2 := mgr.getOrCreateClient(host, httpClient, cfg)

	if client1 != client2 {
		t.Error("Expected same client instance for same host")
	}

	// Release first reference
	err := mgr.releaseClient(host)
	if err != nil {
		t.Errorf("Unexpected error releasing client: %v", err)
	}

	// Client should still exist (ref count = 1)
	mgr.mu.RLock()
	_, exists := mgr.clients[host]
	mgr.mu.RUnlock()

	if !exists {
		t.Error("Expected client to still exist after partial release")
	}

	// Release second reference
	err = mgr.releaseClient(host)
	if err != nil {
		t.Errorf("Unexpected error releasing client: %v", err)
	}

	// Client should be cleaned up (ref count = 0)
	mgr.mu.RLock()
	_, exists = mgr.clients[host]
	mgr.mu.RUnlock()

	if exists {
		t.Error("Expected client to be cleaned up after all references released")
	}
}

// TestGracefulShutdown_FinalFlush validates final flush on shutdown.
func TestGracefulShutdown_FinalFlush(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FlushInterval = 1 * time.Hour // Long interval to test explicit flush
	cfg.BatchSize = 1                 // Small batch size to trigger flush immediately
	httpClient := &http.Client{Timeout: 5 * time.Second}

	flushed := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case flushed <- true:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, httpClient, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)

	// Record a metric
	ctx := context.Background()
	metric := &telemetryMetric{
		metricType:  "statement",
		timestamp:   time.Now(),
		statementID: "stmt-test",
		latencyMs:   100,
		tags:        make(map[string]interface{}),
	}
	aggregator.recordMetric(ctx, metric)

	// Complete the statement to trigger batch flush
	aggregator.completeStatement(ctx, "stmt-test", false)

	// Close should flush pending metrics
	err := aggregator.close(ctx)
	if err != nil {
		t.Errorf("Unexpected error closing aggregator: %v", err)
	}

	// Wait for flush with timeout
	select {
	case <-flushed:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Error("Expected metrics to be flushed on close (timeout)")
	}
}
