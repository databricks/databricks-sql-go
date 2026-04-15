// This file contains performance benchmarks and load tests for the telemetry package.
// Run manually — NOT part of CI:
//
//	go test -bench=. -benchmem -run='^$' ./telemetry/...
//
// Benchmarks measure telemetry overhead on the hot path and validate that telemetry
// adds negligible latency to normal driver operations.
package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkInterceptor_Overhead_Enabled measures telemetry overhead on the hot path
// when telemetry is enabled. This is the worst-case overhead added to every statement.
func BenchmarkInterceptor_Overhead_Enabled(b *testing.B) {
	cfg := DefaultConfig()
	cfg.BatchSize = 1000
	cfg.FlushInterval = 10 * time.Minute // suppress periodic flush during bench

	exporter := newTelemetryExporter("localhost", "test-version", &http.Client{}, cfg)
	agg := newMetricsAggregator(exporter, cfg)
	defer agg.close(context.Background()) //nolint:errcheck

	interceptor := newInterceptor(agg, true)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx2 := interceptor.BeforeExecute(ctx, "session-bench", fmt.Sprintf("stmt-%d", i))
		interceptor.AfterExecute(ctx2, nil)
	}
}

// BenchmarkInterceptor_Overhead_Disabled measures the disabled-path overhead (baseline).
// The delta between Enabled and Disabled is the pure telemetry cost.
func BenchmarkInterceptor_Overhead_Disabled(b *testing.B) {
	cfg := DefaultConfig()
	exporter := newTelemetryExporter("localhost", "test-version", &http.Client{}, cfg)
	agg := newMetricsAggregator(exporter, cfg)
	defer agg.close(context.Background()) //nolint:errcheck

	interceptor := newInterceptor(agg, false)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ctx2 := interceptor.BeforeExecute(ctx, "session-bench", fmt.Sprintf("stmt-%d", i))
		interceptor.AfterExecute(ctx2, nil)
	}
}

// BenchmarkAggregator_RecordMetric measures raw metric recording throughput under
// concurrent writes. This is the critical path for every driver operation.
func BenchmarkAggregator_RecordMetric(b *testing.B) {
	cfg := DefaultConfig()
	cfg.BatchSize = 10000
	cfg.FlushInterval = 10 * time.Minute

	exporter := newTelemetryExporter("localhost", "test-version", &http.Client{}, cfg)
	agg := newMetricsAggregator(exporter, cfg)
	defer agg.close(context.Background()) //nolint:errcheck

	ctx := context.Background()
	metric := &telemetryMetric{
		metricType:  "operation",
		timestamp:   time.Now(),
		sessionID:   "bench-session",
		statementID: "bench-stmt",
		latencyMs:   10,
		tags:        map[string]interface{}{TagOperationType: OperationTypeExecuteStatement},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		agg.recordMetric(ctx, metric)
	}
}

// BenchmarkExporter_Export measures export throughput against a real HTTP server.
func BenchmarkExporter_Export(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.MaxRetries = 0
	httpClient := &http.Client{Timeout: 5 * time.Second}
	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)

	metrics := make([]*telemetryMetric, 10)
	for i := range metrics {
		metrics[i] = &telemetryMetric{
			metricType: "operation",
			timestamp:  time.Now(),
			sessionID:  "bench-session",
			latencyMs:  int64(i),
			tags:       map[string]interface{}{},
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		exporter.export(ctx, metrics)
	}
}

// BenchmarkConcurrentConnections_PerHostSharing measures per-host client sharing
// under concurrent access. Multiple goroutines contend for the same host entry,
// simulating many connections to one warehouse.
func BenchmarkConcurrentConnections_PerHostSharing(b *testing.B) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "bench-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Minute

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			client := manager.getOrCreateClient(host, "test-version", httpClient, cfg)
			if client != nil {
				_ = manager.releaseClient(host)
			}
		}
	})
}

// BenchmarkCircuitBreaker_Execute measures the circuit breaker hot-path overhead
// with the circuit closed (normal operation).
func BenchmarkCircuitBreaker_Execute(b *testing.B) {
	cb := newCircuitBreaker(defaultCircuitBreakerConfig())
	ctx := context.Background()
	fn := func() error { return nil }

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = cb.execute(ctx, fn)
	}
}

// TestLoadTesting_ConcurrentConnections verifies correctness of the aggregator
// and client manager under high concurrency: 100 goroutines, 50 ops each.
func TestLoadTesting_ConcurrentConnections(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test in short mode")
	}

	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	host := "load-test-host.databricks.com"
	httpClient := &http.Client{}
	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Minute

	const goroutines = 100
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	var errors int64

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := manager.getOrCreateClient(host, "test-version", httpClient, cfg)
			if client == nil {
				atomic.AddInt64(&errors, 1)
				return
			}

			interceptor := client.GetInterceptor(true)
			ctx := context.Background()

			for j := 0; j < opsPerGoroutine; j++ {
				ctx2 := interceptor.BeforeExecute(ctx, "session", fmt.Sprintf("stmt-%d", j))
				interceptor.AfterExecute(ctx2, nil)
			}

			_ = manager.releaseClient(host)
		}()
	}

	wg.Wait()

	if atomic.LoadInt64(&errors) > 0 {
		t.Errorf("got %d errors during concurrent load test", atomic.LoadInt64(&errors))
	}

	// All connections released — client should be removed from map
	manager.mu.RLock()
	remaining := len(manager.clients)
	manager.mu.RUnlock()
	if remaining != 0 {
		t.Errorf("expected 0 remaining clients after all releases, got %d", remaining)
	}
}

// TestGracefulShutdown_ReferenceCountingCleanup verifies that clients are cleaned up
// only when the last connection for a host is released (reference counting is correct).
func TestGracefulShutdown_ReferenceCountingCleanup(t *testing.T) {
	manager := &clientManager{
		clients: make(map[string]*clientHolder),
	}

	hosts := []string{
		"host-a.databricks.com",
		"host-b.databricks.com",
		"host-c.databricks.com",
	}
	httpClient := &http.Client{}
	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Minute

	// Open 3 connections per host
	for _, host := range hosts {
		for i := 0; i < 3; i++ {
			if client := manager.getOrCreateClient(host, "test-version", httpClient, cfg); client == nil {
				t.Fatalf("expected client for host %s", host)
			}
		}
	}
	if len(manager.clients) != len(hosts) {
		t.Fatalf("expected %d clients, got %d", len(hosts), len(manager.clients))
	}

	// Release 2 of 3 connections per host — clients must remain
	for _, host := range hosts {
		for i := 0; i < 2; i++ {
			if err := manager.releaseClient(host); err != nil {
				t.Errorf("unexpected error releasing client for %s: %v", host, err)
			}
		}
	}
	if len(manager.clients) != len(hosts) {
		t.Errorf("expected clients to remain with 1 ref each, got %d clients", len(manager.clients))
	}

	// Release the last connection per host — all clients should be cleaned up
	for _, host := range hosts {
		if err := manager.releaseClient(host); err != nil {
			t.Errorf("unexpected error on final release for %s: %v", host, err)
		}
	}

	manager.mu.RLock()
	remaining := len(manager.clients)
	manager.mu.RUnlock()
	if remaining != 0 {
		t.Errorf("expected 0 clients after all releases, got %d", remaining)
	}
}

// TestGracefulShutdown_FinalFlush verifies that metrics buffered since the last periodic
// flush are exported synchronously when the aggregator is closed, not silently dropped.
func TestGracefulShutdown_FinalFlush(t *testing.T) {
	var flushed int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&flushed, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.BatchSize = 100
	cfg.FlushInterval = 10 * time.Minute // prevent auto-flush
	httpClient := &http.Client{Timeout: 5 * time.Second}

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	agg := newMetricsAggregator(exporter, cfg)

	ctx := context.Background()

	// Record a few metrics without filling the batch (no auto-flush triggers)
	for i := 0; i < 5; i++ {
		agg.recordMetric(ctx, &telemetryMetric{
			metricType: "operation",
			timestamp:  time.Now(),
			sessionID:  "test-session",
			latencyMs:  int64(i),
			tags:       map[string]interface{}{TagOperationType: OperationTypeExecuteStatement},
		})
	}

	// Close must flush remaining metrics synchronously before returning
	if err := agg.close(ctx); err != nil {
		t.Errorf("expected no error on close, got %v", err)
	}

	if atomic.LoadInt64(&flushed) == 0 {
		t.Error("expected metrics to be flushed synchronously on close, but no HTTP request was made")
	}
}
