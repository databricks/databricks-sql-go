package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAggregatorClose_WaitsForInFlightWorkerExports verifies that close() does not
// return until every metric picked up by an export worker has been delivered.
//
// Regression test for the race where agg.cancel() fired while a worker was
// mid-HTTP-export, causing EXECUTE_STATEMENT / CLOSE_STATEMENT to be silently lost.
// The fix: step 4 in close() calls agg.inFlight.Wait() before agg.cancel().
func TestAggregatorClose_WaitsForInFlightWorkerExports(t *testing.T) {
	const exportDelay = 100 * time.Millisecond

	var receivedCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req TelemetryRequest
		if err := json.Unmarshal(body, &req); err == nil {
			atomic.AddInt32(&receivedCount, int32(len(req.ProtoLogs)))
		}
		// Simulate slow server — forces the worker to be mid-HTTP-export when close() runs
		time.Sleep(exportDelay)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Second // disable periodic flush — we flush manually
	cfg.BatchSize = 1                    // one metric per batch → one worker export per metric
	httpClient := &http.Client{Timeout: 5 * time.Second}

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	agg := newMetricsAggregator(exporter, cfg)

	ctx := context.Background()

	// Record 5 operation metrics — each triggers an immediate flushUnlocked (terminal op).
	for i := 0; i < 5; i++ {
		agg.recordMetric(ctx, &telemetryMetric{
			metricType: "operation",
			timestamp:  time.Now(),
			tags: map[string]interface{}{
				"operation_type": OperationTypeCloseStatement, // terminal → immediate flush
			},
		})
	}

	// close() must block until all 5 in-flight worker exports complete.
	closeStart := time.Now()
	_ = agg.close(ctx)
	closeDuration := time.Since(closeStart)

	// close() must have waited at least (exportDelay - some tolerance) per export.
	// With 5 metrics and 10 workers running in parallel the minimum wait is exportDelay.
	if closeDuration < exportDelay/2 {
		t.Errorf("close() returned too quickly (%v); expected it to wait for in-flight exports (delay=%v)", closeDuration, exportDelay)
	}

	// All 5 metrics must have been received by the server.
	got := atomic.LoadInt32(&receivedCount)
	if got != 5 {
		t.Errorf("expected 5 metrics received by server, got %d", got)
	}
}

// TestAggregatorClose_DrainsPendingQueueJobsBeforeCancel verifies that metrics
// sitting in the exportQueue (submitted but not yet picked up by a worker) are
// exported synchronously during the drain phase of close(), not lost.
func TestAggregatorClose_DrainsPendingQueueJobsBeforeCancel(t *testing.T) {
	var mu sync.Mutex
	var receivedLogs []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req TelemetryRequest
		if err := json.Unmarshal(body, &req); err == nil {
			mu.Lock()
			receivedLogs = append(receivedLogs, req.ProtoLogs...)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Second // no periodic flush
	cfg.BatchSize = 100                  // large batch — won't auto-flush on size
	httpClient := &http.Client{Timeout: 5 * time.Second}

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)

	// Use a single-worker aggregator with a tiny queue to make the "pending in queue"
	// scenario deterministic: we manually call flushUnlocked to enqueue a job.
	agg := newMetricsAggregator(exporter, cfg)

	ctx := context.Background()

	// Directly submit a job to the export queue (bypassing workers temporarily).
	// We inject the metric as a "connection" type which flushes immediately.
	agg.recordMetric(ctx, &telemetryMetric{
		metricType:  "connection",
		timestamp:   time.Now(),
		sessionID:   "drain-test-session",
		statementID: "drain-test-stmt",
	})

	// close() should drain the queue and export the metric before returning.
	_ = agg.close(ctx)

	mu.Lock()
	count := len(receivedLogs)
	mu.Unlock()

	if count == 0 {
		t.Error("expected metric to be exported during drain phase of close(), got none")
	}
}

// TestAggregatorFlushUnlocked_InFlightAddBeforeSend verifies that inFlight.Add(1) is
// called before the job is sent to exportQueue so that close()'s inFlight.Wait()
// cannot miss a job that a worker picks up before the drain step runs.
func TestAggregatorFlushUnlocked_InFlightAddBeforeSend(t *testing.T) {
	var receivedCount int32

	// Server with a brief delay so workers stay busy during the close() call.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req TelemetryRequest
		if err := json.Unmarshal(body, &req); err == nil {
			atomic.AddInt32(&receivedCount, int32(len(req.ProtoLogs)))
		}
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Second
	cfg.BatchSize = 1
	httpClient := &http.Client{Timeout: 5 * time.Second}

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	agg := newMetricsAggregator(exporter, cfg)
	ctx := context.Background()

	const numMetrics = 20
	for i := 0; i < numMetrics; i++ {
		agg.recordMetric(ctx, &telemetryMetric{
			metricType: "operation",
			timestamp:  time.Now(),
			tags: map[string]interface{}{
				"operation_type": OperationTypeCloseStatement,
			},
		})
	}

	_ = agg.close(ctx)

	got := atomic.LoadInt32(&receivedCount)
	if got != numMetrics {
		t.Errorf("expected %d metrics, got %d — inFlight ordering may be broken", numMetrics, got)
	}
}

// TestAggregatorClose_SafeToCallMultipleTimes verifies that calling close() multiple
// times (via sync.Once) does not panic or deadlock.
func TestAggregatorClose_SafeToCallMultipleTimes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	agg := newMetricsAggregator(exporter, cfg)
	ctx := context.Background()

	// Call close() concurrently several times — must not panic or deadlock.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = agg.close(ctx)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// pass
	case <-time.After(5 * time.Second):
		t.Fatal("close() deadlocked when called concurrently multiple times")
	}
}

// TestAggregatorFlushUnlocked_DropWhenQueueFull verifies that when the export queue
// is full, the batch is silently dropped and inFlight is not left incremented
// (which would cause inFlight.Wait() to block forever in close()).
//
// Strategy: cancel the aggregator context immediately so workers stop draining the queue,
// then fill the queue to capacity and call flushUnlocked once more. The drop path must
// call inFlight.Done() to undo the earlier Add so that inFlight.Wait() returns promptly.
func TestAggregatorFlushUnlocked_DropWhenQueueFull(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Second
	cfg.BatchSize = 1
	httpClient := &http.Client{Timeout: 1 * time.Second}

	// Use a no-op exporter — we never actually export in this test.
	exporter := newTelemetryExporter("http://127.0.0.1:0", "test-version", httpClient, cfg)
	agg := newMetricsAggregator(exporter, cfg)

	// Cancel the aggregator context immediately so workers stop consuming from the queue.
	agg.cancel()
	// Give workers a moment to exit their select loop.
	time.Sleep(20 * time.Millisecond)

	ctx := context.Background()

	// Fill the export queue to capacity with synthetic jobs, each paired with an inFlight.Add.
	for i := 0; i < exportQueueSize; i++ {
		agg.inFlight.Add(1)
		agg.exportQueue <- exportJob{ctx: ctx, metrics: nil}
	}

	// Now call flushUnlocked — the queue is full, so the batch must be dropped.
	// The drop path must call inFlight.Done() to undo the Add it made before the send attempt.
	agg.mu.Lock()
	agg.batch = append(agg.batch, &telemetryMetric{
		metricType: "operation",
		timestamp:  time.Now(),
	})
	agg.flushUnlocked(ctx)
	agg.mu.Unlock()

	// Drain the synthetic queue entries and release their inFlight counts.
	for i := 0; i < exportQueueSize; i++ {
		<-agg.exportQueue
		agg.inFlight.Done()
	}

	// If flushUnlocked properly called Done() on drop, inFlight counter is now at 0
	// and inFlight.Wait() must return immediately (not block forever).
	waitDone := make(chan struct{})
	go func() {
		agg.inFlight.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// pass — inFlight counter was properly balanced on the drop path
	case <-time.After(2 * time.Second):
		t.Fatal("inFlight.Wait() blocked — inFlight counter is unbalanced after queue-full drop")
	}
}

// TestAggregatorClose_RespectsContextTimeout verifies that close() returns promptly
// when the caller's context deadline expires, rather than blocking indefinitely on a
// hung HTTP export.
//
// Regression test for the scenario where a telemetry server is unresponsive and
// conn.Close() would hang forever waiting for in-flight exports to finish.
func TestAggregatorClose_RespectsContextTimeout(t *testing.T) {
	const serverDelay = 5 * time.Second

	// serverGotRequest is signaled when the HTTP handler receives a request,
	// confirming a worker has picked up the job and started an HTTP export.
	serverGotRequest := make(chan struct{}, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Signal that the worker has started the HTTP export.
		select {
		case serverGotRequest <- struct{}{}:
		default:
		}
		// Simulate a hung/slow telemetry server that takes much longer than our timeout.
		time.Sleep(serverDelay)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.FlushInterval = 10 * time.Second
	cfg.BatchSize = 1
	httpClient := &http.Client{Timeout: 10 * time.Second}

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	agg := newMetricsAggregator(exporter, cfg)

	// Record a metric that triggers an immediate flush (terminal op).
	// A worker will pick it up and start a slow HTTP export.
	agg.recordMetric(context.Background(), &telemetryMetric{
		metricType: "operation",
		timestamp:  time.Now(),
		tags:       map[string]interface{}{"operation_type": OperationTypeCloseStatement},
	})

	// Wait for the worker to actually start the HTTP request, rather than using
	// a racy time.Sleep. This ensures close() enters the inFlight.Wait path
	// (step 4) rather than draining the job synchronously (step 3).
	select {
	case <-serverGotRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not pick up the job and start HTTP export in time")
	}

	// Call close() with a short timeout — it must return when the context expires,
	// NOT wait for the full serverDelay.
	timeout := 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	closeStart := time.Now()
	_ = agg.close(ctx)
	closeDuration := time.Since(closeStart)

	// close() must have returned near the timeout, not after serverDelay.
	if closeDuration >= serverDelay {
		t.Errorf("close() blocked for %v (server delay %v); expected it to respect the %v context timeout", closeDuration, serverDelay, timeout)
	}
	// Allow some slack but it should be well under the server delay.
	if closeDuration > 1*time.Second {
		t.Errorf("close() took %v; expected it to return near the %v context timeout", closeDuration, timeout)
	}
}
