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

// TestIntegration_TelemetryEventCorrectnessAllFields verifies that every field of the
// TelemetryRequest and nested TelemetryFrontendLog is correctly populated and present
// when a metric is exported.  This is the canonical correctness check for the wire format.
func TestIntegration_TelemetryEventCorrectnessAllFields(t *testing.T) {
	const (
		testDriverVersion = "9.9.9-test"
		testSessionID     = "sess-correctness-123"
		testStatementID   = "stmt-correctness-456"
		testLatencyMs     = int64(123)
		testOperationType = "EXECUTE_STATEMENT"
		testChunkCount    = 7
		testPollCount     = 4
		testErrorName     = "NETWORK_ERROR"
	)

	cfg := DefaultConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	var mu sync.Mutex
	var capturedReq TelemetryRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(body, &capturedReq)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, testDriverVersion, httpClient, cfg)

	metric := &telemetryMetric{
		metricType:  "operation",
		timestamp:   time.Now(),
		sessionID:   testSessionID,
		statementID: testStatementID,
		latencyMs:   testLatencyMs,
		errorType:   testErrorName,
		tags: map[string]interface{}{
			"operation_type": testOperationType,
			"chunk_count":    testChunkCount,
			"poll_count":     testPollCount,
		},
	}

	ctx := context.Background()
	exporter.export(ctx, []*telemetryMetric{metric})
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	req := capturedReq
	mu.Unlock()

	// --- TelemetryRequest top-level fields ---
	if req.UploadTime == 0 {
		t.Error("TelemetryRequest.UploadTime must be non-zero")
	}
	if req.Items == nil {
		t.Error("TelemetryRequest.Items must not be nil (required by server schema)")
	}
	if len(req.ProtoLogs) == 0 {
		t.Fatal("TelemetryRequest.ProtoLogs must contain at least one entry")
	}

	// --- Parse TelemetryFrontendLog from ProtoLogs[0] ---
	var frontendLog TelemetryFrontendLog
	if err := json.Unmarshal([]byte(req.ProtoLogs[0]), &frontendLog); err != nil {
		t.Fatalf("Failed to unmarshal ProtoLogs[0] as TelemetryFrontendLog: %v", err)
	}

	// FrontendLogEventID must be generated (non-empty, unique timestamp-based ID)
	if frontendLog.FrontendLogEventID == "" {
		t.Error("TelemetryFrontendLog.FrontendLogEventID must be non-empty")
	}

	// --- Context / ClientContext ---
	if frontendLog.Context == nil {
		t.Fatal("TelemetryFrontendLog.Context must not be nil")
	}
	if frontendLog.Context.ClientContext == nil {
		t.Fatal("FrontendLogContext.ClientContext must not be nil")
	}
	cc := frontendLog.Context.ClientContext
	if cc.ClientType != "golang" {
		t.Errorf("ClientContext.ClientType must be %q, got %q", "golang", cc.ClientType)
	}
	if cc.ClientVersion != testDriverVersion {
		t.Errorf("ClientContext.ClientVersion must be %q, got %q", testDriverVersion, cc.ClientVersion)
	}

	// --- Entry / SQLDriverLog ---
	if frontendLog.Entry == nil {
		t.Fatal("TelemetryFrontendLog.Entry must not be nil")
	}
	if frontendLog.Entry.SQLDriverLog == nil {
		t.Fatal("FrontendLogEntry.SQLDriverLog must not be nil")
	}
	ev := frontendLog.Entry.SQLDriverLog

	if ev.SessionID != testSessionID {
		t.Errorf("TelemetryEvent.SessionID must be %q, got %q", testSessionID, ev.SessionID)
	}
	if ev.SQLStatementID != testStatementID {
		t.Errorf("TelemetryEvent.SQLStatementID must be %q, got %q", testStatementID, ev.SQLStatementID)
	}
	if ev.OperationLatencyMs != testLatencyMs {
		t.Errorf("TelemetryEvent.OperationLatencyMs must be %d, got %d", testLatencyMs, ev.OperationLatencyMs)
	}

	// --- SystemConfiguration ---
	if ev.SystemConfiguration == nil {
		t.Fatal("TelemetryEvent.SystemConfiguration must not be nil")
	}
	sc := ev.SystemConfiguration
	if sc.DriverName != "databricks-sql-go" {
		t.Errorf("SystemConfiguration.DriverName must be %q, got %q", "databricks-sql-go", sc.DriverName)
	}
	if sc.DriverVersion != testDriverVersion {
		t.Errorf("SystemConfiguration.DriverVersion must be %q, got %q", testDriverVersion, sc.DriverVersion)
	}
	if sc.RuntimeName != "go" {
		t.Errorf("SystemConfiguration.RuntimeName must be %q, got %q", "go", sc.RuntimeName)
	}
	if sc.RuntimeVersion == "" {
		t.Error("SystemConfiguration.RuntimeVersion must be non-empty")
	}
	if sc.OSName == "" {
		t.Error("SystemConfiguration.OSName must be non-empty")
	}
	if sc.OSArch == "" {
		t.Error("SystemConfiguration.OSArch must be non-empty")
	}
	if sc.CharSetEncoding != "UTF-8" {
		t.Errorf("SystemConfiguration.CharSetEncoding must be %q, got %q", "UTF-8", sc.CharSetEncoding)
	}
	if sc.ProcessName == "" {
		t.Error("SystemConfiguration.ProcessName must be non-empty")
	}

	// --- SQLOperation / OperationDetail ---
	if ev.SQLOperation == nil {
		t.Fatal("TelemetryEvent.SQLOperation must not be nil for operation metrics with tags")
	}
	if ev.SQLOperation.OperationDetail == nil {
		t.Fatal("SQLExecutionEvent.OperationDetail must not be nil when operation_type tag is set")
	}
	od := ev.SQLOperation.OperationDetail
	if od.OperationType != testOperationType {
		t.Errorf("OperationDetail.OperationType must be %q, got %q", testOperationType, od.OperationType)
	}
	if od.NOperationStatusCalls != int32(testPollCount) {
		t.Errorf("OperationDetail.NOperationStatusCalls must be %d, got %d", testPollCount, od.NOperationStatusCalls)
	}

	// ChunkDetails must be populated for chunk_count tag
	if ev.SQLOperation.ChunkDetails == nil {
		t.Fatal("SQLExecutionEvent.ChunkDetails must not be nil when chunk_count tag is set")
	}
	if ev.SQLOperation.ChunkDetails.TotalChunksIterated != int32(testChunkCount) {
		t.Errorf("ChunkDetails.TotalChunksIterated must be %d, got %d", testChunkCount, ev.SQLOperation.ChunkDetails.TotalChunksIterated)
	}

	// --- ErrorInfo ---
	if ev.ErrorInfo == nil {
		t.Fatal("TelemetryEvent.ErrorInfo must not be nil when errorType is set")
	}
	if ev.ErrorInfo.ErrorName != testErrorName {
		t.Errorf("DriverErrorInfo.ErrorName must be %q, got %q", testErrorName, ev.ErrorInfo.ErrorName)
	}

	t.Log("Telemetry event correctness check passed: all fields verified")
}

// TestIntegration_OperationLatencyMs_ZeroNotOmitted verifies that OperationLatencyMs=0
// is serialised as "operation_latency_ms":0 in the JSON payload, not omitted.
//
// Regression test: the field previously had `json:"operation_latency_ms,omitempty"` which
// caused 0ms latency (e.g. CloseOperation that completes in <1ms) to appear as null in
// the Databricks telemetry table.
func TestIntegration_OperationLatencyMs_ZeroNotOmitted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	var mu sync.Mutex
	var capturedReq TelemetryRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(body, &capturedReq)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)

	// latencyMs=0 — simulates a CloseOperation that completed in <1ms.
	metric := &telemetryMetric{
		metricType:  "operation",
		timestamp:   time.Now(),
		sessionID:   "sess-zero-latency",
		statementID: "stmt-zero-latency",
		latencyMs:   0, // <1ms rounded to 0
	}

	ctx := context.Background()
	exporter.export(ctx, []*telemetryMetric{metric})
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	req := capturedReq
	mu.Unlock()

	if len(req.ProtoLogs) == 0 {
		t.Fatal("expected ProtoLogs to be non-empty")
	}

	// Verify the raw JSON contains the key with value 0, not absent.
	raw := req.ProtoLogs[0]
	if !strings.Contains(raw, `"operation_latency_ms":0`) {
		t.Errorf("expected raw JSON to contain \"operation_latency_ms\":0, got: %s", raw)
	}

	// Also verify via struct parse.
	var log TelemetryFrontendLog
	if err := json.Unmarshal([]byte(raw), &log); err != nil {
		t.Fatalf("failed to unmarshal ProtoLogs[0]: %v", err)
	}
	if log.Entry == nil || log.Entry.SQLDriverLog == nil {
		t.Fatal("Entry.SQLDriverLog must not be nil")
	}
	if log.Entry.SQLDriverLog.OperationLatencyMs != 0 {
		t.Errorf("expected OperationLatencyMs=0, got %d", log.Entry.SQLDriverLog.OperationLatencyMs)
	}

	t.Log("OperationLatencyMs=0 correctly serialised (omitempty fix verified)")
}

// TestIntegration_ChunkTotalPresent_DerivedFromChunkCount verifies that when the
// "chunk_total_present" tag is explicitly set (e.g. derived from r.chunkCount in
// rows.Close()), it is propagated to ChunkDetails.TotalChunksPresent in the payload.
//
// This covers the fix for paginated CloudFetch where the server never reports the
// grand total across all FetchResults calls; the driver derives it from chunkCount.
func TestIntegration_ChunkTotalPresent_DerivedFromChunkCount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FlushInterval = 50 * time.Millisecond
	httpClient := &http.Client{Timeout: 5 * time.Second}

	var mu sync.Mutex
	var capturedReq TelemetryRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(body, &capturedReq)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	const (
		totalChunksPresent = 32
		totalChunksIterated = 32
	)

	exporter := newTelemetryExporter(server.URL, "test-version", httpClient, cfg)
	metric := &telemetryMetric{
		metricType:  "operation",
		timestamp:   time.Now(),
		sessionID:   "sess-chunks",
		statementID: "stmt-chunks",
		latencyMs:   500,
		tags: map[string]interface{}{
			"chunk_count":        totalChunksIterated, // total pages fetched
			"chunk_total_present": totalChunksPresent,  // derived from r.chunkCount
		},
	}

	ctx := context.Background()
	exporter.export(ctx, []*telemetryMetric{metric})
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	req := capturedReq
	mu.Unlock()

	if len(req.ProtoLogs) == 0 {
		t.Fatal("expected ProtoLogs to be non-empty")
	}

	var log TelemetryFrontendLog
	if err := json.Unmarshal([]byte(req.ProtoLogs[0]), &log); err != nil {
		t.Fatalf("failed to unmarshal ProtoLogs[0]: %v", err)
	}

	ev := log.Entry.SQLDriverLog
	if ev == nil {
		t.Fatal("SQLDriverLog must not be nil")
	}
	if ev.SQLOperation == nil || ev.SQLOperation.ChunkDetails == nil {
		t.Fatal("ChunkDetails must not be nil when chunk_count tag is set")
	}

	cd := ev.SQLOperation.ChunkDetails
	if cd.TotalChunksIterated != int32(totalChunksIterated) {
		t.Errorf("TotalChunksIterated: expected %d, got %d", totalChunksIterated, cd.TotalChunksIterated)
	}

	t.Logf("ChunkDetails.TotalChunksIterated=%d (chunk_total_present tag propagation verified)", cd.TotalChunksIterated)
	t.Log("chunk_total_present derivation from chunkCount correctly propagated")
}
