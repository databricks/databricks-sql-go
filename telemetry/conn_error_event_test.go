package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// decodeEvents parses a captured TelemetryRequest body into its TelemetryEvents.
func decodeEvents(t *testing.T, body []byte) []*TelemetryEvent {
	t.Helper()
	var req TelemetryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	var evs []*TelemetryEvent
	for _, pl := range req.ProtoLogs {
		var fl TelemetryFrontendLog
		if err := json.Unmarshal([]byte(pl), &fl); err != nil {
			t.Fatalf("unmarshal protoLog: %v", err)
		}
		if fl.Entry != nil && fl.Entry.SQLDriverLog != nil {
			evs = append(evs, fl.Entry.SQLDriverLog)
		}
	}
	return evs
}

// createTelemetryRequest must serialize connParams into DriverConnectionParameters
// (and mirror AuthMech onto the top-level AuthType) for a "connection" metric, and
// must leave those fields nil/empty for a plain statement metric — so existing
// metrics serialize byte-identically to before this change.
func TestCreateTelemetryRequest_ConnParams(t *testing.T) {
	connMetric := &telemetryMetric{
		metricType: "connection",
		timestamp:  time.Now(),
		sessionID:  "sess-1",
		connParams: &DriverConnectionParameters{
			Mode:        "SEA",
			AuthMech:    "PAT",
			EnableArrow: true,
			HTTPPath:    "/sql/1.0/warehouses/abc",
		},
	}
	stmtMetric := &telemetryMetric{
		metricType:  "statement",
		timestamp:   time.Now(),
		sessionID:   "sess-1",
		statementID: "stmt-1",
		latencyMs:   5,
	}

	req, err := createTelemetryRequest([]*telemetryMetric{connMetric, stmtMetric}, "v-test")
	if err != nil {
		t.Fatalf("createTelemetryRequest: %v", err)
	}
	if len(req.ProtoLogs) != 2 {
		t.Fatalf("want 2 protoLogs, got %d", len(req.ProtoLogs))
	}

	// The connection metric carries the params + AuthType mirror.
	var connLog TelemetryFrontendLog
	if err := json.Unmarshal([]byte(req.ProtoLogs[0]), &connLog); err != nil {
		t.Fatal(err)
	}
	ev := connLog.Entry.SQLDriverLog
	if ev.DriverConnectionParameters == nil {
		t.Fatal("connection metric: DriverConnectionParameters must be populated")
	}
	if ev.DriverConnectionParameters.Mode != "SEA" || ev.DriverConnectionParameters.AuthMech != "PAT" {
		t.Errorf("conn params = %+v, want Mode=SEA AuthMech=PAT", ev.DriverConnectionParameters)
	}
	if ev.AuthType != "PAT" {
		t.Errorf("AuthType = %q, want PAT (mirrored from AuthMech)", ev.AuthType)
	}

	// The statement metric must NOT carry conn params or auth type — byte-identical
	// to a pre-change statement metric. Guards the "additive, behavior-preserving"
	// contract for the default (Thrift) path's telemetry.
	var stmtLog TelemetryFrontendLog
	if err := json.Unmarshal([]byte(req.ProtoLogs[1]), &stmtLog); err != nil {
		t.Fatal(err)
	}
	sev := stmtLog.Entry.SQLDriverLog
	if sev.DriverConnectionParameters != nil {
		t.Errorf("statement metric leaked DriverConnectionParameters: %+v", sev.DriverConnectionParameters)
	}
	if sev.AuthType != "" {
		t.Errorf("statement metric leaked AuthType = %q, want empty", sev.AuthType)
	}
}

// Full loop: RecordConnectionConfig flushes a connection event immediately (the
// connection may close before the next batch), and it lands at the HTTP endpoint
// carrying the conn params.
func TestRecordConnectionConfig_EndToEnd(t *testing.T) {
	cfg := DefaultConfig()
	var mu sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, "v-test", "ua", &http.Client{Timeout: 5 * time.Second}, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)
	interceptor := newInterceptor(aggregator, true)

	interceptor.RecordConnectionConfig(context.Background(), "sess-1", &DriverConnectionParameters{
		Mode: "SEA", AuthMech: "PAT", EnableArrow: true,
	})

	// The connection metric flushes immediately; give the async export a moment.
	waitForBodies(t, &mu, &bodies, 1)

	found := false
	mu.Lock()
	for _, b := range bodies {
		for _, ev := range decodeEvents(t, b) {
			if ev.DriverConnectionParameters != nil && ev.DriverConnectionParameters.Mode == "SEA" {
				found = true
			}
		}
	}
	mu.Unlock()
	if !found {
		t.Fatal("connection-config event with Mode=SEA never reached the endpoint")
	}
}

// A standalone connection-scoped error (empty statementID, non-terminal) must NOT
// be silently dropped by the aggregator — it must flush immediately and land. This
// is the latent-bug fix: the old "error" branch dropped a non-terminal error whose
// statementID matched no buffered statement.
func TestConnectionScopedError_NotDropped(t *testing.T) {
	cfg := DefaultConfig()
	var mu sync.Mutex
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := newTelemetryExporter(server.URL, "v-test", "ua", &http.Client{Timeout: 5 * time.Second}, cfg)
	aggregator := newMetricsAggregator(exporter, cfg)

	// A non-terminal error with no statement to attach to (connection-scoped).
	aggregator.recordMetric(context.Background(), &telemetryMetric{
		metricType: "error",
		timestamp:  time.Now(),
		sessionID:  "sess-1",
		errorType:  "sqlstate_42P01", // a SQL error, non-terminal
	})

	waitForBodies(t, &mu, &bodies, 1)

	found := false
	mu.Lock()
	for _, b := range bodies {
		for _, ev := range decodeEvents(t, b) {
			if ev.ErrorInfo != nil && ev.ErrorInfo.ErrorName == "sqlstate_42P01" {
				found = true
			}
		}
	}
	mu.Unlock()
	if !found {
		t.Fatal("connection-scoped error was silently dropped — expected it to flush and land")
	}
}

// waitForBodies polls until at least n request bodies have been captured or times out.
func waitForBodies(t *testing.T, mu *sync.Mutex, bodies *[][]byte, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		got := len(*bodies)
		mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d request body(ies), got %d", n, got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
