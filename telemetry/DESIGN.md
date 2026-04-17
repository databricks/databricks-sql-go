# Telemetry Design

Telemetry is **disabled by default**. It collects driver usage metrics (latency, error types, feature flags) and exports them to the Databricks telemetry service. No SQL text, PII, or query results are ever collected.

---

## Architecture

```
Driver operations
    → Interceptor          (collect + tag metrics via context)
    → metricsAggregator    (aggregate by statement, batch by size/time)
    → telemetryExporter    (HTTP POST with retries)
    → circuitBreaker       (protect against endpoint failures)
    → /telemetry-ext
```

One `metricsAggregator` + `telemetryExporter` is shared per host (managed by `clientManager` with reference counting). This prevents rate limiting when many connections open to the same host.

---

## Components

### `Interceptor` (`interceptor.go`)
Exported hooks called by the driver package:

| Method | When called |
|--------|-------------|
| `BeforeExecute(ctx, sessionID, statementID)` | Before statement execution |
| `BeforeExecuteWithTime(ctx, sessionID, statementID, startTime)` | When statement ID is known only after execution starts |
| `AfterExecute(ctx, err)` | After execution completes |
| `AddTag(ctx, key, value)` | During execution to attach metadata |
| `CompleteStatement(ctx, statementID, failed)` | After rows are fully consumed |
| `RecordOperation(ctx, sessionID, opType, latencyMs)` | For non-statement operations |
| `Close(ctx)` | On connection close — **synchronous blocking** flush; waits for the HTTP export to complete before returning, matching JDBC's `flush(true).get()` behavior |

### `metricsAggregator` (`aggregator.go`)
- Aggregates statement metrics by `statementID` (total latency, chunk count, poll count)
- Flush behavior by metric type:
  - `"connection"` — flushes immediately (lifecycle event, must not be lost)
  - `"operation"` — batched, flushes when batch reaches `BatchSize`
  - `"statement"` — accumulated until `CompleteStatement`, then batched
- Background flush ticker runs every `FlushInterval`
- `flushUnlocked` exports asynchronously; semaphore (8 slots) bounds concurrent goroutines
- `flushSync` exports synchronously; used by `Interceptor.Close` to block until delivery
- `close` is idempotent via `sync.Once`; cancels in-flight periodic exports

### `telemetryExporter` (`exporter.go`)
- HTTP POST to `<host>/telemetry-ext`
- Exponential backoff retries (default: 3 retries, 100ms base delay)
- Retryable statuses: 429, 503, 5xx
- All errors and panics are swallowed; logged at TRACE level only

### `circuitBreaker` (`circuitbreaker.go`)
Sliding window (30 calls), opens when failure rate ≥ 50% over ≥ 20 calls. Recovers after 30s via half-open state (3 successful test calls to close). One circuit breaker per host.

### `clientManager` (`manager.go`)
Singleton. Maintains one `(aggregator + exporter)` per host with reference counting. Last connection to close triggers aggregator shutdown and final flush.

### `featureFlagCache` (`featureflag.go`)
Caches the server-side feature flag per host for 15 minutes. Synchronous fetch on first connection (or cache expiry); concurrent callers receive the stale cached value immediately.

---

## Export Format

Metrics are sent as a `TelemetryRequest` with a `protoLogs` array. Each entry is a JSON-encoded `TelemetryFrontendLog` aligned with the `OssSqlDriverTelemetryLog` proto schema:

```json
{
  "uploadTime": 1234567890000,
  "items": [],
  "protoLogs": [
    "{\"frontend_log_event_id\":\"20240101120000-a1b2c3d4e5f6g7h8\",\"context\":{...},\"entry\":{\"sql_driver_log\":{\"session_id\":\"...\",\"sql_statement_id\":\"...\",\"operation_latency_ms\":42,\"sql_operation\":{\"chunk_details\":{\"total_chunks_iterated\":3},...}}}}"
  ]
}
```

Event IDs are generated with `crypto/rand` + hex encoding (no modulo bias).

System info (OS name/version/arch, Go runtime version) is read once and cached via `sync.Once`.

---

## Configuration

DSN parameter: `enableTelemetry=true|false` (parsed via `strconv.ParseBool`)

Priority (highest to lowest):
1. Client DSN setting (`enableTelemetry=true/false`) — overrides server
2. Server feature flag (`databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver`)
3. Default: disabled

Batch size, flush interval, retry count, and retry delay are also configurable.

---

## Privacy

Only the following fields are exported:
- Session ID, statement ID, workspace ID
- Operation latency, error type (no message/stack trace)
- Chunk count, poll count
- Driver version, OS name, Go runtime version, result format

Tags not in this allowlist are silently dropped before export.
