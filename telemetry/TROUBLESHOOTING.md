# Telemetry Troubleshooting Guide

## Common Issues

### Issue: Telemetry Not Working

**Symptoms:**
- No telemetry data appearing in monitoring dashboards
- Metrics not being collected

**Diagnostic Steps:**

1. **Check if telemetry is enabled:**
   ```go
   // Add this to your connection string to force enable
   dsn := "host:443/sql/1.0/warehouse/abc?forceEnableTelemetry=true"
   ```

2. **Check server-side feature flag:**
   - Feature flag may be disabled on the server
   - Contact your Databricks admin to verify flag status

3. **Check circuit breaker state:**
   - Circuit breaker may have opened due to failures
   - Check logs for "circuit breaker" messages

4. **Verify network connectivity:**
   - Ensure driver can reach telemetry endpoint
   - Check firewall rules for outbound HTTPS

**Solution:**
- Use `forceEnableTelemetry=true` to bypass server checks
- If circuit is open, wait 30 seconds for it to reset
- Check network connectivity and firewall rules

---

### Issue: High Memory Usage

**Symptoms:**
- Memory usage growing over time
- Out of memory errors

**Diagnostic Steps:**

1. **Check if metrics are being flushed:**
   - Default flush interval: 5 seconds
   - Default batch size: 100 metrics

2. **Check circuit breaker state:**
   - If circuit is open, metrics may be accumulating
   - Check logs for repeated export failures

3. **Monitor goroutine count:**
   - Use `runtime.NumGoroutine()` to check for leaks
   - Each connection should have 1 flush goroutine

**Solution:**
- Reduce batch size if needed: `telemetry_batch_size=50`
- Reduce flush interval if needed: `telemetry_flush_interval=3s`
- Disable telemetry temporarily: `enableTelemetry=false`

---

### Issue: Performance Degradation

**Symptoms:**
- Queries running slower than expected
- High CPU usage

**Diagnostic Steps:**

1. **Measure overhead:**
   - Run benchmark tests to measure impact
   - Expected overhead: < 1%

2. **Check if telemetry is actually enabled:**
   - Telemetry should be nearly zero overhead when disabled
   - Verify with `enableTelemetry` parameter

3. **Check export frequency:**
   - Too frequent exports may cause overhead
   - Default: 5 second flush interval

**Solution:**
- Disable telemetry if overhead > 1%: `enableTelemetry=false`
- Increase flush interval: `telemetry_flush_interval=10s`
- Increase batch size: `telemetry_batch_size=200`
- Report issue to Databricks support

---

### Issue: Circuit Breaker Always Open

**Symptoms:**
- No telemetry data being sent
- Logs showing "circuit breaker is open"

**Diagnostic Steps:**

1. **Check telemetry endpoint health:**
   - Endpoint may be experiencing issues
   - Check server status page

2. **Check network connectivity:**
   - DNS resolution working?
   - HTTPS connectivity to endpoint?

3. **Check error rates:**
   - Circuit opens at 50% failure rate (after 20+ calls)
   - Check logs for HTTP error codes

**Solution:**
- Wait 30 seconds for circuit to attempt recovery (half-open state)
- Fix network connectivity issues
- If endpoint is down, circuit will protect driver automatically
- Once endpoint recovers, circuit will close automatically

---

### Issue: "Rate Limited" Errors

**Symptoms:**
- HTTP 429 (Too Many Requests) errors
- Telemetry export failing

**Diagnostic Steps:**

1. **Check if using per-host client sharing:**
   - Multiple connections to same host should share one client
   - Verify clientManager is working correctly

2. **Check export frequency:**
   - Too many exports may trigger rate limiting
   - Default: 5 second flush interval

3. **Check batch size:**
   - Too small batches = more requests
   - Default: 100 metrics per batch

**Solution:**
- Per-host sharing should prevent rate limiting
- If rate limited, circuit breaker will open automatically
- Increase batch size: `telemetry_batch_size=200`
- Increase flush interval: `telemetry_flush_interval=10s`

---

### Issue: Resource Leaks

**Symptoms:**
- Growing number of goroutines
- File descriptors increasing
- Memory not being released

**Diagnostic Steps:**

1. **Check connection cleanup:**
   - Ensure `db.Close()` is being called
   - Check for leaked connections

2. **Check telemetry cleanup:**
   - Each closed connection should release resources
   - Reference counting should clean up per-host clients

3. **Monitor goroutines:**
   ```go
   import "runtime"

   fmt.Printf("Goroutines: %d\n", runtime.NumGoroutine())
   ```

**Solution:**
- Always call `db.Close()` when done
- Use `defer db.Close()` to ensure cleanup
- Report persistent leaks to Databricks support

---

## Diagnostic Commands

### Check Telemetry Configuration

```go
import (
	"database/sql"
	"fmt"
	_ "github.com/databricks/databricks-sql-go"
)

func checkConfig() {
	// This will log configuration at connection time
	db, err := sql.Open("databricks",
		"host:443/sql/1.0/warehouse/abc?forceEnableTelemetry=true")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer db.Close()

	// Run a test query
	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		fmt.Printf("Query error: %v\n", err)
	} else {
		fmt.Printf("Query successful, result: %d\n", result)
	}
}
```

### Force Enable for Testing

```go
// Add to connection string
dsn := "host:443/sql/1.0/warehouse/abc?forceEnableTelemetry=true"
```

### Force Disable for Testing

```go
// Add to connection string
dsn := "host:443/sql/1.0/warehouse/abc?enableTelemetry=false"
```

### Check Circuit Breaker State

Circuit breaker state is internal, but you can infer it from behavior:
- If metrics suddenly stop being sent: circuit likely open
- Wait 30 seconds for half-open state
- Successful requests will close circuit

---

## Performance Tuning

### Reduce Telemetry Overhead

If telemetry is causing performance issues (should be < 1%):

```go
// Option 1: Disable completely
dsn := "host:443/sql/1.0/warehouse/abc?enableTelemetry=false"

// Option 2: Reduce frequency
dsn := "host:443/sql/1.0/warehouse/abc?" +
	"telemetry_flush_interval=30s&" +
	"telemetry_batch_size=500"
```

### Optimize for High-Throughput

For applications with many connections:

```go
// Increase batch size to reduce request frequency
dsn := "host:443/sql/1.0/warehouse/abc?" +
	"telemetry_batch_size=1000&" +
	"telemetry_flush_interval=10s"
```

---

## Debugging Tools

### Enable Debug Logging

The driver uses structured logging. Check your application logs for telemetry-related messages at TRACE or DEBUG level.

### Run Benchmark Tests

```bash
cd telemetry
go test -bench=. -benchmem
```

Expected results:
- BenchmarkInterceptor_Overhead_Enabled: < 1000 ns/op
- BenchmarkInterceptor_Overhead_Disabled: < 100 ns/op

### Run Integration Tests

```bash
cd telemetry
go test -v -run Integration
```

All integration tests should pass.

---

## Privacy Concerns

### What Data Is Collected?

**Collected:**
- Query latency (timing)
- Error codes (numeric)
- Feature usage (booleans)
- Statement IDs (UUIDs)

**NOT Collected:**
- SQL query text
- Query results
- Table/column names
- User identities
- IP addresses

### How to Verify?

The `shouldExportToDatabricks()` function in `telemetry/tags.go` controls what's exported. Review this file to see exactly what tags are allowed.

### Complete Opt-Out

```go
// Add to connection string
dsn := "host:443/sql/1.0/warehouse/abc?enableTelemetry=false"
```

This completely disables telemetry collection and export.

---

## Getting Help

### Self-Service

1. Check this troubleshooting guide
2. Review telemetry/DESIGN.md for architecture details
3. Review telemetry/LAUNCH.md for configuration options
4. Run diagnostic commands above

### Databricks Support

**Internal Users:**
- Slack: #databricks-sql-drivers
- JIRA: PECOBLR project
- Email: drivers-team@databricks.com

**External Customers:**
- Databricks Support Portal
- Include driver version and configuration
- Include relevant log snippets (no sensitive data)

### Reporting Bugs

**Information to Include:**
1. Driver version (`go list -m github.com/databricks/databricks-sql-go`)
2. Go version (`go version`)
3. Operating system
4. Connection string (redact credentials!)
5. Error messages
6. Steps to reproduce

**GitHub Issues:**
https://github.com/databricks/databricks-sql-go/issues

---

## Emergency Disable

If telemetry is causing critical issues:

### Immediate Workaround (Client-Side)

```go
// Add this parameter to all connection strings
enableTelemetry=false
```

### Server-Side Disable (Databricks Admin)

Contact Databricks support to disable the server-side feature flag:
```
databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver = false
```

This will disable telemetry for all connections.

---

## FAQ

**Q: Does telemetry impact query performance?**
A: No, telemetry overhead is < 1% and all operations are async.

**Q: Can I disable telemetry completely?**
A: Yes, use `enableTelemetry=false` in your connection string.

**Q: What happens if the telemetry endpoint is down?**
A: The circuit breaker will open and metrics will be dropped. Your queries are unaffected.

**Q: Does telemetry collect my SQL queries?**
A: No, SQL query text is never collected.

**Q: How long are metrics retained?**
A: This is controlled by Databricks backend, typically 90 days.

**Q: Can I see my telemetry data?**
A: Telemetry data is used for product improvements and is not directly accessible to users.
