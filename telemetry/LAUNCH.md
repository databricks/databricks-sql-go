# Telemetry Launch Plan

## Overview

This document outlines the phased rollout strategy for the Go driver telemetry system. The rollout follows a gradual approach to ensure reliability and user control.

## Launch Phases

### Phase 1: Internal Testing (forceEnableTelemetry=true)

**Target Audience:** Databricks internal users and development teams

**Configuration:**
```go
dsn := "host:443/sql/1.0/warehouse/abc?forceEnableTelemetry=true"
```

**Characteristics:**
- Bypasses all server-side feature flag checks
- Always enabled regardless of server configuration
- Used for internal testing and validation
- Not exposed to external customers

**Success Criteria:**
- No impact on driver reliability or performance
- Telemetry data successfully collected and exported
- Circuit breaker correctly protects against endpoint failures
- No memory leaks or resource issues

**Duration:** 2-4 weeks

---

### Phase 2: Beta Opt-In (enableTelemetry=true)

**Target Audience:** Early adopter customers who want to help improve the driver

**Configuration:**
```go
dsn := "host:443/sql/1.0/warehouse/abc?enableTelemetry=true"
```

**Characteristics:**
- Respects server-side feature flags
- User explicitly opts in
- Server can enable/disable via feature flag
- Can be disabled by user with `enableTelemetry=false`

**Success Criteria:**
- Positive feedback from beta users
- < 1% performance overhead
- No increase in support tickets
- Valuable metrics collected for product improvements

**Duration:** 4-8 weeks

---

### Phase 3: Controlled Rollout (Server-Side Feature Flag)

**Target Audience:** General customer base with gradual percentage rollout

**Configuration:**
- No explicit DSN parameter needed
- Controlled entirely by server-side feature flag
- Users can opt-out with `enableTelemetry=false`

**Rollout Strategy:**
1. **5% rollout** - Monitor for issues (1 week)
2. **25% rollout** - Expand if no issues (1 week)
3. **50% rollout** - Majority validation (2 weeks)
4. **100% rollout** - Full deployment

**Success Criteria:**
- No increase in error rates
- Stable performance metrics
- Valuable insights from collected data
- Low opt-out rate

**Duration:** 6-8 weeks

---

## Configuration Flags Summary

### Flag Priority (Highest to Lowest)

1. **forceEnableTelemetry=true** - Force enable (internal only)
   - Bypasses all server checks
   - Always enabled
   - Use case: Internal testing, debugging

2. **enableTelemetry=false** - Explicit opt-out
   - Always disabled
   - Use case: User wants to disable telemetry

3. **enableTelemetry=true + Server Feature Flag** - User opt-in with server control
   - User wants telemetry
   - Server decides if allowed
   - Use case: Beta opt-in phase

4. **Server Feature Flag Only** - Server-controlled (default)
   - No explicit user preference
   - Server controls enablement
   - Use case: Controlled rollout

5. **Default** - Disabled
   - No configuration
   - Telemetry off by default
   - Use case: New installations

### Configuration Examples

**Internal Testing:**
```go
import (
	"database/sql"
	_ "github.com/databricks/databricks-sql-go"
)

// Force enable for testing
db, err := sql.Open("databricks",
	"host:443/sql/1.0/warehouse/abc?forceEnableTelemetry=true")
```

**Beta Opt-In:**
```go
// Opt-in to beta (respects server flags)
db, err := sql.Open("databricks",
	"host:443/sql/1.0/warehouse/abc?enableTelemetry=true")
```

**Explicit Opt-Out:**
```go
// User wants to disable telemetry
db, err := sql.Open("databricks",
	"host:443/sql/1.0/warehouse/abc?enableTelemetry=false")
```

**Default (Server-Controlled):**
```go
// No telemetry parameter - server decides
db, err := sql.Open("databricks",
	"host:443/sql/1.0/warehouse/abc")
```

---

## Monitoring

### Key Metrics to Monitor

**Performance Metrics:**
- Query latency (p50, p95, p99)
- Memory usage
- CPU usage
- Goroutine count

**Reliability Metrics:**
- Driver error rate
- Connection success rate
- Circuit breaker state transitions
- Telemetry export success rate

**Business Metrics:**
- Feature adoption (CloudFetch, LZ4, etc.)
- Common error patterns
- Query performance distribution

### Alerting Thresholds

**Critical Alerts:**
- Query latency increase > 5%
- Driver error rate increase > 2%
- Memory leak detected (growing > 10% over 24h)

**Warning Alerts:**
- Telemetry export failure rate > 10%
- Circuit breaker open for > 5 minutes
- Feature flag fetch failures > 5%

---

## Rollback Procedures

### Quick Disable (Emergency)

**Server-Side:**
```
Set feature flag to false:
databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver = false
```
- Takes effect immediately for new connections
- Existing connections will respect the flag on next fetch (15 min TTL)

**Client-Side Workaround:**
```go
// Users can add this parameter to disable immediately
enableTelemetry=false
```

### Rollback Steps

1. **Disable Feature Flag** - Turn off server-side flag
2. **Monitor Impact** - Watch for metrics to return to baseline
3. **Investigate Issue** - Analyze logs and telemetry data
4. **Fix and Redeploy** - Address root cause
5. **Re-enable Gradually** - Restart rollout from Phase 1

### Communication Plan

**Internal:**
- Slack notification to #driver-alerts
- PagerDuty alert for on-call engineer
- Incident report in wiki

**External (if needed):**
- Support article on workaround
- Release notes mention (if applicable)
- Direct communication to beta users

---

## Success Metrics

### Phase 1 Success Criteria

- ✅ Zero critical bugs reported
- ✅ Performance overhead < 1%
- ✅ Circuit breaker prevents cascading failures
- ✅ Memory usage stable over 7 days
- ✅ All integration tests passing

### Phase 2 Success Criteria

- ✅ > 50 beta users enrolled
- ✅ < 5% opt-out rate among beta users
- ✅ Positive feedback from beta users
- ✅ Valuable metrics collected
- ✅ No increase in support tickets

### Phase 3 Success Criteria

- ✅ Successful rollout to 100% of users
- ✅ < 1% opt-out rate
- ✅ Performance metrics stable
- ✅ Product insights driving improvements
- ✅ No increase in error rates

---

## Privacy and Compliance

### Data Collected

**Allowed:**
- ✅ Query latency (ms)
- ✅ Error codes (not messages)
- ✅ Feature flags (boolean)
- ✅ Statement IDs (UUIDs)
- ✅ Driver version
- ✅ Runtime info (Go version, OS)

**Never Collected:**
- ❌ SQL query text
- ❌ Query results or data values
- ❌ Table/column names
- ❌ User identities
- ❌ IP addresses
- ❌ Credentials

### Tag Filtering

All tags are filtered through `shouldExportToDatabricks()` before export:
- Tags marked `exportLocal` only: **not exported** to Databricks
- Tags marked `exportDatabricks`: **exported** to Databricks
- Unknown tags: **not exported** (fail-safe)

---

## Timeline

```
Week 1-4:   Phase 1 - Internal Testing
Week 5-12:  Phase 2 - Beta Opt-In
Week 13-20: Phase 3 - Controlled Rollout (5% → 100%)
Week 21+:   Full Production
```

**Total Duration:** ~5 months for full rollout

---

## Contact

**Questions or Issues:**
- Slack: #databricks-sql-drivers
- Email: drivers-team@databricks.com
- JIRA: PECOBLR project

**On-Call:**
- PagerDuty: Databricks Drivers Team
