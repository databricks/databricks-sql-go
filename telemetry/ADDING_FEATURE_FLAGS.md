# Adding New Feature Flags

The feature flag system is designed to be easily extensible. Follow these steps to add a new feature flag:

## Step 1: Add Flag Constant

In `featureflag.go`, add your new flag constant:

```go
const (
    // ... existing constants ...

    // flagEnableTelemetry controls whether telemetry is enabled for the Go driver
    flagEnableTelemetry = "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver"

    // YOUR NEW FLAG - Add it here
    flagEnableNewFeature = "databricks.partnerplatform.clientConfigsFeatureFlags.enableNewFeatureForGoDriver"
)
```

## Step 2: Register Flag for Fetching

In `featureflag.go`, add your flag to `getAllFeatureFlags()`:

```go
func getAllFeatureFlags() []string {
    return []string{
        flagEnableTelemetry,
        flagEnableNewFeature,  // Add your new flag here
    }
}
```

## Step 3: Add Public Method

In `featureflag.go`, add a public method to check your flag:

```go
// isNewFeatureEnabled checks if the new feature is enabled for the host.
// Uses cached value if available and not expired.
func (c *featureFlagCache) isNewFeatureEnabled(ctx context.Context, host string, httpClient *http.Client) (bool, error) {
    return c.getFeatureFlag(ctx, host, httpClient, flagEnableNewFeature)
}
```

## Step 4: Use It

```go
// Example usage in your code:
flagCache := getFeatureFlagCache()
enabled, err := flagCache.isNewFeatureEnabled(ctx, host, httpClient)
if err != nil {
    // Handle error (falls back to false on error with no cache)
}

if enabled {
    // Feature is enabled - use new behavior
} else {
    // Feature is disabled - use old behavior
}
```

## How It Works

### Connector Service Endpoint
Flags are fetched from the connector-service endpoint with driver name and version:
```
GET /api/2.0/connector-service/feature-flags/GOLANG/{driverVersion}
```

The response includes all available flags for the driver:
```json
{
  "flags": [
    {"name": "flagOne", "value": "true"},
    {"name": "flagTwo", "value": "false"}
  ],
  "ttl_seconds": 900
}
```

### 15-Minute Cache
Flags are cached for 15 minutes per host to minimize API calls.

### Graceful Degradation
- If fetch fails but cache exists → returns stale cache (no error)
- If fetch fails and no cache → returns error (caller defaults to false)

### Thread-Safe
Multiple goroutines can safely call feature flag methods concurrently.

## Example: Adding Circuit Breaker Flag

```go
// Step 1: Add constant
const (
    flagEnableTelemetry = "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForGoDriver"
    flagEnableCircuitBreaker = "databricks.partnerplatform.clientConfigsFeatureFlags.enableCircuitBreakerForGoDriver"
)

// Step 2: Register for fetching
func getAllFeatureFlags() []string {
    return []string{
        flagEnableTelemetry,
        flagEnableCircuitBreaker,
    }
}

// Step 3: Add public method
func (c *featureFlagCache) isCircuitBreakerEnabled(ctx context.Context, host string, httpClient *http.Client) (bool, error) {
    return c.getFeatureFlag(ctx, host, httpClient, flagEnableCircuitBreaker)
}

// Step 4: Use it
if enabled, _ := flagCache.isCircuitBreakerEnabled(ctx, host, httpClient); enabled {
    // Use circuit breaker
}
```

## Benefits

✅ **Single HTTP request** - All flags fetched at once
✅ **15-minute caching** - Minimal API calls
✅ **Graceful degradation** - Uses stale cache on errors
✅ **Thread-safe** - Safe for concurrent access
✅ **Easy to extend** - Just 3 simple steps
