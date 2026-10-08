# Adding New Feature Flags

Driver-side consumers use the shared cache in [`internal/featureflags`](../internal/featureflags/cache.go).
Confirm the flag is registered with connector-service, then keep its name and
expected SAFE type at the consuming code. The cache retains every returned flag;
adding a consumer does not require a new cache method or a list of flags to fetch.

## Read a Flag

Use `GetBool`, `GetInt32`, `GetInt64`, `GetDouble`, `GetString`, or `GetStringList`.
The caller supplies an authenticated HTTP client and chooses the expected type:

```go
import "github.com/databricks/databricks-sql-go/internal/featureflags"

request := featureflags.Request{
    Host:          host,
    WorkspaceID:   workspaceID,
    DriverVersion: driverVersion,
    UserAgent:     userAgent,
    HTTPClient:    authenticatedClient,
}

flags := featureflags.GetCache()
flags.Acquire(request.Host, request.WorkspaceID)
defer flags.Release(request.Host, request.WorkspaceID)

enabled, err := flags.GetBool(ctx, request, flagName)
if err != nil {
    enabled = false // Choose a safe fallback for this consumer.
}
```

Acquire once for the consumer's lifetime and release when it closes, not after
each getter. Existing driver connections already manage this in `connector.Connect`
and `conn.Close`; consumers on that path can reuse `conn.featureFlags`.
Acquiring a reference does not fetch flags. The request can be used before session
open if authentication is already available; it does not initialize authentication
or select the kernel backend. Explicit kernel connections currently skip this
driver-side cache and use the kernel's own cache.

## Cache and Failure Behavior

- One GET retrieves all flags from
  `/api/2.0/connector-service/feature-flags/GOLANG/{driverVersion}`.
- Values are shared by workspace ID, with normalized host as a fallback. The
  cache retains values, not credentials or HTTP clients.
- The server's `ttl_seconds` controls refresh; a missing or invalid TTL falls back
  to 15 minutes. Releasing the last consumer removes the workspace entry.
- A cold read waits for the fetch. During a refresh, other readers can use stale
  values. A failed refresh keeps the last successful values; a failed initial
  fetch returns an error.
- `GetBool` returns `false` without an error for missing/null flags. Other getters
  return an error for missing/null flags. All getters reject malformed values or
  values incompatible with the requested type, leaving fallback policy to the caller.
