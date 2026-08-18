# Connection parameters reference

This is a reference for every connection parameter accepted by the driver — as a
**DSN parameter** (`?param=value`) and/or as a **connector option**
(`dbsql.WithX(...)`) — and whether each is supported on the **Thrift** (default)
backend and the **SEA / kernel** backend (`useKernel=true` / `WithUseKernel(true)`).

It is a companion to the [Connection properties](./README.md#connection-properties)
and [Choosing a backend](./README.md#choosing-a-backend-thrift-vs-seakernel)
sections of the README, laid out as one comparison matrix per concern.

## Legend

| Symbol | Meaning |
|:---:|---|
| ✅ | Supported and honored. |
| ❌ | Not supported — **rejected** at connect/execute (wraps `dbsqlerr.ErrNotSupportedByKernel` or `dbsqlerr.ErrRequiresKernelBackend`), never silently ignored. |
| ⚠️ | Accepted but has no effect ("inert" / silently ignored). |
| — | Not applicable. |

**Backend selection.** Both backends are selected once per connection via
`useKernel` / `WithUseKernel(true)`. If the binary was **not** built with the
`databricks_kernel` tag, selecting the kernel backend fails at connect
(wraps `dbsqlerr.ErrKernelNotCompiled`) — it never silently falls back to Thrift.
See [Building](./README.md#building).

Any parameter not listed below (e.g. `ansi_mode`) is passed through as a
**session parameter**. On the **Thrift** path the session-conf map is forwarded freely.
On the **kernel** path conf keys are matched (case-insensitively) against an allowlist —
non-allowlisted keys are dropped with a warning, and a few are hard-rejected — so a conf
that takes effect on Thrift may silently be ignored on kernel. Broadening the kernel
allowlist is tracked in PECOBLR-4153.

## Endpoint & routing

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| *(host)* | `WithServerHostname` | ✅ | ✅ | *(required)* | Workspace hostname. |
| *(path)* | `WithHTTPPath` | ✅ | ✅ | *(required)* | Warehouse/endpoint HTTP path. |
| *(port)* | `WithPort` | ✅ | ❌ | `443` | Kernel connects on **443 only** and rejects any other port. |
| `warehouseId` | `WithWarehouseID` | ⚠️ | ✅ | | Bare warehouse id; the kernel routes by it (preferred over the HTTP path). The Thrift backend **silently ignores** it. |
| `catalog` | `WithInitialNamespace` | ✅ | ✅ | | Initial catalog. Kernel applies it post-connect via `USE CATALOG`. |
| `schema` | `WithInitialNamespace` | ✅ | ✅ | | Initial schema. Kernel applies it post-connect via `USE SCHEMA`. |
| `useKernel` | `WithUseKernel` | ✅ | ✅ | `false` | Select the SEA/kernel backend. Requires a `databricks_kernel` build. |

## Authentication

| Method | DSN | Connector option | Thrift | Kernel |
|---|---|---|:---:|:---:|
| Personal access token (PAT) | `token:<t>@…`, or `accessToken=` / `authType=Pat` | `WithAccessToken` | ✅ | ✅ |
| OAuth machine-to-machine (M2M) | `clientID=`+`clientSecret=` / `authType=OauthM2M` | `WithClientCredentials` | ✅ | ✅ |
| OAuth user-to-machine (U2M) | `authType=OauthU2M` | `WithAuthenticator` (u2m) | ✅ | ✅ |
| Custom token provider / external / static / federated | — | `WithTokenProvider`, `WithExternalToken`, `WithStaticToken`, `WithFederatedTokenProvider*` | ✅ | ❌ |

Notes for the SEA/kernel backend:

- Custom OAuth **M2M scopes** are rejected on the kernel path (the kernel applies its
  own default scopes). Default scopes work on both.
- **U2M** is interactive: on a cache miss, connecting launches the browser and a
  connect-context **deadline is not honored** during the login window. U2M scopes are at
  parity with Thrift. Use PAT or M2M for headless/deadline-bound connects.
- OAuth token caching/refresh is owned by the kernel on the kernel path (no driver
  config).

## Query execution

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `maxRows` | `WithMaxRows` | ✅ | ⚠️ | `100000` | Max rows per fetch. On the kernel path the kernel manages paging, so this is accepted but has no effect. |
| `timeout` | `WithTimeout` | ✅ | ❌ | no timeout | Server-side query timeout, in seconds. On the kernel path use the `STATEMENT_TIMEOUT` session parameter instead. |
| `userAgentEntry` | `WithUserAgentEntry` | ✅ | ✅ | | Identifies your application (partners/ISVs), format `<isv-name+product-name>`. |
| *(session param)* | `WithSessionParams` | ✅ | ⚠️ | | Arbitrary session confs (e.g. `ansi_mode`, `STATEMENT_TIMEOUT`, `QUERY_TAGS`). Allowlisted confs are honored on both; on kernel a non-allowlisted conf is dropped/rejected (see the note above; PECOBLR-4153). |
| *(via session param)* | `WithQueryTags` | ✅ | ✅ | | Session-level query tags (serialized into `QUERY_TAGS`). |
| `timezone` | `WithSessionParams(timezone=…)` | ✅ | ✅ | | Session time zone (e.g. `America/Los_Angeles`). |
| `enableMetricViewMetadata` | `WithEnableMetricViewMetadata` | ✅ | ⚠️ | `false` | Enables metric-view metadata (sets `spark.sql.thriftserver.metadata.metricview.enabled=true`). The driver forwards the conf on both paths, but the kernel currently hard-rejects it (HTTP 400 `INVALID_CONF_VALUE`), so it does not yet take effect on the kernel path (PECOBLR-4142 / PECOBLR-4153). |

## Retry / backoff

| Connector option | Thrift | Kernel | Default | Notes |
|---|:---:|:---:|---|---|
| `WithRetries(retryMax, waitMin, waitMax)` | ✅ | ✅ | `4`, `1s`, `30s` | Retry attempts and exponential-backoff bounds. `retryMax < 0` disables retries. |
| `WithKernelRetryOverallTimeout(d)` | ❌ | ✅ | kernel default (900s) | Cumulative retry budget across all attempts. No Thrift equivalent. |

## Result rendering

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `useArrowNativeDecimal` | `WithArrowNativeDecimal` | ✅ | ⚠️ | `false` | Thrift: return DECIMAL as native Arrow `decimal128` (lossless string when scanned via `database/sql`). The kernel path already renders DECIMAL as the exact string regardless, so the flag is inert there. |
| | `WithKernelDecimalAsFloat(b)` | ❌ | ✅ | `false` | Scan top-level DECIMAL as lossy `float64` instead of the exact string. |

Otherwise results render **byte-for-byte identically** on both backends (scalars,
DECIMAL as exact string, TIMESTAMP / TIMESTAMP_NTZ shifted into the session time zone,
INTERVAL, nested ARRAY / MAP / STRUCT and VARIANT as JSON, GEOMETRY / GEOGRAPHY as WKT,
BINARY as `sql.RawBytes`).

## Cloud Fetch

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `useCloudFetch` | `WithCloudFetch` | ✅ | ⚠️ | `true` | Enable Cloud Fetch. On the kernel path Cloud Fetch is always managed internally, so the flag is inert. |
| `maxDownloadThreads` | `WithMaxDownloadThreads` | ✅ | ⚠️ | `10` | Concurrent download goroutines (Thrift). Inert on the kernel path. |
| | `WithKernelMaxChunksInMemory(n)` | ❌ | ✅ | kernel default (16) | Bounds decompressed Cloud Fetch chunks held in memory — trades large-result throughput for peak memory. |

## TLS

| Connector option | Thrift | Kernel | Notes |
|---|:---:|:---:|---|
| `WithSkipTLSHostVerify()` | ✅ | ✅ | Disable TLS chain + hostname verification. **Use only for internal private-link hostnames** — susceptible to machine-in-the-middle attacks. |
| `WithTransport(http.RoundTripper)` | ✅ | ❌ | Supply a custom HTTP transport (custom CA, mTLS, or proxy). Rejected on the kernel path — the kernel uses its own HTTP stack; use `WithKernelTrustedCerts` / `WithKernelProxy` there. |
| `WithKernelTrustedCerts(pem)` | ❌ | ✅ | Add a PEM CA bundle on top of the system roots (for a re-signing proxy / on-prem CA). Needed because the kernel's TLS stack does not read `SSL_CERT_FILE`. |
| `WithKernelSkipHostnameVerify()` | ❌ | ✅ | Skip **only** the hostname check while keeping chain validation (finer-grained than `WithSkipTLSHostVerify`). |

## Proxy

Both backends honor the standard `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` environment
variables. Note the kernel path accepts **http(s) proxies only**: a `socks*` proxy URL
(honored on the Thrift path) is rejected at connect on the kernel path. Kernel SOCKS
support is tracked in PECOBLR-4152.

| Connector option | Thrift | Kernel | Notes |
|---|:---:|:---:|---|
| *(environment)* | ✅ | ✅ | `HTTP(S)_PROXY` / `NO_PROXY`. |
| `WithKernelProxy(KernelProxy{URL, Username, Password, BypassHosts})` | ❌ | ✅ | Explicit proxy with out-of-band basic-auth credentials and a structured bypass list — the "advanced" fields the env-var path can't express. Overrides the environment proxy; a malformed URL is rejected at connect. |

## Telemetry

Telemetry applies to **both** backends. When `enableTelemetry` is left unset (the
default), a server-side feature flag decides whether telemetry is active; setting it
explicitly overrides the flag. (Exception: on the kernel backend with OAuth **U2M**,
telemetry is skipped entirely to avoid a second interactive browser flow at connect.)

| DSN parameter | Thrift | Kernel | Default | Notes |
|---|:---:|:---:|---|---|
| `enableTelemetry` | ✅ | ✅ | unset (server flag decides) | Force telemetry on/off, overriding the server feature flag. |
| `telemetry_batch_size` | ✅ | ✅ | `200` | Events per batch. |
| `telemetry_flush_interval` | ✅ | ✅ | `30s` | Flush interval. |
| `telemetry_retry_count` | ⚠️ | ⚠️ | — | **Deprecated and ignored** (retries are owned by the HTTP client + circuit breaker); logs a one-time warning. |
| `telemetry_retry_delay` | ⚠️ | ⚠️ | — | **Deprecated and ignored** (see above). |

The kernel path additionally emits a connection-config telemetry event at connect (mode,
auth mechanism/flow, proxy, arrow, query tags, metric-view); the Thrift path's telemetry
is unchanged. See [`telemetry/DESIGN.md`](./telemetry/DESIGN.md).
