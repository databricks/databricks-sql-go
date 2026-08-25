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
| ⚠️ | Accepted but not fully honored — either inert ("silently ignored") or only partially/conditionally honored (e.g. some session confs are honored while others are dropped/rejected on the kernel path). |
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
| *(scheme)* | *(via `WithServerHostname`)* | ✅ | ❌ | `https` | A non-https scheme (`http://…`, or a bare `localhost`, which defaults to `http`) is **rejected** on the kernel path (it connects over https). Honored on Thrift. |
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
| Custom / external / static token provider | — | `WithTokenProvider`, `WithExternalToken`, `WithStaticToken` | ✅ | ❌ |
| Federated token provider | — | `WithFederatedTokenProvider*` | ✅ | ✅ |

Notes for the SEA/kernel backend:

- The kernel snapshots one `WithFederatedTokenProvider*` token during setup;
  `AndClientID` also forwards the SP-wide client ID. Expired tokens require a new connection.
- Custom OAuth **M2M scopes** are rejected on the kernel path (the kernel applies its
  own default scopes). Default scopes work on both.
- **U2M** is interactive: on a cache miss, connecting launches the browser to complete the
  login. Use PAT or M2M for headless connects (they need no browser).
- On the kernel path U2M always uses the in-house `databricks-sql-connector` OAuth app and
  fixed `sql` + `offline_access` scopes, **uniformly across clouds** — it does not forward a
  custom client id or custom scopes. This matches Thrift on AWS/GCP; on **Azure** it
  deliberately differs (Thrift uses the Entra-direct app id / `user_impersonation` scope,
  while the kernel drives a single in-house workspace-federated flow).
- The **connect-context deadline is not honored mid-connect** on the kernel path for *any*
  auth (not just U2M): the context is checked only at entry to session-open, then the
  kernel's blocking session-open runs uninterruptibly, so a slow warehouse cold-start or a
  connect-time network partition can block past the deadline. U2M's browser login is the
  most visible case, but PAT/M2M are equally uninterruptible mid-connect — this is a kernel
  C ABI limitation, not U2M-specific.
- OAuth **U2M** on-disk token caching is controlled by `WithTokenCache(enabled bool)`
  (DSN `tokenCache=true|false`). **Disabled by default** — the driver forwards a disable
  unless you opt in, matching its historical no-persistence posture. When enabled, the
  kernel persists the U2M refresh token to an AES-256 encrypted on-disk cache at
  `~/.config/databricks-sql-kernel/oauth/`, so a later process skips the browser login.
  U2M-only (no effect on PAT/M2M); enable-flag only (no passphrase surface). In-session
  token *refresh* is always owned by the kernel regardless of this flag.

## Query execution

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `maxRows` | `WithMaxRows` | ✅ | ⚠️ | `100000` | Max rows per fetch. On the kernel path the kernel manages paging, so this is accepted but has no effect. |
| `timeout` | `WithTimeout` | ✅ | ❌ | no timeout | Server-side query timeout, in seconds. On the kernel path use the `STATEMENT_TIMEOUT` session parameter instead. |
| `userAgentEntry` | `WithUserAgentEntry` | ✅ | ✅ | | Identifies your application (partners/ISVs), format `<isv-name+product-name>`. |
| *(session param)* | `WithSessionParams` | ✅ | ⚠️ | | Arbitrary session confs (e.g. `ansi_mode`, `STATEMENT_TIMEOUT`, `QUERY_TAGS`). Allowlisted confs are honored on both; on kernel a non-allowlisted conf is dropped/rejected (see the note above; PECOBLR-4153). |
| *(via session param)* | `WithQueryTags` | ✅ | ✅ | | Session-level query tags (serialized into `QUERY_TAGS`). |
| `timezone` | `WithSessionParams(timezone=…)` | ✅ | ✅ | | Session time zone (e.g. `America/Los_Angeles`). |
| `enableMetricViewMetadata` | `WithEnableMetricViewMetadata` | ✅ | ⚠️ | `false` | Enables metric-view metadata (sets `spark.sql.thriftserver.metadata.metricview.enabled=true`). Both paths forward the **identical** conf; the kernel allowlists this key and sends it verbatim — it is **not** rejected driver- or kernel-side. Whether it takes effect on the SEA/kernel path depends on server-side SEA support (a `⚠️` pending confirmation against a live warehouse; PECOBLR-4142 / PECOBLR-4153). |

### Kernel session-conf allowlist

On the **Thrift** path the `WithSessionParams` map is forwarded to the server freely.
On the **kernel** path each key is matched **case-insensitively** against the allowlist
below; a key not on it is **dropped with a warning** (never sent), so a conf that takes
effect on Thrift may silently do nothing on kernel. Broadening the allowlist is tracked in
PECOBLR-4153.

> **Authoritative source.** This table is transcribed from the vendored kernel's
> allowlist (`build/kernel-src/src/config.rs`), which is not part of this repo checkout.
> Only `spark.sql.thriftserver.metadata.metricview.enabled` and the kernel max-chunks key
> have repo-side anchors (`internal/config/config.go`); the remaining keys and the
> uppercase-on-send / `spark.*`-verbatim rules have no CI guard here and may lag as the
> kernel evolves. When in doubt, treat the kernel allowlist as authoritative.

**SET-style SQL parameters** — matched case-insensitively, sent **uppercased** (the server
echoes these uppercase, so `SET`-readback matches):

| Key | Purpose |
|---|---|
| `ANSI_MODE` | Enable/disable ANSI SQL behavior. |
| `COLLATION` | Default collation. |
| `ENABLE_PHOTON` | Toggle the Photon engine. |
| `LEGACY_TIME_PARSER_POLICY` | Legacy datetime parsing behavior. |
| `MAX_FILE_PARTITION_BYTES` | Max bytes per file partition. |
| `QUERY_TAGS` | Query tags (comma-separated `key:value`). This is the key `WithQueryTags` writes. |
| `READ_ONLY_EXTERNAL_METASTORE` | Treat the external metastore as read-only. |
| `STATEMENT_TIMEOUT` | Server-side per-statement timeout (seconds). The real query-timeout knob on the kernel path, since `WithTimeout` is rejected there. |
| `TIMEZONE` | Session time zone (e.g. `UTC`). Also settable via the `timezone` DSN param / `WithSessionParams`. |
| `USE_CACHED_RESULT` | Toggle result caching. |

**Dotted `spark.*` conf** — matched case-insensitively but sent **verbatim** (Spark conf
keys are case-sensitive and must not be uppercased):

| Key | Purpose |
|---|---|
| `spark.sql.thriftserver.metadata.metricview.enabled` | Metric-view metadata; the conf `WithEnableMetricViewMetadata` sets. |

Notes:

- Boolean-valued keys should use the exact strings `"true"` / `"false"` — the kernel does
  not pre-validate values and forwards them as-is.
- **`CAN_CLOUD_DOWNLOAD` is deliberately not allowlisted**: SEA has no such session conf
  (it is accepted at CreateSession but rejected at the first statement with
  `CONFIG_NOT_AVAILABLE`). Disable Cloud Fetch with `WithCloudFetch(false)` and bound its
  memory with `WithKernelMaxChunksInMemory` instead of a raw conf.
- The client-only keys (`cloudfetch_enabled`, `cloudfetch_max_chunks_in_memory`,
  `complex_types_as_json`, `intervals_as_string`, …) are **not** in this allowlist: the
  kernel reads them at session creation and strips them before the SEA wire. The driver
  exposes the relevant ones as dedicated `WithKernel*` options rather than raw confs.

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
| `WithSkipTLSHostVerify()` | ✅ | ✅ | Disable TLS chain + hostname verification. **Use only for internal private-link hostnames** — susceptible to machine-in-the-middle attacks. On the kernel path it maps to the kernel's "accept self-signed" + hostname-skip (relaxes both chain and hostname, matching Thrift). |
| `WithTransport(http.RoundTripper)` | ✅ | ❌ | Supply a custom HTTP transport (custom CA, mTLS, or proxy). **Rejected** on the kernel path (wraps `ErrRequiresKernelBackend`/`ErrNotSupportedByKernel`): a Go `RoundTripper` is executable Go code that can't cross the C ABI into the kernel's own Rust HTTP stack, so it could only be silently ignored. Use the dedicated `WithKernel*` knobs below (custom CA / client-cert mTLS / hostname-skip / proxy). |
| `WithKernelTrustedCerts(pem)` | ❌ | ✅ | Add a PEM CA bundle on top of the system roots (for a re-signing proxy / on-prem CA). Needed because the kernel's rustls stack does not read `SSL_CERT_FILE`. Rejected on Thrift (use `WithTransport` there). |
| `WithKernelClientCertificate(certPEM, keyPEM)` | ❌ | ✅ | Client-certificate **mTLS** identity: a paired PEM certificate + unencrypted private key. Both must be non-empty (an empty pair wraps `ErrInvalidKernelConfig`); the certificate may include intermediates, and a PKCS#8 key is recommended across kernel TLS backends. Server chain/hostname verification stay strict and independently configurable. Rejected on Thrift (`ErrRequiresKernelBackend`) — use `WithTransport` with `tls.Config.Certificates` there. Requires a kernel build pinning [databricks-sql-kernel#289](https://github.com/databricks/databricks-sql-kernel/pull/289) (see `KERNEL_REV`). |
| `WithKernelSkipHostnameVerify()` | ❌ | ✅ | Skip **only** the hostname check while keeping chain validation (finer-grained than `WithSkipTLSHostVerify`). Rejected on Thrift. |

The `WithTransport` rejection is deliberate — a Go transport can't cross the C ABI into the
kernel's Rust HTTP stack. The dedicated kernel TLS knobs cover what a custom transport is
used for:

| Reason for a custom `WithTransport` | Kernel option |
|---|---|
| Custom CA bundle (re-signing proxy / on-prem CA) | `WithKernelTrustedCerts(pem)` |
| Client-certificate mTLS | `WithKernelClientCertificate(certPEM, keyPEM)` |
| Skip hostname check (private-link host) | `WithKernelSkipHostnameVerify()` (hostname only) or `WithSkipTLSHostVerify()` (blanket) |
| HTTP proxy | `WithKernelProxy(...)` / `HTTP(S)_PROXY` env (see [Proxy](#proxy)) |

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

These telemetry knobs are **DSN-only** — there are no `WithX` connector options for them.
An app assembled with `NewConnector(...)` options rather than a DSN cannot tune telemetry:
whether telemetry is enabled falls back to the server feature flag (since `enableTelemetry`
is unset), and `telemetry_batch_size` / `telemetry_flush_interval` use their defaults
(`200` / `30s`).

The kernel path additionally emits a connection-config telemetry event at connect (mode,
auth mechanism/flow, proxy, arrow, query tags, metric-view); the Thrift path's telemetry
is unchanged. See [`telemetry/DESIGN.md`](./telemetry/DESIGN.md).
