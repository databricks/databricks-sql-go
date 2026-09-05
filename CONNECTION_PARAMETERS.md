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
| ⚠️ | Accepted but not fully honored — either inert ("silently ignored") or only partially/conditionally honored. |
| — | Not applicable. |

For fixed-value constraints such as the kernel's port and scheme, ⚠️ means the
supported value is accepted but any other value is hard-rejected; it does not mean
an unsupported value is silently ignored.

**Backend selection.** Both backends are selected once per connection via
`useKernel` / `WithUseKernel(true)`. If the binary was **not** built with the
`databricks_kernel` tag, selecting the kernel backend fails at connect
(wraps `dbsqlerr.ErrKernelNotCompiled`) — it never silently falls back to Thrift.
See [Building](./README.md#building).

Any parameter not listed below (e.g. `ansi_mode`) is passed through as a
**session parameter**. Both backends forward server-bound session confs; the kernel
preserves their names and values unchanged. The kernel's reserved client-result
keys are the exception: it consumes them locally and strips them before the SEA
wire. They are listed under [Kernel client-result confs](#kernel-client-result-confs).

## Endpoint & routing

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| *(host)* | `WithServerHostname` | ✅ | ✅ | *(required)* | Workspace hostname. |
| *(path)* | `WithHTTPPath` | ✅ | ✅ | required on Thrift except localhost; required on kernel unless `warehouseId` is set | Warehouse/endpoint HTTP path. On the kernel path, a bare warehouse id can be used instead. |
| *(port)* | `WithPort` | ✅ | ⚠️ | `443` for connector options; required in a DSN | Kernel accepts **443 only** and rejects any other port. |
| *(scheme)* | *(via `WithServerHostname`)* | ✅ | ⚠️ | `https` | Kernel accepts `https` only and rejects `http`. A bare `localhost` passed to `WithServerHostname` selects `http`; a scheme-less DSN, including localhost, is prefixed with `https`. |
| `warehouseId` | `WithWarehouseID` | ⚠️ | ✅ | | Bare warehouse id. The kernel normally prefers it over the HTTP path; a canonical path carrying `?o=` takes precedence so workspace-org routing is preserved. The Thrift backend **silently ignores** it. |
| `catalog` | `WithInitialNamespace` | ✅ | ✅ | | Initial catalog. Kernel applies it post-connect via `USE CATALOG`. |
| `schema` | `WithInitialNamespace` | ✅ | ✅ | | Initial schema. Kernel applies it post-connect via `USE SCHEMA`. |
| `useKernel` | `WithUseKernel` | ✅ | ✅ | `false` | Select the SEA/kernel backend. Requires a `databricks_kernel` build. |

## Authentication

| Method | DSN | Connector option | Thrift | Kernel |
|---|---|---|:---:|:---:|
| Personal access token (PAT) | `token:<t>@…`, or `accessToken=` / `authType=Pat` | `WithAccessToken` | ✅ | ✅ |
| OAuth machine-to-machine (M2M) | `clientID=` (or `clientId=`) + `clientSecret=` / `authType=OauthM2M` | `WithClientCredentials` | ✅ | ✅ |
| OAuth user-to-machine (U2M) | `authType=OauthU2M` | `WithAuthenticator` (u2m) | ✅ | ✅ |
| Custom / external / static token provider | — | `WithTokenProvider`, `WithExternalToken`, `WithStaticToken` | ✅ | ❌ |
| Custom authenticator | — | `WithAuthenticator(custom)` | ✅ | ❌ |
| Federated token provider | — | `WithFederatedTokenProvider`, `WithFederatedTokenProviderAndClientID` | ✅ | ✅ |

Notes for the SEA/kernel backend:

- The kernel snapshots one federated-provider token during setup;
  `WithFederatedTokenProviderAndClientID` also forwards the SP-wide client ID. Expired
  tokens require a new connection.
- Custom OAuth **M2M scopes** are rejected on the kernel path (the kernel applies its
  own default scopes). Default scopes work on both.
- `WithAuthenticator` is kernel-compatible only when its concrete authenticator is
  one of the supported PAT, M2M, or U2M implementations. An arbitrary custom
  `auth.Authenticator` is supported on Thrift and rejected on the kernel path.
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
- OAuth **U2M** on-disk token caching is controlled by `WithTokenCache` / the `tokenCache`
  DSN param — see the table just below. In-session token *refresh* is always owned by the
  kernel regardless of this flag.

### OAuth U2M token cache

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `tokenCache` | — | ⚠️ | ✅ | `false` (disabled) | **Kernel U2M-only.** Persists the U2M refresh token to an encrypted on-disk cache in the kernel's OS config directory, so a later process skips the browser login. `tokenCache=true` requires `useKernel=true` and is rejected on Thrift with `ErrRequiresKernelBackend`; `tokenCache=false` is a harmless no-op there. No effect on PAT/M2M; enable-flag only (no passphrase surface). |
| — | `WithTokenCache(bool)` | ❌ | ✅ | `false` (disabled) | Either value allocates kernel-only configuration, so even `WithTokenCache(false)` is rejected on Thrift unless paired with `WithUseKernel(true)`. |

## Query execution

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `maxRows` | `WithMaxRows` | ✅ | ⚠️ | `100000` | Max rows per fetch. On the kernel path the kernel manages paging, so this is accepted but has no effect. |
| `timeout` | `WithTimeout` | ✅ | ❌ | no timeout | Server-side query timeout, in seconds. On the kernel path use the `STATEMENT_TIMEOUT` session parameter instead. |
| `userAgentEntry` | `WithUserAgentEntry` | ✅ | ✅ | | Identifies your application (partners/ISVs), format `<isv-name+product-name>`. |
| *(session param)* | `WithSessionParams` | ✅ | ✅ | | Arbitrary server session confs (e.g. `ansi_mode`, `STATEMENT_TIMEOUT`, `QUERY_TAGS`) are forwarded unchanged. |
| *(via session param)* | `WithQueryTags` | ✅ | ✅ | | Session-level query tags (serialized into `QUERY_TAGS`). |
| `timezone` | `WithSessionParams(timezone=…)` | ✅ | ✅ | | Session time zone (e.g. `America/Los_Angeles`). |
| `enableMetricViewMetadata` | `WithEnableMetricViewMetadata` | ✅ | ⚠️ | `false` | Enables metric-view metadata (sets `spark.sql.thriftserver.metadata.metricview.enabled=true`). Both paths forward the identical conf. Whether it takes effect on the SEA/kernel path depends on server-side SEA support (PECOBLR-4142). |

### Kernel session confs

The kernel forwards server-bound `WithSessionParams` entries unchanged and lets the
server validate them. An unsupported key may therefore fail session creation or a later
statement instead of being dropped client-side.

Notes:

- Boolean-valued keys should use the exact strings `"true"` / `"false"` — the kernel does
  not pre-validate values and forwards them as-is.
- **`CAN_CLOUD_DOWNLOAD` is not a valid SEA session conf**: it may be accepted at
  CreateSession and rejected by the first statement with `CONFIG_NOT_AVAILABLE`.
  To disable Cloud Fetch on the kernel path, set the client-result conf
  `cloudfetch_enabled=false`; `WithCloudFetch(false)` is a Thrift option and is inert
  on the kernel path.

### Kernel client-result confs

These reserved keys may be supplied as DSN parameters or through
`WithSessionParams`. The kernel consumes them locally and strips them before the SEA
wire. Thrift does not interpret them as client settings: it forwards them as ordinary
server session confs, where they may be rejected or have no effect. Use the dedicated
Thrift options where available.

| DSN / session-conf key | Dedicated connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `cloudfetch_enabled` | *(none; use `WithSessionParams`)* | ⚠️ | ✅ | `true` | Kernel Cloud Fetch toggle; `false` forces inline results. On Thrift use `WithCloudFetch`. |
| `cloudfetch_link_prefetch_window` | *(none; use `WithSessionParams`)* | ⚠️ | ✅ | `10` | Number of presigned Cloud Fetch links prefetched ahead of the consumer. |
| `cloudfetch_max_chunks_in_memory` | `WithKernelMaxChunksInMemory` | ⚠️ | ✅ | `16` | Maximum decompressed Cloud Fetch chunks held in memory. Values above 256 are clamped with a warning, not rejected. |
| `inline_max_chunks_in_memory` | *(none; use `WithSessionParams`)* | ⚠️ | ✅ | `4` | Inline-Arrow prefetch window; `1` disables prefetching. |
| `complex_types_as_json` | *(none; use `WithSessionParams`)* | ⚠️ | ✅ | `false` | Render ARRAY / MAP / STRUCT / VARIANT / GEOMETRY / GEOGRAPHY as JSON text. |
| `intervals_as_string` | *(none; use `WithSessionParams`)* | ⚠️ | ✅ | `false` | Render INTERVAL / DURATION values as canonical Databricks text. |

Client-result key matching is case-insensitive. Boolean values are `true` or `false`
(case-insensitive, with surrounding whitespace ignored); window values must be
positive integers. Invalid values supplied through the mixed session-conf map are
ignored with a warning and still stripped from the SEA wire.

## HTTP client / retry

| Connector option | Thrift | Kernel | Default | Notes |
|---|:---:|:---:|---|---|
| `WithRetries(retryMax, waitMin, waitMax)` | ✅ | ✅ | `4`, `1s`, `30s` | Retry attempts and exponential-backoff bounds. `retryMax < 0` disables retries. |
| `WithKernelRetryOverallTimeout(d)` | ❌ | ✅ | kernel default (900s) | Cumulative retry budget across all attempts. No Thrift equivalent. |
| `WithKernelMaxConnections(n)` | ❌ | ✅ | kernel default (100) | Maximum idle HTTP connections retained per host, not a hard concurrency cap. `n` must be positive. |

## Result rendering

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `useArrowNativeDecimal` | `WithArrowNativeDecimal` | ✅ | ⚠️ | `false` | Thrift: return DECIMAL as native Arrow `decimal128` (lossless string when scanned via `database/sql`). The kernel path already renders DECIMAL as the exact string regardless, so the flag is inert there. |
| | `WithKernelDecimalAsFloat(b)` | ❌ | ✅ | `false` | Scan top-level DECIMAL as lossy `float64` instead of the exact string. |

With the default client-result settings, results otherwise render **byte-for-byte
identically** on both backends (scalars,
DECIMAL as exact string, TIMESTAMP / TIMESTAMP_NTZ shifted into the session time zone,
INTERVAL, nested ARRAY / MAP / STRUCT and VARIANT as JSON, GEOMETRY / GEOGRAPHY as WKT,
BINARY as `sql.RawBytes`).

## Cloud Fetch

| DSN parameter | Connector option | Thrift | Kernel | Default | Notes |
|---|---|:---:|:---:|---|---|
| `useCloudFetch` | `WithCloudFetch` | ✅ | ⚠️ | `true` | Enable Cloud Fetch on Thrift. This option is inert on the kernel path; use the `cloudfetch_enabled` client-result conf there. |
| `maxDownloadThreads` | `WithMaxDownloadThreads` | ✅ | ⚠️ | `10` | Concurrent download goroutines (Thrift). Inert on the kernel path. |
| | `WithKernelMaxChunksInMemory(n)` | ❌ | ✅ | kernel default (16) | Bounds decompressed Cloud Fetch chunks held in memory — trades large-result throughput for peak memory. |

## TLS

| Connector option | Thrift | Kernel | Notes |
|---|:---:|:---:|---|
| `WithSkipTLSHostVerify()` | ✅ | ✅ | Disable TLS chain + hostname verification. **Use only for internal private-link hostnames** — susceptible to machine-in-the-middle attacks. On the kernel path it maps to the kernel's "accept self-signed" + hostname-skip (relaxes both chain and hostname, matching Thrift). |
| `WithTransport(http.RoundTripper)` | ✅ | ❌ | Supply a custom HTTP transport (custom CA, mTLS, or proxy). **Rejected** on the kernel path (wraps `ErrNotSupportedByKernel`): a Go `RoundTripper` is executable Go code that can't cross the C ABI into the kernel's own Rust HTTP stack, so it could only be silently ignored. Use the dedicated `WithKernel*` knobs below (custom CA / client-cert mTLS / hostname-skip / proxy). |
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

Go wrapper telemetry applies to the default Thrift backend. When `enableTelemetry` is
left unset (the default), a server-side feature flag decides whether wrapper telemetry is
active; setting it explicitly overrides the flag. On the kernel backend, the Go wrapper
skips its telemetry interceptor so it does not duplicate kernel-owned telemetry for the
same connection and statements, and forwards the kernel-owned telemetry knobs into the
kernel config.

| DSN parameter | Thrift | Kernel | Default | Notes |
|---|:---:|:---:|---|---|
| `enableTelemetry` | ✅ | ✅ | unset (server flag decides wrapper telemetry; kernel telemetry defaults off) | Force Go wrapper telemetry on/off on the Thrift path, overriding the server feature flag. On the kernel path, forwarded to kernel-owned telemetry; unset forwards disabled. |
| `telemetry_batch_size` | ✅ | ✅ | `200` wrapper default; kernel default when unset | Events per batch. Forwarded to the kernel only when explicitly set. |
| `telemetry_flush_interval` | ✅ | ✅ | `30s` wrapper default; kernel default when unset | Flush interval. Forwarded to the kernel only when explicitly set. |
| `telemetry_retry_count` | ⚠️ | ⚠️ | — | **Deprecated and ignored** (retries are owned by the HTTP client + circuit breaker); logs a one-time warning. |
| `telemetry_retry_delay` | ⚠️ | ⚠️ | — | **Deprecated and ignored** (see above). |

These telemetry knobs are **DSN-only** — there are no `WithX` connector options for them.
An app assembled with `NewConnector(...)` options rather than a DSN cannot tune telemetry:
wrapper telemetry falls back to the server feature flag (since `enableTelemetry` is
unset), kernel telemetry defaults off, and `telemetry_batch_size` /
`telemetry_flush_interval` use their backend defaults.

The Go wrapper telemetry interceptor is skipped on the kernel path so it does not
duplicate kernel-owned telemetry for the same connection and statements. See
[`telemetry/DESIGN.md`](./telemetry/DESIGN.md).
