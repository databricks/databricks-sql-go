# Databricks SQL Driver for Go

![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)

A [database/sql](https://golang.org/pkg/database/sql) driver for Databricks SQL. It
connects to Databricks SQL Warehouses and clusters and runs queries through Go's
standard `database/sql` interface.

## Contents

- [Quick start](#quick-start)
- [Cloning the repository](#cloning-the-repository)
- [Choosing a backend (Thrift vs SEA/kernel)](#choosing-a-backend-thrift-vs-seakernel)
- [Building](#building)
- [Connecting](#connecting)
  - [DSN (Data Source Name)](#dsn-data-source-name)
  - [Connector object](#connector-object)
- [Connection properties](#connection-properties)
- [Authentication](#authentication)
- [Cloud Fetch](#cloud-fetch)
- [TLS](#tls)
- [Proxy](#proxy)
- [Data types](#data-types)
- [Telemetry](#telemetry)
- [Examples](#examples)
- [Develop](#develop)

## Quick start

```go
import (
  "context"
  "database/sql"
  _ "github.com/databricks/databricks-sql-go"
)

db, err := sql.Open("databricks", "token:********@********.databricks.com:443/sql/1.0/endpoints/********")
if err != nil {
  panic(err)
}
defer db.Close()

rows, err := db.QueryContext(context.Background(), "SELECT 1")
defer rows.Close()
```

See [`doc.go`](./doc.go) for full package documentation or the Databricks documentation
for the [SQL Driver for Go](https://docs.databricks.com/dev-tools/go-sql-driver.html).

> **Using the driver in your own project?** You never clone this repository — you
> add it with `go get github.com/databricks/databricks-sql-go` and `go build`.
> `go get` fetches per-version module archives, not git history, and for a
> default Thrift build it pulls **no** kernel binaries at all. The guidance below
> is only for people who `git clone` this repo directly (contributors / CI).

## Cloning the repository

This driver repo itself is **small**: for the SEA/kernel backend it commits only
the platform-independent C header
(`internal/backend/kernel/include/databricks_kernel.h`). The **prebuilt kernel
binaries** (per-platform `libdatabricks_sql_kernel.a`, ~60–95 MB each) live in a
**separate** repository,
[`databricks-sql-kernel-bindings`](https://github.com/databricks/databricks-sql-kernel-bindings),
one nested Go module per platform. This driver `require`s those modules, so the
SEA/kernel backend works straight from `go get` with **no build step** (see
[SEA/kernel](#seakernel--cgo--a-linked-rust-static-library)). A consumer's
`go get` pulls only the **target platform's** archive at the driver-pinned
version — never all platforms.

A plain `git clone` of *this* repo is therefore cheap. The partial/sparse-clone
guidance matters instead for the **bindings** repo, whose committed archives
(which git cannot delta-compress) accumulate across releases:

```bash
# Cheap history + only your platform's archive materialized:
git clone --filter=blob:none --sparse https://github.com/databricks/databricks-sql-kernel-bindings
cd databricks-sql-kernel-bindings
git sparse-checkout set --no-cone '/*' '!/lib' \
    'lib/darwin_arm64'   # keep only your platform
```

`--filter=blob:none` fetches commits and trees immediately and pulls file blobs
lazily, keeping `.git` small; GitHub serves it by default. CI checkouts of the
bindings repo use `--filter=blob:none` for the same reason.

## Choosing a backend (Thrift vs SEA/kernel)

The driver has **two execution backends**, selected once per connection:

| Backend | Transport | Default? | Build requirement |
|---|---|---|---|
| **Thrift / HiveServer2** | Thrift RPC over HTTP | ✅ yes | pure Go, `CGO_ENABLED=0`, cross-compilable |
| **SEA / kernel** *(experimental)* | Statement Execution API (REST) via the Rust [`databricks-sql-kernel`](https://github.com/databricks/databricks-sql-kernel), over a cgo C ABI | no (opt-in) | `-tags databricks_kernel` **and** `CGO_ENABLED=1`; links the kernel static library |

Thrift is the default and needs no special setup. Select the SEA/kernel backend per
connection either way:

- **Connector option:** `dbsql.WithUseKernel(true)`
- **DSN parameter:** `useKernel=true`

If the binary was **not** built with the `databricks_kernel` tag, selecting the kernel
backend returns an error wrapping `dbsqlerr.ErrKernelNotCompiled` at connect — it never
silently falls back to Thrift.

**Parameter parity.** Parameters are intended to behave identically on both backends.
Where a backend can't honor an option it is **rejected** at connect or execute (wrapping
`dbsqlerr.ErrNotSupportedByKernel`), not silently ignored. The
[Connection properties](#connection-properties) **Protocol** column records, per
parameter, whether it applies to **Both**, **Thrift-only**, or **SEA-only**.

## Building

The two backends differ **at build time**, not just at connect.

### Thrift (default) — pure Go, no extra step

Pure Go, `CGO_ENABLED=0`, `go get`-able, cross-compilable to any `GOOS`/`GOARCH`. No C,
no Rust, no linked native library.

```bash
go build ./...
go test  ./...

# Repo Makefile equivalents (both CGO_ENABLED=0):
make build      # multi-arch pure-Go binaries (linux + darwin)
make test       # pure-Go unit tests
```

Cross-compiling is free, e.g. `GOOS=windows GOARCH=amd64 go build ./...`.

### SEA/kernel — cgo + a linked Rust static library

The kernel backend compiles in **only** under the `databricks_kernel` build tag with
`CGO_ENABLED=1`, and links the Rust kernel's C ABI as a static library
(`libdatabricks_sql_kernel.a`). That archive is **not committed** — build it first.

**Prerequisites for a source build:**

- A C toolchain (cgo) and `CGO_ENABLED=1`.
- A Rust toolchain (`cargo`), pinned to the channel in
  [`rust-toolchain.toml`](./rust-toolchain.toml) so the archive is reproducible.
- Network access to clone the kernel repo at the pinned revision (the
  [`KERNEL_REV`](./KERNEL_REV) file).

```bash
# 1. Build the pinned kernel static lib + C header into the cgo link dir.
#    Clones databricks-sql-kernel @ KERNEL_REV and cargo-builds a self-contained
#    archive with pure-Rust TLS.
make kernel-lib

# 2. Build the driver with the kernel backend linked (implies step 1).
make build-kernel        # == CGO_ENABLED=1 go build -tags databricks_kernel ./...

# Run the kernel-tagged unit tests (no warehouse needed; step 1 implied):
make test-kernel         # == CGO_ENABLED=1 go test -tags databricks_kernel ./...
```

Once the archive exists you can invoke `go` directly, but you must carry **both**
`CGO_ENABLED=1` and `-tags databricks_kernel` — dropping either produces a pure-Go
binary where `WithUseKernel(true)` fails at connect:

```bash
CGO_ENABLED=1 go build -tags databricks_kernel ./...
```

**Cross-compiling.** The source build is host-native only (`cargo` emits a host-native
`.a`, and `make kernel-lib` rejects a cross-build). For a non-host target, either build
on a native per-OS runner, or stage a prebuilt archive and skip the clone + cargo
entirely (no Rust toolchain needed):

```bash
make kernel-lib KERNEL_LOCAL_A=/path/to/libdatabricks_sql_kernel.a \
                KERNEL_LOCAL_HEADER=/path/to/databricks_kernel.h   # header optional
```

### Build differences at a glance

| | Thrift (default) | SEA/kernel |
|---|---|---|
| Build tag | none | `-tags databricks_kernel` |
| cgo | `CGO_ENABLED=0` | `CGO_ENABLED=1` |
| Native lib | none | links `libdatabricks_sql_kernel.a` (not committed) |
| Extra toolchain | none | Rust (`cargo`) + C toolchain |
| Prep step | none | `make kernel-lib` (or stage `.a` via `KERNEL_LOCAL_A`) |
| One-shot build | `go build ./...` | `make build-kernel` |
| Cross-compile | free (any `GOOS`/`GOARCH`) | host-native only; per-OS runner or staged `.a` |

## Connecting

### DSN (Data Source Name)

```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?param=value&param=value
```

The `token:[your token]@` prefix authenticates with a personal access token (PAT). For
other authentication types, omit the prefix and use the `authType`,
`clientID`/`clientSecret`, or `accessToken` parameters — see
[Authentication](#authentication).

```go
db, err := sql.Open("databricks",
  "token:<pat>@<host>:443/sql/1.0/warehouses/<id>?timeout=1000&maxRows=1000")
```

To use the SEA/kernel backend, append `useKernel=true` (and, optionally,
`warehouseId=<id>`):

```
token:<pat>@<host>:443/sql/1.0/warehouses/<id>?useKernel=true
```

### Connector object

You can also connect with a connector object built from functional options:

```go
import (
  "database/sql"
  dbsql "github.com/databricks/databricks-sql-go"
)

connector, err := dbsql.NewConnector(
  dbsql.WithServerHostname(<host>),
  dbsql.WithPort(<port>),
  dbsql.WithHTTPPath(<http path>),
  dbsql.WithAccessToken(<your token>),
  // dbsql.WithUseKernel(true), // opt into the SEA/kernel backend
)
if err != nil {
  log.Fatal(err)
}
db := sql.OpenDB(connector)
defer db.Close()
```

See [`doc.go`](./doc.go) or [`connector.go`](./connector.go) for the full set of
functional options.

## Connection properties

See [CONNECTION_PARAMETERS.md](CONNECTION_PARAMETERS.md) for every connection,
session, and per-statement parameter the driver accepts, and whether each one
applies to the Thrift backend (default), the SEA/kernel backend, or both. The
sections below cover the same parameters inline.

Optional DSN parameters are appended as `?param=value&param=value`; the equivalent
connector options are listed alongside. The **Protocol** column shows applicability:

- **Both** — honored identically on Thrift and SEA/kernel.
- **Thrift only** — honored on Thrift; **rejected** at connect on the kernel path
  (wraps `ErrNotSupportedByKernel`) unless noted "inert" (accepted, no effect).
- **SEA only** — kernel path only. The experimental `WithKernel*` options are
  **rejected** on Thrift when set without `WithUseKernel(true)` (wraps
  `ErrRequiresKernelBackend`); `warehouseId` is the exception — Thrift silently ignores
  it (see its row).

Any parameter not recognized below (e.g. `ansi_mode`, `timezone`) is passed through as a
session parameter on both backends.

### Endpoint & routing

| DSN parameter | Connector option | Protocol | Default | Description |
|---|---|---|---|---|
| *(host)* | `WithServerHostname` | Both | *(required)* | Workspace hostname. |
| *(path)* | `WithHTTPPath` | Both | *(required)* | Warehouse/endpoint HTTP path. |
| *(port)* | `WithPort` | Thrift only | `443` | Kernel connects on **443 only** and rejects any other port. |
| `warehouseId` | `WithWarehouseID` | SEA only | | Bare warehouse id; the kernel routes by it (preferred over the HTTP path). **The Thrift backend ignores it.** |
| `catalog` | `WithInitialNamespace` | Both | | Initial catalog. Kernel applies it post-connect via `USE CATALOG`. |
| `schema` | `WithInitialNamespace` | Both | | Initial schema. Kernel applies it post-connect via `USE SCHEMA`. |
| `useKernel` | `WithUseKernel` | Both | `false` | Select the SEA/kernel backend. Requires a `databricks_kernel` build. |

### Query execution

| DSN parameter | Connector option | Protocol | Default | Description |
|---|---|---|---|---|
| `maxRows` | `WithMaxRows` | Thrift only (inert on kernel) | `100000` | Max rows per fetch. On the kernel path the kernel manages paging, so this is accepted but has no effect. |
| `timeout` | `WithTimeout` | Thrift only | no timeout | Server-side query timeout, in seconds. On the kernel path use the `STATEMENT_TIMEOUT` session parameter instead. |
| `userAgentEntry` | `WithUserAgentEntry` | Both | | Identifies your application (partners/ISVs), format `<isv-name+product-name>`. |
| *(session param)* | `WithSessionParams` | Both | | Arbitrary session confs (e.g. `ansi_mode`, `STATEMENT_TIMEOUT`, `QUERY_TAGS`). |
| *(via session param)* | `WithQueryTags` | Both | | Session-level query tags (serialized into `QUERY_TAGS`). |
| `timezone` | `WithSessionParams(timezone=…)` | Both | | Session time zone (e.g. `America/Los_Angeles`). |
| `enableMetricViewMetadata` | `WithEnableMetricViewMetadata` | Both | `false` | Enables metric-view metadata (`spark.sql.thriftserver.metadata.metricview.enabled=true`). |

### Retry / backoff

| Connector option | Protocol | Default | Description |
|---|---|---|---|
| `WithRetries(retryMax, waitMin, waitMax)` | Both | `4`, `1s`, `30s` | Retry attempts and exponential-backoff bounds. `retryMax < 0` disables retries. |
| `WithKernelRetryOverallTimeout(d)` | SEA only | kernel default (900s) | Cumulative retry budget across all attempts. No Thrift equivalent. |

### Result rendering

| DSN parameter | Connector option | Protocol | Default | Description |
|---|---|---|---|---|
| `useArrowNativeDecimal` | `WithArrowNativeDecimal` | Thrift only (inert on kernel) | `false` | Thrift: return DECIMAL as native Arrow `decimal128` (lossless string when scanned via `database/sql`). The kernel path already renders DECIMAL as the exact string regardless. |
| | `WithKernelDecimalAsFloat(b)` | SEA only | `false` | Scan top-level DECIMAL as lossy `float64` instead of the exact string. |

See [Cloud Fetch](#cloud-fetch), [TLS](#tls), and [Proxy](#proxy) for the remaining
groups. Telemetry parameters are covered under [Telemetry](#telemetry).

## Authentication

| Method | DSN | Connector option | Protocol |
|---|---|---|---|
| Personal access token (PAT) | `token:<t>@…`, or `accessToken=` / `authType=Pat` | `WithAccessToken` | Both |
| OAuth machine-to-machine (M2M) | `clientID=`+`clientSecret=` / `authType=OauthM2M` | `WithClientCredentials` | Both |
| OAuth user-to-machine (U2M) | `authType=OauthU2M` | `WithAuthenticator` (u2m) | Both |
| Custom / external / static token provider | — | `WithTokenProvider`, `WithExternalToken`, `WithStaticToken` | Thrift only |
| Federated token provider | — | `WithFederatedTokenProvider*` | Both |

**PAT** (default): supply `token:<pat>@…` in the DSN, or `WithAccessToken`.

**OAuth M2M**: leave the `token:...@` prefix off and pass the service principal's
`clientID` and `clientSecret`:

```
[host]:443[path]?authType=OauthM2M&clientID=<id>&clientSecret=<secret>
```

`authType=OauthM2M` is optional — supplying `clientID` + `clientSecret` selects M2M.

**OAuth U2M** (interactive browser login):

```
[host]:443[path]?authType=OauthU2M
```

Notes for the SEA/kernel backend:

- The kernel snapshots one `WithFederatedTokenProvider*` token during setup;
  `AndClientID` also forwards the SP-wide client ID. Expired tokens require a new connection.
- Custom OAuth **M2M scopes** are rejected on the kernel path (the kernel applies its
  own default scopes). Default scopes work on both.
- **U2M** is interactive: on a cache miss, connecting launches the browser and a
  connect-context **deadline is not honored** during the login window. U2M scopes are at
  parity with Thrift. Use PAT or M2M for headless/deadline-bound connects.
- Custom token-provider / external / static authenticators are **Thrift
  only**.
- OAuth token caching/refresh is owned by the kernel on the kernel path (no driver
  config).

## Cloud Fetch

Cloud Fetch increases performance of extracting large results by fetching data in
parallel via cloud storage
([more info](https://www.databricks.com/blog/2021/08/11/how-we-achieved-high-bandwidth-connectivity-with-bi-tools.html)).

| DSN parameter | Connector option | Protocol | Default | Description |
|---|---|---|---|---|
| `useCloudFetch` | `WithCloudFetch` | Thrift only (inert on kernel) | `true` | Enable Cloud Fetch. On the kernel path Cloud Fetch is always managed internally, so the flag is inert. |
| `maxDownloadThreads` | `WithMaxDownloadThreads` | Thrift only (inert on kernel) | `10` | Concurrent download goroutines (Thrift). Inert on the kernel path. |
| | `WithKernelMaxChunksInMemory(n)` | SEA only | kernel default (16) | Bounds decompressed Cloud Fetch chunks held in memory — trades large-result throughput for peak memory. |

On the Thrift backend:

```
token:<pat>@<host>:443[path]?useCloudFetch=true&maxDownloadThreads=3
# or disable it entirely:
token:<pat>@<host>:443[path]?useCloudFetch=false
```

## TLS

| Connector option | Protocol | Description |
|---|---|---|
| `WithSkipTLSHostVerify()` | Both | Disable TLS chain + hostname verification. **Use only for internal private-link hostnames** — this is susceptible to machine-in-the-middle attacks. |
| `WithTransport(http.RoundTripper)` | Thrift only | Supply a custom HTTP transport (e.g. a custom CA, mTLS, or proxy). **Rejected** on the kernel path (wraps `ErrNotSupportedByKernel`) — the kernel uses its own HTTP stack; use `WithKernelTrustedCerts` / `WithKernelProxy` there. |
| `WithKernelTrustedCerts(pem)` | SEA only | Add a PEM CA bundle on top of the system roots (for a re-signing proxy / on-prem CA). Needed because the kernel's TLS stack does not read `SSL_CERT_FILE`. |
| `WithKernelClientCertificate(certPEM, keyPEM)` | SEA only | Configure a paired mTLS client certificate and unencrypted private key. Both must be non-empty; PKCS#8 keys are recommended. |
| `WithKernelSkipHostnameVerify()` | SEA only | Skip **only** the hostname check while keeping chain validation (finer-grained than `WithSkipTLSHostVerify`). |

## Proxy

The Thrift and kernel backends both honor the standard `HTTP_PROXY` / `HTTPS_PROXY` /
`NO_PROXY` environment variables.

| Connector option | Protocol | Description |
|---|---|---|
| *(environment)* | Both | `HTTP(S)_PROXY` / `NO_PROXY`. |
| `WithKernelProxy(KernelProxy{URL, Username, Password, BypassHosts})` | SEA only | Explicit proxy with out-of-band basic-auth credentials and a structured bypass list — the "advanced" fields the env-var path can't express. Overrides the environment proxy; a malformed URL is rejected at connect. |

## Data types

Results render **byte-for-byte identically** on both backends. Scalars, DECIMAL (exact
string), TIMESTAMP / TIMESTAMP_NTZ (shifted into the session time zone), INTERVAL, nested
ARRAY / MAP / STRUCT and VARIANT (as JSON), and GEOMETRY / GEOGRAPHY (as WKT) are all
supported. BINARY is returned as `sql.RawBytes`.

Metadata is reached through SQL (`SHOW`, `DESCRIBE`, `information_schema`) on both
backends — the driver exposes no `GetCatalogs`/`GetSchemas`/`GetTables`/`GetColumns` API
(a `database/sql` limitation, not backend-specific).

## Telemetry

The driver includes optional telemetry to help improve performance and reliability; it
applies to both backends. When `enableTelemetry` is left unset (the default), a
**server-side feature flag** decides whether telemetry is active — so it may be enabled
without an explicit opt-in. Setting `enableTelemetry` explicitly overrides the flag.
(One exception: on the kernel backend with OAuth **U2M**, telemetry is skipped entirely
— regardless of `enableTelemetry` — to avoid a second interactive browser flow at
connect.)

```
# force on (regardless of the server flag):
token:<pat>@<host>:443[path]?enableTelemetry=true
# force off:
token:<pat>@<host>:443[path]?enableTelemetry=false
```

| DSN parameter | Default | Description |
|---|---|---|
| `enableTelemetry` | unset (server flag decides) | Force telemetry on/off, overriding the server feature flag. |
| `telemetry_batch_size` | `200` | Events per batch. |
| `telemetry_flush_interval` | `30s` | Flush interval. |
| `telemetry_retry_count` | — | **Deprecated and ignored** (retries are owned by the HTTP client + circuit breaker); logs a one-time warning. |
| `telemetry_retry_delay` | — | **Deprecated and ignored** (see above). |

**Collected:** query latency/performance, error codes (not messages), feature usage,
driver version/environment. **Not collected:** SQL text, query results/values,
table/column names, user identities or credentials. Telemetry has < 1% overhead and is
protected by a circuit breaker. The kernel path additionally emits a connection-config
telemetry event at connect (mode, auth mechanism/flow, proxy, arrow, query tags,
metric-view); the Thrift path's telemetry is unchanged. See
[`telemetry/DESIGN.md`](./telemetry/DESIGN.md).

## Examples

Runnable examples live in [`examples/`](./examples). Notable ones:

- [`examples/workflow`](./examples/workflow) — end-to-end connector setup, logging, query.
- [`examples/oauth`](./examples/oauth) — OAuth U2M and M2M.
- [`examples/parameters`](./examples/parameters) — bound query parameters.
- [`examples/cloudfetch`](./examples/cloudfetch) — large results via Cloud Fetch.
- [`examples/kernel`](./examples/kernel) — the SEA/kernel backend (requires a
  `-tags databricks_kernel`, `CGO_ENABLED=1` build; see [Building](#building)).

## Develop

### Lint

We use `golangci-lint`. In VS Code:

```json
{
  "go.lintTool": "golangci-lint",
  "go.lintFlags": ["--fast"]
}
```

### Unit tests

```bash
go test           # default (Thrift) backend, pure Go
make test-kernel  # kernel-tagged unit tests (requires make kernel-lib)
```

## Issues

If you find any issues, feel free to create an issue or send a pull request directly.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache 2.0](https://github.com/databricks/databricks-sql-go/blob/main/LICENSE)
