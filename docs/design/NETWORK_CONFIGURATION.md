# Design: One Network Policy per Connection

**Status:** Draft for review
**Scope:** Thrift backend. The kernel backend already follows this policy.
**Related:** #413, #498, #562, #374, #557, #225, #223, #270, #307, #308, #129

## Problem

The connector's network settings are `WithTransport`, `WithSkipTLSHostVerify`, environment proxies and
`WithUserAgentEntry`. They reach only some of the driver's outgoing requests. Each request path builds
its own HTTP client, so each path ends up with its own policy.

Requests the Thrift backend sends at `c1f2469`:

| Request | Client | `WithTransport` | `WithSkipTLSHostVerify` | Connector User-Agent |
|---|---|---|---|---|
| Thrift SQL, feature flags, telemetry | `client.RetryableClient(cfg)` (`connector.go:188`, `connector.go:124-135`) | yes | yes, without `WithTransport` | yes (`internal/client/client.go:343`, `connector.go:132`) |
| CloudFetch downloads | `cfg.HTTPClient`, else `http.DefaultClient` (`internal/rows/arrowbased/batchloader.go:62`) | yes | no | no |
| Volume staging PUT/GET/REMOVE | `&http.Client{}` (`connection.go:692`) | no | no | no |
| OAuth M2M discovery and token | copy of `http.DefaultClient` (`auth/oauth/m2m/m2m.go:106`, `auth/oauth/oauth.go:62`) | no | no | no |
| OAuth U2M discovery, code exchange, refresh | oauth2's default client (`auth/oauth/u2m/authenticator.go:58,63,189`) | no | no | no |
| Token federation exchange | `&http.Client{Timeout: 30s}` (`auth/tokenprovider/exchange.go:33,44`) | no | no | no |

The kernel backend sends all of its traffic through one Rust HTTP client. It rejects `WithTransport`
(`kernel_config.go:95-97`) and applies `WithSkipTLSHostVerify` to all kernel traffic
(`kernel_config.go:204-205`).

Consequences:

- **#413.** M2M token requests reach the workspace as `Go-http-client/2.0`, so partner integrations
  can't be attributed in audit logs.
- **OAuth fails where the network options are needed.** The token endpoint is on the workspace host
  (`https://<host>/oidc/v1/token`, or `/oidc/oauth2/v2.0/token` on Azure). A host that needs
  `WithSkipTLSHostVerify`, a custom CA or a custom dialer for SQL needs the same for OAuth. M2M and U2M
  therefore fail on Thrift in exactly the environments those options exist for:
  - `WithSkipTLSHostVerify` was added for private-link hostnames that are not on the workspace
    certificate (#223, #225). M2M, the usual auth for services, can't verify the same hostname.
  - A custom CA for a VPN or TLS-inspecting proxy (#270) goes through `WithTransport`, which OAuth
    ignores. Integrations already pass their own transport, for example the ADBC Databricks driver
    (`go/database.go` in `adbc-drivers/databricks`).
- **Volume staging ignores `WithTransport` entirely.**
- **Every new request path makes the choice again.** #307/#308 added a configurable CloudFetch client,
  and #562 proposed a fourth policy: a process-wide, always-verified OAuth transport.

History: #374 documented the OAuth gap as a "pre-existing limitation, tracked separately" in
`auth/oauth/oauth.go`. It was deferred because threading the transport changes the authenticator
constructors, and no tracking issue was filed. #498, from the #413 reporter, implemented the M2M part
of this design (base transport plus User-Agent) and verified it in `system.access.audit`. It was
closed unmerged.

Root causes:

1. Network settings are an opaque `http.RoundTripper` plus one TLS flag in `config.Config`, and call
   sites read them ad hoc.
2. Authenticators are built while options are applied, before the connector's network settings are
   known. For example, `WithClientCredentials` calls `m2m.NewAuthenticator(..., c.Host)`
   (`connector.go:589-596`). `auth.Authenticator` has no way to receive an HTTP client.

## Goals

- Every request a Thrift connector sends uses the same transport, TLS and proxy settings.
- OAuth discovery and token requests carry the connector User-Agent (#413).
- `WithSkipTLSHostVerify` and the User-Agent mean the same thing on both backends.
- No process-wide client carries per-connection settings.

## Non-goals

- New public options. The design uses `WithTransport`, `WithSkipTLSHostVerify` and environment proxies
  as they exist.
- Kernel changes. Kernel follow-ups are listed under open questions.
- Per-path proxy overrides, such as JDBC's `UseCFProxy`.
- A backend-neutral declarative network config (see Alternatives).

## Cross-driver survey

Pinned commits:
- kernel [`8877579`](https://github.com/databricks/databricks-sql-kernel/tree/887757940d0fd1b63cc0a59cd58c24285f8aaa85)
- JDBC [`840f3a4`](https://github.com/databricks/databricks-jdbc/tree/840f3a407be1dc002c0ddf852143727f1f3b0157)
- Java SDK `v0.118.0`, which JDBC uses for OAuth

### Kernel: one client builder

Every production `reqwest` client comes from `DatabricksHttpClient::build_reqwest_client`
(`src/client/http.rs:514`). It sets:

- the User-Agent (`:524`)
- TLS relaxations (`:543`, `:551`)
- extra roots (`:583`, `:611`)
- the mTLS identity (`:626`)
- the proxy (`:685`)

SQL, CloudFetch, OIDC discovery, M2M, federation, U2M token calls, telemetry and feature flags all use
that client. Requests differ only in what is added per request:

- `execute_without_auth` omits `Authorization` for storage and IdP calls (`http.rs:776`, `:901`).
- U2M calls the raw client (`src/auth/oauth/u2m.rs:408,538`), so it skips retries and custom headers.

This design does not copy two kernel behaviors:

- **`x-databricks-org-id` on storage requests.** The header is added outside the `with_auth` block
  (`http.rs:917`), so it is also sent to presigned storage URLs.
- **Process-wide telemetry clients.** They are keyed by host (`src/telemetry/manager.rs:24-69`), so later
  sessions to a host export through the first session's client, proxy and credentials.

### JDBC: four HTTP stacks

| Stack | Used by | Settings |
|---|---|---|
| Java SDK `CommonsHttpClient` (`ClientConfigurator.java:59-63`) | SEA, OAuth discovery and token, U2M | Proxy from `UseProxy`/`UseSystemProxy` (`:156-168`). TLS from `ConfiguratorUtils.getBaseConnectionManager` (`:129-132`) |
| JDBC Apache client (`DatabricksHttpClient`) | Thrift, CloudFetch, volume data, telemetry, feature flags, token federation | `UseCFProxy` first, then `UseProxy` (`DatabricksHttpClient.java:213-220`). Same TLS setup (`:134`). Connector UA (`:175`) |
| `GlobalAsyncHttpClient` (process-wide) | Volume uploads (`DBFSVolumeClient.java:802,903`) | No proxy, no TLS settings, no UA (`GlobalAsyncHttpClient.java:85-100`) |
| External libraries | GCP credentials, U2M callback server | Outside the driver's settings |

SQL and OAuth share one TLS setup, so `AllowSelfSignedCerts` and the truststore apply to both.

The User-Agent is applied unevenly:

- **The M2M token request carries it.** The SDK wraps that request in an `ApiClient`
  (`TokenEndpointClient.retrieveToken`, `ApiClient.java:243-248`).
- **Host metadata and discovery don't.** The host-metadata GET always calls the HTTP client directly
  (`DatabricksConfig.java:816-817`). So does discovery whenever host metadata returns `oidc_endpoint`
  (`:891-892`). Both send Apache's default User-Agent.
- **The User-Agent registry is JVM-wide** (`UserAgentManager.registerOtherInfo`), so a token request
  carries every `UserAgentEntry` registered in the process.

### Summary

| | Kernel | JDBC | Go Thrift today |
|---|---|---|---|
| SQL and OAuth share TLS settings, including relaxation | yes | yes | no |
| Custom proxy or transport reaches OAuth | yes | mostly | no |
| Connector UA on the token request | yes | yes | no |
| Connector UA on discovery | yes | no | no |
| Paths that ignore all settings | none | volume uploads | staging, M2M, U2M, federation |

Both reference implementations apply one TLS policy to SQL and OAuth. Neither keeps OAuth verified when
the user relaxes TLS.

## Design

### One base transport per connector

`NewConnector` builds the base transport once:

```go
base := cfg.Transport           // WithTransport: the caller owns TLS, proxy and dialing
if base == nil {
	base = PooledTransport(cfg) // env proxy, system roots, InsecureSkipVerify when set
}
```

Every Thrift request goes through `base`, wrapped for its class:

| Class | Requests | Wrapping |
|---|---|---|
| Workspace | Thrift SQL, feature flags, telemetry | Auth transport, retries, connector UA (unchanged) |
| Auth | M2M, U2M and federation discovery and token requests | Connector UA. No auth transport. Redirect policy |
| Storage | CloudFetch downloads, volume staging | Connector UA. No auth, no connection headers |

The connector keeps `base` and the storage client on its config. Staging (`conn`) and the CloudFetch
iterator then stop building their own clients.

### Binding auth clients

Authenticators are created before `base` exists, so the connector binds them afterwards through one
internal interface:

```go
// internal/client
type HTTPClientBinder interface {
	WithHTTPClient(*http.Client) auth.Authenticator
}
```

- `PooledClient` binds any authenticator that implements the interface. Built-in authenticators assert
  it at compile time.
- `WithHTTPClient` returns a copy that shares the original's token state. Connectors that share an
  authenticator therefore keep one cache and one in-flight fetch.
- U2M discovery must go through the bound client, so it moves from `NewAuthenticator` to first use, as
  M2M already does. This matches the kernel, which discovers when the session opens
  (`src/session.rs:1406-1475`, `src/auth/oauth/u2m.rs:274`). It also matches the Java SDK, which
  configures credentials on the first `authenticate()` (`DatabricksConfig.java:261-283`).
- Authenticators the driver doesn't own are left as they are.

A shared authenticator may fetch or refresh through any of the connectors that share it, and the request
carries that connector's User-Agent. This is acceptable because the credentials and host are the same.

### Auth client rules

- **Timeouts stay as they are:** 10s for host config and discovery, and 60s per token request (#557).
- **`CheckRedirect` refuses redirects that change host or downgrade `https` to `http`.** oauth2's
  fallback retries with `client_secret` in the body, and Go replays the body on 307/308.
- **Reentrancy is detected rather than deadlocking.** A custom `WithTransport` that itself calls the same
  authenticator would deadlock, because the token lock is held during the fetch. Auth requests carry a
  context marker, and `Authenticate` returns an error for a marked request instead of taking the lock.

### Process-wide state

- No process-wide client may carry per-connection settings.
- Caches that outlive a connector are keyed by everything that affects their result.
- The only state shared across connectors is an authenticator the caller chose to share.

## Decision: TLS relaxation and the client secret

With `WithSkipTLSHostVerify`, a machine-in-the-middle on the workspace host can read the long-lived
client secret, not just an access token that expires in about an hour.

| Option | Thrift | Kernel | Trade-off |
|---|---|---|---|
| A. Relaxation applies to OAuth | OAuth uses `base` | Unchanged | Matches the kernel and JDBC. M2M works on hosts that need relaxation |
| B. OAuth always verified on both backends | OAuth uses a verified transport | Needs a kernel change, or reject M2M combined with skip-verify | Protects the secret. M2M can't work on hosts that need relaxation |
| C. Verified on Thrift only (#562) | Private verified transport | Unchanged | Backends disagree. No way to customize OAuth networking on Thrift |

**Recommendation: A.**

- **Both reference drivers apply relaxation to OAuth.** Each logs a general warning when relaxation is
  on, not one specific to OAuth:
  - kernel: "TLS certificate validation is disabled — this is insecure…" (`src/client/http.rs:539-551`)
  - JDBC: "Self-signed certificates are allowed… not recommended for production"
    (`ConfiguratorUtils.java:104-105`)

  The Thrift backend should log the same kind of warning once per connector.
- **Combining M2M with relaxation is the main use, not an edge case.** The option exists for private-link
  hostnames (#223, #225), and the token endpoint is on that hostname.
- **Safer follow-up:** a hostname-only option such as `WithKernelSkipHostnameVerify` would be a safer
  tool for private-link hosts on Thrift. That is a separate proposal.

## Compatibility

These are behavior changes for the v2.0.0 changelog:

- M2M, U2M, token federation and volume staging no longer use `http.DefaultClient` or
  `http.DefaultTransport`. Changes an application makes to those globals stop reaching these requests.
  Pass the transport with `WithTransport` instead.
- `WithTransport` now reaches OAuth and volume staging. CloudFetch already used it.
- Without `WithTransport`, `WithSkipTLSHostVerify` now also reaches OAuth, CloudFetch and staging. This
  matches the kernel backend.
- OAuth discovery, token and storage requests send the connector User-Agent.
- U2M discovery runs on first use instead of in `NewConnector` or DSN parsing. An unreachable host
  now fails at the first query or `Ping`, not at `sql.Open`. `database/sql` already documents that
  `Open` may not connect.
- Kernel backend: no change.

## Implementation plan

| Phase | Production (+/−) | Tests | Change |
|---|---|---|---|
| 1. M2M (replaces #562) | ~+110 / −40 | ~+300 | Auth class on `base`, binder interface, shared token state, reentrancy guard, redirect policy |
| 2. Connector-owned base transport | ~+40 / −20 | ~+60 | Build `base` once and share it across classes |
| 3. Storage | ~+25 / −8 | ~+100 | Staging and the CloudFetch fallback on `base` |
| 4. U2M | ~+70 / −30 | ~+120 | Lazy discovery. Bound client for discovery, code exchange and refresh |
| 5. Token federation | ~+40 / −8 | ~+80 | Bind `FederationProvider.httpClient` and keep its 30s cap |
| Docs | ~+30 / −15 | | README, CHANGELOG, godoc |
| **Total** | **~+315 / −120** | **~+660** | 5–6 PRs |

Phase 1 can land before phase 2 by using the transport `PooledClient` already builds. #498 already
implements most of phase 1. Build on it, or credit it if phase 1 is rewritten.

Each phase sends one request per class to an `httptest` TLS server and asserts that:

- the caller's `WithTransport` is used;
- a self-signed server is rejected by default and accepted with `WithSkipTLSHostVerify`;
- auth requests carry the connector User-Agent, and storage requests carry no `Authorization`.

No test mutates a process-wide transport.

## Alternatives considered

1. **Process-wide verified OAuth transport** (#562 at `3609c63`). It fixes #413, but adds a fourth
   policy, leaves no way to customize OAuth networking on Thrift, and disagrees with the kernel backend.
2. **User-Agent only, on `http.DefaultTransport`.** About 40 production lines and no networking change.
   It fixes #413 but leaves OAuth failing where `WithTransport` or `WithSkipTLSHostVerify` are needed.
   This is the fallback if the decision above stalls.
3. **Backend-neutral declarative network config:** `WithProxy`, `WithTrustedCerts`,
   `WithClientCertificate` and `WithDialer`, mapped to both backends. This is a superset of this design
   and would also unify the `WithKernel*` options. It needs public API review, and a dialer option needs
   a bridge into the kernel's HTTP stack. Revisit after this design lands.

## Resolved during drafting

1. **TLS relaxation reaches OAuth (option A).** The kernel and JDBC both do this, and private-link M2M
   needs it. See the decision section.
2. **Storage requests carry the connector User-Agent.**
   - The kernel sets it on every client (`src/client/http.rs:524`).
   - JDBC sends it on CloudFetch and volume GET/PUT/DELETE (`DatabricksHttpClient.java:175`), but not on
     volume uploads.
   - It shows up only in cloud storage access logs, so the stakes are low.
3. **U2M discovery moves to first use.** This matches the kernel and the Java SDK. Idiomatic
   `database/sql` code calls `Ping` rather than relying on `Open` to connect.

## Open questions

1. Kernel follow-ups: `x-databricks-org-id` on presigned storage URLs, and per-host telemetry clients
   bound to the first session.
2. Move `DESIGN_MULTI_STATEMENT_TRANSACTIONS.md` into `docs/design/`?
