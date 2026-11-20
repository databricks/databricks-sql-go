# Design: One Network Policy per Connection

**Status:** Draft for review
**Scope:** Thrift backend. The kernel backend already follows this policy.
**Related:** #413

## Problem

The connector's network settings (`WithTransport`, `WithSkipTLSHostVerify`, environment proxies and
`WithUserAgentEntry`) reach only some outgoing requests, because each request path builds its own HTTP
client.

Thrift backend at `c1f2469`:

| Request | Client | `WithTransport` | `WithSkipTLSHostVerify` | Connector UA |
|---|---|---|---|---|
| Thrift SQL, feature flags, telemetry | `client.RetryableClient(cfg)` (`connector.go:188`) | yes | yes¹ | yes |
| CloudFetch downloads | `cfg.HTTPClient`, else `http.DefaultClient` (`internal/rows/arrowbased/batchloader.go:62`) | yes | no | no |
| Volume staging | `&http.Client{}` (`connection.go:692`) | no | no | no |
| OAuth M2M | copy of `http.DefaultClient` (`auth/oauth/m2m/m2m.go:106`, `auth/oauth/oauth.go:62`) | no | no | no |
| OAuth U2M | oauth2's default client (`auth/oauth/u2m/authenticator.go:58,63,189`) | no | no | no |
| Token federation | `&http.Client{Timeout: 30s}` (`auth/tokenprovider/exchange.go:33,44`) | no | no | no |

¹ Only without `WithTransport`. A custom transport owns its TLS settings.

The kernel backend sends everything through one HTTP client and applies `WithSkipTLSHostVerify` to all
of it (`kernel_config.go:204-205`).

Effects:

- **Unattributed token requests.** M2M token requests reach the workspace as `Go-http-client/2.0`, so
  partner integrations can't be attributed in audit logs (#413).
- **OAuth fails where the network options are used.** The OAuth token endpoint is on the workspace host.
  A host may need `WithSkipTLSHostVerify` for SQL (private-link hostnames not on the workspace
  certificate, #223) or a custom CA through `WithTransport` (#270). OAuth on that host needs the same,
  so M2M and U2M fail on Thrift exactly where those options are used.
- **Volume staging ignores `WithTransport`.**

Root causes:

1. Network settings are an opaque `http.RoundTripper` plus one TLS flag, and each call site reads them
   ad hoc.
2. Authenticators are built while options are being applied (`WithClientCredentials`,
   `connector.go:589-596`), before the network settings are known. `auth.Authenticator` has no way to
   receive an HTTP client.

## Goals

- Every request a Thrift connector sends uses the same transport, TLS and proxy settings.
- OAuth and storage requests carry the connector User-Agent.
- `WithSkipTLSHostVerify` means the same thing on both backends.
- No process-wide client carries per-connection settings.

## Non-goals

- New public options.
- Kernel changes.
- Per-path proxy overrides.

## Prior art

Compared at kernel
[`8877579`](https://github.com/databricks/databricks-sql-kernel/tree/887757940d0fd1b63cc0a59cd58c24285f8aaa85)
and JDBC [`840f3a4`](https://github.com/databricks/databricks-jdbc/tree/840f3a407be1dc002c0ddf852143727f1f3b0157)
with Java SDK `v0.118.0`:

| | Kernel | JDBC | Go Thrift today |
|---|---|---|---|
| HTTP stacks | 1 (`src/client/http.rs:514`) | 4 | one per path |
| SQL and OAuth share TLS settings, including relaxation | yes | yes (`ConfiguratorUtils.getBaseConnectionManager`) | no |
| Custom proxy or transport reaches OAuth | yes | yes² | no |
| Connector UA on token request | yes | yes | no |
| Connector UA on discovery | yes | no | no |
| Connector UA on storage | yes | yes, except volume uploads | no |
| OAuth discovery runs at | session open | first `authenticate()` | first request (M2M), `NewConnector` (U2M) |
| Paths that ignore all settings | none | volume uploads (`GlobalAsyncHttpClient`) | staging, M2M, U2M, federation |

² `UseProxy` and `UseSystemProxy` apply to OAuth. The CloudFetch proxy (`UseCFProxy`) does not.

Neither driver keeps OAuth verified when the user relaxes TLS. Both log a general warning instead
(`src/client/http.rs:539-551`, `ConfiguratorUtils.java:104-105`).

This design avoids two kernel behaviors:

- `x-databricks-org-id` is also sent to presigned storage URLs (`src/client/http.rs:917`).
- Telemetry clients are process-wide and keyed by host, so later sessions to a host export through the
  first session's client (`src/telemetry/manager.rs:24-69`).

## Design

### One base transport per connector

```go
base := cfg.Transport           // WithTransport: the caller owns TLS, proxy and dialing
if base == nil {
	base = PooledTransport(cfg) // env proxy, system roots, InsecureSkipVerify when set
}
```

The connector builds `base` once and sends every request through it, wrapped according to the request's
class:

| Class | Requests | Wrapping |
|---|---|---|
| Workspace | Thrift SQL, feature flags, telemetry | Auth transport, retries, connector UA (unchanged) |
| Auth | M2M, U2M and federation discovery and token requests | Connector UA and redirect policy. No auth transport |
| Storage | CloudFetch downloads, volume staging | Connector UA. No auth or connection headers |

### Binding auth clients

Authenticators exist before `base` does, so `PooledClient` binds them through one internal interface:

```go
type HTTPClientBinder interface {
	WithHTTPClient(*http.Client) auth.Authenticator
}
```

- Built-in authenticators implement the interface and assert it at compile time. Other authenticators
  are left unchanged.
- The returned copy shares the original's token state. Connectors that share an authenticator therefore
  keep one cache and one in-flight fetch, and a refresh goes through whichever connector triggers it.
- U2M discovery moves from `NewAuthenticator` to first use, as M2M discovery already works.

### Auth requests

- **Timeouts:** unchanged. 10s for host config and discovery, and 60s per token request.
- **Redirects:** `CheckRedirect` refuses redirects that change host or downgrade to `http`. oauth2 can
  resend `client_secret` in the request body, and Go replays the body on 307/308.
- **Reentrancy:** auth requests carry a context marker, and `Authenticate` returns an error for a marked
  request instead of waiting on the token lock. A custom transport that calls the same authenticator
  then fails instead of deadlocking.

### Process-wide state

No process-wide client may carry per-connection settings. Any cache that outlives a connector is keyed by
everything that affects its result.

## TLS relaxation and the client secret

With `WithSkipTLSHostVerify`, a machine-in-the-middle can read the long-lived client secret, not just a
short-lived access token.

| Option | Effect |
|---|---|
| A. Relaxation applies to OAuth | Matches the kernel and JDBC. M2M works on private-link hosts |
| B. OAuth always verified on both backends | Protects the secret. M2M can't work on hosts that need relaxation, and the kernel needs a change |
| C. OAuth always verified on Thrift only | The backends disagree, and Thrift has no way to customize OAuth networking |

**Decision: A.** Log a warning once per connector when TLS relaxation is on, as the kernel and JDBC do.

A hostname-only option such as the kernel's `WithKernelSkipHostnameVerify` would be a safer tool for
private-link hosts on Thrift. That is a separate proposal.

## Compatibility

These are behavior changes for the v2.0.0 changelog. The kernel backend is unchanged.

- M2M, U2M, token federation and volume staging no longer use `http.DefaultClient` or
  `http.DefaultTransport`. Changes an application makes to those globals stop reaching these requests.
  Use `WithTransport` instead.
- `WithTransport` now reaches OAuth and volume staging.
- Without `WithTransport`, `WithSkipTLSHostVerify` now also reaches OAuth, CloudFetch and staging.
- OAuth and storage requests send the connector User-Agent.
- U2M discovery errors surface at the first query or `Ping` instead of at `sql.Open`.

## Implementation plan

| Phase | Production (+/−) | Tests | Change |
|---|---|---|---|
| 1. M2M | ~+110 / −40 | ~+300 | Auth class, binder interface, shared token state, reentrancy guard, redirect policy |
| 2. Connector-owned base transport | ~+40 / −20 | ~+60 | Build `base` once and share it |
| 3. Storage | ~+25 / −8 | ~+100 | Staging and CloudFetch on `base` |
| 4. U2M | ~+70 / −30 | ~+120 | Lazy discovery. Bound client for discovery, code exchange and refresh |
| 5. Token federation | ~+40 / −8 | ~+80 | Bind `FederationProvider.httpClient` and keep its 30s cap |
| Docs | ~+30 / −15 | | README, CHANGELOG, godoc |
| **Total** | **~+315 / −120** | **~+660** | |

Phase 1 can use the transport `PooledClient` already builds, so it can land before phase 2.

Tests send one request per class to an `httptest` TLS server and check that:

- `WithTransport` is used;
- a self-signed server is rejected by default and accepted with `WithSkipTLSHostVerify`;
- auth requests carry the connector User-Agent;
- storage requests carry no `Authorization`.

No test mutates a process-wide transport.

## Alternatives considered

- **User-Agent only, on `http.DefaultTransport`.** About 40 production lines. It fixes #413 but leaves
  OAuth failing where `WithTransport` or `WithSkipTLSHostVerify` are needed.
- **Backend-neutral network options** (`WithProxy`, `WithTrustedCerts`, `WithClientCertificate`,
  `WithDialer`) mapped to both backends. This is a superset of this design and would also replace the
  `WithKernel*` options. It needs public API review and a dialer bridge into the kernel's HTTP stack.
  Revisit once this design lands.
