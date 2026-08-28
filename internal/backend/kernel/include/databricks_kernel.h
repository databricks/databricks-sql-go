/*
 * Copyright (c) 2026 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Databricks SQL Kernel — C ABI (v0).
 *
 * Hand-written header for the kernel's custom C ABI (cbindgen
 * integration is a v0.1 concern). The bodies live in `src/c_abi/` in the
 * `databricks-sql-kernel` crate, compiled as a `cdylib` / `staticlib`.
 *
 * This is the surface ODBC / Go consumers link against. It is NOT ADBC:
 * the kernel exposes a Databricks-native session / statement / result /
 * metadata model. Result data crosses the boundary via the Arrow C Data
 * Interface (`FFI_ArrowArray` / `FFI_ArrowSchema`), one array per
 * `kernel_result_stream_next_batch` call (a pull model).
 *
 * ## Error reporting
 *
 * Every fallible entry point returns a `KernelStatusCode`. On any
 * non-`Success` return, call `kernel_get_last_error` to fetch the typed
 * detail for the current thread. The thread-local error buffer is reset
 * at the start of every kernel call, and the `char*` fields it hands
 * back are valid only until the next kernel call on the same thread —
 * copy them out before calling again.
 *
 * ## ABI versioning
 *
 * There is no ABI promise through v0.x: struct layouts and the function
 * set may change freely; ABI stability is committed at v1.0. All handle
 * types below are *opaque* (incomplete) to C — you only ever hold
 * pointers to them, never read their fields — so their Rust-side layout
 * is irrelevant. The one caller-*readable* struct, `KernelError`, is laid
 * out exactly as declared here; copy its fields out before the next
 * `kernel_*` call on the thread.
 *
 * ## Lifecycle & threading notes
 *
 * - **Re-execution invalidates prior handles.** `kernel_statement_execute`
 *   auto-cancels and closes any `kernel_executed_statement_t` previously
 *   returned by the same statement; calls on the stale handle then return
 *   `KernelStatusCode_InvalidStatementHandle`. Don't hold two live
 *   executed handles from one statement.
 * - **A result stream borrows its executed handle.** Closing the executed
 *   handle invalidates any stream obtained from it: subsequent stream
 *   calls return `InvalidStatementHandle` (a defined error, not UB).
 * - **Close is best-effort async.** `kernel_session_close` initiates the
 *   server-side delete before returning — a detached task runs it on the
 *   kernel's process-wide runtime — but does not wait for completion. A
 *   process that exits immediately after `kernel_session_close` may drop
 *   the detached task before it runs, leaving the server session to expire
 *   on its own idle timeout.
 * - **Close the session LAST.** Because close initiates the server-side
 *   delete, any handle still open against that session — a statement /
 *   executed handle, or a metadata result stream (which holds its own
 *   session reference and remains drivable) — must be drained and closed
 *   BEFORE `kernel_session_close`. Driving such a handle afterwards issues
 *   RPCs against a session whose delete has already been initiated and
 *   will surface server errors. Order teardown: result streams → executed
 *   handles → statements → session. (Statement cancellers are not in this
 *   chain — freeing one drives no RPC; see the next bullet.)
 * - **Free statement cancellers too.** A `kernel_statement_canceller_t`
 *   holds its own session reference, so a leaked canceller pins the
 *   client-side session (and its connection pool) past
 *   `kernel_session_close`. Freeing one drives no RPC, so its order is
 *   independent of the chain above: free it once its execute has returned,
 *   regardless of statement/session close.
 */

#ifndef DATABRICKS_KERNEL_H
#define DATABRICKS_KERNEL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ─── Arrow C Data Interface ──────────────────────────────────────────
 *
 * Mirrors the canonical `ArrowArray` / `ArrowSchema` from the Arrow C
 * Data Interface (https://arrow.apache.org/docs/format/CDataInterface.html).
 * The kernel populates these via the `arrow-array` crate's `ffi` module.
 * If the consuming project already declares these (e.g. via Arrow's
 * `abi.h`), define `DATABRICKS_KERNEL_NO_ARROW_CDATA` before including
 * this header to suppress the duplicate definitions.
 */
#ifndef DATABRICKS_KERNEL_NO_ARROW_CDATA

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

#endif /* DATABRICKS_KERNEL_NO_ARROW_CDATA */

/* ─── Status + error ──────────────────────────────────────────────────
 *
 * Discriminants match `crate::c_abi::KernelStatusCode`. Keep this enum in
 * lockstep with that Rust enum (the Rust side enforces a 1:1 mapping
 * against `ErrorCode` with an exhaustive match).
 */
typedef enum KernelStatusCode {
  KernelStatusCode_Success = 0,
  KernelStatusCode_InvalidArgument = 1,
  KernelStatusCode_Unauthenticated = 2,
  KernelStatusCode_PermissionDenied = 3,
  KernelStatusCode_NotFound = 4,
  KernelStatusCode_ResourceExhausted = 5,
  KernelStatusCode_Unavailable = 6,
  KernelStatusCode_Timeout = 7,
  KernelStatusCode_Cancelled = 8,
  KernelStatusCode_DataLoss = 9,
  KernelStatusCode_Internal = 10,
  KernelStatusCode_InvalidStatementHandle = 11,
  KernelStatusCode_NetworkError = 12,
  KernelStatusCode_SqlError = 13,
} KernelStatusCode;

/* Server-side async execution status (mirror of
 * `KernelStatementStatusKind`). Reserved for the async submit path,
 * which is not exposed over the v0 C ABI. */
typedef enum KernelStatementStatusKind {
  KernelStatementStatusKind_Pending = 0,
  KernelStatementStatusKind_Running = 1,
  KernelStatementStatusKind_Succeeded = 2,
  KernelStatementStatusKind_Failed = 3,
  KernelStatementStatusKind_Cancelled = 4,
  KernelStatementStatusKind_Closed = 5,
} KernelStatementStatusKind;

/*
 * Typed last-error detail. Populated by `kernel_get_last_error`. The
 * `char*` fields point into the calling thread's last-error buffer and
 * are valid only until the next kernel call on that thread; a NULL
 * pointer means the field was absent. `vendor_code` / `http_status` are
 * `0` when absent.
 */
typedef struct KernelError {
  /* Discriminant value of `KernelStatusCode`. */
  int32_t code;
  const char* message;
  /* 5 ASCII chars + NUL, or NULL if absent. */
  const char* sql_state;
  int32_t vendor_code;
  uint16_t http_status;
  bool retryable;
  const char* query_id;
} KernelError;

/*
 * Fetch the last error recorded on the calling thread. Returns `false`
 * (leaving `*out` untouched) if no error has been recorded since the
 * last kernel call cleared the buffer.
 */
bool kernel_get_last_error(KernelError* out);

/* ─── Opaque handles ──────────────────────────────────────────────────
 *
 * All handles are owned heap allocations on the Rust side; the C side
 * holds them by pointer and releases each via its matching `*_close` /
 * `*_free`.
 */
typedef struct kernel_session_config_t KernelSessionConfig;
typedef struct kernel_session_t kernel_session_t;
typedef struct kernel_statement_t kernel_statement_t;
typedef struct kernel_executed_statement_t kernel_executed_statement_t;
/* Async-consumption executed handle. Reserved: the async submit path is
 * deferred in v0 (see kernel_statement_submit), so no consumer receives a
 * live instance yet. */
typedef struct kernel_executed_async_statement_t kernel_executed_async_statement_t;
typedef struct kernel_result_stream_t kernel_result_stream_t;
/* Detached canceller for a sync-executing statement, returned by
 * kernel_statement_canceller_new. The returned handle is safe to use from
 * another thread concurrently with the blocking kernel_statement_execute on the
 * originating statement (the _new call itself is NOT — see its doc).
 * Statement-scoped, not execution-scoped: it tracks whichever query is currently
 * in flight on its statement, so a canceller retained across a re-execute
 * cancels the LATER query. Use one per execute (new before execute, free after
 * it returns). */
typedef struct kernel_statement_canceller_t kernel_statement_canceller_t;

/* ─── Session config ──────────────────────────────────────────────────
 *
 * Build a config, set exactly one connection form and exactly one auth
 * form, then hand it to `kernel_session_open` (which consumes it). Only
 * PAT, OAuth M2M, and OAuth U2M (interactive browser flow) auth are
 * exposed over the C ABI.
 */
KernelStatusCode kernel_session_config_new(KernelSessionConfig** out);
void kernel_session_config_free(KernelSessionConfig* config);

/* Connection identity — call exactly one; the last one wins. */
KernelStatusCode kernel_session_config_set_warehouse(KernelSessionConfig* config,
                                                     const char* host,
                                                     const char* warehouse_id);
KernelStatusCode kernel_session_config_set_http_path(KernelSessionConfig* config,
                                                     const char* host,
                                                     const char* http_path);

/* Authentication — call exactly one; the last one wins. */
KernelStatusCode kernel_session_config_set_auth_pat(KernelSessionConfig* config,
                                                    const char* token);
KernelStatusCode kernel_session_config_set_auth_m2m(KernelSessionConfig* config,
                                                    const char* client_id,
                                                    const char* client_secret);
/* OAuth M2M via a JWT private-key client assertion (RFC 7523): the kernel signs
 * a short-lived JWT with the private key and sends it as the client_assertion
 * instead of a client secret. `client_id`, `jwt_key_file` (PEM/PKCS#8 private
 * key path), and `jwt_kid` (key id in the JWT header) are required. Optional
 * (pass NULL to omit): `jwt_passphrase` (encrypted PKCS#8 key), `jwt_algorithm`
 * (RS256/384/512, PS256/384/512, ES256, ES384; NULL → RS256), `scopes`
 * (comma-separated; NULL → kernel default `all-apis`), and `token_url` (explicit
 * token endpoint; NULL → discover via OIDC). */
KernelStatusCode kernel_session_config_set_auth_m2m_jwt(KernelSessionConfig* config,
                                                        const char* client_id,
                                                        const char* jwt_key_file,
                                                        const char* jwt_kid,
                                                        const char* jwt_passphrase,
                                                        const char* jwt_algorithm,
                                                        const char* scopes,
                                                        const char* token_url);
/* OAuth U2M (user-to-machine: authorization code + PKCE, browser flow).
 * All args optional: `client_id` NULL → `databricks-sql-connector` client;
 * `redirect_port` 0 → kernel default (8030); `scopes` (comma-separated)
 * NULL → kernel default (`sql`,`offline_access`; `offline_access`
 * yields a cached, auto-refreshed refresh token). INTERACTIVE: opening the
 * session starts a localhost listener and opens the user's browser. */
KernelStatusCode kernel_session_config_set_auth_u2m(KernelSessionConfig* config,
                                                    const char* client_id,
                                                    uint16_t redirect_port,
                                                    const char* scopes);
/* Azure Entra service-principal M2M. The kernel owns Azure resolution: it
 * builds the Entra token endpoint + `{app}/.default` scope and auto-discovers
 * the tenant from the workspace `/aad/auth` redirect when `azure_tenant_id` is
 * NULL. `azure_client_id` and `azure_client_secret` (Entra app-registration
 * credentials) are required. Optional (pass NULL to omit): `azure_tenant_id`,
 * and `azure_workspace_resource_id` (workspace ARM resource id; when set, the
 * kernel also sends the Azure SP management token +
 * `X-Databricks-Azure-Workspace-Resource-Id` header, so a service principal
 * with an Azure RBAC role but no workspace membership can authenticate; NULL →
 * the data token authenticates alone). Azure AD U2M has no separate setter —
 * use kernel_session_config_set_auth_u2m (the kernel's workspace-federated
 * browser flow works against Azure Databricks workspaces). */
KernelStatusCode kernel_session_config_set_auth_azure_sp(KernelSessionConfig* config,
                                                         const char* azure_client_id,
                                                         const char* azure_client_secret,
                                                         const char* azure_tenant_id,
                                                         const char* azure_workspace_resource_id);

/* Optional SP-wide Workload Identity Federation client id used by mandatory
 * token exchange. Unset selects BYOT / account-wide WIF. */
KernelStatusCode
kernel_session_config_set_identity_federation_client_id(KernelSessionConfig* config,
                                                        const char* client_id);

/* Optional OAuth token-endpoint override, applied to whichever OAuth mode
 * (M2M, M2M-JWT, or U2M) is selected. Points the token exchange at a non-workspace
 * endpoint — e.g. the Azure/Entra token endpoint
 * `https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token` for an Azure
 * service principal. Unset → OIDC discovery against the workspace host.
 * No effect on PAT auth.
 *
 * The override replaces the token endpoint ONLY. For U2M the authorization
 * endpoint always comes from OIDC discovery against the workspace host — it is
 * never derived from this override — so no particular suffix is required and no
 * value is rejected at session open. M2M / M2M-JWT likewise only have their
 * token endpoint replaced. */
KernelStatusCode
kernel_session_config_set_oauth_token_endpoint(KernelSessionConfig* config,
                                               const char* token_endpoint);

/* Optional OAuth scopes (comma-separated), applied to whichever OAuth mode
 * (M2M, M2M-JWT, or U2M) is selected — e.g.
 * `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default` for an Azure service
 * principal (M2M), or `sql,offline_access` (U2M). Mirrors the single
 * `oauth_scopes` the pyo3 / napi bindings expose across both flows. Unset →
 * kernel per-mode default (`all-apis` for M2M; `sql offline_access` for U2M).
 * For U2M and M2M-JWT it takes precedence over any scopes passed positionally
 * to kernel_session_config_set_auth_u2m / kernel_session_config_set_auth_m2m_jwt. */
KernelStatusCode
kernel_session_config_set_oauth_scopes(KernelSessionConfig* config,
                                       const char* scopes);

/* Add (or overwrite) one session-conf entry. Keys are normally server SET
 * parameters forwarded on the SEA wire (allowlist-filtered). A small set of
 * reserved keys are instead CLIENT-ONLY: they tune the kernel locally, are
 * consumed before session creation, and NEVER reach the wire. Client-only keys
 * (matched case-insensitively):
 *
 *   cloudfetch_max_chunks_in_memory  — positive integer; how many decompressed
 *       CloudFetch chunks the kernel holds in memory at once (bounds peak RSS on
 *       large result sets; default 16). A value above the kernel ceiling (256) is
 *       clamped; a non-numeric / non-positive value is ignored (default kept) with
 *       a warning — a bad tuning value never fails session open. */
KernelStatusCode kernel_session_config_set_session_conf(KernelSessionConfig* config,
                                                        const char* key,
                                                        const char* value);

/* Append one HTTP header sent on every request. Call once per header
 * (order preserved); `name` and `value` are both required. */
KernelStatusCode kernel_session_config_set_custom_header(KernelSessionConfig* config,
                                                         const char* name, const char* value);

/* Configure the total timeout for one HTTP request, from connection start
 * through response-body completion. A separate 30s connection timeout also
 * applies, so connection establishment is bounded by the shorter limit.
 * `request_timeout_ms == 0` keeps the kernel default (120s). */
KernelStatusCode kernel_session_config_set_request_timeout(KernelSessionConfig* config,
                                                           uint64_t request_timeout_ms);

/* Initialize kernel logging, process-wide and ONCE (first non-OFF call wins;
 * later calls are no-ops). `level` is OFF/ERROR/WARN/INFO/DEBUG/TRACE
 * (NULL → RUST_LOG, default warn); `file_path` NULL → stderr. Not tied to
 * a session — lets a host route kernel logs into its own log file. OFF returns
 * Success without installing a subscriber or consuming the once-only slot, so
 * a later non-OFF initializer can still win.
 *
 * A non-UTF-8 `file_path` returns InvalidArgument. OFF behaves as described
 * above. For a non-OFF call, the result reflects the first non-OFF call's
 * outcome (a later call's level/file have no effect and are not described by
 * its return): Success when logging was installed against the request; Internal
 * (with a stored last-error) when that install did not fully honour the request
 * — the file could not be opened (falls back to stderr), a callback sink from
 * an earlier kernel_init_logging_callback already won (this file/stderr
 * request had no effect), or another component already owns the global
 * subscriber (no kernel subscriber installed). */
KernelStatusCode kernel_init_logging(const char* level, const char* file_path);

/* One callback-delivered kernel log record. `message` contains the rendered
 * event text and fields recorded directly on that event; fields attached only
 * to enclosing spans are not included. Delivery is synchronous on the thread
 * that emitted the record: the callback must return promptly, or it will delay
 * that kernel operation. Hosts that need asynchronous logging can copy the
 * record into their own bounded queue and return. Strings are valid only during
 * the callback. Calls may be concurrent; copy retained strings and synchronize
 * the destination. `user_data` is the pointer supplied at initialization. */
typedef void (*KernelLogCallback)(const char* level, const char* target,
                                  const char* message, void* user_data);

/* Initialize kernel logging through a host callback. The level and process-wide
 * first-non-OFF-call-wins behavior match kernel_init_logging. OFF returns
 * Success without installing a subscriber or consuming the once-only slot, so
 * a later non-OFF initializer can still win. The callback must remain valid for
 * the process lifetime, return promptly, must not unwind, and must not re-enter
 * the kernel C ABI. A NULL callback returns InvalidArgument. Returns Internal
 * (with a stored last-error) when the callback was NOT installed because a
 * file/stderr sink from an earlier
 * kernel_init_logging already won, a different callback from an earlier
 * kernel_init_logging_callback already won, or another component owns the global
 * subscriber; Success means level OFF was requested (which installs nothing and
 * does not consume the initialization slot) or this exact callback + user_data
 * is the installed sink (a later call with the identical callback and user_data
 * also succeeds). */
KernelStatusCode kernel_init_logging_callback(const char* level,
                                              KernelLogCallback callback,
                                              void* user_data);

/* ─── Proxy / TLS (optional) ──────────────────────────────────────────
 *
 * All optional; defaults are: no explicit proxy (reqwest honours
 * HTTP(S)_PROXY / NO_PROXY env vars) and strict TLS (system trust store,
 * valid chain, hostname verified). Any setter below is a relaxation or
 * override.
 */

/* Route traffic through an HTTP/HTTPS proxy. `url` is required (e.g.
 * "http://proxy.corp:8080"); `username` / `password` / `bypass_hosts`
 * (comma-separated) are optional — pass NULL to omit. */
KernelStatusCode kernel_session_config_set_proxy(KernelSessionConfig* config, const char* url,
                                                 const char* username, const char* password,
                                                 const char* bypass_hosts);

/* Add a PEM-encoded CA-certificate bundle (`pem`/`len` bytes) to the TLS
 * trust store on top of the system roots (corporate re-signing proxy /
 * on-prem CA). */
KernelStatusCode kernel_session_config_set_tls_trusted_certs(KernelSessionConfig* config,
                                                             const uint8_t* pem, size_t len);

/* Accept self-signed / invalid server certs (disables chain validation).
 * Development / on-prem only. */
KernelStatusCode kernel_session_config_set_tls_allow_self_signed(KernelSessionConfig* config,
                                                                 bool allow);

/* Skip the certificate hostname-vs-SNI check. Development / lab only. */
KernelStatusCode kernel_session_config_set_tls_skip_hostname_verification(KernelSessionConfig* config,
                                                                         bool skip);

/* Configure a client certificate and private key for mutual TLS (mTLS).
 * Both values are required and configured atomically. `cert_pem` contains
 * the PEM leaf certificate followed by any intermediate certificates;
 * `key_pem` contains the matching unencrypted PEM private key. PKCS#8 is
 * recommended for portability across the kernel's TLS backends.
 *
 * A null pointer or zero length for either value returns InvalidArgument.
 * The input buffers are copied before this function returns. */
KernelStatusCode kernel_session_config_set_tls_client_certificate(
    KernelSessionConfig* config, const uint8_t* cert_pem, size_t cert_len,
    const uint8_t* key_pem, size_t key_len);

/* Configure the HTTP retry / backoff policy. `min_wait_ms` / `max_wait_ms`
 * are the backoff-wait bounds between attempts (the wait is exponential
 * with jitter, clamped to this range); `max_retries` is the number of
 * retries AFTER the initial attempt (so at most `max_retries + 1` total
 * attempts, and `0` disables retries); `overall_timeout_ms` is the cumulative
 * retry budget across all attempts (`0` keeps the kernel default — pass
 * non-zero to override it). Lets a host's RetryWaitMin / RetryWaitMax /
 * RetryMax / retry-timeout surface override the kernel's built-in policy
 * (default 5 retries, 1s..60s, 900s budget) when this setter is not called.
 *
 * The retry budget is ALWAYS bounded — there is no "unlimited" value. A large
 * `max_retries` is still capped by the (default or overridden) budget: if the
 * exponential backoff sums past `overall_timeout_ms` the remaining attempts do
 * not run, so raise `overall_timeout_ms` alongside a large `max_retries` if you
 * want them all to fire. A non-zero `overall_timeout_ms` smaller than
 * `min_wait_ms` guarantees ZERO retries (the first backoff already exceeds the
 * budget); it is accepted but the kernel logs a warning.
 *
 * `min_wait_ms == 0` or `max_wait_ms < min_wait_ms` returns InvalidArgument.
 * Benefits ODBC too. */
KernelStatusCode kernel_session_config_set_retry_config(KernelSessionConfig* config,
                                                        uint64_t min_wait_ms, uint64_t max_wait_ms,
                                                        uint32_t max_retries,
                                                        uint64_t overall_timeout_ms);

/* Configure telemetry collection/export. Telemetry is disabled by default.
 * `batch_size`, `flush_interval_ms`, and `close_flush_timeout_ms` must be > 0.
 * `max_retries == 0` disables telemetry export retries; `retry_delay_ms == 0`
 * means immediate retry. Telemetry failures are fail-open and never change user
 * operation results. */
KernelStatusCode kernel_session_config_set_telemetry_config(KernelSessionConfig* config,
                                                            bool enabled, size_t batch_size,
                                                            uint64_t flush_interval_ms,
                                                            uint32_t max_retries,
                                                            uint64_t retry_delay_ms,
                                                            uint64_t close_flush_timeout_ms);

/* Configure driver/runtime/system identity supplied by the binding layer.
 * Every string argument is optional: pass NULL for values the binding does not
 * know. The kernel fills any missing fields it can derive before emitting
 * telemetry. Non-NULL strings must be UTF-8. */
KernelStatusCode kernel_session_config_set_driver_system_configuration(
    KernelSessionConfig* config, const char* driver_name, const char* driver_version,
    const char* runtime_name, const char* runtime_version, const char* runtime_vendor,
    const char* os_name, const char* os_version, const char* os_arch,
    const char* client_app_name, const char* locale_name, const char* char_set_encoding,
    const char* process_name);

/* Whether transaction control is ignored (no-oped). `ignore = true` (the
 * default) gives IgnoreTransactions=1 semantics: commit / rollback /
 * autocommit-off become silent no-ops. `ignore = false` enables real
 * multi-statement transactions (Private Preview; requires catalog-managed
 * tables server-side). Shared by all drivers (ODBC / Python / Go / Node). */
KernelStatusCode kernel_session_config_set_ignore_transactions(KernelSessionConfig* config,
                                                               bool ignore);

/* Configure the on-disk OAuth token cache for U2M (browser) auth. Mirrors
 * the EnableTokenCache / TokenCachePassPhrase pair exposed by the reference
 * ODBC driver and JDBC.
 *
 * `enabled = true` (the default) persists the refresh token under
 * ~/.config/databricks-sql-kernel/oauth/ so the user is not sent through
 * the browser on every connection. The file is ALWAYS encrypted
 * (AES-256-CBC, PBKDF2-HMAC-SHA256) — there is no plaintext mode. A
 * non-NULL `passphrase` keys it; NULL or blank falls back to a
 * machine-local derived key, matching the reference driver, which caches
 * with an empty TokenCachePassPhrase. Supplying a passphrase is stronger:
 * the derived key defends against the cache file being read elsewhere (a
 * backup, an image layer, a support bundle), not against an attacker who
 * can already run code as this user.
 *
 * `enabled = false` disables DISK PERSISTENCE ONLY; tokens are still
 * reused and refreshed in memory for the session's lifetime, so this does
 * not re-trigger the browser flow on every refresh. Use it when the
 * process must not write credentials to disk at all. `passphrase` is
 * ignored in that case.
 *
 * Only U2M auth touches the cache; PAT and M2M ignore this setting.
 *
 * The on-disk format is wire-compatible with the JDBC driver's
 * EncryptedFileTokenCache (given the same passphrase). */
KernelStatusCode kernel_session_config_set_u2m_token_cache_config(KernelSessionConfig* config,
                                                                  bool enabled,
                                                                  const char* passphrase);

/* Set one CLIENT-SIDE result knob by name (key = value).
 *
 * Counterpart to kernel_session_config_set_session_conf: that one carries
 * SERVER session confs (ANSI_MODE, TIMEZONE, ...) to the SEA wire; this one
 * sets CLIENT-SIDE knobs that tune how the kernel fetches, buffers, and
 * post-processes results and are NEVER sent to the server. The two channels
 * are deliberately separate (conflating them is what made a client flag
 * masquerade as a rejected server conf).
 *
 * Key/value (rather than one typed setter per knob) so the signature never
 * changes as new result knobs are added, and so a host that already has DSN
 * / connection-string attributes can forward them in a loop. Unknown keys
 * and unparseable values are REJECTED (InvalidArgument + stored error), not
 * silently ignored.
 *
 * Recognised keys (case-insensitive):
 * - "cloudfetch_enabled" = "true"|"false" — false serves every result
 *   inline (SEA disposition = INLINE), mirroring JDBC EnableQueryResultDownload=0.
 * - "cloudfetch_link_prefetch_window" = positive integer.
 * - "cloudfetch_max_chunks_in_memory" = positive integer (bounds peak RSS;
 *   values above the kernel ceiling of 256 are clamped with a warning).
 * - "inline_max_chunks_in_memory" = positive integer (inline-Arrow prefetch
 *   window, used when cloudfetch_enabled = false).
 * - "complex_types_as_json" = "true"|"false" — render ARRAY / MAP / STRUCT /
 *   VARIANT / GEOMETRY / GEOGRAPHY as JSON text (PySQL
 *   _use_arrow_native_complex_types=False, JDBC complexDatatypeSupport=false).
 * - "intervals_as_string" = "true"|"false" — render INTERVAL / DURATION as
 *   canonical Databricks text (for pyarrow-Python bindings).
 *
 * A server session-conf key passed here (e.g. "ANSI_MODE") is rejected —
 * use kernel_session_config_set_session_conf for those. */
KernelStatusCode kernel_session_config_set_client_conf(KernelSessionConfig* config,
                                                       const char* key, const char* value);

/* ─── Session lifecycle ───────────────────────────────────────────────*/

/*
 * Open a session. CONSUMES `config` on both success and failure — do not
 * use or free `config` afterwards. On success, `*out` holds a session
 * handle released with `kernel_session_close`.
 */
KernelStatusCode kernel_session_open(KernelSessionConfig* config, kernel_session_t** out);
KernelStatusCode kernel_session_close(kernel_session_t* session);

/* Report whether a session is still OPEN (its delete has not been
 * initiated). Writes true/false to `*out`. LOCAL check only — no server
 * round-trip, so it does NOT detect a server idle-timeout or dropped
 * link; it reports only whether the handle has been closed/superseded.
 * A host backing SQL_ATTR_CONNECTION_DEAD should read `true` as
 * "not known-dead", not a positive liveness guarantee. No ownership. */
KernelStatusCode kernel_session_is_open(const kernel_session_t* session, bool* out);

/* Actively validate the session against the server by round-tripping a
 * lightweight `SELECT 1` test query (result discarded). This is the
 * I/O-performing complement of kernel_session_is_open: it reaches the
 * server, so it DOES detect a server idle-timeout, a killed warehouse, or
 * a dropped link. Returns Success when the round-trip completes (session
 * alive); on failure returns the mapped status (a transport/availability
 * failure surfaces as Unavailable / NetworkError / Timeout — treat as
 * "connection dead" when backing SQL_ATTR_CONNECTION_DEAD; the full error
 * is in kernel_get_last_error). Runs SQL, so it MUST be called from a
 * native (non-async-runtime) thread. No ownership. */
KernelStatusCode kernel_session_test(const kernel_session_t* session);

/* Construct a new mutable statement bound to this session. */
KernelStatusCode kernel_session_new_statement(kernel_session_t* session,
                                              kernel_statement_t** out);

/* ─── Transaction control ─────────────────────────────────────────────
 *
 * Transaction control is expressed as session-scoped SQL and shared by all
 * drivers. When the session was opened with ignore_transactions = true (the
 * default), these are silent no-ops returning Success. When false (opt-in),
 * they issue SET AUTOCOMMIT / COMMIT / ROLLBACK on the session; real
 * multi-statement transactions require catalog-managed tables server-side.
 * The three mutating calls run SQL and MUST be invoked from a native
 * (non-async-runtime) thread. */

/* Set autocommit mode. false begins an explicit transaction; true returns to
 * auto-commit. Issues `SET AUTOCOMMIT = TRUE|FALSE`. No-op when transactions
 * are ignored. */
KernelStatusCode kernel_session_set_autocommit(kernel_session_t* session, bool enabled);

/* Read the current autocommit mode into `*out`. Local read — no round-trip. */
KernelStatusCode kernel_session_get_autocommit(const kernel_session_t* session, bool* out);

/* Commit the current transaction (issues `COMMIT`). No-op when ignored. */
KernelStatusCode kernel_session_commit(kernel_session_t* session);

/* Roll back the current transaction (issues `ROLLBACK`). No-op when ignored. */
KernelStatusCode kernel_session_rollback(kernel_session_t* session);

/* ─── Statement ───────────────────────────────────────────────────────*/

/* Set the statement's SQL text, replacing any previously set SQL. Also clears
 * every previously-bound parameter (raw and the typed positional binders below),
 * so a reused statement handle starts each query with a fresh parameter set. */
KernelStatusCode kernel_statement_set_sql(kernel_statement_t* stmt, const char* sql);

/* Typed positional parameter binding (1-based ordinal); the kernel infers the
 * SEA wire type from the C type. These are the "typed params" the
 * kernel_statement_bind_parameter contract refers to: within one statement they
 * cannot be combined with kernel_statement_bind_parameter (raw binds) — pick one
 * binding style. The conflict is reported at kernel_statement_execute, not here. */
KernelStatusCode kernel_statement_bind_null(kernel_statement_t* stmt, size_t ordinal);
KernelStatusCode kernel_statement_bind_int64(kernel_statement_t* stmt, size_t ordinal,
                                             int64_t value);
KernelStatusCode kernel_statement_bind_double(kernel_statement_t* stmt, size_t ordinal,
                                              double value);
KernelStatusCode kernel_statement_bind_bool(kernel_statement_t* stmt, size_t ordinal, bool value);
KernelStatusCode kernel_statement_bind_string(kernel_statement_t* stmt, size_t ordinal,
                                              const char* value);

/* Bind a pre-marshalled parameter: host-supplied Databricks type name +
 * stringified value, positional or named. For a host that already stringifies
 * values and knows the SQL type (e.g. the Go driver). name NULL/empty ->
 * positional (ordinal assigned at execute in push order); non-empty -> named.
 * value NULL -> SQL NULL (pass sql_type "VOID"); sql_type must be non-NULL and
 * non-empty (an empty type is rejected here with InvalidArgument).
 *
 * Lifecycle: raw binds ACCUMULATE (each call appends a marker in push order);
 * they carry no ordinal to overwrite by. kernel_statement_set_sql is the sole
 * reset point — it clears ALL prior binds (raw AND the typed positional binders
 * below) so each set_sql begins a fresh parameter set. kernel_statement_execute
 * does NOT clear them, so a host reusing one statement handle across queries must
 * call set_sql before re-binding, or the prior query's markers persist and the
 * server sees the wrong parameter count.
 *
 * Two mutual-exclusion rules hold within one statement, both enforced at
 * kernel_statement_execute (this call always returns Success on a valid
 * arg — the conflict surfaces later as InvalidArgument from execute):
 *   - positional and named raw params cannot be mixed;
 *   - raw params cannot be mixed with the typed positional binders
 *     (kernel_statement_bind_null/int64/double/bool/string above). */
KernelStatusCode kernel_statement_bind_parameter(kernel_statement_t* stmt,
                                                 const char* name,
                                                 const char* sql_type,
                                                 const char* value);

/*
 * Wait-for-result execution. On success, `*out` holds an executed handle
 * released with `kernel_executed_statement_close`.
 */
KernelStatusCode kernel_statement_execute(kernel_statement_t* stmt,
                                          kernel_executed_statement_t** out);

/*
 * Submit-and-return (async). DEFERRED in v0: this always returns
 * `KernelStatusCode_InvalidArgument` (with an explanatory last error) and
 * writes nothing to `*out`. Use the synchronous `kernel_statement_execute`
 * path. Declared so the symbol/contract is visible; wired when an ODBC /
 * Go consumer needs caller-driven polling.
 */
KernelStatusCode kernel_statement_submit(kernel_statement_t* stmt,
                                         kernel_executed_async_statement_t** out);

/*
 * Free the statement lifecycle owner. Does NOT free executed-handle
 * boxes — each executed handle is a separate allocation that you must
 * release with `kernel_executed_statement_close`.
 *
 * ORDERING: close the statement LAST. Executed handles produced by this
 * statement (and any result stream borrowed from them) share a validity
 * flag with the statement; closing the statement invalidates them, after
 * which `kernel_executed_statement_get_result_stream`,
 * `kernel_executed_statement_num_modified_rows`, and
 * `kernel_result_stream_next_batch` on those handles return
 * `KernelStatusCode_InvalidStatementHandle` (a defined error, never UB).
 * Drain / close the result stream, then the executed handle(s), then the
 * statement. The statement must outlive every handle it produced.
 */
KernelStatusCode kernel_statement_close(kernel_statement_t* stmt);

/* ─── Sync-execute cancellation (detached canceller) ──────────────────*/

/*
 * Create a detached canceller for `stmt`. Obtain it BEFORE
 * `kernel_statement_execute`, which blocks the calling thread for the
 * whole query — the statement cannot be cancelled through its own handle
 * mid-execute (that would alias the mutable borrow).
 *
 * UNDEFINED BEHAVIOR: this call itself borrows `stmt` mutably. Calling it
 * while a `kernel_statement_execute` on the same `stmt` is in flight on
 * another thread aliases that execute's own mutable borrow — undefined
 * behavior (memory corruption), not a returned error. Create the canceller on
 * the same thread that owns `stmt`, before dispatching execute. The natural
 * "spawn the blocking execute, then lazily create a canceller when the host
 * decides to cancel" pattern is exactly this misuse — do not do it; create the
 * canceller first, hand it to the cancelling thread, then dispatch execute.
 * Only the RETURNED handle is concurrency-safe: it holds only cloned Arcs, so
 * once you have it, cancelling from another thread while execute runs is fine.
 *
 * The canceller holds its own session reference, so it must be freed
 * regardless of whether the statement or session is closed. On success `*out`
 * holds a canceller released with `kernel_statement_canceller_free`.
 */
KernelStatusCode kernel_statement_canceller_new(
    kernel_statement_t* stmt, kernel_statement_canceller_t** out);

/*
 * Cancel the originating statement, server-side. Statement-scoped: targets
 * the server statement id currently observed for this statement. Idempotent
 * and safe to call concurrently with `kernel_statement_execute`.
 *
 * On Success, `*dispatched` reports whether a cancel RPC was POSTed:
 * true if a server statement id had been observed and the RPC was sent for it,
 * false if no id has been observed yet — nothing has executed, or the query is
 * still in its initial round-trip before the server issued an id. Both cases
 * return Success; `*dispatched` is how a host tells a dispatched cancel from a
 * no-op. `*dispatched` is written on every non-NULL path (false before the RPC,
 * then the RPC outcome on Success), so it holds a defined value once this
 * returns; a NULL `dispatched` is ignored.
 *
 * `dispatched == true` means only that a cancel was POSTed for the currently
 * observed statement id — NOT that a running query was interrupted. The id slot
 * retains the last observed id after a query finishes, so a cancel fired
 * post-terminal (or, in the narrow re-execute handoff, against a superseded id)
 * still POSTs and still reports true; the server treats a cancel of an
 * already-finished statement as its own no-op.
 *
 * This call blocks on the cancel RPC, which on a retryable server error retries
 * with backoff and can run far longer than one round-trip — bounded by the SEA
 * retry budget (the overall_timeout / max_retries defaults in the kernel's retry
 * config), which can reach minutes during a partial server outage. There is no
 * cancel-of-cancel or timeout parameter, so a host wiring this into a Ctrl-C /
 * query-timeout handler should call it from an abandonable/joinable thread rather
 * than the app's main thread.
 */
KernelStatusCode kernel_statement_canceller_cancel(
    kernel_statement_canceller_t* canceller, bool* dispatched);

/*
 * Free a canceller handle. Does not touch the originating statement. The
 * canceller is single-owner: freeing it while another thread is inside
 * kernel_statement_canceller_cancel on this handle is UNDEFINED BEHAVIOR
 * (use-after-free), not merely wasteful. That call blocks on the cancel RPC,
 * which on a retryable server error retries with backoff and can stay in flight
 * far longer than one round-trip (see _cancel), so join or otherwise quiesce any
 * cancelling thread BEFORE freeing.
 */
KernelStatusCode kernel_statement_canceller_free(
    kernel_statement_canceller_t* canceller);

/* ─── Executed statement (sync) ───────────────────────────────────────*/

/*
 * Obtain a result-stream handle that BORROWS `exec`. The stream is valid
 * only as long as `exec` is: close the stream
 * (`kernel_result_stream_close`) before closing `exec`, and keep the
 * parent statement open until the stream is drained/closed (see
 * `kernel_statement_close`). At most one stream per executed handle; a
 * second call returns `KernelStatusCode_InvalidArgument`. Returns
 * `KernelStatusCode_InvalidStatementHandle` if the parent statement was
 * re-executed or closed.
 */
KernelStatusCode kernel_executed_statement_get_result_stream(kernel_executed_statement_t* exec,
                                                             kernel_result_stream_t** out);

/*
 * Rows modified by a DML statement, or -1 if not applicable / unknown.
 * A -1 with NO stored last error means "not applicable"; a -1 after a
 * stored `InvalidStatementHandle` means the parent statement was
 * re-executed or closed (call `kernel_get_last_error` to distinguish).
 */
int64_t kernel_executed_statement_num_modified_rows(const kernel_executed_statement_t* exec);

/*
 * Server statement (query) id of a successfully executed statement, as a
 * NUL-terminated C string — the success-path counterpart to the query id on
 * KernelError. Used by a host driver for EXECUTE_STATEMENT telemetry and
 * query-history correlation. The pointer is BORROWED from the executed handle,
 * valid until kernel_executed_statement_close; do NOT free it, and copy it out
 * to outlive the handle. Returns NULL on a null / invalidated handle (with a
 * stored last error); a live handle from a successful execute never returns NULL.
 */
const char* kernel_executed_statement_query_id(const kernel_executed_statement_t* exec);

KernelStatusCode kernel_executed_statement_close(kernel_executed_statement_t* exec);

/* ─── Result stream ───────────────────────────────────────────────────
 *
 * The schema is exported once; each `next_batch` call exports one Arrow
 * array. End-of-stream is signalled by a RELEASED array (its `release`
 * callback is NULL) returned with `KernelStatusCode_Success`.
 */
KernelStatusCode kernel_result_stream_get_schema(kernel_result_stream_t* stream,
                                                 struct ArrowSchema* out);
KernelStatusCode kernel_result_stream_next_batch(kernel_result_stream_t* stream,
                                                 struct ArrowArray* out_array,
                                                 struct ArrowSchema* out_schema);
KernelStatusCode kernel_result_stream_close(kernel_result_stream_t* stream);

/* ─── Metadata ────────────────────────────────────────────────────────
 *
 * Each returns a self-contained result stream (released via
 * `kernel_result_stream_close`) carrying the server-shaped
 * (JDBC-canonical) columns. Pattern args accept SQL LIKE wildcards;
 * identifier args are exact.
 *
 * NULL string args mean "unfiltered" for the list_* calls (catalogs /
 * schemas / tables / columns). The key/constraint calls differ:
 * `kernel_metadata_primary_keys` requires a non-NULL catalog, schema, AND
 * table (Databricks SHOW KEYS must target a fully-qualified table), while
 * `kernel_metadata_foreign_keys` keys off the foreign (referencing) table
 * — a NULL foreign_table returns an empty result (0 rows, no wire call),
 * not an error. Passing NULL for a required identifier returns
 * `KernelStatusCode_InvalidArgument` with an explanatory last error (see
 * each function's comment).
 */
KernelStatusCode kernel_metadata_list_catalogs(kernel_session_t* session,
                                               kernel_result_stream_t** out);
KernelStatusCode kernel_metadata_list_schemas(kernel_session_t* session, const char* catalog,
                                              const char* schema_pattern,
                                              kernel_result_stream_t** out);
KernelStatusCode kernel_metadata_list_tables(kernel_session_t* session, const char* catalog,
                                             const char* schema_pattern, const char* table_pattern,
                                             const char* table_types_csv,
                                             kernel_result_stream_t** out);
KernelStatusCode kernel_metadata_list_columns(kernel_session_t* session, const char* catalog,
                                              const char* schema_pattern, const char* table_pattern,
                                              const char* column_pattern,
                                              kernel_result_stream_t** out);
/* `catalog`, `schema`, and `table` are all REQUIRED (non-NULL): the
 * underlying SHOW KEYS targets a fully-qualified table. NULL for any of
 * them returns `KernelStatusCode_InvalidArgument`. */
KernelStatusCode kernel_metadata_primary_keys(kernel_session_t* session, const char* catalog,
                                              const char* schema, const char* table,
                                              kernel_result_stream_t** out);
/* Keys off the foreign (referencing) table. A NULL foreign_table means
 * "nothing to look up" and returns an empty result (0 rows, no wire
 * call) — this is the ODBC "primary-keys only" SQLForeignKeys form. When
 * a foreign_table IS given, foreign_catalog and foreign_schema are then
 * required (they scope SHOW FOREIGN KEYS) and a NULL there returns
 * `KernelStatusCode_InvalidArgument`. The parent_* (referenced) triple
 * is optional and filters the referenced side. */
KernelStatusCode kernel_metadata_foreign_keys(kernel_session_t* session, const char* parent_catalog,
                                              const char* parent_schema, const char* parent_table,
                                              const char* foreign_catalog,
                                              const char* foreign_schema, const char* foreign_table,
                                              kernel_result_stream_t** out);
/* List the supported table types (`TABLE`, `VIEW`, `SYSTEM TABLE`, …).
 * No filters and no wire call — the set is derived from the session's
 * transport. Backs ODBC `SQLTables(table_type = SQL_ALL_TABLE_TYPES)`.
 * The stream carries a single `TABLE_TYPE` column (JDBC getTableTypes). */
KernelStatusCode kernel_metadata_list_table_types(kernel_session_t* session,
                                                  kernel_result_stream_t** out);
/* List stored procedures. `catalog` is an exact identifier (NULL →
 * cross-catalog via system.information_schema); `schema_pattern` and
 * `procedure_pattern` are SQL LIKE patterns (NULL → unfiltered). Backs
 * ODBC `SQLProcedures` / JDBC `getProcedures`; the stream carries the
 * JDBC getProcedures columns. */
KernelStatusCode kernel_metadata_procedures(kernel_session_t* session, const char* catalog,
                                            const char* schema_pattern,
                                            const char* procedure_pattern,
                                            kernel_result_stream_t** out);
/* List functions. Same argument semantics as
 * `kernel_metadata_procedures`. Backs JDBC `getFunctions`; the stream
 * carries the JDBC getFunctions columns. */
KernelStatusCode kernel_metadata_functions(kernel_session_t* session, const char* catalog,
                                           const char* schema_pattern,
                                           const char* function_pattern,
                                           kernel_result_stream_t** out);
/* List stored-procedure parameter / return columns (procedures only —
 * function arguments are excluded). `catalog` is an exact identifier
 * (NULL → cross-catalog); `schema_pattern` / `procedure_pattern` /
 * `column_pattern` are SQL LIKE patterns (NULL → unfiltered). Backs ODBC
 * `SQLProcedureColumns` / JDBC `getProcedureColumns`; the stream carries
 * the JDBC getProcedureColumns columns. */
KernelStatusCode kernel_metadata_procedure_columns(kernel_session_t* session, const char* catalog,
                                                   const char* schema_pattern,
                                                   const char* procedure_pattern,
                                                   const char* column_pattern,
                                                   kernel_result_stream_t** out);

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* DATABRICKS_KERNEL_H */
