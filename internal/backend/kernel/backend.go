//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"

// go_kernel_set_retry_config wraps kernel_session_config_set_retry_config. cgo's
// C parser silently drops the direct declaration of that symbol (a known cgo
// quirk: the declaration is valid C — it compiles under gcc and links from the
// archive — but cgo omits it from the generated bindings, so a direct
// C.kernel_session_config_set_retry_config call fails to build with "could not
// determine what … refers to"). A static inline shim forwarding to it IS parsed
// and links fine. The shim must live in the SAME file's preamble as its caller
// (applyRetry, below): a shim placed in another file's cgo preamble in this
// package is itself dropped the same way. Keeps the kernel header unchanged (it
// is valid C, used verbatim by the C-only ODBC consumer).
static inline KernelStatusCode go_kernel_set_retry_config(
    KernelSessionConfig* config, uint64_t min_wait_ms, uint64_t max_wait_ms,
    uint32_t max_retries, uint64_t overall_timeout_ms) {
  return kernel_session_config_set_retry_config(
      config, min_wait_ms, max_wait_ms, max_retries, overall_timeout_ms);
}

static inline KernelStatusCode go_kernel_set_max_connections(
    KernelSessionConfig* config, size_t max_connections) {
  return kernel_session_config_set_max_connections(config, max_connections);
}

static inline KernelStatusCode go_kernel_set_telemetry_config(
    KernelSessionConfig* config, bool enabled, size_t batch_size,
    uint64_t flush_interval_ms, uint32_t max_retries, uint64_t retry_delay_ms,
    uint64_t close_flush_timeout_ms) {
  return kernel_session_config_set_telemetry_config(
      config, enabled, batch_size, flush_interval_ms, max_retries, retry_delay_ms,
      close_flush_timeout_ms);
}

static inline KernelStatusCode go_kernel_set_driver_system_configuration(
    KernelSessionConfig* config, const char* driver_name, const char* driver_version,
    const char* runtime_name, const char* runtime_version, const char* runtime_vendor,
    const char* os_name, const char* os_version, const char* os_arch,
    const char* client_app_name, const char* locale_name, const char* char_set_encoding,
    const char* process_name) {
  return kernel_session_config_set_driver_system_configuration(
      config, driver_name, driver_version, runtime_name, runtime_version, runtime_vendor,
      os_name, os_version, os_arch, client_app_name, locale_name, char_set_encoding,
      process_name);
}
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
)

// kernelSessionSeq is a process-wide monotonic counter for kernel session ids.
// The C ABI exposes no server session-id accessor, so we mint our own stable,
// collision-free id per OpenSession rather than deriving one from the session
// handle pointer (a freed handle's address can be reused, colliding telemetry /
// log correlation across sequential connections).
var kernelSessionSeq atomic.Uint64

// Config (the flat connection config for the kernel backend) is defined in the
// untagged config.go so the connector's pure config-assembly can name it in the
// default CGO_ENABLED=0 build; OpenSession below maps it onto the kernel's C
// setters.

// KernelBackend implements backend.Backend over the kernel C ABI. One backend
// backs one conn, which database/sql serializes to a single goroutine at a time,
// so the kernel session inherits single-owner-ship and needs no locks; the only
// concurrency is the per-statement cancel watcher (see operation.go), which
// touches only the kernel's internal inflight-id slot.
type KernelBackend struct {
	cfg       Config
	session   *C.kernel_session_t
	sessionID string
	valid     bool
}

var _ backend.Backend = (*KernelBackend)(nil)

// New builds a kernel backend without opening the session; the connector calls
// OpenSession immediately after, mirroring the Thrift backend's shape.
func New(cfg Config) *KernelBackend {
	return &KernelBackend{cfg: cfg}
}

// httpPathCarriesOrgRouting reports whether httpPath is a canonical
// warehouses/endpoints path carrying a non-empty ?o=<org> SPOG query. When true,
// OpenSession routes by the http path (set_http_path) even if a warehouse id is
// also set, so the kernel receives the org id it needs for unified-host routing.
//
// The guard is deliberately narrow — it mirrors what the kernel's from_http_path
// requires (a /sql/1.0/warehouses/{id} or /sql/1.0/endpoints/{id} prefix, with or
// without a leading slash) AND a non-empty o= value — so rerouting can never send
// set_http_path a path it would reject. Anything else keeps the existing
// warehouse-id routing untouched.
func httpPathCarriesOrgRouting(httpPath string) bool {
	q := strings.SplitN(httpPath, "?", 2)
	if len(q) != 2 {
		return false // no query string → no org routing
	}
	path, query := q[0], q[1]
	isWarehousePath := strings.HasPrefix(path, "/sql/1.0/warehouses/") ||
		strings.HasPrefix(path, "sql/1.0/warehouses/") ||
		strings.HasPrefix(path, "/sql/1.0/endpoints/") ||
		strings.HasPrefix(path, "sql/1.0/endpoints/")
	if !isWarehousePath {
		return false
	}
	for _, param := range strings.Split(query, "&") {
		if v, ok := strings.CutPrefix(param, "o="); ok && v != "" {
			return true
		}
	}
	return false
}

// OpenSession builds a session config (warehouse/http-path + PAT), opens the
// session, and captures a per-conn id. Called once by the connector at connect
// time. The config handle is consumed by kernel_session_open on success and
// freed by us on any earlier failure.
func (k *KernelBackend) OpenSession(ctx context.Context) error {
	// Fail fast on an already-cancelled context before the blocking kernel_session
	// _open (which the C ABI does not let us interrupt mid-call).
	//
	// Deferred (tracked): this ctx is only checked here, at entry — once inside the
	// blocking kernel_session_open there is no way to honor a deadline/cancel that
	// fires mid-connect (a slow warehouse cold-start or a connect-time network
	// partition blocks until the kernel returns on its own). Same class and same
	// root cause as the CloseSession no-deadline note below (the kernel C ABI
	// exposes no deadline/cancellation on the session-lifecycle calls); the fix is
	// the same kernel-side change (a deadline arg or cancel handle), so it's grouped
	// with that follow-up rather than fixed Go-side with a watchdog here.
	if err := ctx.Err(); err != nil {
		return err
	}
	initKernelLogging()
	klogCtx(ctx, "OpenSession host=%s httpPath=%s warehouse=%s", k.cfg.Host, k.cfg.HTTPPath, k.cfg.WarehouseID)

	var cfg *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&cfg) }); err != nil {
		return fmt.Errorf("kernel: config_new: %w", toConnError(err))
	}
	// kernel_session_open consumes the config on EVERY path — success and
	// failure alike (it reclaims the box up front). So we free the config
	// ourselves only when we bail out BEFORE reaching kernel_session_open (a
	// setter error below); once that call is made, ownership has transferred and
	// a free here would double-free.
	consumed := false
	defer func() {
		if !consumed {
			C.kernel_session_config_free(cfg)
		}
	}()

	// Warehouse addressing: bare id when provided, else the http path (which also
	// carries ?o= org routing for shared hosts).
	//
	// SPOG exception: on a unified/SPOG host the workspace id rides in the http
	// path's ?o=<org>, and the kernel injects x-databricks-org-id ONLY when the
	// path goes through set_http_path (kernel-side ConnectionConfig::from_http_path
	// parses both the warehouse id and ?o=). set_warehouse takes only host+id and
	// drops the org id, so a warehouse-id-addressed SPOG session 303s to /login.
	// The kernel also refuses a caller-supplied x-databricks-org-id custom header
	// (it is kernel-managed), so the org id can reach the kernel ONLY via the path.
	// Therefore, when a warehouse id is set BUT the http path is a canonical
	// warehouses/endpoints path carrying ?o=, route by the path instead: the kernel
	// still parses the same warehouse id out of it, plus the org id it needs.
	host := newCStr(k.cfg.Host)
	defer host.free()
	if k.cfg.WarehouseID != "" && !httpPathCarriesOrgRouting(k.cfg.HTTPPath) {
		wh := newCStr(k.cfg.WarehouseID)
		defer wh.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_warehouse(cfg, host.c, wh.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_warehouse: %w", toConnError(err))
		}
	} else {
		path := newCStr(k.cfg.HTTPPath)
		defer path.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_http_path(cfg, host.c, path.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_http_path: %w", toConnError(err))
		}
	}

	if err := k.setAuth(cfg); err != nil {
		return err
	}

	// User-Agent so query history attributes the kernel path to this driver.
	if k.cfg.UserAgent != "" {
		name := newCStr("User-Agent")
		val := newCStr(k.cfg.UserAgent)
		errSet := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_custom_header(cfg, name.c, val.c)
		})
		name.free()
		val.free()
		if errSet != nil {
			return fmt.Errorf("kernel: set_custom_header[User-Agent]: %w", toConnError(errSet))
		}
	}

	// TLS: crypto/tls's InsecureSkipVerify accepts any server cert, so relax both
	// chain validation and the hostname check — mapping only one would leave the
	// kernel path stricter than the Thrift path it mirrors (a self-signed cert
	// would still be rejected).
	if k.cfg.TLSSkipVerify {
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_tls_allow_self_signed(cfg, C.bool(true))
		}); err != nil {
			return fmt.Errorf("kernel: set_tls_allow_self_signed: %w", toConnError(err))
		}
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_tls_skip_hostname_verification(cfg, C.bool(true))
		}); err != nil {
			return fmt.Errorf("kernel: set_tls_skip_hostname_verification: %w", toConnError(err))
		}
	}

	// Experimental kernel-only TLS: a custom CA bundle and an independent hostname
	// skip (finer-grained than the blanket InsecureSkipVerify above). The kernel's
	// rustls stack ignores SSL_CERT_FILE, so a custom CA must be handed over
	// explicitly.
	if err := k.applyKernelTLS(cfg); err != nil {
		return err
	}

	// Proxy: env-derived or explicit (WithKernelProxy). See applyProxy.
	if err := k.applyProxy(cfg); err != nil {
		return err
	}

	if err := k.applyRequestTimeout(cfg); err != nil {
		return err
	}
	if err := k.applyMaxConnections(cfg); err != nil {
		return err
	}

	// Retry / backoff policy (WithRetries). See applyRetry.
	if err := k.applyRetry(cfg); err != nil {
		return err
	}

	// Kernel-owned telemetry and driver/runtime identity. The Go wrapper
	// telemetry interceptor is disabled on the kernel path, so these setters are
	// what enable kernel-side operation telemetry for this session.
	if err := k.applyTelemetry(cfg); err != nil {
		return err
	}
	if err := k.applyDriverSystemConfiguration(cfg); err != nil {
		return err
	}

	// Session confs (STATEMENT_TIMEOUT, QUERY_TAGS, TIMEZONE, …) — the same map
	// the Thrift backend forwards, applied one key at a time.
	for key, val := range k.cfg.SessionConf {
		ck := newCStr(key)
		cv := newCStr(val)
		errSet := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_session_conf(cfg, ck.c, cv.c)
		})
		ck.free()
		cv.free()
		if errSet != nil {
			return fmt.Errorf("kernel: set_session_conf[%s]: %w", key, toConnError(errSet))
		}
	}

	// Log the resolved retry policy + CloudFetch chunk cap before connecting, so if a
	// customer reports a hung connect or a large-result OOM, on-call can see from the
	// debug log what was actually applied (these are otherwise silent — forwarded to
	// the kernel with no observable trace). Kept at Debug and cheap to format.
	klogCtx(ctx, "OpenSession resolved: retry=%s cloudfetchMaxChunksInMemory=%s",
		describeRetry(k.cfg.Retry), k.cfg.SessionConf[kernelMaxChunksInMemoryConfKey])

	// kernel_session_open takes ownership of cfg here. Its documented C-ABI
	// contract (databricks_kernel.h: "CONSUMES config on both success and failure
	// — do not use or free config afterwards") is what makes the unconditional
	// consumed=true correct: mark it before checking the error so the deferred
	// free never double-frees the already-consumed config. This rests on that
	// header guarantee, NOT an assumption — if a future kernel revision validated
	// args and returned before consuming, this would leak, so the contract must be
	// re-verified against the header when KERNEL_REV is bumped.
	var sess *C.kernel_session_t
	err := call(func() C.KernelStatusCode { return C.kernel_session_open(cfg, &sess) })
	consumed = true
	if err != nil {
		return fmt.Errorf("kernel: session_open: %w", toConnError(err))
	}
	k.session = sess
	k.valid = true
	// The C ABI exposes no server session-id accessor; mint a process-unique id
	// for logging / telemetry correlation. NOT derived from the handle pointer —
	// a freed pointer's address can be reused and collide across connections.
	k.sessionID = fmt.Sprintf("kernel-%d", kernelSessionSeq.Add(1))

	// Initial namespace: the kernel C ABI has no catalog/schema config setter, so
	// select it post-connect with USE CATALOG / USE SCHEMA. A failure here means
	// the session is not in the requested namespace
	// — a correctness precondition, like Thrift's InitialNamespace — so fail the
	// connect and close the session we just opened (the connector does not call
	// CloseSession on an OpenSession error).
	if err := k.applyInitialNamespace(ctx); err != nil {
		// Close the session we just opened, routing through call() so a failed close
		// is logged (via lastError's Warn) rather than silently discarded — mirroring
		// CloseSession. The namespace error is authoritative and returned as-is.
		if closeErr := call(func() C.KernelStatusCode { return C.kernel_session_close(sess) }); closeErr != nil {
			klogCtx(ctx, "close after initial-namespace failure also failed: %v", closeErr)
		}
		k.session = nil
		k.valid = false
		return err
	}

	klogCtx(ctx, "OpenSession OK session=%s", k.sessionID)
	return nil
}

// applyKernelTLS forwards the experimental kernel-only TLS knobs to the session
// config: a custom CA bundle, paired mTLS client identity, and an independent
// hostname-skip. Each is a no-op when its field is unset, so this is safe to call
// unconditionally.
func (k *KernelBackend) applyKernelTLS(cfg *C.KernelSessionConfig) error {
	if len(k.cfg.TLSTrustedCertsPEM) > 0 {
		ca := newCBytes(k.cfg.TLSTrustedCertsPEM)
		defer ca.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_tls_trusted_certs(cfg, ca.ptr, ca.len)
		}); err != nil {
			return fmt.Errorf("kernel: set_tls_trusted_certs: %w", toConnError(err))
		}
	}
	if len(k.cfg.TLSClientCertPEM) > 0 && len(k.cfg.TLSClientKeyPEM) > 0 {
		cert := newCBytes(k.cfg.TLSClientCertPEM)
		defer cert.free()
		key := newCBytes(k.cfg.TLSClientKeyPEM)
		defer key.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_tls_client_certificate(
				cfg,
				cert.ptr,
				cert.len,
				key.ptr,
				key.len,
			)
		}); err != nil {
			return fmt.Errorf("kernel: set_tls_client_certificate: %w", toConnError(err))
		}
	}
	if k.cfg.TLSSkipHostnameVerify {
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_tls_skip_hostname_verification(cfg, C.bool(true))
		}); err != nil {
			return fmt.Errorf("kernel: set_tls_skip_hostname_verification: %w", toConnError(err))
		}
	}
	return nil
}

// applyProxy forwards the HTTP proxy config to the session config. The URL is set
// when either the environment configured one for this endpoint or the caller
// supplied one via WithKernelProxy (resolveKernelProxy decides which). The
// optional basic-auth credentials and bypass list ride along; each is NULL when
// empty (the env path folds credentials into the URL and consumes NO_PROXY during
// resolution, so it leaves them empty — Go's proxy-env convention). A no-op when
// ProxyURL is empty (direct connection). The kernel applies the URL as an explicit
// override of its own env-var behavior.
func (k *KernelBackend) applyProxy(cfg *C.KernelSessionConfig) error {
	if k.cfg.ProxyURL == "" {
		return nil
	}
	url := newCStr(k.cfg.ProxyURL)
	defer url.free()
	user := newCStrOrNull(k.cfg.ProxyUsername)
	defer user.free()
	pass := newCStrOrNull(k.cfg.ProxyPassword)
	defer pass.free()
	bypass := newCStrOrNull(k.cfg.ProxyBypassHosts)
	defer bypass.free()
	if err := call(func() C.KernelStatusCode {
		return C.kernel_session_config_set_proxy(cfg, url.c, user.c, pass.c, bypass.c)
	}); err != nil {
		return fmt.Errorf("kernel: set_proxy: %w", toConnError(err))
	}
	return nil
}

func (k *KernelBackend) applyRequestTimeout(cfg *C.KernelSessionConfig) error {
	timeoutMs := requestTimeoutMilliseconds(k.cfg.RequestTimeout)
	if err := call(func() C.KernelStatusCode {
		return C.kernel_session_config_set_request_timeout(cfg, C.uint64_t(timeoutMs))
	}); err != nil {
		return fmt.Errorf("kernel: set_request_timeout: %w", toConnError(err))
	}
	return nil
}

func (k *KernelBackend) applyMaxConnections(cfg *C.KernelSessionConfig) error {
	if k.cfg.MaxConnections == 0 {
		return nil
	}
	if err := call(func() C.KernelStatusCode {
		return C.go_kernel_set_max_connections(cfg, C.size_t(k.cfg.MaxConnections))
	}); err != nil {
		return fmt.Errorf("kernel: set_max_connections: %w", toConnError(err))
	}
	return nil
}

// applyRetry forwards the driver's HTTP retry / backoff policy to the session
// config. A no-op when Config.Retry is nil, so the kernel's own default policy
// (exponential backoff with jitter, 5 retries, 1s..60s, 900s budget) is preserved
// otherwise. MaxRetries == 0 disables retries; OverallTimeout == 0 keeps the
// kernel's default budget (the setter maps a 0 ms budget to "keep default").
func (k *KernelBackend) applyRetry(cfg *C.KernelSessionConfig) error {
	r := k.cfg.Retry
	if r == nil {
		return nil
	}
	if err := call(func() C.KernelStatusCode {
		// Via the go_kernel_set_retry_config shim in cgo.go — cgo drops the direct
		// declaration of the underlying symbol (see the shim's comment).
		return C.go_kernel_set_retry_config(cfg,
			C.uint64_t(r.MinWait.Milliseconds()), C.uint64_t(r.MaxWait.Milliseconds()),
			C.uint32_t(r.MaxRetries), C.uint64_t(r.OverallTimeout.Milliseconds()))
	}); err != nil {
		return fmt.Errorf("kernel: set_retry_config: %w", toConnError(err))
	}
	return nil
}

const (
	defaultTelemetryBatchSize         = 100
	defaultTelemetryFlushInterval     = 5 * time.Second
	defaultTelemetryMaxRetries        = 3
	defaultTelemetryRetryDelay        = 100 * time.Millisecond
	defaultTelemetryCloseFlushTimeout = 5 * time.Second
)

// applyTelemetry forwards the Go driver's kernel-owned telemetry policy to the
// kernel C ABI. The setter requires positive batch / interval / close-timeout
// values even when the caller did not override them, so zero-valued fields are
// filled with the kernel defaults before crossing the ABI.
func (k *KernelBackend) applyTelemetry(cfg *C.KernelSessionConfig) error {
	t := k.cfg.Telemetry
	if t == nil {
		return nil
	}
	batchSize := t.BatchSize
	if batchSize <= 0 {
		batchSize = defaultTelemetryBatchSize
	}
	flushInterval := t.FlushInterval
	if flushInterval <= 0 {
		flushInterval = defaultTelemetryFlushInterval
	}
	maxRetries := t.MaxRetries
	if maxRetries == 0 {
		maxRetries = defaultTelemetryMaxRetries
	}
	retryDelay := t.RetryDelay
	if retryDelay < 0 {
		retryDelay = 0
	}
	if retryDelay == 0 {
		retryDelay = defaultTelemetryRetryDelay
	}
	closeFlushTimeout := t.CloseFlushTimeout
	if closeFlushTimeout <= 0 {
		closeFlushTimeout = defaultTelemetryCloseFlushTimeout
	}
	if err := call(func() C.KernelStatusCode {
		return C.go_kernel_set_telemetry_config(cfg,
			C.bool(t.Enabled), C.size_t(batchSize), C.uint64_t(flushInterval.Milliseconds()),
			C.uint32_t(maxRetries), C.uint64_t(retryDelay.Milliseconds()),
			C.uint64_t(closeFlushTimeout.Milliseconds()))
	}); err != nil {
		return fmt.Errorf("kernel: set_telemetry_config: %w", toConnError(err))
	}
	return nil
}

// applyDriverSystemConfiguration stamps Go driver/runtime identity onto
// kernel-owned telemetry. Empty strings cross as NULL so the kernel fills any
// fields it can derive itself.
func (k *KernelBackend) applyDriverSystemConfiguration(cfg *C.KernelSessionConfig) error {
	s := k.cfg.DriverSystemConfiguration
	if s == nil {
		return nil
	}
	driverName := newCStrOrNull(s.DriverName)
	defer driverName.free()
	driverVersion := newCStrOrNull(s.DriverVersion)
	defer driverVersion.free()
	runtimeName := newCStrOrNull(s.RuntimeName)
	defer runtimeName.free()
	runtimeVersion := newCStrOrNull(s.RuntimeVersion)
	defer runtimeVersion.free()
	runtimeVendor := newCStrOrNull(s.RuntimeVendor)
	defer runtimeVendor.free()
	osName := newCStrOrNull(s.OSName)
	defer osName.free()
	osVersion := newCStrOrNull(s.OSVersion)
	defer osVersion.free()
	osArch := newCStrOrNull(s.OSArch)
	defer osArch.free()
	clientAppName := newCStrOrNull(s.ClientAppName)
	defer clientAppName.free()
	localeName := newCStrOrNull(s.LocaleName)
	defer localeName.free()
	charSetEncoding := newCStrOrNull(s.CharSetEncoding)
	defer charSetEncoding.free()
	processName := newCStrOrNull(s.ProcessName)
	defer processName.free()

	if err := call(func() C.KernelStatusCode {
		return C.go_kernel_set_driver_system_configuration(cfg,
			driverName.c, driverVersion.c, runtimeName.c, runtimeVersion.c, runtimeVendor.c,
			osName.c, osVersion.c, osArch.c, clientAppName.c, localeName.c,
			charSetEncoding.c, processName.c)
	}); err != nil {
		return fmt.Errorf("kernel: set_driver_system_configuration: %w", toConnError(err))
	}
	return nil
}

// kernelMaxChunksInMemoryConfKey mirrors config.KernelMaxChunksInMemoryConfKey —
// the client-only session conf carrying WithKernelMaxChunksInMemory. Duplicated as
// a local const rather than importing internal/config, which this cgo backend
// otherwise has no dependency on; it is read here only to log the resolved cap.
const kernelMaxChunksInMemoryConfKey = "cloudfetch_max_chunks_in_memory"

// describeRetry renders the resolved retry policy for the OpenSession debug log.
// "kernel-default" means Config.Retry is nil, so the kernel keeps its own policy
// (5 retries, 1s..60s, 900s budget); otherwise it shows the forwarded values, with
// maxRetries=0 being the disable form and overallTimeout=0 meaning "keep default".
func describeRetry(r *RetryConfig) string {
	if r == nil {
		return "kernel-default"
	}
	return fmt.Sprintf("maxRetries=%d minWait=%s maxWait=%s overallTimeout=%s",
		r.MaxRetries, r.MinWait, r.MaxWait, r.OverallTimeout)
}

// setAuth applies the resolved auth form to the session config via exactly one
// kernel_session_config_set_auth_* call. PAT and M2M are plain value setters; U2M
// records the client id / redirect port / scopes and the kernel owns the browser
// (PKCE) flow, started when the session opens. Empty string args are passed as NULL
// so the kernel applies its own defaults (e.g. U2M's public client / default port).
func (k *KernelBackend) setAuth(cfg *C.KernelSessionConfig) error {
	switch k.cfg.Auth.Mode {
	case AuthM2M:
		clientID := newCStr(k.cfg.Auth.ClientID)
		defer clientID.free()
		secret := newCStr(k.cfg.Auth.ClientSecret)
		defer secret.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_auth_m2m(cfg, clientID.c, secret.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_auth_m2m: %w", toConnError(err))
		}
	case AuthU2M:
		// client id / scopes are optional: NULL when empty lets the kernel use its
		// public client / default scopes. resolveKernelAuth fills these with the
		// fixed in-house databricks-sql-connector client and offline_access + sql
		// scopes on every cloud (NOT the cloud-inferred Thrift values), so the
		// kernel's single in-house workspace-federated flow works uniformly.
		clientID := newCStrOrNull(k.cfg.Auth.ClientID)
		defer clientID.free()
		scopes := newCStrOrNull(joinScopes(k.cfg.Auth.Scopes))
		defer scopes.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_auth_u2m(cfg, clientID.c, C.uint16_t(k.cfg.Auth.RedirectPort), scopes.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_auth_u2m: %w", toConnError(err))
		}
		// Configure token-cache persistence for U2M (disabled by default; enabled
		// by WithTokenCache / tokenCache DSN param). Pass nil for passphrase to use
		// the kernel's machine-local derived key (matching ODBC driver defaults).
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_u2m_token_cache_config(cfg, C.bool(k.cfg.TokenCacheEnabled), nil)
		}); err != nil {
			return fmt.Errorf("kernel: set_u2m_token_cache_config: %w", toConnError(err))
		}
	default: // AuthPAT
		tok := newCStr(k.cfg.Auth.Token)
		defer tok.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_auth_pat(cfg, tok.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_auth_pat: %w", toConnError(err))
		}
		if k.cfg.Auth.ClientID != "" {
			clientID := newCStr(k.cfg.Auth.ClientID)
			defer clientID.free()
			if err := call(func() C.KernelStatusCode {
				return C.kernel_session_config_set_identity_federation_client_id(cfg, clientID.c)
			}); err != nil {
				return fmt.Errorf("kernel: set_identity_federation_client_id: %w", toConnError(err))
			}
		}
	}
	return nil
}

// joinScopes renders U2M scopes as the comma-separated form the kernel U2M setter
// expects. Empty (no scopes) yields "" so setAuth passes NULL and the kernel
// applies its default scope set.
func joinScopes(scopes []string) string {
	return strings.Join(scopes, ",")
}

// trySetAuth allocates a throwaway session config, applies auth to it, and frees
// it — a test seam so TestSetAuthByMode can exercise the real cgo setter path
// without putting cgo in a _test.go file (which Go forbids). Not used in
// production. Returns the setter error (nil on success).
func trySetAuth(auth Auth) error {
	var cfg *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&cfg) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(cfg)
	k := &KernelBackend{cfg: Config{Auth: auth}}
	return k.setAuth(cfg)
}

// trySetKernelTLS allocates a throwaway session config, applies the experimental
// TLS knobs from cfg to it, and frees it — the analogous test seam to trySetAuth,
// so a tagged test can exercise the real byte-buffer cgo setter (trusted certs)
// and the hostname-skip setter end to end. Not used in production.
func trySetKernelTLS(cfg Config) error {
	var c *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&c) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(c)
	k := &KernelBackend{cfg: cfg}
	return k.applyKernelTLS(c)
}

// trySetProxy allocates a throwaway session config, applies the proxy config from
// cfg to it, and frees it — the analogous test seam to trySetKernelTLS, so a
// tagged test can exercise the real kernel_session_config_set_proxy cgo setter
// (URL + optional NULL-for-empty username / password / bypass) end to end. Not
// used in production.
func trySetProxy(cfg Config) error {
	var c *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&c) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(c)
	k := &KernelBackend{cfg: cfg}
	return k.applyProxy(c)
}

// trySetRetry allocates a throwaway session config, applies the retry config from
// cfg to it, and frees it — the analogous test seam to trySetProxy, so a tagged
// test can exercise the real kernel_session_config_set_retry_config cgo setter
// (the 4 knobs, plus the InvalidArgument rejection for a zero minimum) end to
// end. Not used in production.
func trySetRetry(cfg Config) error {
	var c *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&c) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(c)
	k := &KernelBackend{cfg: cfg}
	return k.applyRetry(c)
}

// trySetTokenCacheConfig allocates a throwaway session config, applies auth (with
// the given TokenCacheEnabled flag) to it via the production setAuth, and frees it
// — the analogous test seam to trySetRetry. Routing through setAuth (rather than
// calling the setter standalone) means a tagged test exercises the real
// kernel_session_config_set_u2m_token_cache_config wiring inside setAuth's U2M
// branch — including setAuth reading k.cfg.TokenCacheEnabled == true — end to end.
// Not used in production. Returns the setter error (nil on success); non-U2M modes
// take setAuth's other branches and do not touch the token-cache setter.
func trySetTokenCacheConfig(auth Auth, enabled bool) error {
	var cfg *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&cfg) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(cfg)
	k := &KernelBackend{cfg: Config{Auth: auth, TokenCacheEnabled: enabled}}
	return k.setAuth(cfg)
}

// trySetRequestTimeout exercises the request-timeout C setter without opening a
// network session. It is used only by the tagged kernel tests.
func trySetRequestTimeout(cfg Config) error {
	var c *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&c) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(c)
	k := &KernelBackend{cfg: cfg}
	return k.applyRequestTimeout(c)
}

// trySetMaxConnections exercises the max-connections C setter without opening
// a network session. It is used only by the tagged kernel tests.
func trySetMaxConnections(cfg Config) error {
	var c *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&c) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(c)
	k := &KernelBackend{cfg: cfg}
	return k.applyMaxConnections(c)
}

// trySetTelemetry exercises the telemetry C setter without opening a network
// session. It is used only by the tagged kernel tests.
func trySetTelemetry(cfg Config) error {
	var c *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&c) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(c)
	k := &KernelBackend{cfg: cfg}
	return k.applyTelemetry(c)
}

// trySetDriverSystemConfiguration exercises the driver-system-configuration C
// setter without opening a network session. It is used only by the tagged kernel
// tests.
func trySetDriverSystemConfiguration(cfg Config) error {
	var c *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&c) }); err != nil {
		return fmt.Errorf("config_new: %w", err)
	}
	defer C.kernel_session_config_free(c)
	k := &KernelBackend{cfg: cfg}
	return k.applyDriverSystemConfiguration(c)
}

// applyInitialNamespace runs USE CATALOG / USE SCHEMA to select the configured
// initial namespace, since the kernel C ABI exposes no catalog/schema setter.
// Identifiers are backtick-quoted (quoteIdent) so arbitrary names are safe. A
// no-op when neither is set.
func (k *KernelBackend) applyInitialNamespace(ctx context.Context) error {
	if k.cfg.Catalog != "" {
		if err := k.runNamespaceStmt(ctx, "USE CATALOG "+quoteIdent(k.cfg.Catalog)); err != nil {
			return fmt.Errorf("kernel: set initial catalog %q: %w", k.cfg.Catalog, err)
		}
	}
	if k.cfg.Schema != "" {
		if err := k.runNamespaceStmt(ctx, "USE SCHEMA "+quoteIdent(k.cfg.Schema)); err != nil {
			return fmt.Errorf("kernel: set initial schema %q: %w", k.cfg.Schema, err)
		}
	}
	return nil
}

// runNamespaceStmt executes a single side-effecting statement (USE …) and closes
// the operation. USE produces no rows, so the result stream is not read. execute
// always returns a non-nil Operation (the Backend contract), so it is closed on
// both the success and error paths; the execute error is authoritative.
func (k *KernelBackend) runNamespaceStmt(ctx context.Context, sql string) error {
	op, err := k.execute(ctx, backend.ExecRequest{Query: sql})
	_, closeErr := op.Close(ctx)
	if err != nil {
		return err
	}
	return closeErr
}

// CloseSession tears down the server-side session and returns the kernel's
// awaited close result.
//
// Deferred (tracked): this ignores ctx and blocks in the synchronous call() until
// kernel_session_close returns, with no deadline — a stalled kernel-side close
// (e.g. a shutdown-time network partition) can block database/sql pool cleanup.
// A bounded close needs either a kernel_session_close_blocking with a deadline or
// a Go-side watchdog; grouped with the kernel C-ABI follow-ups.
func (k *KernelBackend) CloseSession(ctx context.Context) error {
	if k.session == nil {
		return nil
	}
	klogCtx(ctx, "CloseSession session=%s", k.sessionID)
	err := call(func() C.KernelStatusCode { return C.kernel_session_close(k.session) })
	k.session = nil
	k.valid = false
	return toConnError(err)
}

// SessionValid backs conn.IsValid → pool eviction. No I/O; inspects state
// captured at OpenSession and updated by markSessionDead.
func (k *KernelBackend) SessionValid() bool { return k.valid && k.session != nil }

// markSessionDead marks the session unusable so SessionValid() → conn.IsValid()
// returns false and database/sql discards this conn on return to the pool. Called
// from the statement/read path when a kernel error is session-fatal (isSessionFatal):
// it evicts the dead conn WITHOUT returning driver.ErrBadConn, so the statement is
// never transparently re-run (no duplicate write). The backend
// is single-owner per conn (only the cancel watcher shares state, and it touches
// only the canceller's inflight slot), so this write is race-free.
func (k *KernelBackend) markSessionDead() { k.valid = false }

// evictIfSessionFatal marks the session dead when err is (or wraps) a
// session-fatal KernelError, so a conn whose server session died mid-life is
// evicted from the pool. A no-op for nil, non-KernelError, or non-fatal errors.
// Uses errors.As, not a bare type assertion, so it still fires if a caller wraps
// the KernelError before passing it here.
func (k *KernelBackend) evictIfSessionFatal(err error) {
	var ke *KernelError
	if errors.As(err, &ke) && isSessionFatal(ke.Code) {
		k.markSessionDead()
	}
}

// SessionID is the per-conn id (conn.id). Valid after OpenSession.
func (k *KernelBackend) SessionID() string { return k.sessionID }

// Execute runs a statement to a terminal state via the blocking execute path.
// Per the Backend contract it returns a non-nil Operation even on error so the
// caller can read StatementID / wrap the error / Close uniformly. Bound
// parameters (req.Params) are bound onto the statement in execute (see
// bindParams).
func (k *KernelBackend) Execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	// Staging (Unity Catalog volume PUT/GET/REMOVE) needs a local file transfer the
	// kernel path can't perform, and the kernel C ABI surfaces no IsStagingOperation
	// signal to drive conn.execStagingOperation. Reject it here rather than let
	// IsStaging return false and report success with no file moved (silent data loss).
	// (Bound parameters, by contrast, ARE now supported — bound in execute via the
	// kernel's raw-param C ABI.)
	if isStagingStatement(req.Query) {
		return &kernelOp{}, fmt.Errorf("databricks: staging operations (PUT/GET/REMOVE on a volume) are %w; "+
			"use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	return k.execute(ctx, req)
}
