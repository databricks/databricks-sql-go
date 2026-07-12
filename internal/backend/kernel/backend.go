//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"
*/
import "C"

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/internal/backend"
)

// Config is the flat connection config for the kernel backend. The connector
// fills it from the driver's config so the user-facing options are unchanged
// (this mirrors how the kernel's pyo3/napi bindings take flat connection
// params). Zero-valued fields are simply not applied.
type Config struct {
	Host        string // workspace hostname, no scheme
	HTTPPath    string // e.g. /sql/1.0/warehouses/abc123 (carries ?o= org routing)
	WarehouseID string // bare warehouse id; preferred over HTTPPath when set
	Auth        Auth   // PAT / OAuth M2M / OAuth U2M

	// SessionConf carries server-bound session confs verbatim — the same map the
	// Thrift backend forwards (STATEMENT_TIMEOUT, QUERY_TAGS, TIMEZONE, …).
	SessionConf map[string]string

	// TLSSkipVerify accepts any server cert (maps the driver's
	// WithSkipTLSHostVerify / TLSConfig.InsecureSkipVerify). crypto/tls's
	// InsecureSkipVerify disables both chain validation and the hostname check,
	// so the kernel path relaxes both to match.
	TLSSkipVerify bool

	// ProxyURL configures an HTTP proxy, already resolved for this endpoint from
	// the same HTTP(S)_PROXY / NO_PROXY environment the Thrift path uses (NO_PROXY
	// is applied during resolution). Empty leaves the kernel on a direct
	// connection.
	ProxyURL string

	// Location is the session time zone used to render DATE / TIMESTAMP values,
	// matching the Thrift path which returns them in this location. nil means UTC.
	Location *time.Location

	// Catalog / Schema select the initial namespace. The kernel C ABI has no
	// catalog/schema config setter, so OpenSession applies them post-connect by
	// running USE CATALOG / USE SCHEMA (the OSS ODBC driver's workaround). Empty
	// leaves the session in the server default namespace.
	Catalog string
	Schema  string
}

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

// OpenSession builds a session config (warehouse/http-path + PAT), opens the
// session, and captures a per-conn id. Called once by the connector at connect
// time. The config handle is consumed by kernel_session_open on success and
// freed by us on any earlier failure.
func (k *KernelBackend) OpenSession(ctx context.Context) error {
	// Fail fast on an already-cancelled context before the blocking kernel_session
	// _open (which the C ABI does not let us interrupt mid-call).
	if err := ctx.Err(); err != nil {
		return err
	}
	initKernelLogging()
	klog("OpenSession host=%s httpPath=%s warehouse=%s", k.cfg.Host, k.cfg.HTTPPath, k.cfg.WarehouseID)

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
	host := newCStr(k.cfg.Host)
	defer host.free()
	if k.cfg.WarehouseID != "" {
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

	// Proxy: only when the environment configured one for this endpoint. NO_PROXY
	// was already applied during resolution, so no bypass list is needed here;
	// any credentials are carried in the URL userinfo (Go's proxy-env convention),
	// so username/password are NULL.
	if k.cfg.ProxyURL != "" {
		url := newCStr(k.cfg.ProxyURL)
		defer url.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_proxy(cfg, url.c, nil, nil, nil)
		}); err != nil {
			return fmt.Errorf("kernel: set_proxy: %w", toConnError(err))
		}
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

	// kernel_session_open takes ownership of cfg here, on both success and
	// failure — mark consumed before checking the error so the deferred free
	// never runs on the already-consumed config.
	var sess *C.kernel_session_t
	err := call(func() C.KernelStatusCode { return C.kernel_session_open(cfg, &sess) })
	consumed = true
	if err != nil {
		return fmt.Errorf("kernel: session_open: %w", toConnError(err))
	}
	k.session = sess
	k.valid = true
	// The C ABI exposes no formatted session-id accessor; use the handle pointer
	// as a stable per-conn id for logging / telemetry correlation.
	k.sessionID = fmt.Sprintf("kernel-%p", sess)

	// Initial namespace: the kernel C ABI has no catalog/schema config setter, so
	// select it post-connect with USE CATALOG / USE SCHEMA (the OSS ODBC driver's
	// approach). A failure here means the session is not in the requested namespace
	// — a correctness precondition, like Thrift's InitialNamespace — so fail the
	// connect and close the session we just opened (the connector does not call
	// CloseSession on an OpenSession error).
	if err := k.applyInitialNamespace(ctx); err != nil {
		// Close the session we just opened, routing through call() so a failed close
		// is logged (via lastError's Warn) rather than silently discarded — mirroring
		// CloseSession. The namespace error is authoritative and returned as-is.
		if closeErr := call(func() C.KernelStatusCode { return C.kernel_session_close(sess) }); closeErr != nil {
			klog("close after initial-namespace failure also failed: %v", closeErr)
		}
		k.session = nil
		k.valid = false
		return err
	}

	klog("OpenSession OK session=%s", k.sessionID)
	return nil
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
		// public client / default scopes. We pass Go's cloud-inferred client id when
		// set, so the kernel uses the same client id the Thrift path would.
		clientID := newCStrOrNull(k.cfg.Auth.ClientID)
		defer clientID.free()
		scopes := newCStrOrNull(joinScopes(k.cfg.Auth.Scopes))
		defer scopes.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_auth_u2m(cfg, clientID.c, C.uint16_t(k.cfg.Auth.RedirectPort), scopes.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_auth_u2m: %w", toConnError(err))
		}
	default: // AuthPAT
		tok := newCStr(k.cfg.Auth.Token)
		defer tok.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_auth_pat(cfg, tok.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_auth_pat: %w", toConnError(err))
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

// CloseSession tears down the server-side session. Best-effort: the kernel's
// close is async (see the C header), so an error is logged, not hard-failed.
func (k *KernelBackend) CloseSession(ctx context.Context) error {
	if k.session == nil {
		return nil
	}
	klog("CloseSession session=%s", k.sessionID)
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

// evictIfSessionFatal marks the session dead when err is a session-fatal
// KernelError, so a conn whose server session died mid-life is evicted from the
// pool. A no-op for nil, non-KernelError, or non-fatal errors.
func (k *KernelBackend) evictIfSessionFatal(err error) {
	if ke, ok := err.(*KernelError); ok && isSessionFatal(ke.Code) {
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
	return k.execute(ctx, req)
}
