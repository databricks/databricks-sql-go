//go:build cgo && databricks_kernel

package kernel

/*
#include <stdlib.h>
#include "databricks_kernel.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

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

// OpenSession builds a session config (warehouse/http-path + PAT), opens the
// session, and captures a per-conn id. Called once by the connector at connect
// time. The config handle is consumed by kernel_session_open on success and
// freed by us on any earlier failure.
func (k *KernelBackend) OpenSession(ctx context.Context) error {
	// Fail fast on an already-cancelled context before doing any work, then honor
	// a deadline that fires mid-connect via the ctxWatcher below: kernel_session
	// _open blocks the calling thread inside the C ABI, so a slow warehouse
	// cold-start or a connect-time network partition can't be interrupted by the
	// caller's ctx unless a cancel token is fired from another thread.
	if err := ctx.Err(); err != nil {
		return err
	}
	// Verify the linked kernel library's C-ABI version matches the header the
	// driver compiled against before making any other kernel call — a mismatch
	// means every status code / error struct we read afterward could be
	// misinterpreted. Runs once per process; cheap and cached thereafter.
	if err := checkABIVersion(); err != nil {
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

	// Experimental kernel-only TLS: a custom CA bundle and an independent hostname
	// skip (finer-grained than the blanket InsecureSkipVerify above). The kernel's
	// rustls stack ignores SSL_CERT_FILE, so a custom CA must be handed over
	// explicitly.
	if err := k.applyKernelTLS(cfg); err != nil {
		return err
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

	// Experimental kernel-only CloudFetch toggle (WithKernelCloudFetch). Tri-state:
	// only set it when the caller did, so an unset value leaves the kernel default
	// (CloudFetch on). Do NOT route this through set_session_conf — the kernel owns
	// the can_cloud_download conf and the server rejects it as not user-settable.
	if k.cfg.CloudFetchEnabled != nil {
		enabled := C.bool(*k.cfg.CloudFetchEnabled)
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_cloudfetch_enabled(cfg, enabled)
		}); err != nil {
			return fmt.Errorf("kernel: set_cloudfetch_enabled: %w", toConnError(err))
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

	// kernel_session_open takes ownership of cfg here. Its documented C-ABI
	// contract (databricks_kernel.h: "CONSUMES config on both success and failure
	// — do not use or free config afterwards") is what makes the unconditional
	// consumed=true correct: mark it before checking the error so the deferred
	// free never double-frees the already-consumed config. This rests on that
	// header guarantee, NOT an assumption — if a future kernel revision validated
	// args and returned before consuming, this would leak, so the contract must be
	// re-verified against the header when KERNEL_REV is bumped.
	var sess *C.kernel_session_t
	// Bridge ctx onto a cancel token so a deadline firing mid-connect drops the
	// in-flight connect request rather than blocking until the kernel returns. A
	// non-cancellable ctx yields a nil watcher → NULL token → the plain open path,
	// so there is no watcher overhead on a background context.
	watcher := newCtxWatcher(ctx)
	defer watcher.stop()
	err := call(func() C.KernelStatusCode {
		return C.kernel_session_open_cancellable(cfg, &sess, watcher.tokenPtr())
	})
	consumed = true
	if err != nil {
		// Prefer the caller's ctx error when the connect was interrupted by the
		// deadline; cancelledErr holds the shared dual-%w wrap (see its doc) so
		// errors.Is still matches the ctx error AND the connect failure's server
		// diagnostics stay reachable via errors.As.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return cancelledErr("session_open", ctxErr, toConnError(err))
		}
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
// config: a custom CA bundle, an independent hostname-skip, and an mTLS client
// certificate + key. Each is a no-op when its field is unset, so this is safe to
// call unconditionally.
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
	if k.cfg.TLSSkipHostnameVerify {
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_tls_skip_hostname_verification(cfg, C.bool(true))
		}); err != nil {
			return fmt.Errorf("kernel: set_tls_skip_hostname_verification: %w", toConnError(err))
		}
	}
	// mTLS client identity. The cert and key travel as a pair (WithKernelClientCertificate
	// sets both or neither), forwarded via the single paired setter; checking the
	// cert is enough. The key bytes go to owned Rust memory and are never logged.
	if len(k.cfg.TLSClientCertPEM) > 0 {
		cert := newCBytes(k.cfg.TLSClientCertPEM)
		defer cert.free()
		key := newCBytes(k.cfg.TLSClientKeyPEM)
		defer key.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_tls_client_certificate(cfg, cert.ptr, cert.len, key.ptr, key.len)
		}); err != nil {
			return fmt.Errorf("kernel: set_tls_client_certificate: %w", toConnError(err))
		}
	}
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

// CloseSession tears down the server-side session. Best-effort: kernel_session
// _close initiates the delete without waiting (see the C header), so it does not
// block and an error is logged, not hard-failed.
//
// Stays fire-and-forget deliberately. The C ABI also offers
// kernel_session_close_blocking (awaits DeleteSession, for Python/Node parity),
// but adopting it here would make close a blocking network round-trip with no
// deadline honored — a stalled close (shutdown-time network partition) would then
// block database/sql pool cleanup. Swapping to it is grouped with the
// cancellable-close follow-up so the blocking close ships with a ctx bridge; until
// then fire-and-forget is the safer default.
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
