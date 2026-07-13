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

// Config is the flat connection config for the kernel backend. The connector
// fills it from the driver's config so the user-facing options are unchanged
// (this mirrors how the kernel's pyo3/napi bindings take flat connection
// params). Zero-valued fields are simply not applied.
type Config struct {
	Host        string // workspace hostname, no scheme
	HTTPPath    string // e.g. /sql/1.0/warehouses/abc123 (carries ?o= org routing)
	WarehouseID string // bare warehouse id; preferred over HTTPPath when set
	Token       string // PAT (dapi...)

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

	tok := newCStr(k.cfg.Token)
	defer tok.free()
	if err := call(func() C.KernelStatusCode {
		return C.kernel_session_config_set_auth_pat(cfg, tok.c)
	}); err != nil {
		return fmt.Errorf("kernel: set_auth_pat: %w", toConnError(err))
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
	klog("OpenSession OK session=%s", k.sessionID)
	return nil
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
// caller can read StatementID / wrap the error / Close uniformly.
func (k *KernelBackend) Execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	// Bound parameters are not yet wired for the kernel backend. Reject them with
	// a clear error rather than silently shipping the query with unbound
	// placeholders (which would behave differently than Thrift). Parameters arrive
	// per-query, so this is an execute-time error, not a connect-time one. Return a
	// non-nil Operation per the Backend contract.
	if len(req.Params) > 0 {
		return &kernelOp{}, fmt.Errorf("databricks: query parameters are not yet %w; "+
			"inline the values or use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	// Staging (Unity Catalog volume PUT/GET/REMOVE) needs a local file transfer the
	// kernel path can't perform, and the kernel C ABI surfaces no IsStagingOperation
	// signal to drive conn.execStagingOperation. Reject it here rather than let
	// IsStaging return false and report success with no file moved (silent data loss).
	if isStagingStatement(req.Query) {
		return &kernelOp{}, fmt.Errorf("databricks: staging operations (PUT/GET/REMOVE on a volume) are not yet %w; "+
			"use the default (Thrift) backend", dbsqlerr.ErrNotSupportedByKernel)
	}
	return k.execute(ctx, req)
}
