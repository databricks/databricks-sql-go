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

	"github.com/databricks/databricks-sql-go/internal/backend"
)

// Config is the connection config for the kernel backend: host, the warehouse
// (by bare id or http path), and a PAT.
type Config struct {
	Host        string // workspace hostname, no scheme
	HTTPPath    string // e.g. /sql/1.0/warehouses/abc123 (carries ?o= org routing)
	WarehouseID string // bare warehouse id; preferred over HTTPPath when set
	Token       string // PAT (dapi...)
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
	klog("OpenSession host=%s httpPath=%s warehouse=%s", k.cfg.Host, k.cfg.HTTPPath, k.cfg.WarehouseID)

	var cfg *C.KernelSessionConfig
	if err := call(func() C.KernelStatusCode { return C.kernel_session_config_new(&cfg) }); err != nil {
		return fmt.Errorf("kernel: config_new: %w", toDriverError(err))
	}
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
			return fmt.Errorf("kernel: set_warehouse: %w", toDriverError(err))
		}
	} else {
		path := newCStr(k.cfg.HTTPPath)
		defer path.free()
		if err := call(func() C.KernelStatusCode {
			return C.kernel_session_config_set_http_path(cfg, host.c, path.c)
		}); err != nil {
			return fmt.Errorf("kernel: set_http_path: %w", toDriverError(err))
		}
	}

	tok := newCStr(k.cfg.Token)
	defer tok.free()
	if err := call(func() C.KernelStatusCode {
		return C.kernel_session_config_set_auth_pat(cfg, tok.c)
	}); err != nil {
		return fmt.Errorf("kernel: set_auth_pat: %w", toDriverError(err))
	}

	var sess *C.kernel_session_t
	if err := call(func() C.KernelStatusCode { return C.kernel_session_open(cfg, &sess) }); err != nil {
		return fmt.Errorf("kernel: session_open: %w", toDriverError(err))
	}
	consumed = true // kernel_session_open took ownership of cfg
	k.session = sess
	k.valid = true
	// The C ABI exposes no formatted session-id accessor; use the handle pointer
	// as a stable per-conn id for logging / telemetry correlation.
	k.sessionID = fmt.Sprintf("kernel-%p", sess)
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
	return toDriverError(err)
}

// SessionValid backs conn.IsValid → pool eviction. No I/O; inspects state
// captured at OpenSession.
func (k *KernelBackend) SessionValid() bool { return k.valid && k.session != nil }

// SessionID is the per-conn id (conn.id). Valid after OpenSession.
func (k *KernelBackend) SessionID() string { return k.sessionID }

// Execute runs a statement to a terminal state via the blocking execute path.
// Per the Backend contract it returns a non-nil Operation even on error so the
// caller can read StatementID / wrap the error / Close uniformly.
func (k *KernelBackend) Execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	return k.execute(ctx, req)
}
