package dbsql

import (
	"context"
	"errors"
	"net/http"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/backend"
	"github.com/databricks/databricks-sql-go/internal/backend/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/internal/warehouse_cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckStatusReydenDetection tests that CheckStatus detects SQLSTATE KP001
// and returns a distinct error vs. other ERROR_STATUS codes.
func TestCheckStatusReydenDetection(t *testing.T) {
	t.Run("KP001 on ERROR_STATUS returns ReydenThriftUnsupported marker", func(t *testing.T) {
		sqlState := "KP001"
		errMsg := "Lakehouse/RT is not supported for Thrift protocol"
		reydenResp := &cli_service.TOpenSessionResp{
			Status: &cli_service.TStatus{
				StatusCode:   cli_service.TStatusCode_ERROR_STATUS,
				SqlState:     &sqlState,
				ErrorMessage: &errMsg,
			},
		}

		err := client.CheckStatus(reydenResp)
		require.Error(t, err)
		// Verify it's the Reyden marker.
		assert.True(t, errors.Is(err, dbsqlerr.ErrReydenThriftUnsupported),
			"error should satisfy errors.Is for ErrReydenThriftUnsupported")
	})

	t.Run("non-KP001 ERROR_STATUS returns generic error, NOT Reyden marker", func(t *testing.T) {
		sqlState := "42000" // Syntax error
		errMsg := "a syntax error"
		syntaxResp := &cli_service.TOpenSessionResp{
			Status: &cli_service.TStatus{
				StatusCode:   cli_service.TStatusCode_ERROR_STATUS,
				SqlState:     &sqlState,
				ErrorMessage: &errMsg,
			},
		}

		err := client.CheckStatus(syntaxResp)
		require.Error(t, err)
		// Verify it's NOT the Reyden marker.
		assert.False(t, errors.Is(err, dbsqlerr.ErrReydenThriftUnsupported),
			"generic error should NOT satisfy errors.Is for ErrReydenThriftUnsupported")
	})

	t.Run("SUCCESS_STATUS returns no error", func(t *testing.T) {
		successResp := &cli_service.TOpenSessionResp{
			Status: &cli_service.TStatus{
				StatusCode: cli_service.TStatusCode_SUCCESS_STATUS,
			},
		}

		err := client.CheckStatus(successResp)
		assert.NoError(t, err)
	})
}

// fakeKernelBackend is a mock kernel backend for testing the fallback logic.
type fakeKernelBackend struct {
	openSessionErr error
	sessionID      string
}

var _ backend.Backend = (*fakeKernelBackend)(nil)

func (f *fakeKernelBackend) OpenSession(ctx context.Context) error {
	if f.openSessionErr != nil {
		return f.openSessionErr
	}
	f.sessionID = "kernel-sess-id"
	return nil
}

func (f *fakeKernelBackend) CloseSession(ctx context.Context) error {
	return nil
}

func (f *fakeKernelBackend) SessionValid() bool {
	return f.sessionID != ""
}

func (f *fakeKernelBackend) SessionID() string {
	return f.sessionID
}

func (f *fakeKernelBackend) Execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	return nil, errors.New("not implemented")
}

// fakeThriftBackend is a mock Thrift backend that can be configured to fail with KP001.
type fakeThriftBackend struct {
	openSessionErr error
	sessionID      string
}

var _ backend.Backend = (*fakeThriftBackend)(nil)

func (f *fakeThriftBackend) OpenSession(ctx context.Context) error {
	return f.openSessionErr
}

func (f *fakeThriftBackend) CloseSession(ctx context.Context) error {
	return nil
}

func (f *fakeThriftBackend) SessionValid() bool {
	return f.sessionID != ""
}

func (f *fakeThriftBackend) SessionID() string {
	return f.sessionID
}

func (f *fakeThriftBackend) Execute(ctx context.Context, req backend.ExecRequest) (backend.Operation, error) {
	return nil, errors.New("not implemented")
}

// TestReydenFallback tests the connector's openSessionWithReydenFallback logic.
type TestReydenFallback struct {
	host          string
	warehousePath string
	warehouseID   string
}

func NewTestReydenFallback() *TestReydenFallback {
	return &TestReydenFallback{
		host:          "reyden.example.com",
		warehousePath: "/sql/1.0/warehouses/wh-reyden",
		warehouseID:   "wh-reyden",
	}
}

// makeConnector builds a connector with injected backend factories. Either
// factory may be nil (the path that isn't exercised by a given test).
func (t *TestReydenFallback) makeConnector(
	thriftFactory thriftBackendFactory,
	kernelFactory backendFactory,
) *connector {
	cfg := config.WithDefaults()
	cfg.Host = t.host
	cfg.HTTPPath = t.warehousePath
	return &connector{
		cfg:                  cfg,
		thriftBackendFactory: thriftFactory,
		kernelBackendFactory: kernelFactory,
	}
}

func TestReydenReactiveRecovery(t *testing.T) {
	t.Run("Thrift KP001 rejection recovers onto the kernel backend", func(t *testing.T) {
		defer warehouse_cache.ClearCache()

		tt := NewTestReydenFallback()

		// Fake Thrift backend that rejects OpenSession with the Reyden marker.
		fakeThrift := &fakeThriftBackend{
			openSessionErr: dbsqlerrint.NewReydenThriftUnsupportedError(
				"Lakehouse/RT is not supported for Thrift protocol"),
		}
		kernelBackend := &fakeKernelBackend{}

		conn := tt.makeConnector(
			func(ctx context.Context, cfg *config.Config, client *http.Client) (backend.Backend, error) {
				return fakeThrift, nil
			},
			func(ctx context.Context, cfg *config.Config) (backend.Backend, error) {
				return kernelBackend, nil
			},
		)

		be, _, err := conn.openSessionWithReydenFallback(context.Background())
		require.NoError(t, err)

		// The returned backend is the kernel one, with an open session.
		kb, ok := be.(*fakeKernelBackend)
		require.True(t, ok, "recovery should return the kernel backend, got %T", be)
		assert.Same(t, kernelBackend, kb)
		assert.True(t, kb.SessionValid(), "kernel session should be open")

		// The rejection is remembered for future connects.
		assert.True(t, warehouse_cache.IsKnownReyden(tt.host, tt.warehouseID),
			"warehouse should be marked Reyden after the rejection")
	})
}

func TestReydenPreCheck(t *testing.T) {
	t.Run("Known Reyden warehouse opens kernel directly without Thrift attempt", func(t *testing.T) {
		defer warehouse_cache.ClearCache()

		tt := NewTestReydenFallback()

		// Mark the warehouse as known-Reyden so the pre-check fires.
		warehouse_cache.MarkReyden(tt.host, tt.warehouseID)

		thriftCalled := false
		kernelCalled := false
		conn := tt.makeConnector(
			func(ctx context.Context, cfg *config.Config, client *http.Client) (backend.Backend, error) {
				thriftCalled = true
				return &fakeThriftBackend{}, nil
			},
			func(ctx context.Context, cfg *config.Config) (backend.Backend, error) {
				kernelCalled = true
				return &fakeKernelBackend{}, nil
			},
		)

		be, latency, err := conn.openSessionWithReydenFallback(context.Background())

		assert.NoError(t, err)
		assert.NotNil(t, be)
		assert.True(t, kernelCalled, "pre-check should open the kernel backend")
		assert.False(t, thriftCalled, "pre-check must skip the Thrift OpenSession round-trip")
		assert.GreaterOrEqual(t, latency, int64(0), "latency should be non-negative")
	})
}

func TestReydenCacheMarking(t *testing.T) {
	t.Run("Rejection marks warehouse in cache", func(t *testing.T) {
		defer warehouse_cache.ClearCache()

		tt := NewTestReydenFallback()

		// Verify warehouse is not initially marked.
		assert.False(t, warehouse_cache.IsKnownReyden(tt.host, tt.warehouseID))

		// Mark it.
		warehouse_cache.MarkReyden(tt.host, tt.warehouseID)

		// Verify it's now marked.
		assert.True(t, warehouse_cache.IsKnownReyden(tt.host, tt.warehouseID))
	})

	t.Run("Warehouse ID extraction from HTTP path", func(t *testing.T) {
		tests := []struct {
			path     string
			expected string
		}{
			{"/sql/1.0/warehouses/wh-123", "wh-123"},
			{"/sql/1.0/endpoints/ep-456", "ep-456"},
			{"/sql/1.0/warehouses/wh-123?o=789", "wh-123"},
			{"/sql/protocolv1/o/123/cluster", ""}, // Cluster path, no warehouse ID
		}

		for _, tc := range tests {
			t.Run(tc.path, func(t *testing.T) {
				got := warehouse_cache.ExtractWarehouseID(tc.path)
				assert.Equal(t, tc.expected, got)
			})
		}
	})
}

func TestReydenGuardrail(t *testing.T) {
	t.Run("Explicit UseKernel goes straight to kernel, never touching Thrift", func(t *testing.T) {
		defer warehouse_cache.ClearCache()

		tt := NewTestReydenFallback()

		thriftCalled := false
		kernelBackend := &fakeKernelBackend{}
		conn := tt.makeConnector(
			func(ctx context.Context, cfg *config.Config, client *http.Client) (backend.Backend, error) {
				thriftCalled = true
				return &fakeThriftBackend{}, nil
			},
			func(ctx context.Context, cfg *config.Config) (backend.Backend, error) {
				return kernelBackend, nil
			},
		)
		conn.cfg.UseKernel = true // explicit backend selection

		be, _, err := conn.openSessionWithReydenFallback(context.Background())
		require.NoError(t, err)
		assert.False(t, thriftCalled, "explicit UseKernel must not attempt Thrift")
		_, ok := be.(*fakeKernelBackend)
		assert.True(t, ok, "explicit UseKernel should return the kernel backend, got %T", be)
	})
}

func TestReydenNonReydenError(t *testing.T) {
	t.Run("Non-KP001 Thrift error propagates unchanged, no kernel fallback", func(t *testing.T) {
		defer warehouse_cache.ClearCache()

		tt := NewTestReydenFallback()

		genericErr := errors.New("some server error")
		kernelCalled := false
		conn := tt.makeConnector(
			func(ctx context.Context, cfg *config.Config, client *http.Client) (backend.Backend, error) {
				return &fakeThriftBackend{openSessionErr: genericErr}, nil
			},
			func(ctx context.Context, cfg *config.Config) (backend.Backend, error) {
				kernelCalled = true
				return &fakeKernelBackend{}, nil
			},
		)

		_, _, err := conn.openSessionWithReydenFallback(context.Background())
		require.Error(t, err)
		assert.Same(t, genericErr, err, "the original Thrift error should propagate unchanged")
		assert.False(t, kernelCalled, "a non-Reyden error must not trigger the kernel fallback")
		assert.False(t, warehouse_cache.IsKnownReyden(tt.host, tt.warehouseID),
			"a non-Reyden error must not mark the warehouse")
	})
}

func TestReydenDoubleFailureChaining(t *testing.T) {
	t.Run("Kernel open failure after Thrift rejection chains both errors", func(t *testing.T) {
		defer warehouse_cache.ClearCache()

		tt := NewTestReydenFallback()

		thriftErr := dbsqlerrint.NewReydenThriftUnsupportedError(
			"Lakehouse/RT is not supported for Thrift protocol")
		kernelErr := errors.New("kernel open failed")
		conn := tt.makeConnector(
			func(ctx context.Context, cfg *config.Config, client *http.Client) (backend.Backend, error) {
				return &fakeThriftBackend{openSessionErr: thriftErr}, nil
			},
			func(ctx context.Context, cfg *config.Config) (backend.Backend, error) {
				return &fakeKernelBackend{openSessionErr: kernelErr}, nil
			},
		)

		_, _, err := conn.openSessionWithReydenFallback(context.Background())
		require.Error(t, err)
		// Both the kernel failure (the actionable one) and the original Thrift
		// rejection must be reachable in the returned error chain.
		assert.ErrorIs(t, err, kernelErr, "chain should contain the kernel failure")
		assert.ErrorIs(t, err, dbsqlerr.ErrReydenThriftUnsupported,
			"chain should preserve the Thrift rejection marker")
	})
}

func TestReydenSkipDriverTelemetryFollowsActualBackend(t *testing.T) {
	// Driver telemetry must be skipped whenever the ACTIVE backend is the
	// kernel — including after a Reyden recovery, where cfg.UseKernel stays
	// false. The decision is derived from the backend, not the config.
	assert.True(t, shouldSkipDriverTelemetry(&fakeKernelBackend{}),
		"kernel backend owns telemetry; driver telemetry should be skipped")
	assert.False(t, shouldSkipDriverTelemetry(&thrift.Backend{}),
		"thrift backend: driver telemetry should stay active")
}

func TestReydenDefaultBuildKernelNotCompiled(t *testing.T) {
	t.Run("Default build's newKernelBackend returns 'not compiled' error", func(t *testing.T) {
		// In the default pure-Go build, newKernelBackend returns an error indicating
		// the kernel backend is not compiled in. This naturally exercises the
		// error-chaining path for double-failure scenarios.
		cfg := config.WithDefaults()
		cfg.UseKernel = true

		// Attempt to create a kernel backend in the default build.
		// (This would fail with ErrKernelNotCompiled outside of a
		// databricks_kernel+CGO_ENABLED=1 build.)
		be, err := newKernelBackend(context.Background(), cfg)

		// In the default build, this should fail.
		assert.Nil(t, be)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, dbsqlerr.ErrKernelNotCompiled),
			"default build should not have kernel backend compiled in")
	})
}
