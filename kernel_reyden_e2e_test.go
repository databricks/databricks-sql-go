//go:build cgo && databricks_kernel

package dbsql

import (
	"os"
	"strings"
	"testing"
)

// Reyden is the SEA reference engine — the same kernel path as the normal kernel E2E
// suite, just pointed at a different read-only warehouse. The leg reuses the
// TestKernelE2E* funcs verbatim and only changes the target warehouse, mirroring the
// databricks-driver-test reyden leg. Selected by TEST_WAREHOUSE=reyden +
// DATABRICKS_REYDEN_HTTP_PATH; absent either, isReydenLeg() is false and the suite is
// inert (runs on the normal warehouse exactly as before).

// reydenHTTPPath returns the Reyden warehouse HTTP path from
// DATABRICKS_REYDEN_HTTP_PATH, or "" when unset.
func reydenHTTPPath() string {
	return os.Getenv("DATABRICKS_REYDEN_HTTP_PATH")
}

// isReydenLeg reports whether this run targets Reyden: TEST_WAREHOUSE=reyden AND a
// reyden path is configured. When false the kernel suite uses the normal warehouse.
func isReydenLeg() bool {
	return strings.EqualFold(os.Getenv("TEST_WAREHOUSE"), "reyden") && reydenHTTPPath() != ""
}

// skipOnReyden skips a test on the reyden leg for a surface Reyden genuinely doesn't
// support (documented reyden-vs-dbsql divergence). No-op off the reyden leg.
func skipOnReyden(t *testing.T, reason string) {
	t.Helper()
	if isReydenLeg() {
		t.Skipf("reyden-skip: %s", reason)
	}
}

// TestIsReydenLeg pins the leg selector: the leg is active only when
// TEST_WAREHOUSE=reyden (case-insensitively) AND a reyden path is configured, so a
// missing path or a non-reyden warehouse always falls back to the normal warehouse.
// Pure env logic — no warehouse needed, so it runs in the kernel unit-test job.
func TestIsReydenLeg(t *testing.T) {
	const path = "/sql/1.0/warehouses/reyden123"
	cases := []struct {
		name      string
		warehouse string
		reydenEnv string
		want      bool
	}{
		{"reyden + path", "reyden", path, true},
		{"case-insensitive warehouse", "ReYdEn", path, true},
		{"reyden, no path", "reyden", "", false},
		{"path set, non-reyden warehouse", "dbsql", path, false},
		{"path set, warehouse unset", "", path, false},
		{"both unset", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("TEST_WAREHOUSE", c.warehouse)
			t.Setenv("DATABRICKS_REYDEN_HTTP_PATH", c.reydenEnv)
			if got := isReydenLeg(); got != c.want {
				t.Errorf("isReydenLeg() = %v, want %v", got, c.want)
			}
			if got := reydenHTTPPath(); got != c.reydenEnv {
				t.Errorf("reydenHTTPPath() = %q, want %q", got, c.reydenEnv)
			}
		})
	}
}

// TestSkipOnReyden proves the gate skips iff the reyden leg is active. On the leg,
// skipOnReyden must t.Skip (runtime.Goexit), so the line after it is unreachable; off
// the leg it must be a no-op and execution must continue past it.
func TestSkipOnReyden(t *testing.T) {
	t.Run("on-leg skips", func(t *testing.T) {
		t.Setenv("TEST_WAREHOUSE", "reyden")
		t.Setenv("DATABRICKS_REYDEN_HTTP_PATH", "/sql/1.0/warehouses/reyden123")
		skipOnReyden(t, "unit-test divergence")
		t.Error("skipOnReyden did not skip on the reyden leg") // unreachable if it skipped
	})
	t.Run("off-leg is a no-op", func(t *testing.T) {
		t.Setenv("TEST_WAREHOUSE", "")
		t.Setenv("DATABRICKS_REYDEN_HTTP_PATH", "")
		skipOnReyden(t, "unit-test divergence")
		if t.Skipped() {
			t.Error("skipOnReyden skipped off the reyden leg")
		}
	})
}
