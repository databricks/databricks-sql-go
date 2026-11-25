//go:build cgo && databricks_kernel

package kernel

import (
	"context"
	"errors"
	"strings"
	"testing"

	dbsqlerr "github.com/databricks/databricks-sql-go/v2/errors"
)

const portTestSecret = "pSecretPw"

// TestOpenSessionPortRejection checks the WithPort contract end to end through the real
// kernel. OpenSession sets the host before the port, so the host setter has no port to
// check yet. The kernel's kernel_session_config_set_port then rejects the credentialed
// host with InvalidArgument, before any connection is made. The error must wrap both
// sentinels and must not carry the credentials.
func TestOpenSessionPortRejection(t *testing.T) {
	k := New(Config{
		Host:        "user:" + portTestSecret + "@h.databricks.com",
		Port:        8443,
		WarehouseID: "wh",
		Auth:        Auth{Mode: AuthPAT, Token: "test-token"},
	})
	err := k.OpenSession(context.Background())
	if err == nil {
		t.Fatal("OpenSession accepted credentials with a custom port; want the kernel to reject it")
	}
	if !errors.Is(err, dbsqlerr.ErrInvalidKernelConfig) || !errors.Is(err, dbsqlerr.ErrNotSupportedByKernel) {
		t.Errorf("err = %v, want both ErrInvalidKernelConfig and ErrNotSupportedByKernel", err)
	}
	if msg := err.Error(); strings.Contains(msg, portTestSecret) || strings.Contains(msg, "user:") {
		t.Errorf("error leaks credentials: %q", msg)
	}
}
