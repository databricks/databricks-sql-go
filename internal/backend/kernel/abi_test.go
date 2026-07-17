package kernel

import (
	"strings"
	"testing"
)

// TestCompareABI covers both verdicts of the ABI-version handshake without the
// cgo symbols, so it runs under the default CGO_ENABLED=0 build. The matching
// (happy) path is additionally proven against the real linked library by
// TestABIVersionMatches in the cgo build; the mismatch path — the runtime hazard
// the check exists for (a driver header linked against a differently-built
// prebuilt .a) — is otherwise unreachable from a test, since a real build always
// links a matching .a + header.
func TestCompareABI(t *testing.T) {
	// Matching versions must produce no error: checkABIVersion opens the session.
	for _, v := range []uint32{0, 1, 42} {
		if err := compareABI(v, v); err != nil {
			t.Errorf("compareABI(%d, %d) = %v, want nil for matching versions", v, v, err)
		}
	}

	// Mismatched versions must produce an error so checkABIVersion refuses to open
	// (rather than silently misread status codes / error-struct fields). The
	// message must name both versions and the remediation so an operator hitting a
	// stale prebuilt .a knows what to rebuild.
	err := compareABI(2, 1)
	if err == nil {
		t.Fatal("compareABI(2, 1) = nil, want an error for mismatched versions")
	}
	msg := err.Error()
	for _, want := range []string{"ABI version mismatch", "reports 2", "expects 1", "make kernel-lib"} {
		if !strings.Contains(msg, want) {
			t.Errorf("mismatch error %q does not contain %q", msg, want)
		}
	}
}
