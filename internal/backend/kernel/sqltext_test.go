package kernel

import (
	"errors"
	"testing"
)

// checkQueryText must reject statement text with an interior NUL (the kernel's
// set_sql ABI would truncate it, running a shorter statement than Thrift) but
// accept ordinary queries and an empty string. Runs under CGO_ENABLED=0.
func TestCheckQueryText(t *testing.T) {
	if err := checkQueryText("SELECT 1"); err != nil {
		t.Errorf("plain query: got %v, want nil", err)
	}
	if err := checkQueryText(""); err != nil {
		t.Errorf("empty query: got %v, want nil", err)
	}
	if err := checkQueryText("SELECT '\x00'"); !errors.Is(err, errQueryNUL) {
		t.Errorf("interior NUL: got %v, want errQueryNUL", err)
	}
	// A NUL anywhere (including mid-identifier) must be caught, not just leading.
	if err := checkQueryText("SELECT 1\x00; DROP"); !errors.Is(err, errQueryNUL) {
		t.Errorf("embedded NUL: got %v, want errQueryNUL", err)
	}
}
