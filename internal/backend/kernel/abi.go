package kernel

import "fmt"

// compareABI reports whether the C-ABI version compiled into the linked kernel
// library (got) matches the version the driver's header declares (want): it
// returns a descriptive, operator-facing error on mismatch and nil when they
// agree.
//
// It is split out from checkABIVersion (in the cgo-tagged cgo.go) as pure,
// cgo-free logic so the mismatch verdict and its message are unit-testable under
// the default CGO_ENABLED=0 build. The negative path can't otherwise be reached
// from a test: a real build always links a .a and header produced together, and
// abiVersions() reads fixed cgo constants with no injection seam. checkABIVersion
// runs abiVersions() through this once per process (sync.Once-cached), on the
// first connect.
func compareABI(got, want uint32) error {
	if got != want {
		return fmt.Errorf("databricks: kernel ABI version mismatch: linked library reports %d, "+
			"driver header expects %d; rebuild the kernel static lib and header together (make kernel-lib)", got, want)
	}
	return nil
}
