//go:build cgo && databricks_kernel && darwin && arm64

package kernel

// Link flags for darwin/arm64. NOTE: this platform is not yet exercised in CI
// (M0 is linux/amd64); the flags below are the intended shape but must be
// validated on a mac before darwin is enabled.
//
// Two darwin-specific differences from linux:
//   - Apple's ld64 does NOT accept the GNU `-l:<file>.a` extension, so the
//     archive is passed as a positional input by absolute ${SRCDIR} path
//     instead. Since only the .a is placed under lib/darwin_arm64 (see
//     kernel-lib.sh), there is no .so to accidentally prefer.
//   - -lc++ (not -lstdc++) is the macOS C++ runtime; @loader_path keeps any
//     dynamic reference resolvable relative to the built binary.

/*
#cgo LDFLAGS: ${SRCDIR}/lib/darwin_arm64/libdatabricks_sql_kernel.a -lc++ -lm -Wl,-rpath,@loader_path
*/
import "C"
