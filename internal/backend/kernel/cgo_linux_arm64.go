//go:build cgo && databricks_kernel && linux && arm64

package kernel

// Link flags for linux/arm64 (e.g. AWS Graviton). Identical in form to
// cgo_linux.go's amd64 flags: the same GNU ld runs on both Linux arches, so the
// -l:<file>.a static-forcing form (which stops the linker preferring a
// same-named .so and baking in an rpath) and the -lstdc++/-lm/-ldl transitive
// system deps carry over unchanged. Only the ${SRCDIR}-relative lib dir differs
// (lib/linux_arm64), where `make kernel-lib` drops a natively built arm64 archive.
//
// NOTE: not yet exercised in CI — no allow-listed linux/arm64 runner exists in the
// org today, so the arm64 kernel .a is built and linked only on a native arm64 host
// (host == target for kernel-lib.sh). The flags are the intended shape; validate on
// an arm64 runner before relying on this in a release.

/*
#cgo LDFLAGS: -L${SRCDIR}/lib/linux_arm64 -l:libdatabricks_sql_kernel.a -lstdc++ -lm -ldl
*/
import "C"
