//go:build cgo && databricks_kernel && windows && amd64

package kernel

// Link flags for windows/amd64. NOTE: this platform is not yet exercised in CI
// (M0 is linux/amd64); the flags below are the intended shape but must be
// validated on windows before it is enabled.
//
// cgo on windows uses the mingw/gcc toolchain, which links GNU archives (.a) —
// NOT the MSVC .lib that `cargo build --target x86_64-pc-windows-msvc` emits.
// So the kernel must be built for the windows-gnu target
// (`--target x86_64-pc-windows-gnu`) to produce a mingw-compatible .a; that
// target selection is the build step's responsibility. -lws2_32/-lwsock32 are
// the kernel's Winsock deps and -lrstrtmgr is Restart Manager (pulled in by the
// Rust std/dep graph on windows).

/*
#cgo LDFLAGS: -L${SRCDIR}/lib/windows_amd64 -l:libdatabricks_sql_kernel.a -lws2_32 -lwsock32 -lrstrtmgr
*/
import "C"
