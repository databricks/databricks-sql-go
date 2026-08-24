//go:build cgo && databricks_kernel && windows && amd64

package kernel

// Link flags for windows/amd64, validated on a windows-server-latest CI runner
// (see the build-and-test-kernel-windows job).
//
// cgo on windows uses the mingw/gcc toolchain, which links GNU archives (.a) —
// NOT the MSVC .lib that `cargo build --target x86_64-pc-windows-msvc` emits.
// So the kernel must be built for the windows-gnu target
// (`--target x86_64-pc-windows-gnu`) to produce a mingw-compatible .a; that
// target selection is the build step's responsibility.
//
// System libs the kernel's Rust std/dep graph pulls in on windows (mingw does
// NOT auto-link these transitively, so each must be named explicitly or the
// final cgo link fails with `undefined reference`):
//   -lws2_32 / -lwsock32  Winsock (sockets)
//   -lrstrtmgr            Restart Manager
//   -lbcrypt              BCryptGenRandom — Rust's getrandom crate (RNG)
//   -lntdll               NtCreateNamedPipeFile — Rust std child_pipe (native NT API)

/*
#cgo LDFLAGS: -L${SRCDIR}/lib/windows_amd64 -l:libdatabricks_sql_kernel.a -lws2_32 -lwsock32 -lrstrtmgr -lbcrypt -lntdll
*/
import "C"
