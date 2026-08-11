//go:build cgo && databricks_kernel && databricks_kernel_dynlib && darwin && arm64

package kernel

// DYNAMIC-LINK variant of cgo_darwin.go (PoC). Instead of statically linking
// libdatabricks_sql_kernel.a into the binary, this links the kernel as a SHARED
// library (libdatabricks_sql_kernel.dylib) that is loaded at run time. Built by
// adding BOTH `-tags databricks_kernel` AND `-tags databricks_kernel_dynlib`;
// the static cgo_darwin.go carries a `!databricks_kernel_dynlib` guard so the
// two never both compile (see its build line).
//
// What differs from the static link:
//   - `-L${SRCDIR}/lib_dyn/darwin_arm64 -ldatabricks_sql_kernel` links against
//     the .dylib (the linker resolves `-lX` to libX.dylib) instead of naming
//     the .a as a positional input. Nothing from the .dylib is copied into the
//     Go binary; only a reference (a load command) is recorded.
//   - `-Wl,-rpath,@loader_path/lib_dyn/darwin_arm64` tells the produced binary
//     WHERE to find the .dylib at run time: @loader_path is the directory of the
//     binary itself, so the .dylib is expected at
//     <binary_dir>/lib_dyn/darwin_arm64/libdatabricks_sql_kernel.dylib. A real
//     release would put the .dylib right next to the binary and use
//     `-rpath,@loader_path`. The dylib's install_name must be
//     `@rpath/libdatabricks_sql_kernel.dylib` (set with install_name_tool -id;
//     it defaults to the absolute build path, which would NOT be relocatable).
//
// The Arrow C-Data import path (rows.go, arrow-go cdata) is UNCHANGED — dynamic
// vs static linking is invisible above the link layer.

/*
#cgo LDFLAGS: -L${SRCDIR}/lib_dyn/darwin_arm64 -ldatabricks_sql_kernel -lc++ -lm -Wl,-rpath,@loader_path/lib_dyn/darwin_arm64 -Wl,-rpath,${SRCDIR}/lib_dyn/darwin_arm64
*/
import "C"
