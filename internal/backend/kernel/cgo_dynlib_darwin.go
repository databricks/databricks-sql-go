//go:build cgo && databricks_kernel && databricks_kernel_dynlib && darwin && arm64

package kernel

// DYNAMIC-LINK variant for darwin/arm64: link the kernel as a SHARED library
// (libdatabricks_sql_kernel.dylib) loaded at run time, instead of statically
// linking the .a into the binary. Selected by adding BOTH `-tags
// databricks_kernel` and `-tags databricks_kernel_dynlib`; the static
// cgo_darwin.go is guarded with `!databricks_kernel_dynlib` so exactly one link
// variant compiles.
//
// The kernel is the closed-source shared library; it ships as a separate,
// signed, per-OS artifact next to the application binary (see
// DYNAMIC_LINK_RELEASE.md). Dynamic linking keeps that proprietary blob OUT of
// every consumer binary and lets it be security-patched / swapped without a
// driver rebuild, and lets ODBC and the Go driver share one artifact.
//
// Link flags:
//   - `-L${SRCDIR}/lib/darwin_arm64 -ldatabricks_sql_kernel` links against the
//     .dylib (the linker resolves `-lX` to libX.dylib), recording only a
//     reference — nothing from the kernel is copied into the Go binary.
//   - `-Wl,-rpath,@loader_path` tells the produced binary to look for the .dylib
//     in its OWN directory at run time (the release layout: .dylib beside the
//     binary). A second rpath at ${SRCDIR}/lib/darwin_arm64 lets `go test` find
//     the staged .dylib during development.
//
// The .dylib's install_name MUST be `@rpath/libdatabricks_sql_kernel.dylib`
// (cargo's default is the absolute build path, which is not relocatable). The
// kernel build sets this; `make kernel-lib` verifies it. See DYNAMIC_LINK_RELEASE.md.
//
// The Arrow C-Data import path (rows.go, arrow-go cdata) is UNCHANGED — dynamic
// vs static linking is invisible above the link layer.

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo LDFLAGS: -L${SRCDIR}/lib/darwin_arm64 -ldatabricks_sql_kernel -lc++ -lm -Wl,-rpath,@loader_path -Wl,-rpath,${SRCDIR}/lib/darwin_arm64
#include "databricks_kernel.h"
*/
import "C"
