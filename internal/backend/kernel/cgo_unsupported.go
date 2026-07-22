//go:build cgo && databricks_kernel && !(linux && amd64) && !(linux && arm64) && !(darwin && arm64) && !(windows && amd64)

package kernel

// This file is compiled only on GOOS/GOARCH combinations the kernel backend does
// not support. Per-platform link flags (cgo_<os>.go) exist only for linux/amd64,
// linux/arm64, darwin/arm64, and windows/amd64; on any other target there is no
// static archive to link, so cgo.go's C ABI calls would otherwise fail at the LINK
// step with an opaque "undefined reference to kernel_*". The Makefile's host==target
// guard does not catch this — it happily source-builds a host .a on e.g. an Intel
// Mac, only for the link to fall over with no matching LDFLAGS file. Referencing an
// undefined identifier here fails earlier, at COMPILE time, with a message that
// names the supported targets — a legible build error instead of a linker dump.
//
// Broader OS/arch coverage is tracked in the distribution design (native per-OS
// runners or a staged prebuilt .a); until then this guard makes the supported
// boundary explicit rather than latent.
const _ = kernel_backend_supports_only_linux_amd64_linux_arm64_darwin_arm64_and_windows_amd64
