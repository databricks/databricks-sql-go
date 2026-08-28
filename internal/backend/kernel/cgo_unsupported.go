//go:build cgo && databricks_kernel && !(linux && amd64) && !(linux && arm64) && !(linux && arm) && !(darwin && arm64) && !(darwin && amd64) && !(windows && amd64) && !(windows && arm64)

package kernel

// This file is compiled only on GOOS/GOARCH combinations the kernel backend does
// not support. Per-platform link shims (cgo_<os>[_<arch>].go) exist for
// linux/amd64, linux/arm64, linux/arm, darwin/arm64, darwin/amd64, windows/amd64,
// and windows/arm64; on any other target there is no static archive to link, so
// cgo.go's C ABI calls would otherwise fail at the LINK step with an opaque
// "undefined reference to kernel_*". Referencing an undefined identifier here
// fails earlier, at COMPILE time, with a message that names the supported
// targets — a legible build error instead of a linker dump.
//
// The supported set maps 1:1 to the per-platform modules published by
// github.com/databricks/databricks-sql-kernel-bindings. Adding a platform =
// publish its bindings module + add a cgo_<os>_<arch>.go shim + drop it from this
// exclusion list.
const _ = kernel_backend_supports_only_linux_amd64_linux_arm64_linux_arm_darwin_arm64_darwin_amd64_windows_amd64_and_windows_arm64
