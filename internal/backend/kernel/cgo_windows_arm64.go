//go:build cgo && databricks_kernel && windows && arm64

package kernel

// windows/arm64 link wiring: blank-import the external per-platform module
// github.com/databricks/databricks-sql-kernel-bindings/lib/windows_arm64 (databricks-sql-kernel-bindings) so cgo collects its
// `#cgo LDFLAGS` at link time and pulls libdatabricks_sql_kernel.a into the
// binary. Same build constraint as that module's prebuilt.go.
import _ "github.com/databricks/databricks-sql-kernel-bindings/lib/windows_arm64"
