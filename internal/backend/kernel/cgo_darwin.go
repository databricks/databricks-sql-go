//go:build cgo && databricks_kernel && darwin && arm64

package kernel

// darwin/arm64 link wiring. The kernel static archive and its `#cgo LDFLAGS`
// live in the external per-platform module github.com/databricks/databricks-sql-kernel-bindings/lib/darwin_arm64 (the
// databricks-sql-kernel-bindings repo). This file blank-imports it for the link
// side-effect: cgo collects that module's `#cgo LDFLAGS` at final link time,
// pulling libdatabricks_sql_kernel.a into the binary. Same build constraint as
// that module's prebuilt.go so the two are always selected together.
import _ "github.com/databricks/databricks-sql-kernel-bindings/lib/darwin_arm64"
