package kernel

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag, so
// the per-column type mapping that newKernelRows uses is unit-tested in the default
// CGO_ENABLED=0 build.

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/databricks/databricks-sql-go/internal/arrowscan"
)

// columnTypeInfos derives the database/sql type metadata for each top-level field of a
// result schema. variantTypeName reports VARIANT columns by name (the opt-in
// WithEnableVariantTypeName); every other column keeps its ColumnTypeInfoForField mapping.
func columnTypeInfos(fields []arrow.Field, variantTypeName bool) []arrowscan.ColumnTypeInfo {
	infos := make([]arrowscan.ColumnTypeInfo, len(fields))
	for i, f := range fields {
		infos[i] = arrowscan.ColumnTypeInfoForField(f)
		if variantTypeName {
			infos[i] = arrowscan.ApplyVariantTypeName(infos[i], f)
		}
	}
	return infos
}
