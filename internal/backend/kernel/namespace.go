package kernel

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// quoteIdent is pure Go (no cgo) and is unit-tested in the default CGO_ENABLED=0
// matrix; OpenSession (tagged) calls it to build the USE CATALOG / USE SCHEMA
// statements that select the initial namespace.

import "strings"

// quoteIdent renders name as a backtick-quoted Databricks SQL identifier, doubling
// any embedded backtick. The kernel C ABI exposes no catalog/schema config setter,
// so the initial namespace is applied post-connect by running USE CATALOG / USE
// SCHEMA (the same workaround the OSS ODBC driver uses); quoting makes those
// statements injection-safe for arbitrary identifier text.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
