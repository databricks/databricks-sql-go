package kernel

import (
	"errors"
	"strings"
)

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag:
// the interior-NUL guard on the SQL statement text is pure Go, so it is unit-tested
// under CGO_ENABLED=0 (see sqltext_test.go). The tagged execute path (operation.go)
// calls checkQueryText before newCStr(req.Query).

// errQueryNUL rejects statement text containing an interior NUL. The kernel's
// set_sql C ABI takes the query as a NUL-terminated C string with no length, so a
// NUL would silently truncate it — executing a different, shorter statement than
// intended — whereas the Thrift path sends the statement length-prefixed and runs
// it whole. This is the statement-text counterpart to errParamNUL (bindparams.go),
// which guards bound values for the identical reason. Fail loudly rather than
// diverge from Thrift.
var errQueryNUL = errors.New("query text contains a NUL byte, which the kernel set_sql ABI cannot carry")

// checkQueryText validates the statement text before the cgo layer C-string-marshals
// it. Returns errQueryNUL when the query contains an interior NUL.
func checkQueryText(query string) error {
	if strings.IndexByte(query, 0) >= 0 {
		return errQueryNUL
	}
	return nil
}
