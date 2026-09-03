package kernel

import (
	"errors"
	"strings"
)

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag:
// the interior-NUL guard on the serialized query-tags string is pure Go, so it is
// unit-tested under CGO_ENABLED=0 (see querytags_nul_test.go). The tagged execute
// path (operation.go) calls checkQueryTags before newCStr(serialized).

// errQueryTagsNUL rejects a serialized query-tags string containing an interior
// NUL. The kernel's set_query_tags C ABI takes it as a NUL-terminated C string
// with no length, so a NUL would silently truncate it — sending fewer/corrupted
// tags than intended — whereas the Thrift path puts the same tags in
// confOverlay["query_tags"] and transmits them whole. This is the query-tags
// counterpart to errQueryNUL (sqltext.go) and errParamNUL (bindparams.go), which
// guard SQL text and bound values for the identical reason. Fail loudly rather
// than diverge from Thrift.
var errQueryTagsNUL = errors.New("query tags contain a NUL byte, which the kernel set_query_tags ABI cannot carry")

// checkQueryTags validates the serialized query-tags string before the cgo layer
// C-string-marshals it. Returns errQueryTagsNUL when it contains an interior NUL.
func checkQueryTags(serialized string) error {
	if strings.IndexByte(serialized, 0) >= 0 {
		return errQueryTagsNUL
	}
	return nil
}
