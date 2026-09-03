package kernel

import (
	"errors"
	"testing"
)

// checkQueryTags must reject a serialized query-tags string with an interior NUL
// (the kernel's set_query_tags ABI would truncate it, sending fewer tags than
// Thrift) but accept ordinary serialized tags and an empty string. Runs under
// CGO_ENABLED=0.
func TestCheckQueryTags(t *testing.T) {
	if err := checkQueryTags("team:eng,job:etl,production"); err != nil {
		t.Errorf("plain tags: got %v, want nil", err)
	}
	if err := checkQueryTags(""); err != nil {
		t.Errorf("empty: got %v, want nil", err)
	}
	// NUL inside a value (querytags.Serialize escapes only \\, :, and , — never NUL).
	if err := checkQueryTags("team:e\x00ng"); !errors.Is(err, errQueryTagsNUL) {
		t.Errorf("NUL in value: got %v, want errQueryTagsNUL", err)
	}
	// NUL inside a key must be caught too.
	if err := checkQueryTags("te\x00am:eng"); !errors.Is(err, errQueryTagsNUL) {
		t.Errorf("NUL in key: got %v, want errQueryTagsNUL", err)
	}
}
