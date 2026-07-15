package kernel

import (
	"errors"
	"testing"

	"github.com/databricks/databricks-sql-go/internal/backend"
)

// paramBindArg is the parity-critical param mapping (positional vs named, SQL NULL
// vs empty string). The live kernel==Thrift proof (TestKernelParamsVsThrift) needs
// a warehouse and only runs in the credentialed nightly job, so this untagged test
// is the PR-time guard that a refactor can't silently break the mapping. It runs
// under CGO_ENABLED=0.
func TestParamBindArg(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	cases := []struct {
		name  string
		param backend.Param
		want  bindArg
	}{
		{
			// A named parameter (:n markers) keeps its name and value.
			name:  "named value",
			param: backend.Param{Name: "n", Type: "BIGINT", Value: strPtr("42")},
			want:  bindArg{name: "n", typ: "BIGINT", value: "42", isNull: false},
		},
		{
			// An empty Name is positional (? markers) — the kernel assigns the ordinal
			// in push order. The name must stay empty so the cgo layer sends NULL.
			name:  "positional value",
			param: backend.Param{Name: "", Type: "STRING", Value: strPtr("hello")},
			want:  bindArg{name: "", typ: "STRING", value: "hello", isNull: false},
		},
		{
			// A nil Value is SQL NULL, carried with its type (VOID). isNull must be set
			// so the cgo layer sends a NULL value pointer, not an empty string.
			name:  "nil value is SQL NULL",
			param: backend.Param{Name: "n", Type: "VOID", Value: nil},
			want:  bindArg{name: "n", typ: "VOID", value: "", isNull: true},
		},
		{
			// The critical distinction: a pointer to the empty string is a REAL empty
			// string value, NOT SQL NULL. isNull must stay false so the empty string is
			// bound as a value — conflating the two would diverge from the Thrift path.
			name:  "empty-string value is not NULL",
			param: backend.Param{Name: "s", Type: "STRING", Value: strPtr("")},
			want:  bindArg{name: "s", typ: "STRING", value: "", isNull: false},
		},
		{
			// A positional SQL NULL: empty name AND nil value together.
			name:  "positional NULL",
			param: backend.Param{Name: "", Type: "VOID", Value: nil},
			want:  bindArg{name: "", typ: "VOID", value: "", isNull: true},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := paramBindArg(c.param)
			if got != c.want {
				t.Errorf("paramBindArg(%+v) = %+v, want %+v", c.param, got, c.want)
			}
		})
	}
}

// checkParamValue must reject an interior NUL (the kernel bind ABI would truncate
// it) but accept ordinary values, a trailing NUL-free string, and a NULL param.
func TestCheckParamValue(t *testing.T) {
	if err := checkParamValue(bindArg{value: "a\x00b"}); !errors.Is(err, errParamNUL) {
		t.Errorf("interior NUL: got %v, want errParamNUL", err)
	}
	if err := checkParamValue(bindArg{value: "plain"}); err != nil {
		t.Errorf("plain value: got %v, want nil", err)
	}
	// A NULL param carries no value string, so the NUL check must not apply.
	if err := checkParamValue(bindArg{isNull: true}); err != nil {
		t.Errorf("NULL param: got %v, want nil", err)
	}
}
