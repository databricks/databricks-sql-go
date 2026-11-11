package kernel

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

// structOfPanic runs arrow.StructOf(fields...) — the exact call
// cdata.ImportCArrowSchema / cdata.ImportCRecordBatch make for the top-level
// result struct — and returns the recovered panic message, or "" if none.
func structOfPanic(fields []arrow.Field) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	_ = arrow.StructOf(fields...)
	return ""
}

// RESULTFETCH-020 (issue #517): the SEA/kernel backend panics with
// `arrow: duplicate field with name "a"` for SELECT 1 AS a, 2 AS a because
// arrow-go v12's arrow.StructOf rejects duplicate child names. The SEA e2e
// cannot run in this job (CGO_ENABLED=0, no databricks_kernel tag), so this
// pure-Go test reproduces the exact failing library call and asserts that the
// names produced by uniqueFieldNames make that call succeed.
func TestDuplicateColumnNamesStructOfImport(t *testing.T) {
	orig := []string{"a", "a"}
	fields := []arrow.Field{
		{Name: orig[0], Type: arrow.PrimitiveTypes.Int32},
		{Name: orig[1], Type: arrow.PrimitiveTypes.Int32},
	}

	// Library-level repro: the pinned arrow-go v12 rejects duplicate names.
	// This is an intentional tripwire: if it fires after an arrow-go bump, the
	// library now accepts duplicate child names, so the uniqueFieldNames
	// workaround should be removed (along with this test) — do not loosen it.
	if msg := structOfPanic(fields); !strings.Contains(msg, `duplicate field with name "a"`) {
		t.Fatalf("tripwire: arrow.StructOf no longer panics on duplicate field names (got %q); "+
			"arrow-go now accepts duplicates, so remove the uniqueFieldNames dupnames workaround "+
			"and this test instead of loosening the assertion", msg)
	}

	// With the import-safe names, the same call must not panic.
	names, changed := uniqueFieldNames(orig)
	if !changed {
		t.Errorf("uniqueFieldNames(%v): changed=false, want true", orig)
	}
	if len(names) != len(fields) {
		t.Fatalf("uniqueFieldNames(%v) returned %d names, want %d", orig, len(names), len(fields))
	}
	renamed := make([]arrow.Field, len(fields))
	for i, f := range fields {
		f.Name = names[i]
		renamed[i] = f
	}
	if msg := structOfPanic(renamed); msg != "" {
		t.Fatalf("arrow.StructOf with uniqueFieldNames output %v panicked: %s", names, msg)
	}
}

func TestUniqueFieldNames(t *testing.T) {
	assertUnique := func(t *testing.T, in, out []string) {
		t.Helper()
		if len(out) != len(in) {
			t.Fatalf("got %d names, want %d", len(out), len(in))
		}
		seen := map[string]bool{}
		for _, n := range out {
			if seen[n] {
				t.Fatalf("names not unique: %v", out)
			}
			seen[n] = true
		}
	}

	cases := []struct {
		name string
		in   []string
	}{
		{"pair a", []string{"a", "a"}},
		{"pair id", []string{"id", "id"}},
		{"collides with existing suffix", []string{"a", "a_1", "a"}},
		{"triple", []string{"a", "a", "a"}},
		{"mixed", []string{"x", "a", "y", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := append([]string(nil), tc.in...)
			out, changed := uniqueFieldNames(in)
			if !reflect.DeepEqual(in, tc.in) {
				t.Fatalf("input mutated: got %v, want %v", in, tc.in)
			}
			if !changed {
				t.Errorf("changed=false, want true for %v", tc.in)
			}
			assertUnique(t, tc.in, out)
			// Names that were already unique (first occurrence) keep their name.
			if out[0] != tc.in[0] {
				t.Errorf("first name changed: got %q, want %q", out[0], tc.in[0])
			}
		})
	}

	t.Run("already unique is unchanged", func(t *testing.T) {
		in := []string{"x", "y"}
		out, changed := uniqueFieldNames(in)
		if changed {
			t.Errorf("changed=true, want false")
		}
		if !reflect.DeepEqual(out, in) {
			t.Errorf("got %v, want %v", out, in)
		}
	})
	t.Run("empty and nil", func(t *testing.T) {
		if out, changed := uniqueFieldNames(nil); changed || len(out) != 0 {
			t.Errorf("nil: got %v, %v", out, changed)
		}
		if out, changed := uniqueFieldNames([]string{}); changed || len(out) != 0 {
			t.Errorf("empty: got %v, %v", out, changed)
		}
	})
}

// The native Arrow accessor (GetArrowBatches) must expose the ORIGINAL
// duplicate names with both columns' values intact (spec: col 0 and col 1 both
// named `a`, row 0 values 1 and 2 as Int32).
func TestRestoreDuplicateFieldNames(t *testing.T) {
	alloc := memory.NewGoAllocator()
	importSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "a_1", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	b0 := array.NewInt32Builder(alloc)
	b0.Append(1)
	c0 := b0.NewArray()
	b0.Release()
	b1 := array.NewInt32Builder(alloc)
	b1.Append(2)
	c1 := b1.NewArray()
	b1.Release()
	rec := array.NewRecord(importSchema, []arrow.Array{c0, c1}, 1)
	c0.Release()
	c1.Release()
	defer rec.Release()

	orig := []string{"a", "a"}

	sch, err := restoreSchemaNames(importSchema, orig)
	if err != nil {
		t.Fatalf("restoreSchemaNames: %v", err)
	}
	if len(sch.Fields()) != 2 || sch.Field(0).Name != "a" || sch.Field(1).Name != "a" {
		t.Fatalf("restored schema fields: got %v, want [a a]", sch.Fields())
	}

	out, err := restoreRecordNames(rec, orig)
	if err != nil {
		t.Fatalf("restoreRecordNames: %v", err)
	}
	defer out.Release()
	if out.NumCols() != 2 || out.NumRows() != 1 {
		t.Fatalf("restored record shape: %d cols x %d rows, want 2 x 1", out.NumCols(), out.NumRows())
	}
	if out.ColumnName(0) != "a" || out.ColumnName(1) != "a" {
		t.Fatalf("restored record names: got [%s %s], want [a a]", out.ColumnName(0), out.ColumnName(1))
	}
	v0, ok0 := out.Column(0).(*array.Int32)
	v1, ok1 := out.Column(1).(*array.Int32)
	if !ok0 || !ok1 {
		t.Fatalf("columns not Int32: %T, %T", out.Column(0), out.Column(1))
	}
	if v0.Value(0) != 1 || v1.Value(0) != 2 {
		t.Fatalf("values: got (%d, %d), want (1, 2)", v0.Value(0), v1.Value(0))
	}
}

// A names/column-count mismatch is an invariant violation: it must surface as
// an error rather than silently exposing import-only names (e.g. "a_1"), and
// must not leak or over-release the caller's record.
func TestRestoreNamesCountMismatch(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	importSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "a_1", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	b := array.NewInt32Builder(alloc)
	b.Append(1)
	c0 := b.NewArray()
	b.Append(2)
	c1 := b.NewArray()
	b.Release()
	rec := array.NewRecord(importSchema, []arrow.Array{c0, c1}, 1)
	c0.Release()
	c1.Release()

	for _, names := range [][]string{{"a"}, {"a", "a", "a"}, nil} {
		if sch, err := restoreSchemaNames(importSchema, names); err == nil || sch != nil {
			t.Errorf("restoreSchemaNames(%v): got (%v, %v), want (nil, error)", names, sch, err)
		}
		if out, err := restoreRecordNames(rec, names); err == nil || out != nil {
			t.Errorf("restoreRecordNames(%v): got (%v, %v), want (nil, error)", names, out, err)
			if out != nil {
				out.Release()
			}
		}
	}
	if _, err := restoreSchemaNames(nil, []string{"a"}); err == nil {
		t.Error("restoreSchemaNames(nil): want error")
	}
	if _, err := restoreRecordNames(nil, []string{"a"}); err == nil {
		t.Error("restoreRecordNames(nil): want error")
	}

	// The caller's single release must free everything (no Retain on error).
	rec.Release()
}
