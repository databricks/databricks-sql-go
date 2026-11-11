package kernel

import (
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

// Duplicate top-level column names (SELECT 1 AS a, 2 AS a) are valid SQL, but
// arrow-go v12's arrow.StructOf — which cdata.ImportCArrowSchema and
// cdata.ImportCRecordBatch call for the top-level "+s" struct — panics with
// `arrow: duplicate field with name "a"`. These helpers let the cgo import
// path hand arrow-go unique names and then restore the original names on the
// exposed schema/record (arrow.NewSchema and array.NewRecord allow duplicates).
// (arrow-go v13+ dropped the StructOf restriction; this is the v12 workaround.)

// uniqueFieldNames returns names made unique for import, and whether any name
// had to change. The first occurrence of each name keeps it; later duplicates
// get the lowest "<name>_<n>" suffix that collides with no other name. It does
// not mutate the input; when nothing changes the input slice is returned.
func uniqueFieldNames(names []string) ([]string, bool) {
	seen := make(map[string]struct{}, len(names))
	dup := false
	for _, n := range names {
		if _, ok := seen[n]; ok {
			dup = true
			break
		}
		seen[n] = struct{}{}
	}
	if !dup {
		return names, false
	}

	taken := make(map[string]struct{}, len(names))
	for _, n := range names {
		taken[n] = struct{}{}
	}
	out := make([]string, len(names))
	used := make(map[string]struct{}, len(names))
	for i, n := range names {
		if _, ok := used[n]; !ok {
			out[i] = n
			used[n] = struct{}{}
			continue
		}
		for k := 1; ; k++ {
			cand := fmt.Sprintf("%s_%d", n, k)
			_, inTaken := taken[cand]
			_, inUsed := used[cand]
			if !inTaken && !inUsed {
				out[i] = cand
				used[cand] = struct{}{}
				break
			}
		}
	}
	return out, true
}

// restoreSchemaNames returns sch with its top-level field names replaced by
// names (which may contain duplicates). Types, nullability, and field/schema
// metadata are preserved. A names/field-count mismatch is an internal
// invariant violation (names come from the same C schema) and returns an error
// rather than silently exposing the import-only names (e.g. "a_1").
func restoreSchemaNames(sch *arrow.Schema, names []string) (*arrow.Schema, error) {
	if sch == nil {
		return nil, fmt.Errorf("kernel: restore column names: nil schema")
	}
	// Copy: Schema.Fields() may return the schema's backing slice.
	fields := append([]arrow.Field(nil), sch.Fields()...)
	if len(names) != len(fields) {
		return nil, fmt.Errorf("kernel: restore column names: have %d names for %d columns", len(names), len(fields))
	}
	for i, n := range names {
		fields[i].Name = n
	}
	md := sch.Metadata()
	return arrow.NewSchema(fields, &md), nil
}

// restoreRecordNames returns rec re-wrapped under a schema whose top-level
// field names are names (which may contain duplicates). On success the
// returned record owns its own reference; in all cases the caller still
// releases rec. A names/column-count mismatch returns an error (see
// restoreSchemaNames) and no record.
func restoreRecordNames(rec arrow.Record, names []string) (arrow.Record, error) {
	if rec == nil {
		return nil, fmt.Errorf("kernel: restore column names: nil record")
	}
	sch, err := restoreSchemaNames(rec.Schema(), names)
	if err != nil {
		return nil, err
	}
	return array.NewRecord(sch, rec.Columns(), rec.NumRows()), nil
}
