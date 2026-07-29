package kernel

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// It holds the pure normalization of the C ABI's affected-row count so the rule is
// unit-testable in the default CGO_ENABLED=0 build (like paramBindArg in bindparams.go),
// separate from the cgo call site in operation.go.

// normalizeAffectedRows maps the kernel C ABI's modified-row count onto the value
// database/sql's Result.RowsAffected() reports, matching the Thrift path.
//
// The C ABI returns -1 for "not applicable / unknown": DDL, SELECT, or a warehouse
// that doesn't surface the counter (the kernel's num_modified_rows Option<i64> is
// None). The Thrift path reports 0 in exactly those cases (TGetOperationStatusResp
// defaults NumModifiedRows to 0), so the -1 sentinel is folded to 0 for parity.
// A real DML count (>= 0) passes through unchanged.
func normalizeAffectedRows(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}
