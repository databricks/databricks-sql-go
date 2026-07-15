package kernel

import "github.com/databricks/databricks-sql-go/internal/backend"

// This file is intentionally NOT behind the `cgo && databricks_kernel` build tag.
// It holds the pure decision of how one backend.Param maps onto the kernel's
// raw-param bind arguments (positional vs named, SQL NULL vs empty string). That
// decision is the parity-critical part — a wrong name/NULL mapping silently
// diverges from the Thrift path's toSparkParameters — while the cgo wrapper around
// it (bindParams in operation.go) is just C-string marshaling. Keeping the decision
// here lets it be unit-tested under CGO_ENABLED=0 (see bindparams_test.go); the
// live kernel==Thrift proof (TestKernelParamsVsThrift) needs a warehouse and only
// runs in the credentialed nightly job, so this hermetic test is the PR-time guard.

// bindArg is the kernel-side view of one bound parameter: the exact (name, type,
// value, isNull) tuple the cgo bind call forwards to kernel_statement_bind_parameter.
// A nameless arg is positional (ordinal assigned kernel-side in push order); an
// isNull arg is SQL NULL (its value is not sent). It deliberately holds no C types,
// so it can be constructed and asserted in a non-cgo test.
type bindArg struct {
	// name is the parameter name; empty means positional. Passed to the kernel as
	// NULL-for-empty (newCStrOrNull), which is how the kernel distinguishes a named
	// bind from a positional one.
	name string
	// typ is the Databricks SQL type name the server expects (e.g. "BIGINT",
	// "STRING", "VOID" for NULL). Always sent.
	typ string
	// value is the already-stringified wire value. Only meaningful when !isNull;
	// an empty string here (with isNull false) is a real empty-string value, NOT
	// SQL NULL — the two must not be conflated.
	value string
	// isNull is true for a SQL NULL parameter (backend.Param.Value == nil). When
	// true the kernel is passed a NULL value pointer; when false the value string is
	// sent verbatim, so "" binds an empty string rather than NULL.
	isNull bool
}

// paramBindArg maps one backend.Param onto its kernel bind arguments. It mirrors
// the driver's neutral param contract (see backend.Param): an empty Name is a
// positional parameter, and a nil Value is SQL NULL (carried with its type, e.g.
// "VOID"). A non-nil Value — including a pointer to the empty string — is a real
// value and must not be treated as NULL. This is the same distinction the Thrift
// path's toSparkParameters makes, so the two backends bind identically.
func paramBindArg(p backend.Param) bindArg {
	a := bindArg{name: p.Name, typ: p.Type}
	if p.Value == nil {
		a.isNull = true
		return a
	}
	a.value = *p.Value
	return a
}
