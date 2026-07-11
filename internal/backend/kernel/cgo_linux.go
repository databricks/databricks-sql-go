//go:build cgo && databricks_kernel && linux && amd64

package kernel

// Link flags for linux/amd64. The static archive is forced with the
// -l:<file>.a form (a GNU-ld extension) so the linker never prefers a
// same-named .so — the kernel's cargo build emits both a .a and a .so into the
// same dir, and a bare -ldatabricks_sql_kernel would pick the .so and bake in
// an rpath. -lstdc++/-lm/-ldl are the kernel's transitive system deps.
//
// The path is ${SRCDIR}-relative; `make kernel-lib` drops the archive at
// ${SRCDIR}/lib/linux_amd64/libdatabricks_sql_kernel.a.

/*
#cgo LDFLAGS: -L${SRCDIR}/lib/linux_amd64 -l:libdatabricks_sql_kernel.a -lstdc++ -lm -ldl
*/
import "C"
