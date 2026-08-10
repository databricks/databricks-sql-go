# PoC: dynamic loading of the kernel (pure-Go, CGO_ENABLED=0)

**Status:** working proof-of-concept, control-plane only. Verified live.
**Build tag:** `databricks_kernel_dynamic` (separate from the shipped
`cgo && databricks_kernel` path — the two never build together).

## What this proves

The shipped kernel backend static-links `libdatabricks_sql_kernel.a` through cgo.
That forces `CGO_ENABLED=1`, a C toolchain on every builder, and it breaks Go's
free cross-compilation — the three things the SEA/kernel release design flags as
blockers to ever making SEA the default backend.

This PoC loads the kernel **shared** library (`.dylib`/`.so`/`.dll`) at run time
with [`ebitengine/purego`](https://github.com/ebitengine/purego) and **no cgo**.
It is the model `gosnowflake` uses for its own closed-source native core.

Verified end-to-end against a live warehouse (pecotesting), built `CGO_ENABLED=0`:

```
OK dlopen: bound kernel C ABI from libdatabricks_sql_kernel.dylib (CGO_ENABLED=0)
OK config: http_path + PAT set
OK session_open: connected to warehouse over SEA via the kernel
OK execute: server queryId="01f19506-ee6a-1f18-813f-4e9156bd2a4e" numModifiedRows=-1
PROOF: pure-Go (CGO_ENABLED=0) drove the closed-source kernel control plane end-to-end.
```

Also verified:
- **No kernel source change.** The kernel's `Cargo.toml` already declares
  `crate-type = [..., "cdylib", ...]`, so `cargo build` already emits the
  `.dylib` this PoC loads. The C ABI functions are `#[no_mangle] extern "C"`, so
  they are exported from the shared library as-is.
- **No user CUJ change.** `WithUseKernel` and the existing build tags are
  untouched. The default pure-Go build is unchanged and does **not** pull in
  purego (confirmed: `go list -deps ./...` shows no purego in the default build).

## Scope boundary (the honest part)

This PoC covers the **control plane** only: dlopen → config → session open →
execute → query-id / affected-rows → teardown. That is the entire happy path for
DML/DDL and any non-result statement.

It does **not** cover the **data plane** (Arrow result batches), for one concrete
reason: the cgo backend imports result batches via `arrow-go/v12`'s `cdata`
package (`ImportCRecordBatch`), and **`cdata` is itself a cgo package** — every
non-test file in it does `import "C"`. So a fully `CGO_ENABLED=0` result-fetch
path can't just reuse `cdata`. Closing the data plane needs a separate decision:

1. a purego-based Arrow C-Data importer (reimplement the small struct import
   without cgo), or
2. move to `arrow-go` v18 and re-evaluate, or
3. accept a hybrid: purego control plane + a thin cgo shim only for C-Data import
   (loses full `CGO_ENABLED=0`, keeps dynamic loading).

That decision — plus the run-time library-discovery story (rpath / env var) and
the glibc-vs-musl matrix — is the follow-up, and is why this is a PoC PR rather
than a backend replacement.

## Run it

```sh
CGO_ENABLED=0 go build -tags databricks_kernel_dynamic ./internal/backend/kernel/

DBX_KERNEL_DYLIB=/abs/path/to/libdatabricks_sql_kernel.dylib \
DBX_KERNEL_HOST=<warehouse-hostname> \
DBX_KERNEL_HTTPATH=/sql/1.0/warehouses/<id> \
DBX_KERNEL_TOKEN=<pat> \
CGO_ENABLED=0 go test -tags databricks_kernel_dynamic \
  -run TestDynamicLoaderControlPlane ./internal/backend/kernel/ -v
```

## PoC-only shortcuts (not for merge as-is)

- `go.mod` has a `replace github.com/ebitengine/purego => /tmp/purego-src` so the
  branch builds offline in a sandbox. A real PR drops the replace and lets the
  module proxy fetch the pinned `v0.10.2`.
- The dylib path is passed via env var. Real code resolves it next to the
  executable (rpath) or a documented env var.
- `cKernelError` is a hand-mirrored struct layout. A real PR adds
  `unsafe.Sizeof`/`Offsetof` assertions (the cgo path has equivalent guards).
