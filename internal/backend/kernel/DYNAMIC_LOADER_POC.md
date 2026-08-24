# PoC: dynamic loading of the kernel (pure-Go, CGO_ENABLED=0)

**Status:** working proof-of-concept — **control plane AND data plane**, verified
live on pecotesting, with a head-to-head latency benchmark showing no regression.
**Build tag:** `databricks_kernel_dynamic` (separate from the shipped
`cgo && databricks_kernel` path — the two never build together).

## Update: data plane now works too (pure Go)

The earlier limitation ("control plane only, because arrow-go's `cdata` importer
is cgo") is **resolved**. `cdata_pure.go` is a pure-Go port of arrow-go v12.0.1's
C-Data *import* path: it reads the flat `ArrowSchema`/`ArrowArray` structs via
`unsafe`, invokes each struct's `release` callback with `purego.SyscallN`, and
builds `arrow.Record`s zero-copy with `array.NewData` — no cgo. `dynRows`
(dynamic_rows.go) pulls batches through the dlopen'd `kernel_result_stream_*`
and scans them with the SAME `arrowscan` scanner the cgo path uses, so values are
identical to the cgo backend by construction.

Verified live on pecotesting, built `CGO_ENABLED=0` (TestDynamicLoaderDataPlane):

```
OK scalars row = [1 2 3.5 hello true 19.99 <nil>]      (int/bigint/double/string/bool/decimal/null)
OK fetched 1000 rows in order across batches
OK nested row = [[1,2,3] {"k":1} {"a":1,"b":"x"}]       (array/map/struct as JSON)
OK temporal/binary/float row = [2021-07-01 … , [26 191], 3.3, -0.01,
                                 9999999999999999999999999999.99]  (exact high-precision decimal)
OK empty result set drained cleanly
OK fetched 100000 rows (multi-batch, likely CloudFetch)
PROOF: pure-Go (CGO_ENABLED=0) fetched + scanned result rows end-to-end.
```

The high-precision decimal returning byte-exact (`…9999.99`, not a float
approximation) is the strongest evidence the buffer import is correct.

## Latency: no regression (head-to-head on pecotesting)

Identical 500k-row × 3-col query, same drain-all-rows loop, same arrowscan
scanner; the only difference is static-cgo+cgo-cdata vs dynamic-purego+pure-import
(`BenchmarkCgoLargeResult` vs `BenchmarkDynLargeResult`):

| Path | ns/op (500k rows) |
| --- | --- |
| cgo static  | 7.91 s |
| purego dyn  | 6.17 s / 6.61 s / 8.59 s (repeated) |

End-to-end time is dominated by warehouse execution + network, so the two are
statistically indistinguishable — purego is never meaningfully slower, and the
per-cell path has no cgo boundary crossing. **No latency regression.**

Both `.a` and `.dylib` were built from the SAME pinned kernel rev
(`KERNEL_REV`, tls-rustls) for a fair comparison.

## Test matrix (all green)

- Default pure-Go suite (`CGO_ENABLED=0`, no tags): `go test ./...` — pass.
- Dynamic-tagged (`CGO_ENABLED=0 -tags databricks_kernel_dynamic`): pass;
  e2e/data-plane pass live on pecotesting.
- cgo-tagged (`CGO_ENABLED=1 -tags databricks_kernel`): unit pass; full
  `TestKernelE2E*` suite passes live on pecotesting (0 failures) — confirms the
  change does not regress the existing static path.

## Original PoC (control plane)

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
