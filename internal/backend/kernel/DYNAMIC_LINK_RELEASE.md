# Dynamic linking (.so/.dylib/.dll) for the kernel — release design + working PoC

**Decision:** ship the closed-source kernel as a **shared library** (dynamic
linking), not a static `.a`. cgo is accepted; the Arrow import path stays cgo +
arrow-go `cdata` (zero-copy C-Data, the ADBC driver-manager model). This
documents the dynamic-linking release end to end, with a PoC verified live on
pecotesting.

**Why dynamic, not static — and why closed-source makes this the right call:**
the kernel is **proprietary**. Static linking (`.a`) would **bake the
closed-source machine code into every consumer binary** — every customer's Go
executable would carry a copy of the kernel, and a security fix would require
every downstream to rebuild and re-ship. A shared library keeps the proprietary
blob a **single, separately-distributed, signable artifact** that is
version-controlled, patchable in place (drop-in replace the `.so`, no consumer
rebuild), and **shared with the ODBC driver** (which already loads a shared
library). This is exactly how ADBC ships its drivers. Static linking is
mentioned below only as the *interim opt-in bridge* while the shared-library
release infrastructure is built — it is not the destination.

This is NOT the purego/CGO_ENABLED=0 path (that PoC — `dynamic_loader.go`,
`cdata_pure.go` — is moot now that cgo is accepted, and is parked).

**Branches:** `databricks-sql-go` → `kernel-dynamic-lib-poc`; `databricks-sql-kernel`
→ `kernel-dynamic-lib-poc` (adds `kernel_abi_version()`).

---

## What dynamic linking changes (and what it doesn't)

The kernel is compiled to a **shared library** (`.dylib`/`.so`/`.dll`) instead of
a static archive (`.a`). At build time the Go binary records only a *reference*
to the library; at run time the OS loads the library from a search path (rpath).

- **Unchanged:** the entire Arrow C-Data import path (`rows.go`, arrow-go
  `cdata`), all backend/operation logic, the public API, `WithUseKernel`, and the
  `CGO_ENABLED=0` pure-Go Thrift fallback for non-kernel users. Dynamic vs static
  is invisible above the link layer.
- **Changed:** one per-OS cgo `LDFLAGS` line (link the shared lib + set an rpath),
  and the packaging/release flow (ship the `.so` beside the binary).

## PoC results (darwin/arm64, live on pecotesting)

Behind a new build tag `databricks_kernel_dynlib` (added alongside
`databricks_kernel`), `cgo_dynlib_darwin.go` links the `.dylib` instead of the
`.a`. Verified:

- **Binary references the kernel externally**, not baked in:
  `otool -L` → `@rpath/libdatabricks_sql_kernel.dylib`.
- **Size drops from ~61 MB (static .a) to ~12 MB** (kernel code is now external).
- **rpath baked in** so the loader finds the lib at run time
  (`@loader_path/lib/darwin_arm64` + an absolute dev path).
- **20/20 `TestKernelE2E*` subtests pass on pecotesting** through the
  runtime-loaded dylib (all scalar/decimal/temporal/nested types).
- **Genuinely external (negative proof):** moving the dylib away makes the binary
  fail at load with `dyld: Library not loaded: @rpath/libdatabricks_sql_kernel.dylib`
  and prints the exact rpath search order; restoring it works again.
- **Isolation:** the static `.a` path and the default pure-Go build both still
  build unchanged (the dynlib tag guards `cgo_darwin.go` with
  `!databricks_kernel_dynlib`).

## Real code changes needed to ship this

### Kernel repo (databricks-sql-kernel)
1. **Publish the shared library as a release artifact**, per-OS/arch, built with
   `crate-type = ["cdylib", ...]` (already declared) and `tls-rustls`. Sign it.
2. **Set a relocatable install name / soname at build:**
   - macOS: `install_name_tool -id @rpath/libdatabricks_sql_kernel.dylib` (the
     cargo default is the absolute build path — NOT relocatable; the PoC had to
     fix this). Better: set it at link time via
     `RUSTFLAGS=-Clink-arg=-Wl,-install_name,@rpath/...`.
   - Linux: build with `-Wl,-soname,libdatabricks_sql_kernel.so.<MAJOR>`.
3. **Add an ABI-version symbol** `uint32_t kernel_abi_version(void)` so the Go
   side can detect a mismatched library at load (see Versioning). Important while
   the kernel is pre-1.0 ("ABI may change freely").

### Go driver repo (databricks-sql-go)
1. **New dynamic-link cgo files** per OS (the PoC has darwin; add linux/windows):
   - darwin: `-L<dir> -ldatabricks_sql_kernel -Wl,-rpath,@loader_path/...`
   - linux:  `-L<dir> -ldatabricks_sql_kernel -Wl,-rpath,$ORIGIN/...`
   - windows: import lib + ship the `.dll` beside the `.exe` (no rpath concept;
     DLL is found via the executable directory).
2. **`make kernel-lib` fetches the `.so`/`.dylib`** (not the `.a`) from the
   published release, verifies its checksum, and places it where the rpath points.
3. **Add a load-time ABI check** calling `kernel_abi_version()` and failing with a
   clear error on mismatch (turns a silent crash into an actionable message).
4. **Decide static-vs-dynamic exposure:** either replace the static path, or keep
   both (static default + `databricks_kernel_dynlib` opt-in, as the PoC does).

## `.so` publishing — how it works

- Kernel CI builds the shared lib per platform, signs it, attaches it to the
  **kernel's own release line** (design-doc option **D** — correct owner,
  kernel-versioned, and the SAME artifact serves ODBC, which already dlopens a
  shared lib).
- The Go driver pins a kernel version (`KERNEL_REV` / release tag). `make
  kernel-lib` downloads the matching signed `.so`, verifies it, drops it next to
  the build output.
- At run time the OS loads it via the rpath. For distribution, the `.so` ships
  **beside the application binary** (rpath `@loader_path` / `$ORIGIN`), so each
  deployment carries its own copy — no system-wide install, no cross-app
  interference.

## Versioning (the runtime contract dynamic linking introduces)

Static linking freezes the version at build (mismatch = compile error). Dynamic
linking resolves it at run time, so a wrong/missing `.so` is a **runtime**
problem that must be guarded:

1. **`kernel_abi_version()` check** on load — **IMPLEMENTED in this PoC.** The
   kernel exposes `kernel_abi_version()` (kernel branch); the Go driver calls it
   once at `OpenSession` (`checkKernelABIVersion` in `cgo.go`, expected version
   `expectedKernelABIVersion`) and, on mismatch, fails **at connect** with a
   clear error ("the kernel shared library next to the binary is the wrong
   version") instead of crashing. Verified: forcing a mismatch produces the error
   and a clean `FAIL`, not a segfault. Essential while the C ABI is pre-1.0
   ("may change freely"). gosnowflake does the equivalent.
2. **soname major** (`libkernel.so.1` on Linux; compatibility-version on macOS):
   bump the major on any ABI break so the OS *also* refuses a mismatched major
   before the Go check even runs. (Kernel build-side; the `kernel_abi_version()`
   check works regardless and is the portable belt-and-suspenders.)
3. **Pin** the expected version in the driver source (`expectedKernelABIVersion`,
   bumped in lockstep with `KERNEL_REV`).

**ODBC and Go can run different kernel versions** because they are separate
processes with isolated memory — each ships/loads its own `.so`. (Only a single
process loading *both* would need matching majors + symbol care; not a normal
deployment.)

## Recommendation (phased) — dynamic is the destination

- **Destination (the decision): dynamic `.so`** published on the kernel release
  line, signed, soname-versioned, with the `kernel_abi_version()` load check
  (already built). This keeps the closed-source kernel a separate, signable,
  patchable artifact **out of every customer binary**, and shares one lib with
  ODBC. This is what we ship.
- **Interim bridge only (not the goal):** while the signed shared-library release
  pipeline is being stood up, the existing static `.a` path can remain as the
  opt-in — its versioning is a build-time non-problem, so it's a safe stopgap.
  It is a bridge, *not* the target, precisely because static bakes the
  proprietary kernel into every consumer binary and can't be patched without a
  rebuild.
- The `CGO_ENABLED=0` pure-Go Thrift fallback stays intact throughout, so
  non-kernel users are never forced into cgo — no user CUJ change.

## PoC files

Go driver (`kernel-dynamic-lib-poc`):
- `cgo_dynlib_darwin.go` — dynamic-link LDFLAGS (tag: `databricks_kernel_dynlib`)
- `cgo_darwin.go` — guarded with `!databricks_kernel_dynlib` so static/dynamic
  never both compile
- `cgo.go` — `checkKernelABIVersion()` + `expectedKernelABIVersion`
- `backend.go` — calls the ABI check at `OpenSession`
- `lib/darwin_arm64/` (gitignored) — the `.dylib` with install_name `@rpath/...`

Kernel (`kernel-dynamic-lib-poc`):
- `src/c_abi/session.rs` — `kernel_abi_version()` + `KERNEL_C_ABI_VERSION`
- `include/databricks_kernel.h` — declaration

## Build/run the PoC

```sh
# stage the dylib + fix its install name (a packaging step; kernel CI would do this)
cp <kernel>/target/release/libdatabricks_sql_kernel.dylib internal/backend/kernel/lib/darwin_arm64/
install_name_tool -id @rpath/libdatabricks_sql_kernel.dylib \
  internal/backend/kernel/lib/darwin_arm64/libdatabricks_sql_kernel.dylib

# build + run e2e dynamically linked
export CGO_LDFLAGS_ALLOW='-Wl,-rpath,@loader_path.*|-Wl,-rpath,/.*'
DATABRICKS_PECOTESTING_HTTP_PATH2=/sql/1.0/warehouses/<id> \
CGO_ENABLED=1 go test -tags "databricks_kernel databricks_kernel_dynlib" \
  -run TestKernelE2E ./... -v
```
