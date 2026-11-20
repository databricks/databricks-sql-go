# Lessons

<!-- distilled-through: 2026-11-18 -->

## Configuration and authentication
- L-0001 (1×, #446): Verify kernel session parameters and proxies separately from Thrift: kernel allowlists configuration keys and supports HTTP(S) proxies, while Thrift may accept additional keys/schemes.
- L-0002 (2×, #449): Kernel U2M uses workspace OIDC uniformly across clouds with its registered client/scopes, not Azure Thrift's Entra app. Preserve stable scope ordering in slice comparisons.
- L-0003 (1×, #444): External JWTs on the kernel PAT path may undergo mandatory federation exchange; verify pinned-kernel same-issuer/non-JWT exceptions before adding driver-side exchange.
- L-0004 (5×, #449 #444 #455): Update mirrored auth/ABI/default documentation together. Cite exact external kernel refs for assumptions, and avoid numeric native defaults without a pinned source.
- L-0005 (1×, #456): Track explicit configuration separately from non-empty values so deliberately supplied empty credential pairs are rejected rather than treated as absent.
- L-0006 (1×, #455): A context check before blocking native session-open does not bound the call itself; verify cancellation/deadline coverage through the pinned ABI.
- L-0007 (2×, #458 #492): WithKernel options may opt into kernel even for false values. Kernel-only DSN carriers should allocate KernelExperimental only when behavior changes; test DSN and option routing separately.
- L-0008 (3×, #457 #451): Preserve native default/unset sentinels when forwarding timeouts and limits. Round positive sub-unit durations consistently so they never become zero/unset.
- L-0009 (1×, #470): Reject interior NUL in user strings passed to length-less C setters before cgo marshaling; keep validation in pure Go so CGO_ENABLED=0 tests exercise it.
- L-0010 (1×, #479): Validate configuration before cache short-circuits so errors do not depend on global cache state.
- L-0011 (1×, #485): Match URI schemes case-insensitively with the colon delimiter, then verify the chosen backend accepts the scheme.
- L-0012 (1×, #484): Functional options storing pointers must allocate a fresh copy inside each application; test reusing one option across two independently mutable objects.
- L-0013 (3×, #566 #497 #484): Use optional/pointer values or Set flags (set in every entry point and kept by DeepCopy) when omitted differs from explicit zero/false/default, e.g. server rollout overrides or Thrift-prefilled defaults.
- L-0014 (1×, #493): Inject broad default OAuth scopes only for an empty caller scope set; never append them to explicit restricted scopes.
- L-0015 (1×, #493): Check complementary ABI setters before declaring a backend option unsupported because one primary setter lacks the argument.
- L-0016 (1×, #496): Typed JSON flag getters require stored JSON literals; producers must encode strings/lists and document this representation.
- L-0017 (1×, #536): Kernel no-CloudFetch uses can_cloud_download=false for paged inline results. Remove case-insensitive/conflicting caller keys before forcing it, and verify server acceptance live.
- L-0018 (1×, #536): buildKernelConfig also serves Thrift Reyden recovery. Test newly active options through openSessionWithReydenFallback and document changed shared-DSN behavior.
- L-0019 (1×, #539): Avoid adjacent same-typed boolean parameters controlling ownership; use distinct types/options and derive combined lifecycle state in one place.
- L-0020 (1×, #556): Check integer duration values against MinInt64/unit and MaxInt64/unit before multiplication; report overflow with the parameter name.
- L-0060 [phase=review] (1×, #566): Kernel applies case variants of client-only session-conf keys in sorted byte order, so the canonical lowercase key wins; check pinned session.rs before flagging map-order nondeterminism.
- L-0061 (1×, #566): Forwarding a formerly inert option to a backend is a behavior change: label it so in CHANGELOG, explain the effect of existing tuned values, and test set values, not only unset.

## Native boundaries and lifecycle
- L-0021 (1×, #450): Never place fabricated Go pointers or cgo.Handle values into C pointer slots. Use NULL with a global sink or a real C allocation.
- L-0022 (2×, #450): Foreign-thread callbacks should copy borrowed data and nonblockingly enqueue bounded work. Invoke user code and I/O outside internal locks and away from native callback threads.
- L-0023 (1×, #450): Check retryable preconditions outside sync.Once.Do; an early return inside still consumes the Once.
- L-0024 (1×, #450): Swappable writer proxies must preserve richer interfaces such as zerolog.LevelWriter and delegate when supported.
- L-0025 (1×, #450): Surface queue overflow, sampling and truncation through bounded warnings so consumers can distinguish dropped output from no output.
- L-0026 (2×, #440 #479): Keep build-tag constraints aligned across real shims, unsupported guards and stub tests; missing terms can silently select no implementation.
- L-0027 (1×, #479): Verify errors.Is/errors.As through the production-shaped wrapping chain; bare-sentinel fakes do not test real error matching.
- L-0028 (1×, #484): Handle lifetime independence across cgo objects must be confirmed against the pinned kernel ABI/tests and recorded as a load-bearing ownership assumption.
- L-0029 (1×, #484): Pin Go constants mirrored from C macros with compile-time drift assertions rather than optional tagged runtime tests.
- L-0030 (2×, #497): New ABI symbols require released bindings pins in go.mod/go.sum, not only KERNEL_REV. Hand-mirrored layouts must match the published header field order, types and size.
- L-0031 (1×, #496): Keep ABI/header/pin migrations focused enough to verify binding compatibility and bisect independently of feature behavior.
- L-0032 (1×, #496): Shared singleflight fetches need detached, bounded contexts; individual waiters honor their own cancellation without aborting shared work.
- L-0033 (1×, #295 #371): Result downloads deliberately outlive QueryContext cancellation. Preserve the detached rows-fetch contract and bound downloads with explicit timeouts/client options.
- L-0034 (1×, #555): Retry non-idempotent RPCs on proven pre-send dial failures only; generic transport failures and redirected attempts may have delivered the request.

## Arrow results and metadata
- L-0035 (2×, #492): Malformed server rows must return wrapped errors, not panic. Keep ColumnType metadata consistent with produced values and use physical-type fallback when logical conversion is unavailable.
- L-0036 (2×, #509): Preserve iterator errors separately from EOF, expose them exactly once through shared pending-error logic, and update every sibling iterator. Explicit Close clears pending errors; internal release must preserve them.
- L-0037 (2×, #510): Derive column metadata from cached result-set Arrow schema, independent of current-page scanner lifetime. Timestamp timezone metadata distinguishes Thrift TIMESTAMP from NTZ.
- L-0038 (1×, #509): HasNext may fetch remotely. Drain local batches and check finished state first; preserve boolean short-circuit order and assert early Close sends no FetchResults RPC.
- L-0039 (1×, log:2026-11-12): Duplicate-name support can differ between Arrow schemas, struct/cdata imports and IPC. Test both kernel and Thrift boundaries against the current Arrow version.
- L-0040 (1×, #550 #535): New Arrow code uses github.com/apache/arrow-go/v18 RecordBatch APIs; avoid copying legacy arrow/go/v12 imports.
- L-0041 (1×, #563 #567): Derive logical result types from field metadata with ColumnTypeInfoForField, including empty SEA string-backed schemas; test zero-row, NULL-only and populated results on both backends.

## Verification and dependencies
- L-0042 (3×, #458 #510 #567): Compile/vet relevant tagged tests explicitly, including databricks_kernel,e2e where needed, and register E2E names in nightly filters. Default unit tests omit tagged coverage.
- L-0043 (1×, #458): Test seams must call real production methods; parallel copies of native calls prove signatures without proving production wiring.
- L-0044 (1×, #457): Telemetry duration units follow receiver schema: bare names are seconds, and _ms/_millis names are milliseconds.
- L-0045 (2×, #440): Publish real dependency versions: replace directives do not apply downstream, and module graph resolution requires every go.mod dependency regardless of build tags.
- L-0046 (1×, #440): Verification, checksum and lint gates must propagate failures; do not hide them with unconditional success.
- L-0047 (2×, #463 #440 #469): Confirm which archive the build links. Default kernel builds use published bindings; local source builds need explicit go.work/replace routing, and KERNEL_REV alone does not ship native behavior.
- L-0048 (1×, #516): Linter fixes must preserve production pointer/value error types; use interface-typed test variables or narrowly justified suppressions.
- L-0049 (1×, #516): Fork CI without registry credentials must install tools from prebuilt releases rather than go install; keep fork-behavior comments accurate.
- L-0050 (1×, #525): Allocation/performance changes need AllocsPerRun assertions or benchmarks; output parity alone cannot detect a performance regression.
- L-0051 (2×, #526 #534 #464): Make targeted go get updates and preserve deliberate indirect security pins/hashes and require-block layout. Revalidate suppressions on tagged/untagged graphs and document security versions/advisories.
- L-0052 (1×, #527): Fast paths need byte/error parity against the original over exhaustive/random inputs and proof the fast path runs; keep expensive sweeps out of short tests.
- L-0053 (1×, #536): Prove no-storage-egress behavior through a workspace-only CONNECT proxy, asserting complete results and zero blocked attempts.
- L-0054 (2×, #559 #558 #555): Join test goroutines with bounded cleanup and context cancellation. Synchronize with channels and report callback failures without FailNow/Fatal from foreign goroutines.
- L-0055 (1×, #561): Root Go files must remain package dbsql/dbsql_test even behind tags; declare developer tools with the go.mod tool directive.

## Bot workflow
- L-0056 (1×, #442): Use concrete serving-endpoints/model/invocations MODEL_ENDPOINT URLs; anthropic/invocations produces an invalid translated CLI route.
- L-0057 (1×, #442): Schedule plus workflow_dispatch workflows should declare numeric knobs as strings and parse them downstream.
- L-0058 (2×, #442): Omit unused prompt-file config keys instead of referencing missing files; explicitly unignore and seed any required tracked file under ignored directories.
- L-0059 (4×, log:2026-10-21): Reuse completed inspections, report unavailable native/E2E environments promptly, and distinguish skipped coverage or compile failures from assertion-based reproduction. Add runnable unit coverage where possible.
