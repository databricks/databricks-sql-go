# Release History

## v1.15.1 (2026-09-01)
- Pin the seven per-platform kernel bindings modules to v1.0.0.
- Disable kernel telemetry by default when `enableTelemetry` is unset; explicit `true` and `false` values are unchanged (databricks/databricks-sql-go#464).
- Bump `golang.org/x/crypto` to v0.55.0 for security scanning (databricks/databricks-sql-go#464).

## v1.15.0 (2026-08-28)
- **New experimental SEA/kernel backend (opt-in).** Set `WithUseKernel(true)` (or `useKernel=true` in the DSN) to route execution through the Statement Execution API instead of Thrift, backed by the Rust `databricks-sql-kernel` over cgo. It requires a build with `-tags databricks_kernel` and `CGO_ENABLED=1`; the default Thrift build is unchanged and returns a clear error if the kernel backend is selected without the tag. The prebuilt kernel binaries ship as per-platform Go modules, so `go get` pulls the one for your platform automatically — **no Rust toolchain or build step** — across 7 platforms (linux amd64/arm64/arm, darwin amd64/arm64, windows amd64/arm64). Kernel-backend features in this release: mTLS client certificates (`WithKernelClientCertificate`), identity federation (`WithFederatedTokenProvider*`), U2M on-disk token cache (`WithTokenCache`), configurable request timeout, opt-in lossy float64 decimals (`WithKernelDecimalAsFloat`), `GetArrowBatches` on the public `Rows` interface, kernel logs routed through the driver logger, cause-categorized telemetry errors, and an Azure U2M OAuth fix (databricks/databricks-sql-go#393, #399, #412, #440).
- Bump `golang.org/x/mod` to v0.40.0 to clear CVE-2026-56864 / CVE-2026-56865 (databricks/databricks-sql-go#460)

## v1.14.0 (2026-07-13)
- **Minimum Go version is now 1.25.0** (previously 1.20): the `go` directive was raised to 1.25.0 while clearing OSV-Scanner findings and updating dependencies. Consumers building with an older toolchain will need to upgrade Go (databricks/databricks-sql-go#368)
- Fix panic in `InitThriftClient` when the endpoint URL is malformed: the thrift transport was type-asserted to `*thrift.THttpClient` before the error from `NewTHttpClientWithOptions` was checked, so a URL that fails to parse (nil transport) caused `interface conversion: thrift.TTransport is nil, not *thrift.THttpClient`; the error is now returned instead (databricks/databricks-sql-go#394)
- Stop using `html/template` in the U2M OAuth callback page. Reachable use of `html/template` disabled the Go linker's dead-code elimination for the entire binary of any application importing this driver; the page is now rendered with plain string building and explicit HTML escaping, restoring full DCE (databricks/databricks-sql-go#343)
- Fix DECIMAL precision loss inside complex types: DECIMAL values nested in STRUCT, ARRAY, and MAP columns are now rendered losslessly with their exact scale (e.g. `19.99` instead of `19.990000000000002`), matching the behavior of top-level DECIMAL columns (databricks/databricks-sql-go#253)

## v1.13.1 (2026-07-07)
- Expose native Arrow decimal handling: add the `WithArrowNativeDecimal` connector option and the `useArrowNativeDecimal` DSN parameter so DECIMAL columns can be returned as native Arrow `decimal128` (via `GetArrowBatches`) instead of strings. When scanned through `database/sql`, native DECIMAL values are returned as lossless, scale-applied strings (databricks/databricks-sql-go#274)

## v1.13.0 (2026-06-04)
- Add SPOG (unified) host support: extract the org-id from the cluster HTTP path for non-Thrift requests, and fix U2M/M2M OAuth on unified AWS hosts (databricks/databricks-sql-go#367, #374)
- Cap CloudFetch Arrow batches to the server-declared row count to avoid over-reading (databricks/databricks-sql-go#372)
- Detach result streaming from `QueryContext` cancellation so in-flight results aren't dropped when the query context is cancelled (databricks/databricks-sql-go#373)

## v1.12.0 (2026-05-25)
- Retry transient S3 errors in CloudFetch downloads and staging PUT/GET/REMOVE operations (databricks/databricks-sql-go#355, #361)
- Telemetry: normalize host key for per-host client + breaker registries; stop retrying into 429s, honour Retry-After, fix userAgent (databricks/databricks-sql-go#354, #364)
- Bump dependencies to clear Go-1.20-compatible CVEs: golang-jwt, x/net, protobuf, go-jose v3.0.5 (CVE-2026-34986) (databricks/databricks-sql-go#360, #363)

## v1.11.1 (2026-05-20)
- Fix CloudFetch goroutine leak that retained Arrow buffers after Close (databricks/databricks-sql-go#357)

## v1.11.0 (2026-04-16)
- Enable telemetry by default with DSN-controlled priority (databricks/databricks-sql-go#320, #321, #322, #349)
- Add SPOG (Custom URL) routing support via `x-databricks-org-id` header (databricks/databricks-sql-go#347)
- Add statement-level query tag support (databricks/databricks-sql-go#341)
- Add AI coding agent detection to User-Agent header (databricks/databricks-sql-go#326)
- Fix CloudFetch returning stale column names from cached results (databricks/databricks-sql-go#351)
- Fix resource leak: close staging Rows in execStagingOperation (databricks/databricks-sql-go#325)

## v1.10.0 (2026-02-19)
- Add token federation / token provider support for OAuth (databricks/databricks-sql-go#290, #291, #292)
- Internal: add foundational telemetry infrastructure, disabled by default (databricks/databricks-sql-go#297, #304, #305, #311, #319)
- Fix type inference for int64/uint64 (BIGINT) and float64 (DOUBLE) (databricks/databricks-sql-go#316)
- Fix context loss in polling and connection close operations (databricks/databricks-sql-go#295)
- CloudFetch: allow configuration of HTTP client (databricks/databricks-sql-go#308)
- Add metric view metadata support (databricks/databricks-sql-go#286)

## v1.9.0 (2025-09-17)
- Added support for query tags
- Passing session params in open session request instead of SET commands (databricks/databricks-sql-go#283)

## v1.8.0 (2025-07-24)
- Add Arrow IPC Iterator

## v1.7.1 (2025-05-20)

- Add schema to ArrowBatchIterator (databricks/databricks-sql-go#267)
- Update thrift client library after cleaning up unused fields and structs (databricks/databricks-sql-go#268)
- Add nil handling for isStagingOperation to handle older DBR versions (databricks/databricks-sql-go#266)
- Security: Fix CVE-2024-45337 - Update golang.org/x/crypto (databricks/databricks-sql-go#264)

## v1.7.0 (2025-04-09)

- Enable cloud fetch mode by default (databricks/databricks-sql-go#260)
- Handle thrift protocol version for conditional feature support (direct results, LZ4 compression, Arrow support, parameterized queries) (databricks/databricks-sql-go#261)

## v1.6.2 (2025-03-18)

- Support positional query parameters (databricks/databricks-sql-go#247)
- Add custom auth headers into cloud fetch request (databricks/databricks-sql-go#249)
- Security: GO-2024-2947 - Update go-retryablehttp (databricks/databricks-sql-go#251)
- Security: CVE-2025-27144 - Resolve vulnerability in go-jose (databricks/databricks-sql-go#258)
- Bugfix: Handle incorrect EOF in fetchResultPage when TFetchResults call fails with an error (databricks/databricks-sql-go#255)

## v1.6.1 (2024-08-27)

- Fix CloudFetch "row number N is not contained in any arrow batch" error (databricks/databricks-sql-go#234)

## v1.6.0 (2024-07-31)

- Security: Resolve HIGH vulnerability in x/net (CVE-2023-39325) (databricks/databricks-sql-go#233 by @anthonycrobinson)
- Expose `dbsql.ConnOption` type (databricks/databricks-sql-go#202 by @shelldandy)
- Fix a connection leak in PingContext (databricks/databricks-sql-go#240 by @jackyhu-db)

## v1.5.7 (2024-06-05)

- Reverted dependencies upgrade because of compatibility issues (databricks/databricks-sql-go#228)
- Add more debug logging (databricks/databricks-sql-go#227)

## v1.5.6 (2024-05-28)

- Added connection option `WithSkipTLSHostVerify` (databricks/databricks-sql-go#225 by @jackyhu-db)

## v1.5.5 (2024-04-16)

- Fix: handle `nil` values passed as query parameter (databricks/databricks-sql-go#199 by @esdrasbeleza)
- Fix: provide content length on staging file put (databricks/databricks-sql-go#217 by @candiduslynx)
- Fix formatting of *float64 parameters (databricks/databricks-sql-go#215 by @esdrasbeleza)
- Fix: use correct tenant ID for different Azure domains (databricks/databricks-sql-go#210 by @tubiskasaroos)

## v1.5.4 (2024-04-10)

- Added OAuth support for GCP (databricks/databricks-sql-go#189 by @rcypher-databricks)
- Staging operations: stream files instead of loading into memory (databricks/databricks-sql-go#197 by @mdibaiee)
- Staging operations: don't panic on REMOVE (databricks/databricks-sql-go#205 by @candiduslynx)
- Fix formatting of Date/Time query parameters (databricks/databricks-sql-go#207 by @candiduslynx)

## v1.5.3 (2024-01-17)
- Bug fix for ArrowBatchIterator.HasNext(). Incorrectly returned true for result sets with zero rows.

## v1.5.2 (2023-11-17)
- Added .us domain to inference list for AWS OAuth
- Bug fix for OAuth m2m scopes, updated m2m authenticator to use "all-apis" scope.

## v1.5.1 (2023-10-17)
- Logging improvements
- Added handling for staging remove

## v1.5.0 (2023-10-02)
- Named parameter support
- Better handling of bad connection errors and specifying server protocol
- OAuth implementation
- Expose Arrow batches to users
- Add support for staging operations

## v1.4.0 (2023-08-09)
- Improve error information when query terminates in unexpected state
- Do not override global logger time format
- Enable Transport configuration for http client
- fix: update arrow to v12
- Updated doc.go for retrieving query id and connection id
- Bug fix issue 147: BUG with reading table that contains copied map
- Allow WithServerHostname to specify protocol

## v1.3.1 (2023-06-23)

- bug fix for panic when executing non record producing statements using DB.Query()/DB.QueryExec()

## v1.3.0 (2023-06-07)

- allow client provided authenticator
- more robust retry behaviour
- bug fix for null values in complex types

## v1.2.0 (2023-04-20)

- Improved error types and info

## v1.1.0 (2023-03-06)

- Feat: Support ability to retry on specific failures
- Fetch results in arrow format 
- Improve error message and retry behaviour

## v1.0.1 (2023-01-05)

Fixing cancel race condition 

## v1.0.0 (2022-12-20)

- Package doc (doc.go)
- Handle FLOAT values as float32
- Fix for result.AffectedRows
- Use new ctx when closing operation after cancel 
- Set default port to 443 

## v1.0.0-rc.1 (2022-12-19)

- Package doc (doc.go)
- Handle FLOAT values as float32
- Fix for result.AffectedRows
- Add or edit documentation above methods
- Tweaks to readme 
- Use new ctx when closing operation after cancel

## 0.2.2 (2022-12-12)

- Handle parsing negative years in dates
- fix thread safety issue 

## 0.2.1 (2022-12-05)

- Don't ignore error in InitThriftClient 
- Close optimization for Rows 
- Close operation after executing statement
- Minor change to examples
- P&R improvements 

## 0.1.x (Unreleased)

- Fix thread safety issue in connector

## 0.2.0 (2022-11-18)

- Support for DirectResults
- Support for context cancellation and timeout
- Session parameters (e.g.: timezone)
- Thrift Protocol update
- Several logging improvements
- Added better examples. See [workflow](https://github.com/databricks/databricks-sql-go/blob/main/examples/workflow/main.go)
- Added dbsql.NewConnector() function to help initialize DB
- Many other small improvements and bug fixes
- Removed support for client-side query parameterization
- Removed need to start DSN with "databricks://"

## 0.1.4 (2022-07-30)

- Fix: Could not fetch rowsets greater than the value of `maxRows` (#18)
- Updated default user agent
- Updated README and CONTRIBUTING

## 0.1.3 (2022-06-16)

- Add escaping of string parameters.

## 0.1.2 (2022-06-10)

- Fix timeout units to be milliseconds instead of nanos.

## 0.1.1 (2022-05-19)

- Fix module name

## 0.1.0 (2022-05-19)

- Initial release
