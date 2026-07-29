Repo-specific review guidance for `databricks-sql-go` (the Databricks SQL
`database/sql` driver for Go). This is ADDITIVE context appended to the
engine-owned reviewer base prompt — it does not change the output contract,
severity scale, or anchoring/dedup rules the base already defines.

You are reviewing a Go `database/sql` driver. Work through each review axis against
the changed code — a clean-looking diff still warrants checking every one; don't
stop at the first pass or finalize with "looks good" until you've actually
considered these:

- **Correctness & logic:** off-by-one, inverted/incorrect conditionals, wrong
  parameter passing, broken control flow, state left inconsistent, results
  silently dropped, `database/sql` contract violations (driver.Rows/Stmt/Conn
  semantics, `driver.ErrBadConn` handling, NULL/`sql.RawBytes` lifetimes).
- **Errors & resources:** unchecked errors (errcheck), swallowed/over-broad error
  handling, `err` shadowing, missing `defer rows.Close()` / `stmt.Close()`,
  leaked goroutines or HTTP bodies not closed, context cancellation not honored.
- **Concurrency:** data races on shared connector/connection state, maps written
  without a lock, goroutine leaks, misuse of `context.Context` deadlines/cancel.
- **Tests & coverage:** behavior changed without a test; assertions removed or
  weakened; tests that can't actually fail; missing edge cases. New/changed
  behavior should carry unit coverage (mocked/`httptest`), and where observable
  end-to-end, a `driver_e2e_test.go` test.
- **Edge cases & inputs:** nil / empty / boundary values, large result sets,
  CloudFetch/Arrow batching, retries/timeouts, partial failure, encoding.
- **Contracts & API:** exported signature/behavior changes that break callers;
  doc comments that no longer match; documented invariants violated. Public-API
  stability matters.
- **Security:** SQL injection via parameter handling, credential/token handling
  (never logged), TLS/proxy config, unsafe use of `unsafe`.
- **Repo conventions:** `gofmt -s`; the `.golangci.yml` linters (errcheck, gosec,
  govet, staticcheck, ineffassign, bodyclose, nakedret, …); **depguard** — a new
  third-party import must be added to the allowlist or lint fails; DCO sign-off is
  required on every commit. When a finding is convention-anchored, cite the rule.

Landmarks for this repo:
- Conventions live in `CONTRIBUTING.md` and `.golangci.yml`. Build/test via the
  `Makefile` (`make test`, `make lint`, `make linux`).
- Source is the root `dbsql` package (`connection.go`, `connector.go`,
  `statement.go`, `rows`, `parameters.go`, …) + `internal/` (Thrift client,
  fetcher). Tests are `*_test.go` alongside; `driver_e2e_test.go` holds the
  live-warehouse e2e tests (credential-gated, `t.Skip` without creds). The
  SEA/kernel backend (`kernel_*.go`, build tag `databricks_kernel`) is opt-in and
  CGO-only; `build/` (native lib scripts) and `testdata/` (fixtures) are generated
  boundaries — flag hand-edits to them, and to the `stringer`-generated block in
  `internal/client/client.go`.
