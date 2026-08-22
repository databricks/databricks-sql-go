# Engineer-bot learning log

This file is the canonical, human-gated knowledge log shared by two consumers
configured in `.bot/config.yaml`:

- `retrospective.log_path` — the retrospective flow APPENDS a dated section here
  when it extracts a durable, reusable learning (via a human-gated rolling PR).
- `author.knowledge_log` — the author phase READS this file so fixes benefit from
  what has been learned. Seeded here so the read path is never a missing file.

No learnings have been recorded yet. Dated sections are appended below by the
retrospective flow.
## Entries

### 2026-08-13: learnings since 2026-08-12T17:42:15Z
- **Context:** PR #442 configured a bot's `MODEL_ENDPOINT`; using the generic `.../serving-endpoints/anthropic/invocations` form 400'd every scheduled run (`Unsupported native API path .../anthropic/invocations/v1/messages`), confirmed empirically against the sibling databricks-sql-python cron.
  **Rule:** Point Databricks bot `MODEL_ENDPOINT` at the concrete `.../serving-endpoints/<model-name>/invocations` form, never `.../serving-endpoints/anthropic/invocations` — `translate_endpoint` early-returns on URLs already containing `/serving-endpoints/anthropic`, leaving the `/invocations` suffix so the CLI appends `/v1/messages` and hits HTTP 400.
- **Context:** PR #442's learning workflow declared a `workflow_dispatch` input as a string (`window-hours`) with a comment noting that `type: number` breaks the run.
  **Rule:** In GitHub Actions, a `workflow_dispatch` input declared `type: number` fails the whole run at startup ("workflow file issue") when the workflow also has a `schedule` trigger — declare numeric dispatch inputs as `type: string` and coerce to int downstream (e.g. via argparse).
- **Context:** PR #442 initially set `retrospective.system_prompt: prompts/retrospective_system.md`, a file that did not exist; the maintainer confirmed the engine treats a set-but-missing prompt path as a hard error, which would have failed the daily cron every run.
  **Rule:** For engine config keys that reference a file (e.g. `system_prompt`): an UNSET key falls back to the engine's built-in default, but a SET key pointing at a missing file is a hard error — omit the key entirely rather than point it at a nonexistent path.
- **Context:** PR #442 needed the retrospective flow to commit `.claude/knowledge/learning-log.md`, but the repo `.gitignore` ignored all of `.claude`; the fix scoped the ignore (`.claude/*` + `!.claude/knowledge/` + `.claude/knowledge/*` + `!.claude/knowledge/learning-log.md`) and seeded the file so the author read path never hits a missing file.
  **Rule:** When a bot/tool must commit a file under a normally-ignored directory, add scoped `.gitignore` negation for exactly that file (ignoring intermediate dirs still hides children, so re-include each level) and seed the file, so both the write (commit) and read paths resolve.

### 2026-08-21: learnings since 2026-08-20T17:32:23Z
- **Context:** PR #446 corrected CONNECTION_PARAMETERS.md to document how session confs and proxies diverge between the Thrift and kernel (SEA) backends.
  **Rule:** The kernel/SEA path is stricter than Thrift: session-conf keys are matched case-insensitively against an allowlist (non-allowlisted keys are dropped with a warning, a few hard-rejected e.g. HTTP 400 INVALID_CONF_VALUE), and only http(s) proxies are accepted (socks* URLs honored on Thrift are rejected at connect) — so a conf/proxy that works on Thrift may be silently inert or rejected on kernel; verify kernel behavior separately when adding or relying on any session parameter or proxy feature.

### 2026-08-22: learnings since 2026-08-21T17:32:18Z
- **Context:** PR #449 fixed Azure OAuth U2M on the kernel backend — `resolveKernelAuth` had been forwarding the cloud-inferred client id + scopes (`a.U2MClientID()` / `oauth.GetScopes`) that the Thrift path infers from the host, which routed the kernel's browser to a broken AAD authorize URL on Azure.
  **Rule:** The kernel/SEA backend runs ONE cloud-blind in-house workspace-federated U2M flow (OIDC discovery against `{host}/oidc`, no Azure branching), so its auth mapping must forward the fixed in-house `databricks-sql-connector` client + `offline_access`+`sql` scopes uniformly across all clouds — do NOT forward the Azure Entra-direct app id / `user_impersonation` scope the Thrift path uses, even though both paths authorize identically on AWS/GCP.
- **Context:** PR #449 replaced `oauth.GetScopes(host, nil)` with a hardcoded scope slice and initially wrote `[]string{"sql", "offline_access"}`, producing a High-severity failure: one test asserted the reversed order and contradicted its sibling test, because `resolveKernelAuth` returns `[]string{"offline_access", "sql"}`.
  **Rule:** OAuth scopes are a space-delimited unordered set per spec, but `reflect.DeepEqual` on `[]string` is order-sensitive — when hardcoding a scope slice that replaces `oauth.GetScopes` (which appends `offline_access` first, then `sql`), preserve the exact element order and keep all sibling assertions consistent, or DeepEqual comparisons/tests will fail spuriously.
- **Context:** PR #444 added `WithFederatedTokenProvider*` on the kernel path; for account-wide federation (no client id) the driver hands the kernel the raw un-exchanged external subject token via `set_auth_pat`, unlike the Thrift path which exchanges in-driver via `FederationProvider`. Reviewers repeatedly questioned whether this silently fails; a maintainer confirmed the kernel's behavior.
  **Rule:** The kernel performs a mandatory server-side token exchange for tokens presented on the PAT path UNLESS the token is same-issuer or non-JWT — so handing a raw external-IdP JWT subject token to `set_auth_pat` (client id only set for SP-wide federation) correctly federates account-wide without driver-side exchange. Rely on this documented kernel guarantee rather than assuming un-exchanged tokens are treated as literal PATs.
- **Context:** In both PR #449 and PR #444, reviewers (Copilot, peco-review-bot) flagged multiple stale/contradictory doc+comment sites after a kernel-auth behavior change — the behavior is mirrored across `doc.go`, `README.md`, `CONNECTION_PARAMETERS.md`, `internal/backend/kernel/auth.go` (Auth struct + provider-interface docs), `backend.go` (setAuth comment), and `auth/oauth/u2m/authenticator.go`.
  **Rule:** This repo documents kernel/Thrift auth semantics redundantly across many files; when changing behavior on one auth path, sweep ALL mirrored doc/comment sites in the same PR (not just the function you edited) or reviewers will flag stale contradictions.
