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
