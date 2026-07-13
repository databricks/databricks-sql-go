#!/usr/bin/env bash
#
# kernel-lib.sh — build (or copy in) the Databricks SQL kernel static library
# for the cgo `databricks_kernel` backend.
#
# Mode 1 (source build, the default and the only thing that works today): clone
# the kernel at the pinned KERNEL_REV, `cargo build` a self-contained static
# archive with pure-Rust TLS, and drop the .a + C header where the per-platform
# cgo_<os>.go files link them (${SRCDIR}/lib/<os>_<arch> and ${SRCDIR}/include,
# both .gitignore'd). Kernel releases publish no binary assets yet, so building
# from a pinned commit is the current distribution model (mirrors the ODBC
# driver's Corrosion-from-source build).
#
# Local override: set KERNEL_LOCAL_A (and optionally KERNEL_LOCAL_HEADER) to
# copy an already-built archive instead of cloning + building — the analogue of
# the ODBC driver's KERNEL_LOCAL_PATH.
#
# Invoked by `make kernel-lib`, which supplies the environment below. It can
# also be run directly with the same variables set.
#
# Required env (Makefile provides these):
#   KERNEL_REV        kernel commit SHA to build (from the repo-root KERNEL_REV)
#   KERNEL_REPO       git URL of the kernel repo
#   KERNEL_SRC        working dir for the kernel checkout (gitignored)
#   KERNEL_LIB_DIR    dest dir for the .a (…/lib/<os>_<arch>)
#   KERNEL_INC_DIR    dest dir for the header (…/include)
# Optional env:
#   KERNEL_LOCAL_A        path to a prebuilt libdatabricks_sql_kernel.a to copy
#   KERNEL_LOCAL_HEADER   path to a databricks_kernel.h to copy (defaults to a
#                         header next to KERNEL_LOCAL_A, else the checkout's)

set -euo pipefail

: "${KERNEL_LIB_DIR:?KERNEL_LIB_DIR must be set (run via 'make kernel-lib')}"
: "${KERNEL_INC_DIR:?KERNEL_INC_DIR must be set (run via 'make kernel-lib')}"

LIB_NAME="libdatabricks_sql_kernel.a"
HEADER_NAME="databricks_kernel.h"

log() { printf '[kernel-lib] %s\n' "$*" >&2; }

emit_checksum() {
  # A reproducibility breadcrumb: the archive path + its content hash.
  local a="$KERNEL_LIB_DIR/$LIB_NAME"
  if command -v sha256sum >/dev/null 2>&1; then
    log "artifact: $a"
    log "sha256: $(sha256sum "$a" | cut -d' ' -f1)"
  elif command -v shasum >/dev/null 2>&1; then
    log "artifact: $a"
    log "sha256: $(shasum -a 256 "$a" | cut -d' ' -f1)"
  fi
}

mkdir -p "$KERNEL_LIB_DIR" "$KERNEL_INC_DIR"

# ── Local override: copy a prebuilt archive, skip clone + cargo entirely. ──────
if [ -n "${KERNEL_LOCAL_A:-}" ]; then
  [ -f "$KERNEL_LOCAL_A" ] || { log "KERNEL_LOCAL_A not found: $KERNEL_LOCAL_A"; exit 1; }
  local_header="${KERNEL_LOCAL_HEADER:-}"
  if [ -z "$local_header" ]; then
    # Prefer a header next to the archive; fall back to the checkout's include/.
    if [ -f "$(dirname "$KERNEL_LOCAL_A")/$HEADER_NAME" ]; then
      local_header="$(dirname "$KERNEL_LOCAL_A")/$HEADER_NAME"
    elif [ -n "${KERNEL_SRC:-}" ] && [ -f "$KERNEL_SRC/include/$HEADER_NAME" ]; then
      local_header="$KERNEL_SRC/include/$HEADER_NAME"
    fi
  fi
  [ -n "$local_header" ] && [ -f "$local_header" ] || {
    log "header not found; set KERNEL_LOCAL_HEADER to a $HEADER_NAME"; exit 1; }
  log "copying prebuilt archive from $KERNEL_LOCAL_A"
  cp "$KERNEL_LOCAL_A" "$KERNEL_LIB_DIR/$LIB_NAME"
  cp "$local_header" "$KERNEL_INC_DIR/$HEADER_NAME"
  emit_checksum
  exit 0
fi

# ── Mode 1: build from source at the pinned rev. ──────────────────────────────
: "${KERNEL_REV:?KERNEL_REV must be set (run via 'make kernel-lib')}"
: "${KERNEL_REPO:?KERNEL_REPO must be set (run via 'make kernel-lib')}"
: "${KERNEL_SRC:?KERNEL_SRC must be set (run via 'make kernel-lib')}"

# Cache short-circuit: if a previously built archive + header are present AND the
# rev-stamp beside the archive matches KERNEL_REV, the artifact is already what
# this pin would produce — skip the git fetch + cargo build entirely (and don't
# even require a Rust toolchain). `kernel-lib` is .PHONY and CI re-invokes it on
# every run after restoring the .a cache (keyed on KERNEL_REV), so without this
# guard the build re-ran and overwrote the just-restored archive every time.
# The stamp (not just file existence) is what makes this safe against a local
# KERNEL_REV bump with a stale on-disk .a: a changed pin fails the match and
# forces a rebuild. In CI the .a-cache key already includes KERNEL_REV, so a pin
# change misses the cache and the artifact is simply absent — either way a bump
# rebuilds.
REV_STAMP="$KERNEL_LIB_DIR/.kernel-rev"
if [ -f "$KERNEL_LIB_DIR/$LIB_NAME" ] && [ -f "$KERNEL_INC_DIR/$HEADER_NAME" ] &&
   [ -f "$REV_STAMP" ] && [ "$(cat "$REV_STAMP")" = "$KERNEL_REV" ]; then
  log "cache hit: $LIB_NAME already built for $KERNEL_REV — skipping source build"
  emit_checksum
  exit 0
fi

# Reject a source cross-build. `cargo build` below has no --target, so it emits
# the HOST triple's archive — but KERNEL_LIB_DIR is named for the TARGET
# (GOOS/GOARCH). If they differ, copying the host .a into the target dir would
# silently produce a wrong-arch artifact. Multi-OS is served by native per-OS
# runners (host == target, so this passes) or by staging a prebuilt cross-target
# .a via KERNEL_LOCAL_A (handled above, before this check) — not by cross-
# building the kernel from source, which the distribution design defers to the
# download path. Fail loud rather than mislink. Only enforced when the Makefile
# passes the host vars; a direct script call without them skips the check.
if [ -n "${KERNEL_GOOS:-}" ] && [ -n "${KERNEL_GOHOSTOS:-}" ] &&
   { [ "$KERNEL_GOOS" != "$KERNEL_GOHOSTOS" ] || [ "${KERNEL_GOARCH:-}" != "${KERNEL_GOHOSTARCH:-}" ]; }; then
  log "refusing source cross-build: target ${KERNEL_GOOS}/${KERNEL_GOARCH} != host ${KERNEL_GOHOSTOS}/${KERNEL_GOHOSTARCH}."
  log "build on a native ${KERNEL_GOOS} runner, or stage a prebuilt archive with KERNEL_LOCAL_A=<path>."
  exit 1
fi

command -v cargo >/dev/null 2>&1 || {
  log "cargo not found — the source build needs a Rust toolchain."
  log "install rustup, or use 'make kernel-lib KERNEL_LOCAL_A=<path>' with a prebuilt .a."
  exit 1
}

# Make $KERNEL_SRC a git repo pointed at the kernel remote, WITHOUT assuming an
# empty destination. CI caches build/kernel-src/target/ (for incremental Rust
# builds) but not .git, so on a cache hit the dir exists with a target/ subtree
# and no repo — a plain `git clone` would abort ("destination path already
# exists and is not an empty directory"). `git init` is idempotent and works
# whether the dir is absent, empty, or holds a restored target/; the kernel's
# own .gitignore excludes /target, so the later checkout leaves it untouched.
mkdir -p "$KERNEL_SRC"
if [ ! -d "$KERNEL_SRC/.git" ]; then
  log "initializing git repo in $KERNEL_SRC (-> $KERNEL_REPO)"
  git -C "$KERNEL_SRC" init --quiet
fi
if git -C "$KERNEL_SRC" remote get-url origin >/dev/null 2>&1; then
  git -C "$KERNEL_SRC" remote set-url origin "$KERNEL_REPO"
else
  git -C "$KERNEL_SRC" remote add origin "$KERNEL_REPO"
fi

log "fetching + checking out $KERNEL_REV"
git -C "$KERNEL_SRC" fetch --all --tags --quiet || true
# checkout -f: this is a tool-managed, gitignored scratch checkout of a pinned
# third-party repo, so force past any leftover files (e.g. a source tree left
# behind if .git was lost). -f overwrites tracked-path collisions but leaves the
# gitignored target/ alone, so a cache-restored build tree survives. Without -f,
# an untracked-file conflict would masquerade as "commit not reachable" and send
# us down the PR-head fallback for the wrong reason.
#
# KERNEL_REV may be a bare commit that only lives on a PR ref (e.g. #163's head
# before it merges), so try the commit directly, then the PR head ref.
if ! git -C "$KERNEL_SRC" checkout -f --quiet "$KERNEL_REV" 2>/dev/null; then
  log "commit not directly reachable; trying PR head refs"
  git -C "$KERNEL_SRC" fetch --quiet origin '+refs/pull/*/head:refs/remotes/origin/pr/*' || true
  git -C "$KERNEL_SRC" checkout -f --quiet "$KERNEL_REV" || {
    log "could not check out $KERNEL_REV — is it pushed/fetchable?"; exit 1; }
fi
log "kernel at $(git -C "$KERNEL_SRC" rev-parse --short HEAD)"

# --no-default-features --features tls-rustls: pure-Rust TLS (no system OpenSSL)
# keeps the archive self-contained and cross-compile-tractable. The kernel's
# default is tls-native, so the override is required.
# --locked: build against the kernel's committed Cargo.lock rather than
# re-resolving, so a fixed KERNEL_REV yields a fixed dependency graph (paired
# with the pinned rustc in rust-toolchain.toml, the .a is reproducible). Fails
# loud if the lock is stale instead of silently pulling newer deps.
log "cargo build --release --locked --no-default-features --features tls-rustls"
( cd "$KERNEL_SRC" && cargo build --release --locked --no-default-features --features tls-rustls )

src_a="$KERNEL_SRC/target/release/$LIB_NAME"
src_h="$KERNEL_SRC/include/$HEADER_NAME"
[ -f "$src_a" ] || { log "expected archive not produced: $src_a"; exit 1; }
[ -f "$src_h" ] || { log "expected header not found: $src_h"; exit 1; }

cp "$src_a" "$KERNEL_LIB_DIR/$LIB_NAME"
cp "$src_h" "$KERNEL_INC_DIR/$HEADER_NAME"
# Record the rev this archive was built for so a later run can short-circuit
# (see the cache short-circuit above). Written last, only after a successful
# build, so a stamp never claims an artifact that isn't there.
printf '%s\n' "$KERNEL_REV" > "$REV_STAMP"
emit_checksum
