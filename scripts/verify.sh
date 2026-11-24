#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && cd "$REPO_ROOT"
export PATH="$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin:$HOME/go/bin:${GOPATH:-$HOME/go}/bin:$PATH"
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"

MODE="full"
for a in "$@"; do case "$a" in --staged) MODE="staged";; --full) MODE="full";; esac; done

CHANGED_FILES=""
if [ "$MODE" = "staged" ]; then
    CHANGED_FILES=$(git diff --cached --name-only 2>/dev/null || true)
    [ -z "$CHANGED_FILES" ] && { echo "✅ [Aerial Verify] Nothing staged."; exit 0; }
    unf=$(gofmt -l $CHANGED_FILES 2>/dev/null | grep -v -E '(^|/)vendor/' || true)
    [ -n "$unf" ] && for f in $unf; do gofmt -w "$f" && git add "$f"; done
else
    unf=$(gofmt -l . 2>/dev/null | grep -v -E '(^|/)vendor/' || true)
    [ -z "$unf" ] || { echo "🚨 [Aerial Verify] Unformatted Go files: $unf" >&2; exit 1; }
fi

if command -v buildifier >/dev/null 2>&1; then
    bzl=$(git ls-files '*.bazel' '*.bzl' 2>/dev/null || true)
    [ -n "$bzl" ] && buildifier -mode=check -lint=warn $bzl
fi

for f in rules/*/*.md rules/*/*/*.md GEMINI.md AGENTS.md; do
    [ -f "$f" ] && [ "$(wc -c < "$f")" -gt 23040 ] && { echo "🚨 [Aerial Verify] $f exceeds 23040 bytes" >&2; exit 1; }
done

[ -x scripts/check-cnpg-immutability.sh ] && ./scripts/check-cnpg-immutability.sh "$REPO_ROOT"
bazel run //scripts/code-health -- -root "$REPO_ROOT" -baseline "$REPO_ROOT/.code-health-baseline.json"

TARGET_PKGS=""
if [ "$MODE" = "full" ] || echo "$CHANGED_FILES" | grep -q -E '(^\.golangci\.ya?ml|^scripts/|^go\.(mod|sum)$|^build/|^MODULE\.bazel|^\.github/)'; then
    MODS=$(discover_go_services)
    LINT_ARGS=$(for m in $MODS; do printf './%s/... ' "$m"; done)
    SH_TESTS=$(bazel query "kind('sh_test rule', //...)" 2>/dev/null)
    TEST_TARGETS=$(bazel query "kind('go_test rule', //...)" 2>/dev/null)
else
    b_pkgs=()
    for f in $CHANGED_FILES; do
        [ -e "$f" ] || continue
        case "$f" in *.go) TARGET_PKGS="$TARGET_PKGS $(dirname "$f")" ;; esac
        d=$(dirname "$f")
        while [ -n "$d" ] && [ "$d" != "." ] && [ "$d" != "/" ]; do
            [ -f "$d/BUILD.bazel" ] && { b_pkgs+=("//${d}:all"); break; }
            d=$(dirname "$d")
        done
    done
    MODS=""; LINT_ARGS=""; SH_TESTS=""; TEST_TARGETS=""
    if [ ${#b_pkgs[@]} -gt 0 ]; then
        RDEPS=$(bazel query "rdeps(//..., set($(printf '%s ' "${b_pkgs[@]}")))" 2>/dev/null || true)
        r_pkgs=$(bazel query "kind('go_library|go_binary|go_test', set($RDEPS))" 2>/dev/null | grep -E '^//(pkg|services|mcp|scripts)/' | sed -E 's#^//([^:]+):.*#\1#' | sort -u || true)
        LINT_ARGS=$(for p in $r_pkgs; do printf './%s ' "$p"; done)
        MODS=$(for p in $r_pkgs; do case "$p" in pkg|pkg/*) echo pkg;; services/*|mcp/*|scripts/*) echo "$p" | cut -d/ -f1,2;; esac; done | sort -u)
        SH_TESTS=$(bazel query "kind('sh_test rule', set($RDEPS))" 2>/dev/null || true)
        TEST_TARGETS=$(bazel query "kind('go_test rule', set($RDEPS))" 2>/dev/null || true)
    fi
fi

[ -n "$LINT_ARGS" ] && GOTOOLCHAIN="${GOTOOLCHAIN:-go1.27.3}" golangci-lint run --concurrency "${AERIAL_LINT_CONCURRENCY:-4}" --allow-parallel-runners --config "$REPO_ROOT/.golangci.yml" $LINT_ARGS
[ -x scripts/check-deadcode.sh ] && [ -n "$MODS" ] && ./scripts/check-deadcode.sh $MODS

# shellcheck disable=SC2086
[ -n "$SH_TESTS" ] && bazel test $SH_TESTS

if [ -n "$TEST_TARGETS" ]; then
    rm -rf bazel-testlogs && cov_status=0
    # shellcheck disable=SC2086
    bazel coverage --keep_going --instrumentation_filter="^//(services|mcp|pkg|scripts|images)[/:]" $TEST_TARGETS || cov_status=1
    cov_files=$(find bazel-testlogs/ -name "coverage.dat")
    [ -n "$cov_files" ] || { echo "🚨 [Aerial Verify] No coverage files produced." >&2; exit 1; }
    # shellcheck disable=SC2086
    awk -v mode="$MODE" -v filter="$TARGET_PKGS" -f "$REPO_ROOT/scripts/lib/coverage-gate.awk" $cov_files || exit 1
    [ "$cov_status" -eq 0 ] || { echo "🚨 [Aerial Verify] One or more test targets failed." >&2; exit 1; }
fi

if [ "$MODE" = "full" ] || echo "$CHANGED_FILES" | grep -q -E "^services/console/(ui|static)/"; then
    [ -f services/console/ui/playwright.config.ts ] && command -v npm >/dev/null 2>&1 && command -v go >/dev/null 2>&1 && \
        { ! (cd services/console/ui && node -e "require('@playwright/test').chromium.launch().then(b => b.close()).catch(() => process.exit(1))" >/dev/null 2>&1) || (cd services/console/ui && npm run --silent build && npm run --silent e2e); }
fi
if [ "$MODE" = "staged" ] && git diff --quiet 2>/dev/null; then
    tree_sha=$(git write-tree 2>/dev/null || true); git_dir=$(git rev-parse --git-dir 2>/dev/null || true)
    [ -n "$git_dir" ] && [ -n "$tree_sha" ] && printf '%s\n' "$tree_sha" > "$git_dir/.aerial-verified-tree"
fi
echo "✅ [Aerial Verify] $MODE verification passed."
