#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
export PATH="$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin:$HOME/go/bin:${GOPATH:-$HOME/go}/bin:$PATH"
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh"
ensure_bazel_build_files "$REPO_ROOT"

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
    if [ -f "$f" ] && [ "$(wc -c < "$f")" -gt 23040 ]; then
        echo "🚨 [Aerial Verify] $f exceeds 23040 bytes" >&2
        exit 1
    fi
done

[ -x scripts/check-cnpg-immutability.sh ] && ./scripts/check-cnpg-immutability.sh "$REPO_ROOT"
(cd scripts/code-health && go run . -root ../.. -baseline ../../.code-health-baseline.json)

MODS=""
if [ "$MODE" = "full" ] || echo "$CHANGED_FILES" | grep -q -E '(^pkg/|^\.golangci\.ya?ml|^scripts/|^go\.(mod|sum)$|^build/|^MODULE\.bazel|^\.github/)'; then
    MODS=$(discover_go_services)
else
    for m in $(discover_go_services); do echo "$CHANGED_FILES" | grep -q "^$m/" && MODS="$MODS $m"; done
fi

for m in $MODS; do (cd "$m" && GOTOOLCHAIN="${GOTOOLCHAIN:-go1.27.3}" golangci-lint run --concurrency "${AERIAL_LINT_CONCURRENCY:-4}" --allow-parallel-runners --path-prefix="$m/" --config "$REPO_ROOT/.golangci.yml" ./...); done
[ -x scripts/check-deadcode.sh ] && ./scripts/check-deadcode.sh $MODS

M_TESTS="//web/theme:theme_test //services/docs-service:syntax_test //services/homepage:homepage_test //services/grafana:dashboards_test"
if command -v kubectl >/dev/null 2>&1 && command -v atlas >/dev/null 2>&1; then
    bazel test //k8s:manifests_test //services/eri/pkg/db/migrations:atlas_validate_test $M_TESTS
elif [ -n "${CI:-}" ]; then
    echo "🚨 [Aerial Verify] kubectl and atlas are required in CI" >&2; exit 1
else
    echo "⚠️  [Aerial Verify] kubectl or atlas missing; skipping manifest and atlas tests outside CI." >&2
    bazel test $M_TESTS
fi

TEST_TARGETS=""; TARGET_PKGS=""
if [ "$MODE" = "full" ]; then
    TEST_TARGETS=$(bazel query "kind('go_test rule', //...)")
else
    b_pkgs=()
    for f in $CHANGED_FILES; do
        case "$f" in *.go)
            [ -f "$f" ] || continue
            d=$(dirname "$f"); TARGET_PKGS="$TARGET_PKGS $d"
            while [ -n "$d" ] && [ "$d" != "." ] && [ "$d" != "/" ]; do
                [ -f "$d/BUILD.bazel" ] && { b_pkgs+=("//${d}:all"); break; }; d=$(dirname "$d")
            done ;;
        esac
    done
    [ ${#b_pkgs[@]} -gt 0 ] && TEST_TARGETS=$(bazel query "kind('go_test rule', set($(printf '%s ' "${b_pkgs[@]}")))")
fi

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

if [ "$MODE" = "full" ] || echo "$CHANGED_FILES" | grep -q -E "^(services/console/ui|web/theme)/"; then
    [ -f services/console/ui/package.json ] && command -v npm >/dev/null 2>&1 && (cd services/console/ui && { { [ -d node_modules ] && [ ! package-lock.json -nt node_modules/.package-lock.json ]; } || npm ci --no-audit --no-fund --silent; } && npm run --silent typecheck && npm run --silent test:coverage && npm run --silent build)
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
