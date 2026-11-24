#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && cd "$REPO_ROOT"
export PATH="$HOME/go/bin:${GOPATH:-$HOME/go}/bin:$PATH"
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"
MODE="full"; for a in "$@"; do case "$a" in --staged) MODE="staged";; --full) MODE="full";; esac; done
CHANGED_FILES=""; [ "$MODE" = "staged" ] && { CHANGED_FILES=$(git diff --cached --name-only 2>/dev/null || true); [ -n "$CHANGED_FILES" ] || { echo "✅ [Aerial Verify] Nothing staged."; exit 0; }; }
unf=$(gofmt -l . 2>/dev/null | grep -v -E '(^|/)vendor/' || true); [ -z "$unf" ] || { echo "🚨 [Aerial Verify] Unformatted Go files: $unf" >&2; exit 1; }
command -v buildifier >/dev/null 2>&1 && { bzl=$(git ls-files '*.bazel' '*.bzl' 2>/dev/null || true); [ -n "$bzl" ] && buildifier -mode=check -lint=warn $bzl; }
for f in rules/*/*.md rules/*/*/*.md GEMINI.md AGENTS.md; do [ -f "$f" ] && [ "$(wc -c < "$f")" -gt 23040 ] && { echo "🚨 [Aerial Verify] $f exceeds 23040 bytes" >&2; exit 1; }; done
[ -x scripts/check-cnpg-immutability.sh ] && ./scripts/check-cnpg-immutability.sh "$REPO_ROOT"
run_ch="false"
if [ "$MODE" = "full" ]; then run_ch="true"; else
    for f in $CHANGED_FILES; do case "$f" in *.go|*.sh|.code-health-baseline.json) run_ch="true"; break ;; esac; done
fi
[ "$run_ch" = "true" ] && (cd scripts/code-health && go run . -root ../.. -baseline ../../.code-health-baseline.json)
MODS=""
if [ "$MODE" = "full" ] || echo "$CHANGED_FILES" | grep -q -E '(^pkg/|^\.golangci\.ya?ml|^scripts/|^go\.(mod|sum)$|^build/|^MODULE\.bazel|^\.github/)'; then
    MODS=$(discover_go_services)
else
    for m in $(discover_go_services); do echo "$CHANGED_FILES" | grep -q "^$m/" && MODS="$MODS $m"; done
fi
if [ -n "$MODS" ] && { [ "${CI:-}" = "true" ] || [ "${GITHUB_ACTIONS:-}" = "true" ]; }; then
    command -v deadcode >/dev/null 2>&1 || { echo "🚨 [Aerial Verify] deadcode required in CI" >&2; exit 1; }
fi
for m in $MODS; do
    (cd "$m" && golangci-lint run ./...)
    if command -v deadcode >/dev/null 2>&1; then
        if [ "$m" = "pkg" ]; then (cd "$m" && deadcode -test ./...); else
            fl="-filter=^github\.com/azylman/aerial/${m}(/|$)"
            p=$(if [ "$m" = "services/eri" ]; then go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./services/eri/... ./services/eri-restate/...; else (cd "$m" && go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./...); fi)
            [ -n "$p" ] && deadcode "$fl" $p; (cd "$m" && deadcode -test "$fl" ./...)
        fi
    fi
done
bazel test //k8s:manifests_test //web/theme:theme_test //services/docs-service:syntax_test //services/homepage:homepage_test //services/grafana:dashboards_test //services/eri/pkg/db/migrations:atlas_validate_test
TEST_TARGETS=""; TARGET_PKGS=""
if [ "$MODE" = "full" ]; then
    TEST_TARGETS=$(bazel query "kind('go_test rule', //...)" 2>/dev/null || true)
else
    go_changed=()
    for f in $CHANGED_FILES; do case "$f" in *.go) [ -f "$f" ] && go_changed+=("$f") && TARGET_PKGS="$TARGET_PKGS github.com/azylman/aerial/$(dirname "$f")" ;; esac; done
    if [ ${#go_changed[@]} -gt 0 ]; then
        f_set=$(printf '%s ' "${go_changed[@]}")
        TEST_TARGETS=$(bazel query "kind('go_test rule', rdeps(//..., same_pkg_direct_rdeps(set($f_set))))" 2>/dev/null || true)
    fi
fi
if [ -n "$TEST_TARGETS" ]; then
    rm -rf bazel-testlogs && cov_status=0
    # shellcheck disable=SC2086
    bazel coverage --keep_going $TEST_TARGETS || cov_status=1
    cov_files=$(find bazel-testlogs -name "coverage.dat")
    if [ -n "$cov_files" ]; then
        # shellcheck disable=SC2086
        awk -v mode="$MODE" -v filter="$TARGET_PKGS" '
            BEGIN { split(filter, fa, " "); for (i in fa) want[fa[i]] = 1 }
            /^mode:/ { next }
            NF >= 3 {
                b = $1; s = $(NF-1) + 0; c = $NF + 0
                if (!(b in stmts)) { stmts[b] = s; f = b; sub(/:.*$/, "", f); sub(/\/[^/]+$/, "", f); pkg[b] = f }
                if (c > 0) cov[b] = 1
            }
            END {
                for (b in stmts) { p = pkg[b]; p_tot[p] += stmts[b]; if (cov[b]) p_cov[p] += stmts[b] }
                fail = 0
                for (p in p_tot) {
                    if (mode == "staged" && filter != "" && !(p in want)) continue
                    if (p_tot[p] > 0 && ((p_cov[p] / p_tot[p]) * 100.0) < 95.0) {
                        printf "🚨 [Aerial Verify] %s: %.1f%% below 95.0%% floor\n", p, (p_cov[p]/p_tot[p])*100.0 > "/dev/stderr"; fail = 1
                    }
                }
                if (fail) exit 1
            }
        ' $cov_files || exit 1
    fi
    [ "$cov_status" -eq 0 ] || { echo "🚨 [Aerial Verify] One or more test targets failed." >&2; exit 1; }
fi
if [ "$MODE" = "full" ] || echo "$CHANGED_FILES" | grep -q -E "^(services/console/ui|web/theme)/"; then
    [ -f services/console/ui/package.json ] && command -v npm >/dev/null 2>&1 && (cd services/console/ui && { [ -d node_modules ] || npm ci --no-audit --no-fund --silent; } && npm run --silent typecheck && npm run --silent test:coverage && npm run --silent build)
fi
if [ -f services/console/ui/playwright.config.ts ] && command -v npm >/dev/null 2>&1 && command -v go >/dev/null 2>&1; then
    (cd services/console/ui && node -e "require('@playwright/test').chromium.launch().then(b => b.close()).catch(() => process.exit(1))" >/dev/null 2>&1) && (cd services/console/ui && npm run --silent e2e)
fi
if [ "$MODE" = "staged" ] && git diff --cached --quiet 2>/dev/null; then
    tree_sha=$(git write-tree 2>/dev/null || true)
    git_dir=$(git rev-parse --git-dir 2>/dev/null || true)
    [ -n "$git_dir" ] && [ -n "$tree_sha" ] && printf '%s\n' "$tree_sha" > "$git_dir/.aerial-verified-tree"
fi
echo "✅ [Aerial Verify] $MODE verification passed."
