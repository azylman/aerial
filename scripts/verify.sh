#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && cd "$REPO_ROOT"
export PATH="$HOME/go/bin:${GOPATH:-$HOME/go}/bin:$PATH"
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"
MODE="full"; for arg in "$@"; do case "$arg" in --staged) MODE="staged" ;; --full) MODE="full" ;; esac; done
CHANGED_FILES=""
if [ "$MODE" = "staged" ]; then
    CHANGED_FILES=$(git diff --cached --name-only 2>/dev/null || true)
    [ -n "$CHANGED_FILES" ] || { echo "✅ [Aerial Verify] Nothing staged."; exit 0; }
fi
unformatted=$(gofmt -l . 2>/dev/null | grep -v -E '(^|/)vendor/' || true)
[ -z "$unformatted" ] || { echo "🚨 [Aerial Verify] Unformatted Go files: $unformatted" >&2; exit 1; }
if command -v buildifier >/dev/null 2>&1; then
    bzl_files=$(git ls-files '*.bazel' '*.bzl' 2>/dev/null || true)
    [ -n "$bzl_files" ] && buildifier -mode=check -lint=warn $bzl_files
fi
for f in rules/*/*.md rules/*/*/*.md GEMINI.md AGENTS.md; do
    [ -f "$f" ] && [ "$(wc -c < "$f")" -gt 23040 ] && { echo "🚨 [Aerial Verify] $f exceeds 23040 bytes" >&2; exit 1; }
done
run_ch="false"
if [ "$MODE" = "full" ]; then run_ch="true"; else
    for f in $CHANGED_FILES; do case "$f" in *.go|*.sh|.code-health-baseline.json) run_ch="true"; break ;; esac; done
fi
[ "$run_ch" = "true" ] && (cd scripts/code-health && go run . -root ../.. -baseline ../../.code-health-baseline.json)
MODS=""
if [ "$MODE" = "full" ]; then MODS=$(discover_go_services); else
    for m in $(discover_go_services); do echo "$CHANGED_FILES" | grep -q "^$m/" && MODS="$MODS $m"; done
fi
if [ -n "$MODS" ] && { [ "${CI:-}" = "true" ] || [ "${GITHUB_ACTIONS:-}" = "true" ]; }; then
    command -v deadcode >/dev/null 2>&1 || { echo "🚨 [Aerial Verify] deadcode required in CI" >&2; exit 1; }
fi
for m in $MODS; do
    (cd "$m" && golangci-lint run ./...)
    if command -v deadcode >/dev/null 2>&1; then
        if [ "$m" = "pkg" ]; then (cd "$m" && deadcode -test ./...); else
            f_flag="-filter=^github\.com/azylman/aerial/${m}(/|$)"
            if [ "$m" = "services/eri" ]; then
                m_pkgs=$(go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./services/eri/... ./services/eri-restate/...)
            else
                m_pkgs=$(cd "$m" && go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./...)
            fi
            [ -n "$m_pkgs" ] && deadcode $f_flag $m_pkgs
            (cd "$m" && deadcode -test $f_flag ./...)
        fi
    fi
done
bazel test //k8s:manifests_test //web/theme:theme_test //services/docs-service:syntax_test //services/homepage:homepage_test //services/grafana:dashboards_test //services/eri/pkg/db/migrations:atlas_validate_test
TEST_TARGETS=""
if [ "$MODE" = "full" ]; then
    TEST_TARGETS=$(bazel query "kind('go_test rule', //...)" 2>/dev/null || true)
else
    go_changed=()
    for f in $CHANGED_FILES; do case "$f" in *.go) [ -f "$f" ] && go_changed+=("$f") ;; esac; done
    if [ ${#go_changed[@]} -gt 0 ]; then
        f_set=$(printf '%s ' "${go_changed[@]}")
        TEST_TARGETS=$(bazel query "kind('go_test rule', rdeps(//..., same_pkg_direct_rdeps(set($f_set))))" 2>/dev/null || true)
    fi
fi
if [ -n "$TEST_TARGETS" ]; then
    rm -rf bazel-testlogs
    # shellcheck disable=SC2086
    bazel coverage $TEST_TARGETS
    for cov in $(find bazel-testlogs -name "coverage.dat"); do
        awk '
            /^mode:/ { next }
            NF >= 3 { tot += $(NF-1); if ($NF > 0) cov += $(NF-1) }
            END {
                if (tot > 0 && (cov / tot) * 100.0 < 95.0) {
                    printf "🚨 [Aerial Verify] Coverage %.1f%% below 95.0%% floor\n", (cov/tot)*100.0 > "/dev/stderr"
                    exit 1
                }
            }
        ' "$cov" || exit 1
    done
fi
if [ "$MODE" = "full" ] || echo "$CHANGED_FILES" | grep -q -E "^(services/console/ui|web/theme)/"; then
    if [ -f services/console/ui/package.json ] && command -v npm >/dev/null 2>&1; then
        (cd services/console/ui && { [ -d node_modules ] || npm ci --no-audit --no-fund --silent; } && npm run --silent typecheck && npm run --silent test:coverage && npm run --silent build)
    fi
fi
if [ "$MODE" = "staged" ] && git diff --cached --quiet 2>/dev/null; then
    tree_sha=$(git write-tree 2>/dev/null || true)
    git_dir=$(git rev-parse --git-dir 2>/dev/null || true)
    [ -n "$git_dir" ] && [ -n "$tree_sha" ] && printf '%s\n' "$tree_sha" > "$git_dir/.aerial-verified-tree"
fi
echo "✅ [Aerial Verify] $MODE verification passed."
