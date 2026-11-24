#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
export PATH="$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin:$HOME/go/bin:${GOPATH:-$HOME/go}/bin:$PATH"

MODS="${*:-}"
[ -z "$MODS" ] && exit 0

if ! command -v deadcode >/dev/null 2>&1; then
    go install golang.org/x/tools/cmd/deadcode@v0.52.0 || {
        echo "🚨 [Aerial Verify] deadcode required" >&2
        exit 1
    }
fi

run_dc() {
    local out
    if ! out="$("$@" 2>&1)"; then
        echo "🚨 [Aerial Verify] deadcode execution failed: $out" >&2
        exit 1
    fi
    printf '%s\n' "$out"
}

for m in $MODS; do
    if [ "$m" = "pkg" ]; then
        mp=$(go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./...)
        if [ -n "$mp" ]; then
            dp=$(run_dc deadcode -filter='^github\.com/azylman/aerial/pkg/' $mp)
            dp=$(echo "$dp" | grep -v -E '((^|/)pkg/(.*/)?[^/]*test/[^/]+:|pkg/pubsub/memory\.go:|unreachable func: ResetSensitiveTokensForTest$)' || true)
            if [ -n "$dp" ]; then
                echo "🚨 [Aerial Verify] Unreachable code in pkg: $dp" >&2
                exit 1
            fi
        fi
        dt=$(run_dc deadcode -test -filter='^github\.com/azylman/aerial/pkg/' $(go list ./...))
        if [ -n "$dt" ]; then
            echo "🚨 [Aerial Verify] Unreachable test code in pkg: $dt" >&2
            exit 1
        fi
    else
        fl="-filter=^github\.com/azylman/aerial/${m}(/|$)"
        if [ "$m" = "services/eri" ]; then
            p=$(go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./services/eri/... ./services/eri-restate/...)
        else
            p=$(cd "$m" && go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./...)
        fi
        if [ -n "$p" ]; then
            dp=$(run_dc deadcode "$fl" $p)
            if [ -n "$dp" ]; then
                echo "🚨 [Aerial Verify] Unreachable code in $m: $dp" >&2
                exit 1
            fi
        fi
        dt=$(cd "$m" && run_dc deadcode -test "$fl" ./...)
        if [ -n "$dt" ]; then
            echo "🚨 [Aerial Verify] Unreachable test code in $m: $dt" >&2
            exit 1
        fi
    fi
done
