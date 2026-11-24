#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MODS="${*:-}"
[ -z "$MODS" ] && exit 0

if ! command -v deadcode >/dev/null 2>&1; then
    go install golang.org/x/tools/cmd/deadcode@v0.52.0 || {
        echo "🚨 [Aerial Verify] deadcode required" >&2
        exit 1
    }
fi

for m in $MODS; do
    if [ "$m" = "pkg" ]; then
        mp=$(go list -f '{{if eq .Name "main"}}{{.ImportPath}}{{end}}' ./...)
        if [ -n "$mp" ]; then
            dp=$(deadcode -filter='^github\.com/azylman/aerial/pkg/' $mp | grep -v -E '((^|/)pkg/(.*/)?[^/]*test/[^/]+:|pkg/pubsub/memory\.go:|unreachable func: ResetSensitiveTokensForTest$)' || true)
            if [ -n "$dp" ]; then
                echo "🚨 [Aerial Verify] Unreachable code in pkg: $dp" >&2
                exit 1
            fi
        fi
        dt=$(deadcode -test -filter='^github\.com/azylman/aerial/pkg/' $(go list ./...) 2>/dev/null || true)
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
            dp=$(deadcode "$fl" $p 2>/dev/null || true)
            if [ -n "$dp" ]; then
                echo "🚨 [Aerial Verify] Unreachable code in $m: $dp" >&2
                exit 1
            fi
        fi
        dt=$(cd "$m" && deadcode -test "$fl" ./... 2>/dev/null || true)
        if [ -n "$dt" ]; then
            echo "🚨 [Aerial Verify] Unreachable test code in $m: $dt" >&2
            exit 1
        fi
    fi
done
