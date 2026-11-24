#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi

if ! command -v node >/dev/null 2>&1; then
    echo "🚨 Error: node is required to run dashboards test" >&2
    exit 1
fi

cd services/grafana
for jf in dashboards/*.json; do
    [ -f "$jf" ] || continue
    node -e "JSON.parse(require('fs').readFileSync(process.argv[1], 'utf8'))" "$jf"
done
