#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi
if [ -n "${NODE_BIN:-}" ] && [ -x "$NODE_BIN" ]; then export PATH="$(cd "$(dirname "$NODE_BIN")" && pwd):$PATH"; fi

if ! command -v node >/dev/null 2>&1; then
    echo "🚨 Error: node is required to run docs syntax tests" >&2
    exit 1
fi

node --check services/docs-service/app/assets/js/plugins/docsify-mermaid.js
node --check services/docs-service/app/assets/js/plugins/docsify-theme.js
