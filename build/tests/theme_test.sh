#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi

if ! command -v node >/dev/null 2>&1; then
    echo "🚨 Error: node is required to run theme tests" >&2
    exit 1
fi

cd web/theme
node --check aerial-theme.js
node --test aerial-theme.test.js
