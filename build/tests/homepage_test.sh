#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi

if ! command -v node >/dev/null 2>&1; then
    echo "🚨 Error: node is required to run homepage tests" >&2
    exit 1
fi

cd services/homepage
node --check lib/merge.js
node --check prepare-config.js
if [ -d "test" ]; then
    node --test test/*.test.js
fi
