#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi

if ! command -v atlas >/dev/null 2>&1; then
    echo "🚨 Error: atlas CLI is required to validate database migrations" >&2
    exit 1
fi

cd services/eri/pkg/db
atlas migrate validate --dir file://migrations
