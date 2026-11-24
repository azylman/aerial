#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi

ATLAS="${ATLAS_BIN:-atlas}"
[ -x "$ATLAS" ] && ATLAS="$(cd "$(dirname "$ATLAS")" && pwd)/$(basename "$ATLAS")"

if ! command -v "$ATLAS" >/dev/null 2>&1; then
    echo "🚨 Error: atlas CLI is required to validate database migrations" >&2
    exit 1
fi

cd services/eri/pkg/db
"$ATLAS" migrate validate --dir file://migrations
