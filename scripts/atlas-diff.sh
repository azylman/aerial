#!/usr/bin/env bash
set -euo pipefail

[ "${1:-}" = "--config" ] && shift 2
if [ $# -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Usage: $0 [--config <path>] <migration_name> [target_dir]" >&2; exit 1
fi
MIGRATION_NAME="$1"; TARGET_DIR="${2:-}"
if [ -z "$TARGET_DIR" ]; then
    if [ -f "./atlas.hcl" ]; then TARGET_DIR="."
    else TARGET_DIR="$(dirname "$(find . -maxdepth 3 -name atlas.hcl | head -n1)")"; fi
fi

if [ -z "${ATLAS_DEV_URL:-}" ]; then
    PG_BIN="$(dirname "$(which pg_ctl 2>/dev/null || find /usr/lib/postgresql/19/bin -name pg_ctl 2>/dev/null | head -n1)")"
    PG_TMP=$(mktemp -d)
    trap '"$PG_BIN/pg_ctl" -D "$PG_TMP/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$PG_TMP"' EXIT
    "$PG_BIN/initdb" -D "$PG_TMP/data" -U postgres --no-sync --auth=trust >/dev/null
    "$PG_BIN/pg_ctl" -D "$PG_TMP/data" -l "$PG_TMP/log" -o "-k $PG_TMP -h '' -F" start >/dev/null
    psql -h "$PG_TMP" -U postgres -d postgres -c "CREATE DATABASE dev;" -c "\c dev" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
    ATLAS_DEV_URL="postgres://postgres@/dev?host=$PG_TMP&sslmode=disable"
fi

(cd "$TARGET_DIR" && atlas migrate diff "$MIGRATION_NAME" --env local --var dev_url="$ATLAS_DEV_URL" && atlas migrate validate)
