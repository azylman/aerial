#!/usr/bin/env bash
set -euo pipefail

[ "${1:-}" = "--config" ] && shift 2
if [ $# -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Usage: $0 [--config <path>] <migration_name> [target_dir]" >&2; exit 1
fi
command -v atlas >/dev/null 2>&1 || { echo "ERROR: 'atlas' CLI not found" >&2; exit 1; }
MIGRATION_NAME="$1"; TARGET_DIR="${2:-}"
if [ -n "$TARGET_DIR" ]; then
    [ -f "$TARGET_DIR/atlas.hcl" ] || { echo "ERROR: '$TARGET_DIR' lacks atlas.hcl" >&2; exit 1; }
else
    mapfile -t HCLS < <(find . -maxdepth 3 -name atlas.hcl 2>/dev/null | sort)
    [ ${#HCLS[@]} -eq 1 ] || { echo "ERROR: expected exactly 1 atlas.hcl, found ${#HCLS[@]}" >&2; exit 1; }
    TARGET_DIR="$(dirname "${HCLS[0]}")"
fi

if [ -z "${ATLAS_DEV_URL:-}" ]; then
    [ "$(id -u)" -eq 0 ] && { echo "ERROR: initdb/pg_ctl cannot run as root; set ATLAS_DEV_URL" >&2; exit 1; }
    PG_BIN=""
    for d in "$(dirname "$(which pg_ctl 2>/dev/null || true)")" /usr/lib/postgresql/19/bin; do
        [ -x "$d/initdb" ] && [ -x "$d/pg_ctl" ] && { PG_BIN="$d"; break; }
    done
    [ -n "$PG_BIN" ] || { echo "ERROR: PostgreSQL 19 tools (initdb, pg_ctl) not found" >&2; exit 1; }
    PG_TMP=$(mktemp -d)
    trap '"$PG_BIN/pg_ctl" -D "$PG_TMP/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf "$PG_TMP"' EXIT
    "$PG_BIN/initdb" -D "$PG_TMP/data" -U postgres --no-sync --auth=trust >/dev/null
    "$PG_BIN/pg_ctl" -D "$PG_TMP/data" -l "$PG_TMP/log" -o "-k $PG_TMP -h '' -F" start >/dev/null
    psql -v ON_ERROR_STOP=1 -h "$PG_TMP" -U postgres -d postgres -c "CREATE DATABASE dev;" >/dev/null
    psql -v ON_ERROR_STOP=1 -h "$PG_TMP" -U postgres -d dev -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null
    ATLAS_DEV_URL="postgres://postgres@/dev?host=$PG_TMP&sslmode=disable"
fi

(cd "$TARGET_DIR" && atlas migrate diff "$MIGRATION_NAME" --env local --var dev_url="$ATLAS_DEV_URL" && atlas migrate validate --env local --var dev_url="$ATLAS_DEV_URL")
