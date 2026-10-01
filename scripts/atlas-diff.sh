#!/usr/bin/env bash
set -euo pipefail

# scripts/atlas-diff.sh - Universal Atlas Declarative Migration Diff Tool
# Computes declarative schema diffs across any repository (aerial, aerial-sidecars)
# using an ephemeral pgvector dev container, updates atlas.sum, and validates integrity.

usage() {
    cat <<EOF
Usage: $0 <migration_name> [target_dir]

Arguments:
  migration_name   Name of the migration to generate (e.g. add_user_settings, create_banana_orders)
  target_dir       Optional path to directory containing atlas.hcl (auto-discovered if omitted)

Environment Variables:
  ATLAS_DEV_URL    Optional external Postgres dev URL (skips ephemeral container if provided)
  ATLAS_IMAGE      Docker image for dev DB (default: pgvector/pg16:latest)

Examples:
  $0 add_facts_metadata
  $0 add_banana_orders sidecars/banana
EOF
    exit 1
}

if [ $# -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

MIGRATION_NAME="$1"
TARGET_DIR="${2:-}"

# 1. Check prerequisites
if ! command -v atlas >/dev/null 2>&1; then
    echo "ERROR: 'atlas' CLI is not installed or not in PATH." >&2
    echo "       Install via: curl -sSf https://atlasgo.sh | sh" >&2
    exit 1
fi

# 2. Auto-discover target directory containing atlas.hcl
if [ -n "$TARGET_DIR" ]; then
    if [ ! -f "${TARGET_DIR}/atlas.hcl" ]; then
        echo "ERROR: Specified target directory '${TARGET_DIR}' does not contain atlas.hcl" >&2
        exit 1
    fi
else
    if [ -f "./atlas.hcl" ]; then
        TARGET_DIR="."
    else
        mapfile -t FOUND_HCLS < <(find . -maxdepth 3 -name "atlas.hcl" 2>/dev/null | sort)
        if [ ${#FOUND_HCLS[@]} -eq 0 ]; then
            echo "ERROR: No atlas.hcl found in current directory or subdirectories." >&2
            exit 1
        elif [ ${#FOUND_HCLS[@]} -eq 1 ]; then
            TARGET_DIR="$(dirname "${FOUND_HCLS[0]}")"
        else
            echo "ERROR: Multiple atlas.hcl files found. Please specify target_dir:" >&2
            for f in "${FOUND_HCLS[@]}"; do
                echo "  - $(dirname "$f")" >&2
            done
            exit 1
        fi
    fi
fi

TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"
echo "🧭 [atlas-diff] Target configuration: ${TARGET_DIR}/atlas.hcl" >&2

# 3. Dev Database Setup
DEV_CONTAINER=""
cleanup() {
    local exit_code=$?
    if [ -n "$DEV_CONTAINER" ]; then
        echo "🧹 [atlas-diff] Cleaning up ephemeral dev container: ${DEV_CONTAINER}..." >&2
        docker rm -f "$DEV_CONTAINER" >/dev/null 2>&1 || true
    fi
    exit $exit_code
}
trap cleanup EXIT INT TERM ERR

DEV_URL="${ATLAS_DEV_URL:-}"

if [ -z "$DEV_URL" ]; then
    if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
        echo "ERROR: Docker is required to spin up an ephemeral pgvector dev container." >&2
        echo "       Alternatively, provide ATLAS_DEV_URL to use an existing database." >&2
        exit 1
    fi

    ATLAS_IMAGE="${ATLAS_IMAGE:-pgvector/pgvector:pg16}"
    DEV_CONTAINER="aerial-atlas-dev-$$-$(date +%s)"
    
    # Check if shared network exists (e.g. aerial-net)
    NET_ARGS=()
    if docker network inspect aerial-net >/dev/null 2>&1; then
        NET_ARGS=(--network aerial-net)
    fi

    echo "🚀 [atlas-diff] Spawning ephemeral dev container: ${DEV_CONTAINER} (${ATLAS_IMAGE})..." >&2
    docker run -d \
        --name "$DEV_CONTAINER" \
        "${NET_ARGS[@]}" \
        -p 0:5432 \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=dev \
        -e POSTGRES_DB=dev \
        "$ATLAS_IMAGE" >/dev/null

    # Wait for PostgreSQL to be ready
    echo "⏳ [atlas-diff] Waiting for dev database to become ready..." >&2
    READY=0
    for i in $(seq 1 30); do
        if docker exec "$DEV_CONTAINER" pg_isready -U postgres -q 2>/dev/null && \
           docker exec "$DEV_CONTAINER" psql -U postgres -d postgres -c "SELECT 1;" >/dev/null 2>&1; then
            READY=1
            break
        fi
        sleep 1
    done

    if [ "$READY" -ne 1 ]; then
        echo "ERROR: Dev database in container ${DEV_CONTAINER} failed to become ready." >&2
        exit 1
    fi

    # Pre-install vector extension in template1 and dev database with retries
    EXT_READY=0
    for i in $(seq 1 10); do
        if docker exec "$DEV_CONTAINER" psql -U postgres -d template1 -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1 && \
           docker exec "$DEV_CONTAINER" psql -U postgres -d dev -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1; then
            EXT_READY=1
            break
        fi
        sleep 0.5
    done

    if [ "$EXT_READY" -ne 1 ]; then
        echo "ERROR: Failed to install vector extension in dev container ${DEV_CONTAINER}." >&2
        exit 1
    fi

    # Determine connection string: test direct container name resolution first
    if [ ${#NET_ARGS[@]} -gt 0 ]; then
        DEV_URL="postgres://postgres:dev@${DEV_CONTAINER}:5432/dev?sslmode=disable"
    else
        HOST_PORT=$(docker port "$DEV_CONTAINER" 5432 2>/dev/null | head -n1 | awk -F':' '{print $NF}')
        if [ -z "$HOST_PORT" ]; then
            echo "ERROR: Failed to resolve mapped host port for dev container." >&2
            exit 1
        fi
        DEV_URL="postgres://postgres:dev@127.0.0.1:${HOST_PORT}/dev?sslmode=disable"
    fi
fi

# 4. Execute Atlas Diff
echo "⚡ [atlas-diff] Computing migration diff for: ${MIGRATION_NAME}..." >&2
(
    cd "$TARGET_DIR"
    atlas migrate diff "$MIGRATION_NAME" \
        --env local \
        --var dev_url="$DEV_URL"
)

# 5. Validate Migration Integrity
echo "🔍 [atlas-diff] Validating migration directory integrity..." >&2
MIG_DIR=$(grep -E 'dir\s*=' "${TARGET_DIR}/atlas.hcl" 2>/dev/null | head -n1 | sed -E 's/.*dir\s*=\s*"([^"]+)".*/\1/' || true)
if [ -z "$MIG_DIR" ]; then
    MIG_DIR="file://migrations"
elif [[ "$MIG_DIR" != file://* ]]; then
    MIG_DIR="file://${MIG_DIR}"
fi

(
    cd "$TARGET_DIR"
    atlas migrate validate --dir "$MIG_DIR"
)

echo "================================================================================" >&2
echo "✅ [atlas-diff] Migration '${MIGRATION_NAME}' generated and validated successfully!" >&2
echo "📝 Remember to stage both schema.sql and migrations/ before committing." >&2
echo "================================================================================" >&2
