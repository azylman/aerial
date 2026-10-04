#!/usr/bin/env bash
set -euo pipefail

# scripts/coverage-query.sh - Query production dead code and coverage metrics from PostgreSQL
# Usage: ./scripts/coverage-query.sh [service] [days_dormant]

SERVICE="${1:-}"
DAYS="${2:-7}"
POSTGRES_URL="${POSTGRES_URL:-postgres://aerial:aerial_secure_pass@127.0.0.1:5432/aerial?sslmode=disable}"

WHERE_CLAUSE="WHERE (last_executed_at < NOW() - INTERVAL '$DAYS days' OR last_executed_at IS NULL)"
if [ -n "$SERVICE" ]; then
    WHERE_CLAUSE="$WHERE_CLAUSE AND service = '$SERVICE'"
fi

SQL="SELECT service, function_name, file, coverage_percent, COALESCE(to_char(last_executed_at, 'YYYY-MM-DD HH24:MI:SS'), 'NEVER') AS last_executed FROM production_code_coverage $WHERE_CLAUSE ORDER BY service, file, function_name;"

echo "🔍 [coverage-query] Dormant functions (unexecuted for >= $DAYS days):"
psql "$POSTGRES_URL" -c "$SQL"
