#!/usr/bin/env bash
set -euo pipefail

# scripts/coverage-query.sh - Query production dead code and coverage metrics from PostgreSQL
# Usage: ./scripts/coverage-query.sh [service] [days_dormant]

SERVICE="${1:-}"
DAYS="${2:-7}"

if ! [[ "$DAYS" =~ ^[0-9]+$ ]]; then
    echo "error: days_dormant must be a non-negative integer" >&2
    exit 2
fi

SQL="SELECT service, function_name, file, coverage_percent, COALESCE(to_char(last_executed_at, 'YYYY-MM-DD HH24:MI:SS'), 'NEVER') AS last_executed
FROM production_code_coverage
WHERE (last_executed_at < NOW() - (:days * INTERVAL '1 day') OR last_executed_at IS NULL)
  AND (:'service' = '' OR service = :'service')
ORDER BY service, file, function_name;"

echo "🔍 [coverage-query] Dormant functions (unexecuted for >= $DAYS days):"
if [ -n "${POSTGRES_URL:-}" ]; then
    printf '%s\n' "$SQL" | psql -w -A "$POSTGRES_URL" -v "service=$SERVICE" -v "days=$DAYS" -f -
else
    printf '%s\n' "$SQL" | psql -w -A -v "service=$SERVICE" -v "days=$DAYS" -f -
fi
