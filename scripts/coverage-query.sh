#!/usr/bin/env bash
set -euo pipefail

# scripts/coverage-query.sh - Query production dead code and coverage metrics from PostgreSQL
# Usage: POSTGRES_URL=postgres://... ./scripts/coverage-query.sh [service] [days_dormant]

if [ -z "${POSTGRES_URL:-}" ]; then
    echo "usage: POSTGRES_URL=postgres://user:pass@host:5432/db $0 [service] [days_dormant]" >&2
    echo "error: POSTGRES_URL is not set" >&2
    exit 2
fi

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
printf '%s\n' "$SQL" | psql -A "$POSTGRES_URL" -v "service=$SERVICE" -v "days=$DAYS" -f -
