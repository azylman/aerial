#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/atlas-dev-db.sh"
config=/local/atlas-dev.yaml
dry_run=false
while (($#)); do
    case "$1" in
        --config) config=${2:?missing config path}; shift 2 ;;
        --dry-run) dry_run=true; shift ;;
        *) echo "Usage: $0 [--config PATH] [--dry-run]" >&2; exit 2 ;;
    esac
done
atlas_dev_config_load "$config"
# List metadata first, then recheck every identity and eligibility field immediately
# before DROP in a fresh catalog query. No FORCE: a connection racing DROP wins safely.
candidates=$(atlas_dev_psql <<'SQL'
SELECT oid, datname FROM pg_database WHERE NOT datistemplate ORDER BY oid;
SQL
)
while IFS='|' read -r oid name; do
    [[ "$oid" =~ ^[0-9]+$ ]] || continue
    [[ "$name" =~ ^${ATLAS_DB_NAME_PREFIX}_([0-9]{10})_([0-9a-f]{16})$ ]] || continue
    epoch=${BASH_REMATCH[1]}
    now=$(date +%s)
    ((10#$epoch <= now && now - 10#$epoch >= ATLAS_DB_MAX_AGE_SECONDS)) || continue
    if ! atlas_dev_psql -v oid="$oid" -v name="$name" -v owner="$ATLAS_DB_USER" \
        -v epoch="$epoch" -v max_age="$ATLAS_DB_MAX_AGE_SECONDS" -v dry_run="$dry_run" <<'SQL'
SELECT CASE WHEN :'dry_run'::boolean THEN format('SELECT %L;', 'Would drop ' || datname)
            ELSE format('DROP DATABASE %I;', datname) END
FROM pg_database
WHERE oid = :'oid'::oid AND datname = :'name'
  AND datdba = (SELECT oid FROM pg_roles WHERE rolname = :'owner')
  AND NOT datistemplate
  AND datname <> current_database()
  AND :'epoch'::bigint <= extract(epoch FROM clock_timestamp())
  AND extract(epoch FROM clock_timestamp()) - :'epoch'::bigint >= :'max_age'::bigint
  AND NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datid = :'oid'::oid)
\gexec
SQL
    then
        echo "WARNING: disposable database cleanup skipped an uncertain or busy candidate" >&2
    fi
done <<< "$candidates"
