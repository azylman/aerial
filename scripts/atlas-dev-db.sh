#!/usr/bin/env bash
# Shared disposable database lifecycle; source this file, do not execute it.
atlas_dev_config_load() {
    local parsed
    parsed=$(python3 - "$1" <<'PY'
import re, sys
keys = ['host', 'port', 'maintenance_database', 'user', 'template', 'name_prefix', 'max_age_seconds']
values = {'port': '5432', 'maintenance_database': 'postgres', 'max_age_seconds': '86400'}
seen = set()
try:
    for line in open(sys.argv[1], encoding='utf-8'):
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        match = re.fullmatch(r'([a-z_]+):\s*([a-zA-Z0-9_.-]+|\'[a-zA-Z0-9_.-]+\'|"[a-zA-Z0-9_.-]+")\s*(?:#.*)?\n?', line)
        if not match or match[1] not in keys or match[1] in seen:
            raise ValueError('expected unique flat scalar configuration keys')
        key, value = match.groups()
        values[key] = value.strip('\'"')
        seen.add(key)
    if any(key not in values for key in keys):
        raise ValueError('missing required configuration key')
    if not 1 <= int(values['port']) <= 65535 or not 1 <= int(values['max_age_seconds']) <= 2147483647:
        raise ValueError('invalid port or max_age_seconds')
    values['port'] = str(int(values['port']))
    values['max_age_seconds'] = str(int(values['max_age_seconds']))
    for key in ['maintenance_database', 'user', 'template', 'name_prefix']:
        if not re.fullmatch('[a-z_][a-z0-9_]*', values[key]) or len(values[key]) > 63:
            raise ValueError('invalid PostgreSQL identifier')
    if len(values['name_prefix']) > 30:
        raise ValueError('name_prefix too long')
    print('\n'.join(values[key] for key in keys))
except (OSError, ValueError) as error:
    print('Invalid Atlas dev configuration: ' + str(error), file=sys.stderr)
    sys.exit(1)
PY
    ) || return
    local -a fields
    mapfile -t fields <<< "$parsed"
    ATLAS_DB_HOST=${fields[0]} ATLAS_DB_PORT=${fields[1]}
    ATLAS_DB_MAINTENANCE_DATABASE=${fields[2]} ATLAS_DB_USER=${fields[3]}
    ATLAS_DB_TEMPLATE=${fields[4]} ATLAS_DB_NAME_PREFIX=${fields[5]}
    ATLAS_DB_MAX_AGE_SECONDS=${fields[6]}
    : "${ATLAS_DEV_PASSWORD:?ATLAS_DEV_PASSWORD is required}"
    export PGPASSWORD="$ATLAS_DEV_PASSWORD"
}

atlas_dev_psql() {
    psql -X -q -A -t -v ON_ERROR_STOP=1 -h "$ATLAS_DB_HOST" -p "$ATLAS_DB_PORT" \
        -U "$ATLAS_DB_USER" -d "$ATLAS_DB_MAINTENANCE_DATABASE" "$@"
}

atlas_dev_create() {
    local random
    random=$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n') || return
    DEV_DB_NAME="${ATLAS_DB_NAME_PREFIX}_$(date +%s)_${random}"
    # Record ownership before sending CREATE: a failed connection may still have committed.
    atlas_dev_psql -v name="$DEV_DB_NAME" -v template="$ATLAS_DB_TEMPLATE" <<'SQL' || return
CREATE DATABASE :"name" TEMPLATE :"template";
SQL
    DEV_URL="postgres://${ATLAS_DB_USER}@${ATLAS_DB_HOST}:${ATLAS_DB_PORT}/${DEV_DB_NAME}"
}

atlas_dev_drop() {
    atlas_dev_psql -v name="$1" <<'SQL'
DROP DATABASE IF EXISTS :"name";
SQL
}
