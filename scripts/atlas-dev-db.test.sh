#!/usr/bin/env bash
# Hermetic boundary fixtures: psql/Atlas/Docker are never contacted.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir "$TMP/bin" "$TMP/target"
printf 'env "local" {}\n' > "$TMP/target/atlas.hcl"
cat > "$TMP/config.yaml" <<'YAML'
host: localhost
port: 5432
maintenance_database: postgres
user: atlas_runner
template: atlas_vector
name_prefix: atlas_dev
max_age_seconds: 86400
YAML
cat > "$TMP/bin/psql" <<'MOCK'
#!/usr/bin/env bash
sql=$(cat)
printf '%s\n' "$*" >> "$TEST_ARGS"
if [[ $sql == *'SELECT oid, datname'* ]]; then cat "${TEST_CANDIDATES:-/dev/null}"; exit 0; fi
printf '%s\n' "$sql" >> "$TEST_SQL"
case "$sql" in
  *'CREATE DATABASE'*) exit "${CREATE_STATUS:-0}" ;;
  *'DROP DATABASE'*) exit "${DROP_STATUS:-0}" ;;
esac
MOCK
cat > "$TMP/bin/atlas" <<'MOCK'
#!/usr/bin/env bash
if [[ ${ATLAS_SIGNAL:-} != '' ]]; then kill -"$ATLAS_SIGNAL" "$PPID"; fi
exit "${ATLAS_STATUS:-0}"
MOCK
cat > "$TMP/bin/docker" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$TEST_DOCKER"
exit 88
MOCK
chmod +x "$TMP/bin/"*
export PATH="$TMP/bin:$PATH" TEST_DOCKER="$TMP/docker" TEST_ARGS="$TMP/args" TEST_SQL="$TMP/sql" ATLAS_DEV_PASSWORD='private test password'
# Exercise the actual entrypoint: status preservation, signal cleanup and warnings.
while read -r name atlas drop signal expected; do
  : > "$TEST_SQL"
  set +e
  ATLAS_STATUS=$atlas DROP_STATUS=$drop ATLAS_SIGNAL=${signal//\'/} bash "$ROOT/scripts/atlas-diff.sh" --config "$TMP/config.yaml" test "$TMP/target" > "$TMP/out" 2>&1
  actual=$?
  set -e
  [[ $actual == "$expected" ]] || { cat "$TMP/out"; echo "FAIL entrypoint $name: $actual"; exit 1; }
  [[ $(cat "$TEST_SQL") == *'CREATE DATABASE'* ]] || exit 1
  [[ $(cat "$TEST_SQL") == *'DROP DATABASE'* ]] || exit 1
  if [[ $drop != 0 ]]; then [[ $(cat "$TMP/out") == *WARNING* ]] || exit 1; fi
done <<'CASES'
success 0 0 '' 0
atlas_error 6 0 '' 6
both_fail 6 9 '' 6
drop_error 0 9 '' 0
term 0 0 TERM 143
int 0 0 INT 130
CASES
# Failed configured setup never falls back to Docker and retains cleanup state.
: > "$TEST_SQL"
set +e
CREATE_STATUS=7 bash "$ROOT/scripts/atlas-diff.sh" --config "$TMP/config.yaml" test "$TMP/target" > "$TMP/out" 2>&1
actual=$?
set -e
[[ $actual == 7 && $(cat "$TEST_SQL") == *'DROP DATABASE'* ]]
# Reject ambiguous configuration before any provisioning request.
for extra in 'user: duplicate' 'unknown: value' 'nested:'; do
  cp "$TMP/config.yaml" "$TMP/invalid.yaml"
  printf '%s\n' "$extra" >> "$TMP/invalid.yaml"
  : > "$TEST_SQL"
  if bash "$ROOT/scripts/atlas-diff.sh" --config "$TMP/invalid.yaml" test "$TMP/target" > "$TMP/out" 2>&1; then exit 1; fi
  [[ ! -s $TEST_SQL ]]
done
# Replace the existing age so rejection exercises numeric bounds, not duplicates.
for age in -1 0; do
  sed "s/max_age_seconds: 86400/max_age_seconds: $age/" "$TMP/config.yaml" > "$TMP/invalid.yaml"
  : > "$TEST_SQL"
  if bash "$ROOT/scripts/atlas-diff.sh" --config "$TMP/invalid.yaml" test "$TMP/target" > "$TMP/out" 2>&1; then exit 1; fi
  [[ ! -s $TEST_SQL ]]
done
# Keep the template's extension outside Atlas's database reset scope.
bash -c 'set -euo pipefail; source "$1/scripts/atlas-dev-db.sh"; atlas_dev_config_load "$2"; atlas_dev_create; [[ $DEV_URL == "postgres://atlas_runner@localhost:5432/$DEV_DB_NAME?search_path=public" ]]' _ "$ROOT" "$TMP/config.yaml"
# Concurrent creation must not reuse a name (psql is an external-boundary fixture).
for n in 1 2 3 4; do
  bash -c 'set -euo pipefail; source "$1/scripts/atlas-dev-db.sh"; atlas_dev_config_load "$2"; atlas_dev_create; echo "$DEV_DB_NAME"' _ "$ROOT" "$TMP/config.yaml" > "$TMP/name$n" &
done
wait
[[ $(cat "$TMP"/name* | sort -u | wc -l) == 4 ]]
# Name/age eligibility runs before catalog rechecks;
# database-side identity/owner/session predicates are covered by rollout canaries.
export TEST_CANDIDATES="$TMP/candidates"
now=$(date +%s)
old=$((now - 90000))
fresh=$((now - 10))
future=$((now + 90000))
printf '%s\n' "10|atlas_dev_${old}_0123456789abcdef" "11|atlas_dev_${fresh}_0123456789abcdef" "12|atlas_dev_${future}_0123456789abcdef" "13|application" "14|atlas_dev_${old}_bad" > "$TEST_CANDIDATES"
for mode in dry live; do
  : > "$TEST_ARGS"
  args=()
  [[ $mode != dry ]] || args=(--dry-run)
  bash "$ROOT/scripts/atlas-dev-cleanup.sh" --config "$TMP/config.yaml" "${args[@]}"
  [[ $(cat "$TEST_ARGS") == *'oid=10'* ]] || exit 1
  for excluded in 11 12 13 14; do
    [[ $(cat "$TEST_ARGS") != *"oid=$excluded"* ]] || exit 1
  done
done
# Assert argument wiring; real catalog protections require PostgreSQL canaries.
[[ $(cat "$TEST_ARGS") == *'owner=atlas_runner'* ]]
[[ $(cat "$TEST_ARGS") == *"name=atlas_dev_${old}_0123456789abcdef"* ]]
# Decimal normalization avoids Bash's octal interpretation of leading zeros.
sed 's/86400/086400/' "$TMP/config.yaml" > "$TMP/decimal.yaml"
bash -c 'source "$1/scripts/atlas-dev-db.sh"; atlas_dev_config_load "$2"; [[ $ATLAS_DB_MAX_AGE_SECONDS == 86400 ]]' _ "$ROOT" "$TMP/decimal.yaml"
# A caller-owned URL skips every database creation/deletion path.
: > "$TEST_SQL"
ATLAS_DEV_URL='postgres://caller/owned' bash "$ROOT/scripts/atlas-diff.sh" test "$TMP/target" > "$TMP/out" 2>&1
[[ ! -s $TEST_SQL ]]
# Default Docker fallback remains available when no runtime config exists.
if [[ ! -e /local/atlas-dev.yaml ]]; then
  : > "$TEST_DOCKER"
  set +e
  bash "$ROOT/scripts/atlas-diff.sh" test "$TMP/target" > "$TMP/out" 2>&1
  actual=$?
  set -e
  [[ $actual == 1 ]]
  [[ $(cat "$TEST_DOCKER") == info ]]
fi
printf 'Atlas dev lifecycle tests passed\n'
