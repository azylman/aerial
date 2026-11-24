#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; SCRIPT="${DIR}/aerial-build.sh"
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
export PATH="$TMPD/bin:$PATH" VANADIS_URL="http://127.0.0.1:4025" TMPD="$TMPD"
mkdir -p "$TMPD/bin"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; method="GET"; auth=""
for ((i=1; i<=$#; i++)); do
  a="${!i}"; [[ "$a" =~ http:// ]] && url="$a"
  [[ "$a" =~ ^(POST|GET)$ ]] && method="$a"
  [ "$a" = "-H" ] && { j=$((i+1)); [[ "${!j}" =~ Authorization ]] && auth="${!j}"; }
done
echo "$auth" > "$TMPD/last_auth"
[ "$method" = "POST" ] && [ ! -t 0 ] && { cat >/dev/null || true; }
[[ "$url" =~ fail7 ]] && exit 7
[[ "$url" =~ unreachable ]] && exit 7
if [ "$method" = "POST" ] && [[ "$url" =~ /builds$ ]]; then
  echo '{"id":"b1","chunks":[{"id":"c1","chunk_index":0,"mode":"pr","repo":"aerial","title":"t","effort":"low","status":"pending","depends_on":[]}]}'
  printf "\n200"; exit 0
fi
if [ "$method" = "GET" ] && [[ "$url" =~ /builds/b1$ ]]; then
  echo '{"id":"b1","status":"running"}'; printf "\n200"; exit 0
fi
if [ "$method" = "POST" ] && [[ "$url" =~ /builds/b1/cancel$ ]]; then
  echo '{"id":"b1","status":"cancelled"}'; printf "\n200"; exit 0
fi
echo '{"error":"not found"}'; printf "\n404"; exit 0
CURL_EOF
chmod +x "$TMPD/bin/curl"
assert() { "$@" || { echo "ASSERT FAILED: $*" >&2; exit 1; }; }
assert_fail() { ! "$@" 2>/dev/null || { echo "EXPECTED FAILURE: $*" >&2; exit 1; }; }
assert_fail "$SCRIPT"
assert_fail "$SCRIPT" unknown-cmd
assert_fail "$SCRIPT" enqueue
assert_fail "$SCRIPT" enqueue --file
assert_fail "$SCRIPT" enqueue "$TMPD/nonexistent.json"
echo "not json" > "$TMPD/bad.json" && assert_fail "$SCRIPT" enqueue "$TMPD/bad.json"
echo '{"chunks":[{"id":"c1"}]}' > "$TMPD/no_dep.json" && assert_fail "$SCRIPT" enqueue "$TMPD/no_dep.json"
assert_fail "$SCRIPT" status
assert_fail "$SCRIPT" cancel
cat << 'JSON_EOF' > "$TMPD/valid.json"
{"chunks":[{"id":"c1","chunk_index":0,"mode":"pr","repo":"aerial","title":"t","effort":"low","depends_on":[]}]}
JSON_EOF
res_enq=$("$SCRIPT" enqueue --file "$TMPD/valid.json")
assert echo "$res_enq" | jq -e '.turn_action == "end_turn" and .id == "b1"'
res_stat=$("$SCRIPT" status b1)
assert echo "$res_stat" | jq -e '.status == "running"'
res_canc=$("$SCRIPT" cancel b1)
assert echo "$res_canc" | jq -e '.status == "cancelled"'
res_fb=$(VANADIS_URL="http://unreachable:4025" VANADIS_FALLBACK_URL="http://127.0.0.1:4025" "$SCRIPT" status b1)
assert echo "$res_fb" | jq -e '.status == "running"'
echo "secret-tok-123" > "$TMPD/token"
AERIAL_SA_TOKEN_PATH="$TMPD/token" "$SCRIPT" status b1 >/dev/null
assert grep -q "Bearer secret-tok-123" "$TMPD/last_auth"
pad=$(head -c 145000 /dev/zero | tr '\0' 'x')
printf '{"chunks":[{"id":"c1","chunk_index":0,"mode":"pr","repo":"aerial","title":"t","effort":"low","depends_on":[],"pad":"%s"}]}' "$pad" > "$TMPD/large.json"
res_lg=$("$SCRIPT" enqueue "$TMPD/large.json")
assert echo "$res_lg" | jq -e '.turn_action == "end_turn"'
err_out=$(VANADIS_URL="http://fail7:4025" "$SCRIPT" status b1 2>&1 || true)
assert echo "$err_out" | grep -q "Failed to connect; acceptance unknown"
echo "other_key: 1" > "$TMPD/cfg1.yaml"; echo "vanadis_url: http://127.0.0.1:4025" > "$TMPD/cfg2.yaml"
assert_fail env -u VANADIS_URL CONFIG_PATH="$TMPD/cfg1.yaml" AERIAL_LOCAL_CONFIG_PATH="$TMPD/none" AERIAL_SHARED_CONFIG_PATH="$TMPD/none" "$SCRIPT" status b1
assert [ "$(env -u VANADIS_URL CONFIG_PATH="$TMPD/cfg1.yaml" AERIAL_LOCAL_CONFIG_PATH="$TMPD/cfg2.yaml" "$SCRIPT" status b1 | jq -r .status)" = "running" ]
echo "ok - aerial-build hermetic tests passed"
