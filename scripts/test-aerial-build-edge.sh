#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; SCRIPT="${DIR}/aerial-build.sh"
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
export PATH="$TMPD/bin:$PATH" VANADIS_URL="http://127.0.0.1:4025" TMPD="$TMPD"
mkdir -p "$TMPD/bin"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; method="GET"
for ((i=1; i<=$#; i++)); do
  a="${!i}"; [[ "$a" =~ http:// ]] && url="$a"
  [[ "$a" =~ ^(POST|GET)$ ]] && method="$a"
done
[ "$method" = "POST" ] && [ ! -t 0 ] && cat > "$TMPD/last_body"
[ -n "${MOCK_EXIT:-}" ] && { echo "curl: fail https://u:secret_pass_leak@host" >&2; exit "$MOCK_EXIT"; }
[ -n "${MOCK_STATUS:-}" ] && { echo '{"err":1}'; printf "\n%s" "$MOCK_STATUS"; exit 0; }
if [ "$method" = "POST" ] && [[ "$url" =~ /builds$ ]]; then
  echo '{"id":"b1","chunks":[{"id":"c1","chunk_index":0,"mode":"pr","repo":"aerial","title":"t","effort":"low","status":"pending","depends_on":[]}]}'
  printf "\n200"; exit 0
fi
echo '{"id":"b1","status":"running"}'; printf "\n200"; exit 0
CURL_EOF
chmod +x "$TMPD/bin/curl"
assert() { "$@" || { echo "ASSERT FAILED: $*" >&2; exit 1; }; }
assert_fail() { ! "$@" 2>/dev/null || { echo "EXPECTED FAILURE: $*" >&2; exit 1; }; }

cat << 'JSON_EOF' > "$TMPD/valid.json"
{"chunks":[{"id":"c1","chunk_index":0,"mode":"pr","repo":"aerial","title":"t","effort":"low","depends_on":[]}]}
JSON_EOF

# 1. Thread ID injection into request body
DISCORD_THREAD_ID="thr_101" "$SCRIPT" enqueue "$TMPD/valid.json" >/dev/null
assert [ "$(jq -r .thread_id "$TMPD/last_body")" = "thr_101" ]
env -u DISCORD_THREAD_ID DISCORD_CHANNEL_ID="chan_202" "$SCRIPT" enqueue "$TMPD/valid.json" >/dev/null
assert [ "$(jq -r .thread_id "$TMPD/last_body")" = "chan_202" ]
echo '{"chunks":[{"id":"c1","chunk_index":0,"mode":"pr","repo":"aerial","title":"t","effort":"low","depends_on":[]}],"thread_id":"orig_999"}' > "$TMPD/with_tid.json"
DISCORD_THREAD_ID="thr_101" "$SCRIPT" enqueue "$TMPD/with_tid.json" >/dev/null
assert [ "$(jq -r .thread_id "$TMPD/last_body")" = "orig_999" ]

# 2. Non-2xx HTTP 500 error reporting
err_500=$(MOCK_STATUS=500 "$SCRIPT" status b1 2>&1 || true)
assert echo "$err_500" | grep -q "failed with HTTP 500"

# 3. Transport failures (exits 52, 28, 6) & secret sanitization
for sc in "52:Empty reply" "28:timed out" "6:resolve host"; do
  c="${sc%%:*}"; exp="${sc#*:}"
  out=$(MOCK_EXIT="$c" "$SCRIPT" status b1 2>&1 || true)
  assert echo "$out" | grep -q "$exp"
  assert echo "$out" | grep -q "acceptance unknown"
  ! echo "$out" | grep -q "secret_pass_leak" || { echo "ASSERT FAILED: leaked secret" >&2; exit 1; }
done

# 4. VANADIS_URL unconfigured error
no_url=$(env -u VANADIS_URL CONFIG_PATH="/dev/null" AERIAL_LOCAL_CONFIG_PATH="$TMPD/none" AERIAL_SHARED_CONFIG_PATH="$TMPD/none" "$SCRIPT" status b1 2>&1 || true)
assert echo "$no_url" | grep -q "VANADIS_URL is not configured"

echo "ok - aerial-build edge tests passed"
