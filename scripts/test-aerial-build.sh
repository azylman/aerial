#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; SCRIPT="${DIR}/aerial-build.sh"
TMPD="$(mktemp -d)"; trap 'rm -rf "$TMPD"' EXIT
export PATH="$TMPD/bin:$PATH" VANADIS_URL="http://127.0.0.1:4025"
mkdir -p "$TMPD/bin"
cat << 'CURL_EOF' > "$TMPD/bin/curl"
#!/usr/bin/env bash
url=""; method="GET"
for a in "$@"; do [[ "$a" =~ http:// ]] && url="$a"; [[ "$a" =~ ^(POST|GET)$ ]] && method="$a"; done
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

# Validation tests
assert_fail "$SCRIPT"
assert_fail "$SCRIPT" unknown-cmd
assert_fail "$SCRIPT" enqueue
assert_fail "$SCRIPT" enqueue "$TMPD/nonexistent.json"
echo "not json" > "$TMPD/bad.json" && assert_fail "$SCRIPT" enqueue "$TMPD/bad.json"
echo '{"chunks":[{"id":"c1"}]}' > "$TMPD/no_dep.json" && assert_fail "$SCRIPT" enqueue "$TMPD/no_dep.json"
assert_fail "$SCRIPT" status
assert_fail "$SCRIPT" cancel

# Functional tests
cat << 'JSON_EOF' > "$TMPD/valid.json"
{"chunks":[{"id":"c1","chunk_index":0,"mode":"pr","repo":"aerial","title":"t","effort":"low","depends_on":[]}]}
JSON_EOF
res_enq=$("$SCRIPT" enqueue "$TMPD/valid.json")
assert echo "$res_enq" | jq -e '.turn_action == "end_turn" and .id == "b1"'

res_stat=$("$SCRIPT" status b1)
assert echo "$res_stat" | jq -e '.status == "running"'

res_canc=$("$SCRIPT" cancel b1)
assert echo "$res_canc" | jq -e '.status == "cancelled"'

echo "ok - aerial-build hermetic tests passed"
