#!/usr/bin/env bash
set -euo pipefail

# scripts/test-aerial-build.sh - Hermetic unit & integration tests for scripts/aerial-build.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AERIAL_BUILD="${SCRIPT_DIR}/aerial-build.sh"

PASSED=0
FAILED=0

assert_eq() {
    local expected="$1"
    local actual="$2"
    local msg="$3"
    if [ "$expected" = "$actual" ]; then
        echo "✅ PASS: $msg"
        PASSED=$((PASSED + 1))
    else
        echo "❌ FAIL: $msg"
        echo "   Expected: '$expected'"
        echo "   Actual:   '$actual'"
        FAILED=$((FAILED + 1))
    fi
}

assert_contains() {
    local needle="$1"
    local haystack="$2"
    local msg="$3"
    if printf '%s' "$haystack" | grep -Fq -- "$needle"; then
        echo "✅ PASS: $msg"
        PASSED=$((PASSED + 1))
    else
        echo "❌ FAIL: $msg"
        echo "   Expected to contain: '$needle'"
        echo "   Actual:              '$haystack'"
        FAILED=$((FAILED + 1))
    fi
}

echo "🧪 Running hermetic tests for scripts/aerial-build.sh..."

TMP_DIR="$(mktemp -d /tmp/test-aerial-build.XXXXXX)"
trap 'rm -rf "$TMP_DIR"' EXIT

MOCK_BIN="${TMP_DIR}/bin"
mkdir -p "$MOCK_BIN"
CAPTURED_REQ="${TMP_DIR}/captured_req.txt"
CAPTURED_BODY="${TMP_DIR}/captured_body.json"

cat << 'EOF' > "${MOCK_BIN}/curl"
#!/usr/bin/env bash
set -euo pipefail

METHOD="GET"
URL=""
BODY=""

while [ $# -gt 0 ]; do
    case "$1" in
        -X)
            METHOD="$2"
            shift 2
            ;;
        -d)
            BODY="$2"
            shift 2
            ;;
        -H|-w|--connect-timeout)
            shift 2
            ;;
        -s|-S|-sS)
            shift
            ;;
        *)
            URL="$1"
            shift
            ;;
    esac
done

if [[ "$URL" == *"vanadis.unreachable"* ]]; then
    exit 6
fi

echo "${METHOD} ${URL}" > "${MOCK_CAPTURE_REQ}"
if [ -n "$BODY" ]; then
    printf '%s' "$BODY" > "${MOCK_CAPTURE_BODY}"
fi

if [ "${MOCK_HTTP_STATUS:-200}" -ge 400 ]; then
    printf '{"error":"simulated failure"}\n%s\n' "${MOCK_HTTP_STATUS}"
    exit 0
fi

case "${METHOD}:${URL}" in
    POST:*/builds)
        printf '{"status":"accepted","build_id":"build-test-101","chunk_count":2,"chunks":[{"id":"c1","chunk_index":0,"title":"Chunk 1","instructions":"huge instructions blob","effort":"high","status":"pending"}]}\n202\n'
        ;;
    GET:*/builds/build-test-101)
        printf '{"build":{"id":"build-test-101","status":"running"},"chunks":[]}\n200\n'
        ;;
    POST:*/builds/build-test-101/cancel)
        printf '{"status":"cancelled","build_id":"build-test-101"}\n200\n'
        ;;
    *)
        printf '{"error":"not found"}\n404\n'
        ;;
esac
EOF
chmod +x "${MOCK_BIN}/curl"

export PATH="${MOCK_BIN}:${PATH}"
export MOCK_CAPTURE_REQ="$CAPTURED_REQ"
export MOCK_CAPTURE_BODY="$CAPTURED_BODY"
unset DISCORD_THREAD_ID DISCORD_CHANNEL_ID AERIAL_TARGET_ID || true

# Test 1: Executable permission and --help
assert_eq "true" "$([ -x "$AERIAL_BUILD" ] && echo true || echo false)" "aerial-build.sh is executable"
HELP_OUT="$("$AERIAL_BUILD" --help)"
assert_contains "Usage:" "$HELP_OUT" "--help displays usage"

# Test 2: Missing file error
MISSING_ERR="$("$AERIAL_BUILD" enqueue "${TMP_DIR}/does-not-exist.json" 2>&1 || true)"
assert_contains "Plan file not found" "$MISSING_ERR" "enqueue fails cleanly on missing file"

# Test 3: Invalid JSON error
BAD_JSON="${TMP_DIR}/bad.json"
echo "{not valid json" > "$BAD_JSON"
INVALID_ERR="$("$AERIAL_BUILD" enqueue "$BAD_JSON" 2>&1 || true)"
assert_contains "Invalid JSON in plan file" "$INVALID_ERR" "enqueue fails cleanly on invalid JSON"

# Test 4: Enqueue with automatic DISCORD_THREAD_ID injection and turn_action: end_turn
PLAN_JSON="${TMP_DIR}/plan.json"
cat << 'EOF' > "$PLAN_JSON"
{
  "repo": "aerial",
  "tier": 2,
  "objective": "Implement Vanadis",
  "chunks": [
    {"title": "Chunk 1", "instructions": "Do chunk 1"}
  ]
}
EOF

ENQUEUE_OUT="$(VANADIS_URL="http://127.0.0.1:4025" DISCORD_THREAD_ID="1555888999" "$AERIAL_BUILD" enqueue --file "$PLAN_JSON")"
assert_eq "end_turn" "$(printf '%s' "$ENQUEUE_OUT" | jq -r '.turn_action')" "enqueue response includes turn_action: end_turn"
assert_eq "build-test-101" "$(printf '%s' "$ENQUEUE_OUT" | jq -r '.build_id')" "enqueue response includes build_id"
assert_eq "Chunk 1" "$(printf '%s' "$ENQUEUE_OUT" | jq -r '.chunks[0].title')" "enqueue response retains chunk metadata"
assert_eq "null" "$(printf '%s' "$ENQUEUE_OUT" | jq -r '.chunks[0].instructions')" "enqueue response strips verbose chunk instructions"
assert_eq "1555888999" "$(jq -r '.thread_id' "$CAPTURED_BODY")" "DISCORD_THREAD_ID injected into request payload when thread_id is missing"
assert_eq "POST http://127.0.0.1:4025/builds" "$(cat "$CAPTURED_REQ")" "enqueue dispatches POST /builds"

# Test 5: DNS fallback from vanadis.unreachable to VANADIS_FALLBACK_URL
FALLBACK_OUT="$(VANADIS_URL="http://vanadis.unreachable:4025" VANADIS_FALLBACK_URL="http://127.0.0.1:4025" DISCORD_CHANNEL_ID="1555000111" "$AERIAL_BUILD" enqueue "$PLAN_JSON")"
assert_eq "end_turn" "$(printf '%s' "$FALLBACK_OUT" | jq -r '.turn_action')" "enqueue succeeds via fallback URL when primary fails to resolve"
assert_eq "POST http://127.0.0.1:4025/builds" "$(cat "$CAPTURED_REQ")" "fallback used http://127.0.0.1:4025/builds"
assert_eq "1555000111" "$(jq -r '.thread_id' "$CAPTURED_BODY")" "DISCORD_CHANNEL_ID injected as fallback thread_id"

# Test 6: status command
STATUS_OUT="$(VANADIS_URL="http://127.0.0.1:4025" "$AERIAL_BUILD" status build-test-101)"
assert_eq "running" "$(printf '%s' "$STATUS_OUT" | jq -r '.build.status')" "status command returns build status"
assert_eq "GET http://127.0.0.1:4025/builds/build-test-101" "$(cat "$CAPTURED_REQ")" "status dispatches GET /builds/<id>"

# Test 7: cancel command
CANCEL_OUT="$(VANADIS_URL="http://127.0.0.1:4025" "$AERIAL_BUILD" cancel build-test-101)"
assert_eq "cancelled" "$(printf '%s' "$CANCEL_OUT" | jq -r '.status')" "cancel command returns cancelled status"
assert_eq "POST http://127.0.0.1:4025/builds/build-test-101/cancel" "$(cat "$CAPTURED_REQ")" "cancel dispatches POST /builds/<id>/cancel"

# Test 8: HTTP failure handling (500 Internal Server Error)
HTTP_ERR="$(VANADIS_URL="http://127.0.0.1:4025" MOCK_HTTP_STATUS=500 "$AERIAL_BUILD" status build-test-101 2>&1 || true)"
assert_contains "failed with HTTP 500" "$HTTP_ERR" "HTTP 500 error is reported and exits non-zero"

echo "--------------------------------------------------------"
echo "Test results: $PASSED passed, $FAILED failed"
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
echo "🎉 All aerial-build.sh tests passed!"
