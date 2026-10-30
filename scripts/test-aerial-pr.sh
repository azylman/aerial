#!/usr/bin/env bash
set -euo pipefail

# scripts/test-aerial-pr.sh - Unit & integration tests for unified aerial-pr.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AERIAL_PR="${SCRIPT_DIR}/aerial-pr.sh"
CONFIG_PR="${SCRIPT_DIR}/aerial-config-pr.sh"
SIDECARS_PR="${SCRIPT_DIR}/aerial-sidecars-pr.sh"

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

echo "🧪 Running unit tests for unified aerial-pr.sh..."

# Test 1: Helper unit tests (normalization & remote parsing)
# Source aerial-pr.sh internal functions for testing by running in a subshell with a mock test hook
TEST_OUTPUT=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    echo \"core:\$(normalize_repo core)\"
    echo \"aerial:\$(normalize_repo aerial)\"
    echo \"config:\$(normalize_repo config)\"
    echo \"aerial-config:\$(normalize_repo aerial-config)\"
    echo \"sidecars:\$(normalize_repo sidecars)\"
    echo \"aerial-sidecars:\$(normalize_repo aerial-sidecars)\"
    echo \"azylman/aerial:\$(normalize_repo azylman/aerial)\"
    echo \"azylman/aerial-config.git:\$(normalize_repo azylman/aerial-config.git)\"
    echo \"aerial.git:\$(normalize_repo aerial.git)\"
    echo \"custom-repo:\$(normalize_repo custom-repo)\"
")

assert_eq "core:aerial" "$(echo "$TEST_OUTPUT" | grep '^core:')" "normalize_repo core -> aerial"
assert_eq "aerial:aerial" "$(echo "$TEST_OUTPUT" | grep '^aerial:')" "normalize_repo aerial -> aerial"
assert_eq "config:aerial-config" "$(echo "$TEST_OUTPUT" | grep '^config:')" "normalize_repo config -> aerial-config"
assert_eq "aerial-config:aerial-config" "$(echo "$TEST_OUTPUT" | grep '^aerial-config:')" "normalize_repo aerial-config -> aerial-config"
assert_eq "sidecars:aerial-sidecars" "$(echo "$TEST_OUTPUT" | grep '^sidecars:')" "normalize_repo sidecars -> aerial-sidecars"
assert_eq "aerial-sidecars:aerial-sidecars" "$(echo "$TEST_OUTPUT" | grep '^aerial-sidecars:')" "normalize_repo aerial-sidecars -> aerial-sidecars"
assert_eq "azylman/aerial:aerial" "$(echo "$TEST_OUTPUT" | grep '^azylman/aerial:')" "normalize_repo azylman/aerial -> aerial"
assert_eq "azylman/aerial-config.git:aerial-config" "$(echo "$TEST_OUTPUT" | grep '^azylman/aerial-config.git:')" "normalize_repo azylman/aerial-config.git -> aerial-config"
assert_eq "aerial.git:aerial" "$(echo "$TEST_OUTPUT" | grep '^aerial.git:')" "normalize_repo aerial.git -> aerial"
assert_eq "custom-repo:custom-repo" "$(echo "$TEST_OUTPUT" | grep '^custom-repo:')" "normalize_repo custom-repo -> custom-repo"

# Test 2: Remote URL extraction
REMOTE_OUTPUT=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    extract_remote_repo \"https://github.com/azylman/aerial-sidecars.git\"
    echo \"url1:\${PARSED_OWNER}/\${PARSED_REPO}\"
    extract_remote_repo \"git@github.com:azylman/aerial-config.git\"
    echo \"url2:\${PARSED_OWNER}/\${PARSED_REPO}\"
    extract_remote_repo \"https://github.com/myorg/myrepo\"
    echo \"url3:\${PARSED_OWNER}/\${PARSED_REPO}\"
    extract_remote_repo \"https://github.com/azylman/aerial-sidecars/\"
    echo \"url4:\${PARSED_OWNER}/\${PARSED_REPO}\"
    extract_remote_repo \"https://github.com/myorg/repo.with.dots.git\"
    echo \"url5:\${PARSED_OWNER}/\${PARSED_REPO}\"
")

assert_eq "url1:azylman/aerial-sidecars" "$(echo "$REMOTE_OUTPUT" | grep '^url1:')" "extract_remote_repo https://github.com/azylman/aerial-sidecars.git"
assert_eq "url2:azylman/aerial-config" "$(echo "$REMOTE_OUTPUT" | grep '^url2:')" "extract_remote_repo git@github.com:azylman/aerial-config.git"
assert_eq "url3:myorg/myrepo" "$(echo "$REMOTE_OUTPUT" | grep '^url3:')" "extract_remote_repo https://github.com/myorg/myrepo"
assert_eq "url4:azylman/aerial-sidecars" "$(echo "$REMOTE_OUTPUT" | grep '^url4:')" "extract_remote_repo https://github.com/azylman/aerial-sidecars/ (trailing slash)"
assert_eq "url5:myorg/repo.with.dots" "$(echo "$REMOTE_OUTPUT" | grep '^url5:')" "extract_remote_repo https://github.com/myorg/repo.with.dots.git (dots in name)"

# Test 3: Wrapper scripts existence and executable permissions
assert_eq "true" "$([ -x "$CONFIG_PR" ] && echo true || echo false)" "aerial-config-pr.sh is executable"
assert_eq "true" "$([ -x "$SIDECARS_PR" ] && echo true || echo false)" "aerial-sidecars-pr.sh is executable"

# Test 4: Pre-dispatch flag parsing works with invalid command (should show usage without erroring on --repo)
USAGE_OUT=$(bash "${AERIAL_PR}" --repo aerial-config invalid-cmd 2>&1 || true)
assert_eq "true" "$(echo "$USAGE_OUT" | grep -q 'Usage:' && echo true || echo false)" "--repo flag handled before invalid command displays usage"

# Test 5: Universal verification contract in a temporary scratch repository
TMP_TEST_DIR=$(mktemp -d /tmp/aerial-pr-test.XXXXXX)
trap 'rm -rf "$TMP_TEST_DIR"' EXIT

git init "$TMP_TEST_DIR" >/dev/null
git -C "$TMP_TEST_DIR" config user.name "Tester"
git -C "$TMP_TEST_DIR" config user.email "tester@example.com"
git -C "$TMP_TEST_DIR" remote add origin "https://github.com/azylman/aerial-sidecars.git"
mkdir -p "$TMP_TEST_DIR/scripts"

# Case 5A: verify.sh succeeds
cat << 'EOF' > "$TMP_TEST_DIR/scripts/verify.sh"
#!/bin/sh
exit 0
EOF
chmod +x "$TMP_TEST_DIR/scripts/verify.sh"
echo "hello" > "$TMP_TEST_DIR/test.txt"
git -C "$TMP_TEST_DIR" add test.txt

VERIFY_SUCCESS=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    run_preflight_verification \"${TMP_TEST_DIR}\"
    echo \"verify_result:\$?\"
")
assert_eq "verify_result:0" "$VERIFY_SUCCESS" "Universal pre-flight contract succeeds when verify.sh passes"

# Case 5B: verify.sh fails and preserves workspace
cat << 'EOF' > "$TMP_TEST_DIR/scripts/verify.sh"
#!/bin/sh
echo "Simulated linter failure" >&2
exit 1
EOF
chmod +x "$TMP_TEST_DIR/scripts/verify.sh"

VERIFY_FAIL_DIR=$(mktemp -d /tmp/aerial-pr-verify-fail.XXXXXX)
git init "$VERIFY_FAIL_DIR" >/dev/null
mkdir -p "$VERIFY_FAIL_DIR/scripts"
cp "$TMP_TEST_DIR/scripts/verify.sh" "$VERIFY_FAIL_DIR/scripts/verify.sh"
echo "dirty" > "$VERIFY_FAIL_DIR/dirty.txt"
git -C "$VERIFY_FAIL_DIR" add dirty.txt

VERIFY_FAIL_OUT=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    SCRATCH_DIR_CLEANUP=\"${VERIFY_FAIL_DIR}\"
    run_preflight_verification \"${VERIFY_FAIL_DIR}\" || echo \"verify_failed_as_expected\"
" 2>&1)

assert_eq "true" "$(echo "$VERIFY_FAIL_OUT" | grep -q 'verify_failed_as_expected' && echo true || echo false)" "Verification failure propagates non-zero exit code"
assert_eq "true" "$([ -d "$VERIFY_FAIL_DIR" ] && echo true || echo false)" "Scratch directory is preserved when verification fails"
rm -rf "$VERIFY_FAIL_DIR"

# Test 6: Target ID inheritance and rejection of -t / --target / --target-id
# Case 6A: submit -t rejects with explicit error
SUBMIT_T_ERR=$(bash "${AERIAL_PR}" submit -t 1234567890 2>&1 || true)
assert_eq "true" "$(echo "$SUBMIT_T_ERR" | grep -q "ERROR: Target ID cannot be specified manually (-t is removed)" && echo true || echo false)" "submit -t rejects with explicit error"

# Case 6B: submit --target rejects with explicit error
SUBMIT_TARGET_ERR=$(bash "${AERIAL_PR}" submit --target 1234567890 2>&1 || true)
assert_eq "true" "$(echo "$SUBMIT_TARGET_ERR" | grep -q "ERROR: Target ID cannot be specified manually (--target is removed)" && echo true || echo false)" "submit --target rejects with explicit error"

# Case 6C: submit --target-id=... rejects with explicit error
SUBMIT_TARGET_ID_ERR=$(bash "${AERIAL_PR}" submit --target-id=1234567890 2>&1 || true)
assert_eq "true" "$(echo "$SUBMIT_TARGET_ID_ERR" | grep -q "ERROR: Target ID cannot be specified manually (--target-id=1234567890 is removed)" && echo true || echo false)" "submit --target-id= rejects with explicit error"

# Case 6D: schedule_pr_followup, merge_pr, get_deploy_status, and resolve_hangar_url are completely eliminated
FOLLOWUP_FUNC_EXISTS=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    for fn in schedule_pr_followup merge_pr get_deploy_status resolve_hangar_url; do
        type \"\$fn\" 2>&1 || true
    done
")
assert_eq "4" "$(echo "$FOLLOWUP_FUNC_EXISTS" | grep -Fc 'not found')" "legacyfollowup/merge/deploy-status functions are eliminated"

SUBMIT_USAGE_CHECK=$(bash "${AERIAL_PR}" --help 2>&1 || true)
assert_eq "true" "$(echo "$SUBMIT_USAGE_CHECK" | grep -Fq 'scheduler-mcp' && echo false || echo true)" "Usage text does not reference scheduler-mcp"
assert_eq "true" "$(echo "$SUBMIT_USAGE_CHECK" | grep -Fq -- '--no-schedule' && echo false || echo true)" "Usage text does not reference --no-schedule"
assert_eq "true" "$(echo "$SUBMIT_USAGE_CHECK" | grep -Fq '[-d <delay>]' && echo false || echo true)" "Usage text does not reference delay options"

SCHEDULE_MCP_REFS=$(grep -n "scheduler-mcp" "${AERIAL_PR}" || true)
assert_eq "" "$SCHEDULE_MCP_REFS" "aerial-pr.sh contains zero references to scheduler-mcp"

# Case 6E: Target ID environment precedence via resolve_target_id
TARGET_PRECEDENCE_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true

    # Subtest 1: AERIAL_TARGET_ID wins and passes through unmodified
    res1=\$(AERIAL_TARGET_ID=\"target_aerial\" DISCORD_THREAD_ID=\"target_thread\" DISCORD_CHANNEL_ID=\"target_channel\" resolve_target_id)
    [ \"\$res1\" = \"target_aerial\" ] && echo \"target1:ok\" || echo \"target1:fail:\$res1\"

    # Subtest 2: DISCORD_THREAD_ID wins when AERIAL_TARGET_ID is unset
    res2=\$(unset AERIAL_TARGET_ID; DISCORD_THREAD_ID=\"target_thread\" DISCORD_CHANNEL_ID=\"target_channel\" resolve_target_id)
    [ \"\$res2\" = \"target_thread\" ] && echo \"target2:ok\" || echo \"target2:fail\"

    # Subtest 3: DISCORD_CHANNEL_ID wins when thread is unset
    res3=\$(unset AERIAL_TARGET_ID DISCORD_THREAD_ID; DISCORD_CHANNEL_ID=\"target_channel\" resolve_target_id)
    [ \"\$res3\" = \"target_channel\" ] && echo \"target3:ok\" || echo \"target3:fail\"

    # Subtest 4: returns empty string when no Discord target context exists (delegating fallback to webhooks-router)
    res4=\$(unset AERIAL_TARGET_ID DISCORD_THREAD_ID DISCORD_CHANNEL_ID ANTIGRAVITY_APP_DATA_DIR ANTIGRAVITY_CONVERSATION_ID; resolve_target_id)
    [ -z \"\$res4\" ] && echo \"target4:ok\" || echo \"target4:fail:\$res4\"

    # Subtest 5: Explicit argument takes top priority when passed to resolve_target_id
    res5=\$(AERIAL_TARGET_ID=\"target_aerial\" resolve_target_id \"1559768078131269684\")
    [ \"\$res5\" = \"1559768078131269684\" ] && echo \"target5:ok\" || echo \"target5:fail:\$res5\"

    # Subtest 6: Resolves thread_id from active transcript.jsonl when env target/thread/channel are unset
    FAKE_APP_DIR=\"${TMP_TEST_DIR}/fake_app\"
    mkdir -p \"\${FAKE_APP_DIR}/brain/conv-123/.system_generated/logs\"
    echo '{\"content\":\"- channel_id: 1542423172400291873\\n- thread_id: 1559768078131269684\"}' > \"\${FAKE_APP_DIR}/brain/conv-123/.system_generated/logs/transcript.jsonl\"
    res6=\$(unset AERIAL_TARGET_ID DISCORD_THREAD_ID DISCORD_CHANNEL_ID; ANTIGRAVITY_APP_DATA_DIR=\"\${FAKE_APP_DIR}\" ANTIGRAVITY_CONVERSATION_ID=\"conv-123\" resolve_target_id)
    [ \"\$res6\" = \"1559768078131269684\" ] && echo \"target6:ok\" || echo \"target6:fail:\$res6\"
")
assert_eq "true" "$(echo "$TARGET_PRECEDENCE_CHECK" | grep -q 'target1:ok' && echo true || echo false)" "Target precedence: AERIAL_TARGET_ID takes first priority"
assert_eq "true" "$(echo "$TARGET_PRECEDENCE_CHECK" | grep -q 'target2:ok' && echo true || echo false)" "Target precedence: DISCORD_THREAD_ID takes second priority"
assert_eq "true" "$(echo "$TARGET_PRECEDENCE_CHECK" | grep -q 'target3:ok' && echo true || echo false)" "Target precedence: DISCORD_CHANNEL_ID takes third priority"
assert_eq "true" "$(echo "$TARGET_PRECEDENCE_CHECK" | grep -q 'target4:ok' && echo true || echo false)" "Target precedence: returns empty string when no Discord context exists"
assert_eq "true" "$(echo "$TARGET_PRECEDENCE_CHECK" | grep -q 'target5:ok' && echo true || echo false)" "Target precedence: explicit argument takes top priority"
assert_eq "true" "$(echo "$TARGET_PRECEDENCE_CHECK" | grep -q 'target6:ok' && echo true || echo false)" "Target precedence: resolves thread_id from transcript.jsonl when env vars unset"

# Case 9: --vanadis-chunk global flag parsing and AERIAL_VANADIS_CHUNK_ID env injection
VANADIS_FLAG_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --vanadis-chunk=chunk-xyz-123 --source-only 2>/dev/null || true
    echo \"\${VANADIS_CHUNK_ID}\"
")
assert_eq "chunk-xyz-123" "$VANADIS_FLAG_CHECK" "--vanadis-chunk=<id> sets VANADIS_CHUNK_ID"

VANADIS_ENV_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    export AERIAL_VANADIS_CHUNK_ID=chunk-from-env-456
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    echo \"\${VANADIS_CHUNK_ID}\"
")
assert_eq "chunk-from-env-456" "$VANADIS_ENV_CHECK" "AERIAL_VANADIS_CHUNK_ID populates VANADIS_CHUNK_ID automatically"

VANADIS_OVERRIDE_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    export AERIAL_VANADIS_CHUNK_ID=chunk-from-env-456
    source \"${AERIAL_PR}\" --vanadis-chunk=chunk-cli-override-789 --source-only 2>/dev/null || true
    local_init=\$(grep 'local vanadis_chunk_id=' \"${AERIAL_PR}\")
    _check_local() { eval \"\$local_init\"; echo \"\$vanadis_chunk_id\"; }
    echo \"\${VANADIS_CHUNK_ID}:\$(_check_local)\"
")
assert_eq "chunk-cli-override-789:chunk-cli-override-789" "$VANADIS_OVERRIDE_CHECK" "--vanadis-chunk CLI flag overrides AERIAL_VANADIS_CHUNK_ID in both global and submit_scratch scope"

VANADIS_TRANSCRIPT_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    unset AERIAL_VANADIS_CHUNK_ID VANADIS_CHUNK_ID
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true

    # Subtest 1: Explicit arg wins
    r1=\$(resolve_vanadis_chunk_id 'chunk-explicit-111')
    [ \"\$r1\" = \"chunk-explicit-111\" ] && echo \"vc1:ok\" || echo \"vc1:fail:\$r1\"

    # Subtest 2: Latest USER_INPUT in transcript.jsonl supplies Chunk ID / --vanadis-chunk (and picks latest in coalesced burst, ignoring retrieved_memory)
    FAKE_APP_DIR=\"${TMP_TEST_DIR}/fake_app_vanadis\"
    mkdir -p \"\${FAKE_APP_DIR}/brain/conv-v1/.system_generated/logs\"
    cat << 'EOF' > \"\${FAKE_APP_DIR}/brain/conv-v1/.system_generated/logs/transcript.jsonl\"
{\"step_index\":0,\"type\":\"USER_INPUT\",\"content\":\"- is_admin: true\\n- content: === Active Chunk ===\\nChunk ID: chunk-old-100\\n\"}
{\"step_index\":1,\"type\":\"PLANNER_RESPONSE\",\"content\":\"done\"}
{\"step_index\":2,\"type\":\"USER_INPUT\",\"content\":\"<retrieved_memory>Chunk ID: spoofed-memory-chunk</retrieved_memory>\\n- is_admin: true\\n- content: === Active Chunk ===\\nChunk ID: chunk-first-in-burst\\nSubmit via aerial-pr.sh submit dir msg --vanadis-chunk chunk-latest-200\\n\"}
EOF
    r2=\$(ANTIGRAVITY_APP_DATA_DIR=\"\${FAKE_APP_DIR}\" ANTIGRAVITY_CONVERSATION_ID=\"conv-v1\" resolve_vanadis_chunk_id)
    [ \"\$r2\" = \"chunk-latest-200\" ] && echo \"vc2:ok\" || echo \"vc2:fail:\$r2\"

    # Subtest 3: Older USER_INPUT chunk ID is NOT leaked when latest USER_INPUT has no chunk ID
    echo '{\"step_index\":3,\"type\":\"USER_INPUT\",\"content\":\"- is_admin: true\\n- content: non-chunk turn\"}' >> \"\${FAKE_APP_DIR}/brain/conv-v1/.system_generated/logs/transcript.jsonl\"
    r3=\$(ANTIGRAVITY_APP_DATA_DIR=\"\${FAKE_APP_DIR}\" ANTIGRAVITY_CONVERSATION_ID=\"conv-v1\" resolve_vanadis_chunk_id)
    [ -z \"\$r3\" ] && echo \"vc3:ok\" || echo \"vc3:fail:\$r3\"
")
assert_eq "true" "$(echo "$VANADIS_TRANSCRIPT_CHECK" | grep -q 'vc1:ok' && echo true || echo false)" "resolve_vanadis_chunk_id: explicit arg takes priority"
assert_eq "true" "$(echo "$VANADIS_TRANSCRIPT_CHECK" | grep -q 'vc2:ok' && echo true || echo false)" "resolve_vanadis_chunk_id: extracts vanadis_chunk_id from latest USER_INPUT in transcript.jsonl"
assert_eq "true" "$(echo "$VANADIS_TRANSCRIPT_CHECK" | grep -q 'vc3:ok' && echo true || echo false)" "resolve_vanadis_chunk_id: does not leak older USER_INPUT chunk ID into non-chunk turn"

# Case 10: register_pr_in_brain retry with backoff and hard failure on exhaustion
REGISTER_PR_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    ATTEMPT_FILE=\"${TMP_TEST_DIR}/reg_attempts\"

    curl() { echo '{\"status\":\"registered\"}'; }
    export -f curl
    AERIAL_PR_REGISTER_RETRIES=3 AERIAL_PR_REGISTER_RETRY_DELAY=0 register_pr_in_brain 'http://eri.aerial/internal/pr/register' '{}' 101 '1560698400780062884' 2>/dev/null && echo 'reg1:ok'

    echo 0 > \"\$ATTEMPT_FILE\"
    curl() {
        local c=\$(( \$(cat \"\$ATTEMPT_FILE\") + 1 )); echo \"\$c\" > \"\$ATTEMPT_FILE\"
        [ \"\$c\" -lt 2 ] && echo '{\"error\":\"transient\"}' || echo '{\"status\":\"registered\"}'
    }
    export -f curl
    AERIAL_PR_REGISTER_RETRIES=3 AERIAL_PR_REGISTER_RETRY_DELAY=0 register_pr_in_brain 'http://eri.aerial/internal/pr/register' '{}' 102 '1560698400780062884' 2>/dev/null && echo \"reg2:ok:\$(cat \"\$ATTEMPT_FILE\")\"

    echo 0 > \"\$ATTEMPT_FILE\"
    curl() {
        local c=\$(( \$(cat \"\$ATTEMPT_FILE\") + 1 )); echo \"\$c\" > \"\$ATTEMPT_FILE\"
        echo '{\"error\":\"Failed to attach PR to Vanadis chunk\"}'
    }
    export -f curl
    AERIAL_PR_REGISTER_RETRIES=3 AERIAL_PR_REGISTER_RETRY_DELAY=0 register_pr_in_brain 'http://eri.aerial/internal/pr/register' '{}' 103 '1560698400780062884' 2>/dev/null || echo \"reg3:failed_as_expected:\$(cat \"\$ATTEMPT_FILE\")\"
")
assert_eq "true" "$(echo "$REGISTER_PR_CHECK" | grep -q 'reg1:ok' && echo true || echo false)" "register_pr_in_brain succeeds on first attempt"
assert_eq "true" "$(echo "$REGISTER_PR_CHECK" | grep -q 'reg2:ok:2' && echo true || echo false)" "register_pr_in_brain retries transient error and succeeds on attempt 2"
assert_eq "true" "$(echo "$REGISTER_PR_CHECK" | grep -q 'reg3:failed_as_expected:3' && echo true || echo false)" "register_pr_in_brain fails non-zero after exhausting all 3 attempts"

# Case 10b: PR registration endpoint probing and AERIAL_PR_REGISTER_URL override
PR_ENDPOINT_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true

    # Test 1: AERIAL_PR_REGISTER_URL override used directly
    res1=\$(AERIAL_PR_REGISTER_URL='http://custom-router.aerial:4020' bash -c '
        if [ -n \"\${AERIAL_PR_REGISTER_URL:-}\" ]; then
            if [[ \"\${AERIAL_PR_REGISTER_URL}\" == */internal/pr/register ]]; then
                echo \"\${AERIAL_PR_REGISTER_URL}\"
            else
                echo \"\${AERIAL_PR_REGISTER_URL%/}/internal/pr/register\"
            fi
        fi
    ')
    [ \"\$res1\" = 'http://custom-router.aerial:4020/internal/pr/register' ] && echo 'ep1:ok'

    # Test 2: Default fallback when no candidates respond
    curl() { return 1; }
    export -f curl
    res2=\$(bash -c '
        brain_reg_url=\"\"
        for cand in \"http://webhooks-router.aerial\" \"http://127.0.0.1:4020\" \"http://127.0.0.1:8088\" \"http://eri.aerial\" \"http://brain.aerial\" \"http://host.docker.internal:8088\" \"http://127.0.0.1:8080\" \"http://aerial-brain:8080\"; do
            probe_code=\$(curl -s -o /dev/null -w \"%{http_code}\" --connect-timeout 1 -m 2 -X POST \"\${cand}/internal/pr/register\" 2>/dev/null || true)
            if [ \"\$probe_code\" = \"400\" ] || [ \"\$probe_code\" = \"200\" ]; then
                brain_reg_url=\"\${cand}/internal/pr/register\"
                break
            fi
        done
        if [ -z \"\$brain_reg_url\" ]; then
            brain_reg_url=\"http://webhooks-router.aerial/internal/pr/register\"
        fi
        echo \"\$brain_reg_url\"
    ')
    [ \"\$res2\" = 'http://webhooks-router.aerial/internal/pr/register' ] && echo 'ep2:ok'

    # Test 3: First responding candidate is selected (webhooks-router candidate)
    curl() {
        if [[ \"\$*\" == *\"http://webhooks-router.aerial/internal/pr/register\"* ]]; then
            printf '400'
        fi
    }
    export -f curl
    res3=\$(bash -c '
        brain_reg_url=\"\"
        for cand in \"http://webhooks-router.aerial\" \"http://127.0.0.1:4020\" \"http://127.0.0.1:8088\" \"http://eri.aerial\" \"http://brain.aerial\" \"http://host.docker.internal:8088\" \"http://127.0.0.1:8080\" \"http://aerial-brain:8080\"; do
            probe_code=\$(curl -s -o /dev/null -w \"%{http_code}\" --connect-timeout 1 -m 2 -X POST \"\${cand}/internal/pr/register\" 2>/dev/null || true)
            if [ \"\$probe_code\" = \"400\" ] || [ \"\$probe_code\" = \"200\" ]; then
                brain_reg_url=\"\${cand}/internal/pr/register\"
                break
            fi
        done
        echo \"\$brain_reg_url\"
    ')
    [ \"\$res3\" = 'http://webhooks-router.aerial/internal/pr/register' ] && echo 'ep3:ok'
")
assert_eq "true" "$(echo "$PR_ENDPOINT_CHECK" | grep -q 'ep1:ok' && echo true || echo false)" "AERIAL_PR_REGISTER_URL override appends /internal/pr/register"
assert_eq "true" "$(echo "$PR_ENDPOINT_CHECK" | grep -q 'ep2:ok' && echo true || echo false)" "PR register endpoint defaults to webhooks-router fallback"
assert_eq "true" "$(echo "$PR_ENDPOINT_CHECK" | grep -q 'ep3:ok' && echo true || echo false)" "PR register endpoint probes webhooks-router.aerial first"

# Case 11: rebase_staged_changes rebases onto advanced upstream before verification and aborts on conflict
REBASE_STAGED_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    REMOTE_DIR=\$(mktemp -d /tmp/aerial-pr-rebase-remote.XXXXXX)
    WORK_DIR=\$(mktemp -d /tmp/aerial-pr-rebase-work.XXXXXX)
    UPSTREAM_DIR=\$(mktemp -d /tmp/aerial-pr-rebase-up.XXXXXX)
    trap 'rm -rf \"\$REMOTE_DIR\" \"\$WORK_DIR\" \"\$UPSTREAM_DIR\"' EXIT

    git init -q --bare \"\$REMOTE_DIR\"
    git clone -q \"\$REMOTE_DIR\" \"\$UPSTREAM_DIR\"
    git -C \"\$UPSTREAM_DIR\" config user.name 'Tester'
    git -C \"\$UPSTREAM_DIR\" config user.email 'tester@example.com'
    git -C \"\$UPSTREAM_DIR\" checkout -q -b main
    echo 'base' > \"\$UPSTREAM_DIR/shared.txt\"
    git -C \"\$UPSTREAM_DIR\" add .
    git -C \"\$UPSTREAM_DIR\" commit -q -m 'initial'
    git -C \"\$UPSTREAM_DIR\" push -q -u origin main

    git clone -q -b main \"\$REMOTE_DIR\" \"\$WORK_DIR\"
    git -C \"\$WORK_DIR\" config user.name 'Tester'
    git -C \"\$WORK_DIR\" config user.email 'tester@example.com'
    git -C \"\$WORK_DIR\" checkout -q -b feat/test

    # Advance upstream main with a non-conflicting commit
    echo 'upstream-new' > \"\$UPSTREAM_DIR/upstream.txt\"
    git -C \"\$UPSTREAM_DIR\" add .
    git -C \"\$UPSTREAM_DIR\" commit -q -m 'upstream advance'
    git -C \"\$UPSTREAM_DIR\" push -q origin main
    UP_SHA=\$(git -C \"\$UPSTREAM_DIR\" rev-parse HEAD)

    # Stage local feature file in work dir and run rebase_staged_changes
    cd \"\$WORK_DIR\"
    echo 'feature-change' > local.txt
    git add local.txt
    if rebase_staged_changes main 'feat: local' 2>/dev/null; then
        HEAD_NOW=\$(git rev-parse HEAD)
        STAGED_FILES=\$(git diff --cached --name-only)
        if [ \"\$HEAD_NOW\" = \"\$UP_SHA\" ] && [ \"\$STAGED_FILES\" = \"local.txt\" ] && [ -f upstream.txt ]; then
            echo 'rebase_clean:ok'
        else
            echo \"rebase_clean:fail:\$HEAD_NOW:\$STAGED_FILES\"
        fi
    fi

    # Advance upstream with a conflicting edit to shared.txt
    echo 'upstream-conflict' > \"\$UPSTREAM_DIR/shared.txt\"
    git -C \"\$UPSTREAM_DIR\" add .
    git -C \"\$UPSTREAM_DIR\" commit -q -m 'upstream conflict'
    git -C \"\$UPSTREAM_DIR\" push -q origin main

    echo 'local-conflict' > \"\$WORK_DIR/shared.txt\"
    git -C \"\$WORK_DIR\" add shared.txt
    SCRATCH_DIR_CLEANUP=\"\$WORK_DIR\"
    if ! rebase_staged_changes main 'feat: conflict' 2>/dev/null; then
        if [ -z \"\$SCRATCH_DIR_CLEANUP\" ] && git diff --cached --name-only | grep -q 'shared.txt'; then
            echo 'rebase_conflict:aborted_cleanly'
        fi
    fi
")
assert_eq "true" "$(echo "$REBASE_STAGED_CHECK" | grep -q 'rebase_clean:ok' && echo true || echo false)" "rebase_staged_changes rebases onto advanced upstream and keeps changes staged"
assert_eq "true" "$(echo "$REBASE_STAGED_CHECK" | grep -q 'rebase_conflict:aborted_cleanly' && echo true || echo false)" "rebase_staged_changes fails closed on merge conflict, restores staged changes, and preserves workspace"

# Case 12: override_roborev validation, mock execution, comment verification, rerun fallback, and output formatting
ROBOREV_OVERRIDE_CHECK=$(
    REPO_TEST_MODE=1
    source "${AERIAL_PR}" --source-only 2>/dev/null || true

    # Subtest 1: Missing or non-numeric PR number validation
    err1=$(override_roborev 2>&1) || true
    echo "$err1" | grep -q "Valid numeric pr_num required" && echo "val1:ok" || echo "val1:fail"

    err1b=$(override_roborev not-a-number "valid rebuttal" 2>&1) || true
    echo "$err1b" | grep -q "Valid numeric pr_num required" && echo "val1b:ok" || echo "val1b:fail"

    # Subtest 2: Missing or blank rebuttal validation
    err2=$(override_roborev 42 2>&1) || true
    echo "$err2" | grep -q "Technical rebuttal cannot be empty" && echo "val2:ok" || echo "val2:fail"

    err2b=$(override_roborev 42 "   " 2>&1) || true
    echo "$err2b" | grep -q "Technical rebuttal cannot be empty" && echo "val2b:ok" || echo "val2b:fail"

    # Subtest 3: Missing reason file validation
    err3=$(override_roborev -f /nonexistent/rebuttal.txt 42 2>&1) || true
    echo "$err3" | grep -q "Rebuttal file not found" && echo "val3:ok" || echo "val3:fail"

    # Subtest 4: Missing GITHUB_PAT validation
    err4=$(unset GITHUB_PAT; override_roborev 42 "rebuttal" 2>&1) || true
    echo "$err4" | grep -q "GITHUB_PAT environment variable must be set" && echo "val4:ok" || echo "val4:fail"

    # Subtest 5: Closed PR rejection
    export GITHUB_PAT="mock_pat"
    curl() {
        if [[ "$*" =~ /pulls/ ]]; then
            echo '{"state":"closed","head":{"sha":"abc123"},"html_url":"https://github.com/azylman/aerial/pull/42"}'
            echo '200'
        fi
    }
    err5=$(override_roborev 42 "valid rebuttal" 2>&1) || true
    echo "$err5" | grep -q "Pull Request #42 is closed" && echo "closed:ok" || echo "closed:fail"

    # Subtest 6: Missing workflow run fails closed
    curl() {
        if [[ "$*" =~ /pulls/ ]]; then
            echo '{"state":"open","head":{"sha":"0123456789abcdef0123456789abcdef01234567"},"html_url":"https://github.com/azylman/aerial/pull/42"}'
            echo '200'
        elif [[ "$*" =~ /issues/42/comments ]]; then
            echo '{"id":101}'
            echo '201'
        elif [[ "$*" =~ /actions/runs ]]; then
            echo '{"workflow_runs":[]}'
            echo '200'
        fi
    }
    err6=$(override_roborev 42 "valid rebuttal" 2>&1) || true
    echo "$err6" | grep -q "No RoboRev workflow run found" && echo "missing_run:ok" || echo "missing_run:fail"

    # Subtest 7: Happy path with primary rerun (rerun-failed-jobs)
    COMMENT_FILE="$(mktemp /tmp/roborev-comment-test.XXXXXX)"
    RERUN_FILE="$(mktemp /tmp/roborev-rerun-test.XXXXXX)"
    curl() {
        local url=""
        local method="GET"
        local data=""
        while [ $# -gt 0 ]; do
            case "$1" in
                -X) method="$2"; shift 2 ;;
                -d) data="$2"; shift 2 ;;
                https://*) url="$1"; shift ;;
                *) shift ;;
            esac
        done
        if [[ "$url" =~ /pulls/42 ]]; then
            echo '{"state":"open","head":{"sha":"fedcba9876543210fedcba9876543210fedcba98"},"html_url":"https://github.com/azylman/aerial/pull/42"}'
            echo '200'
        elif [[ "$url" =~ /issues/42/comments ]]; then
            echo "$data" > "$COMMENT_FILE"
            echo '{"id":202}'
            echo '201'
        elif [[ "$url" =~ /actions/runs\? ]]; then
            echo '{"workflow_runs":[{"id":778899,"name":"RoboRev Code Review","path":".github/workflows/roborev.yml"}]}'
            echo '200'
        elif [[ "$url" =~ /actions/runs/778899/rerun-failed-jobs ]]; then
            echo "rerun-failed-jobs-called" > "$RERUN_FILE"
            echo '{"status":"queued"}'
            echo '201'
        fi
    }
    out7=$(override_roborev --repo aerial 42 "Independent evaluation confirms this is a false positive.")
    comment_body=$(jq -r '.body' "$COMMENT_FILE" 2>/dev/null || echo "")
    rm -f "$COMMENT_FILE" "$RERUN_FILE"
    if echo "$out7" | jq -e '.status == "overridden" and .run_id == 778899 and .turn_action == "end_turn" and .commit_sha == "fedcba9876543210fedcba9876543210fedcba98"' >/dev/null 2>&1 && \
       echo "$comment_body" | grep -F -q '## roborev-override (`fedcba9`)' && \
       echo "$comment_body" | grep -q "Independent evaluation confirms this is a false positive."; then
        echo "happy_path:ok"
    else
        echo "happy_path:fail:$out7"
    fi

    # Subtest 8: Fallback from rerun-failed-jobs (422) to full workflow rerun (201)
    FALLBACK_FILE="$(mktemp /tmp/roborev-fallback-test.XXXXXX)"
    curl() {
        local url=""
        while [ $# -gt 0 ]; do
            case "$1" in
                https://*) url="$1"; shift ;;
                *) shift ;;
            esac
        done
        if [[ "$url" =~ /pulls/42 ]]; then
            echo '{"state":"open","head":{"sha":"aabbccddeeff00112233445566778899aabbccdd"},"html_url":"https://github.com/azylman/aerial/pull/42"}'
            echo '200'
        elif [[ "$url" =~ /issues/42/comments ]]; then
            echo '{"id":303}'
            echo '201'
        elif [[ "$url" =~ /actions/runs\? ]]; then
            echo '{"workflow_runs":[{"id":889900,"name":"RoboRev Code Review","path":".github/workflows/roborev.yml"}]}'
            echo '200'
        elif [[ "$url" =~ /actions/runs/889900/rerun-failed-jobs ]]; then
            echo '{"message":"no failed jobs to rerun"}'
            echo '422'
        elif [[ "$url" =~ /actions/runs/889900/rerun ]]; then
            echo "fallback-rerun-called" > "$FALLBACK_FILE"
            echo '{"status":"queued"}'
            echo '201'
        fi
    }
    out8=$(override_roborev --repo aerial 42 "Fallback test rebuttal")
    fallback_recorded=$(cat "$FALLBACK_FILE" 2>/dev/null || echo "")
    rm -f "$FALLBACK_FILE"
    if echo "$out8" | jq -e '.status == "overridden" and .run_id == 889900' >/dev/null 2>&1 && \
       [ "$fallback_recorded" = "fallback-rerun-called" ]; then
        echo "fallback_rerun:ok"
    else
        echo "fallback_rerun:fail:$out8"
    fi

    # Subtest 9: Reason file flag (-f) and reason string flag (-m)
    REASON_FILE="$(mktemp /tmp/roborev-reason-file-test.XXXXXX)"
    echo "Reason from file content" > "$REASON_FILE"
    curl() {
        local url=""
        local data=""
        while [ $# -gt 0 ]; do
            case "$1" in
                -d) data="$2"; shift 2 ;;
                https://*) url="$1"; shift ;;
                *) shift ;;
            esac
        done
        if [[ "$url" =~ /pulls/42 ]]; then
            echo '{"state":"open","head":{"sha":"11223344556677889900aabbccddeeff11223344"},"html_url":"https://github.com/azylman/aerial/pull/42"}'
            echo '200'
        elif [[ "$url" =~ /issues/42/comments ]]; then
            echo '{"id":404}'
            echo '201'
        elif [[ "$url" =~ /actions/runs\? ]]; then
            echo '{"workflow_runs":[{"id":990011,"name":"RoboRev Code Review"}]}'
            echo '200'
        elif [[ "$url" =~ /actions/runs/990011/rerun-failed-jobs ]]; then
            echo '{"status":"queued"}'
            echo '201'
        fi
    }
    out9=$(override_roborev -f "$REASON_FILE" 42)
    rm -f "$REASON_FILE"
    if echo "$out9" | jq -e '.status == "overridden"' >/dev/null 2>&1; then
        echo "flags_test:ok"
    else
        echo "flags_test:fail:$out9"
    fi

    # Subtest 10: Resolving Continuous Delivery (docker-publish.yml) workflow run when standalone roborev.yml is absent
    curl() {
        local url=""
        while [ $# -gt 0 ]; do
            case "$1" in
                https://*) url="$1"; shift ;;
                *) shift ;;
            esac
        done
        if [[ "$url" =~ /pulls/42 ]]; then
            echo '{"state":"open","head":{"sha":"3344556677889900aabbccddeeff001122334455"},"html_url":"https://github.com/azylman/aerial/pull/42"}'
            echo '200'
        elif [[ "$url" =~ /issues/42/comments ]]; then
            echo '{"id":505}'
            echo '201'
        elif [[ "$url" =~ /actions/runs\? ]]; then
            echo '{"workflow_runs":[{"id":112233,"name":"Continuous Delivery","path":".github/workflows/docker-publish.yml"}]}'
            echo '200'
        elif [[ "$url" =~ /actions/runs/112233/rerun-failed-jobs ]]; then
            echo '{"status":"queued"}'
            echo '201'
        fi
    }
    out10=$(override_roborev --repo aerial 42 "Testing unified workflow resolution")
    if echo "$out10" | jq -e '.status == "overridden" and .run_id == 112233' >/dev/null 2>&1; then
        echo "unified_wf:ok"
    else
        echo "unified_wf:fail:$out10"
    fi
)
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'val1:ok' && echo true || echo false)" "override_roborev validates missing PR number"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'val1b:ok' && echo true || echo false)" "override_roborev validates non-numeric PR number"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'val2:ok' && echo true || echo false)" "override_roborev validates missing rebuttal"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'val2b:ok' && echo true || echo false)" "override_roborev validates blank rebuttal"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'val3:ok' && echo true || echo false)" "override_roborev validates non-existent reason file"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'val4:ok' && echo true || echo false)" "override_roborev validates missing GITHUB_PAT"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'closed:ok' && echo true || echo false)" "override_roborev rejects closed PR"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'missing_run:ok' && echo true || echo false)" "override_roborev fails closed when workflow run missing"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'happy_path:ok' && echo true || echo false)" "override_roborev posts comment, triggers rerun-failed-jobs, and outputs json"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'fallback_rerun:ok' && echo true || echo false)" "override_roborev falls back to full workflow rerun when rerun-failed-jobs returns 4xx"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'flags_test:ok' && echo true || echo false)" "override_roborev supports -f reason file"
assert_eq "true" "$(echo "$ROBOREV_OVERRIDE_CHECK" | grep -q 'unified_wf:ok' && echo true || echo false)" "override_roborev resolves Continuous Delivery workflow run when roborev.yml is absent"

CLI_DISPATCH_CHECK=$(bash -c "
    \"${AERIAL_PR}\" override-roborev 2>&1 || true
")
assert_eq "true" "$(echo "$CLI_DISPATCH_CHECK" | grep -q 'Valid numeric pr_num required for roborev override' && echo true || echo false)" "CLI dispatch: override-roborev dispatches to override_roborev"

CLI_DISPUTE_CHECK=$(bash -c "
    \"${AERIAL_PR}\" dispute-roborev 2>&1 || true
")
assert_eq "true" "$(echo "$CLI_DISPUTE_CHECK" | grep -q 'Valid numeric pr_num required for roborev override' && echo true || echo false)" "CLI dispatch: dispute-roborev dispatches to override_roborev"

# Case 13: format_aerial_pr_trailer embeds single-line self-healing HTML comment
TRAILER_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    echo \"t1:\$(format_aerial_pr_trailer '1562162630997581946' 'build-123-chunk-2')\"
    echo \"t2:\$(format_aerial_pr_trailer '' '')\"
")
assert_eq 't1:<!-- aerial-pr: {"target_id":"1562162630997581946","vanadis_chunk_id":"build-123-chunk-2"} -->' "$(echo "$TRAILER_CHECK" | grep '^t1:')" "format_aerial_pr_trailer formats compact JSON trailer comment"
assert_eq "t2:" "$(echo "$TRAILER_CHECK" | grep '^t2:')" "format_aerial_pr_trailer returns empty string when all metadata fields are empty"

# Case 14: push_scratch_branch falls back to --force-with-lease when rebased branch already exists on remote
PUSH_REBASED_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    REMOTE_DIR=\$(mktemp -d /tmp/aerial-pr-push-remote.XXXXXX)
    WORK_DIR=\$(mktemp -d /tmp/aerial-pr-push-work.XXXXXX)
    UPSTREAM_DIR=\$(mktemp -d /tmp/aerial-pr-push-up.XXXXXX)
    trap 'rm -rf \"\$REMOTE_DIR\" \"\$WORK_DIR\" \"\$UPSTREAM_DIR\"' EXIT

    git init -q --bare \"\$REMOTE_DIR\"
    git clone -q \"\$REMOTE_DIR\" \"\$UPSTREAM_DIR\"
    git -C \"\$UPSTREAM_DIR\" config user.name 'Tester'
    git -C \"\$UPSTREAM_DIR\" config user.email 'tester@example.com'
    git -C \"\$UPSTREAM_DIR\" checkout -q -b main
    echo 'base' > \"\$UPSTREAM_DIR/base.txt\"
    git -C \"\$UPSTREAM_DIR\" add . && git -C \"\$UPSTREAM_DIR\" commit -q -m 'initial'
    git -C \"\$UPSTREAM_DIR\" push -q -u origin main

    git clone -q -b main \"\$REMOTE_DIR\" \"\$WORK_DIR\"
    git -C \"\$WORK_DIR\" config user.name 'Tester'
    git -C \"\$WORK_DIR\" config user.email 'tester@example.com'
    git -C \"\$WORK_DIR\" checkout -q -b feat/existing-pr
    echo 'v1' > \"\$WORK_DIR/feat.txt\"
    git -C \"\$WORK_DIR\" add . && git -C \"\$WORK_DIR\" commit -q -m 'feat v1'
    git -C \"\$WORK_DIR\" push -q -u origin feat/existing-pr

    echo 'upstream-v2' > \"\$UPSTREAM_DIR/up.txt\"
    git -C \"\$UPSTREAM_DIR\" add . && git -C \"\$UPSTREAM_DIR\" commit -q -m 'upstream v2'
    git -C \"\$UPSTREAM_DIR\" push -q origin main

    cd \"\$WORK_DIR\"
    echo 'v2' > feat.txt
    git add feat.txt
    rebase_staged_changes main 'feat v2' 2>/dev/null
    git commit -q -m 'feat v2'
    if push_scratch_branch feat/existing-pr 2>/dev/null; then
        echo 'push_rebased:ok'
    else
        echo 'push_rebased:fail'
    fi

    # Single-branch clone checkout via FETCH_HEAD (no prior refs/remotes/origin/feat/existing-pr)
    SINGLE_DIR=\$(mktemp -d /tmp/aerial-pr-push-single.XXXXXX)
    git clone -q --depth 20 --single-branch -b main \"\$REMOTE_DIR\" \"\$SINGLE_DIR\"
    git -C \"\$SINGLE_DIR\" config user.name 'Tester'
    git -C \"\$SINGLE_DIR\" config user.email 'tester@example.com'
    git -C \"\$SINGLE_DIR\" fetch -q origin feat/existing-pr
    git -C \"\$SINGLE_DIR\" checkout -q -b feat/existing-pr FETCH_HEAD
    echo 'upstream-v3' > \"\$UPSTREAM_DIR/up.txt\"
    git -C \"\$UPSTREAM_DIR\" add . && git -C \"\$UPSTREAM_DIR\" commit -q -m 'upstream v3'
    git -C \"\$UPSTREAM_DIR\" push -q origin main
    cd \"\$SINGLE_DIR\"
    echo 'v3' > feat.txt
    git add feat.txt
    rebase_staged_changes main 'feat v3' 2>/dev/null
    git commit -q -m 'feat v3'
    if push_scratch_branch feat/existing-pr 2>/dev/null && [ \"\${PUSH_UPDATED_EXISTING}\" = \"1\" ]; then
        echo 'push_single_branch:ok'
    else
        echo 'push_single_branch:fail'
    fi
    rm -rf \"\$SINGLE_DIR\"
")
assert_eq "true" "$(echo "$PUSH_REBASED_CHECK" | grep -q 'push_rebased:ok' && echo true || echo false)" "push_scratch_branch updates existing remote branch cleanly after rebase"
assert_eq "true" "$(echo "$PUSH_REBASED_CHECK" | grep -q 'push_single_branch:ok' && echo true || echo false)" "push_scratch_branch updates existing remote branch from --single-branch FETCH_HEAD checkout"

# Case 15: submit_scratch idempotency across verify failure retry, pre-committed changes, and existing open PRs
SUBMIT_IDEMPOTENCY_CHECK=$(bash -c "
    REMOTE_DIR=\$(mktemp -d /tmp/aerial-pr-idem-remote.XXXXXX)
    UPSTREAM_DIR=\$(mktemp -d /tmp/aerial-pr-idem-up.XXXXXX)
    WORK_DIR=\$(mktemp -d /tmp/aerial-pr-idem-work.XXXXXX)
    trap 'rm -rf \"\$REMOTE_DIR\" \"\$UPSTREAM_DIR\" \"\$WORK_DIR\"' EXIT

    git init -q --bare \"\$REMOTE_DIR\"
    git clone -q \"\$REMOTE_DIR\" \"\$UPSTREAM_DIR\"
    git -C \"\$UPSTREAM_DIR\" config user.name 'Tester'
    git -C \"\$UPSTREAM_DIR\" config user.email 'tester@example.com'
    git -C \"\$UPSTREAM_DIR\" checkout -q -b main
    mkdir -p \"\$UPSTREAM_DIR/scripts\"
    cat << 'EOF' > \"\$UPSTREAM_DIR/scripts/verify.sh\"
#!/bin/sh
[ ! -f .fail_verify ]
EOF
    chmod +x \"\$UPSTREAM_DIR/scripts/verify.sh\"
    git -C \"\$UPSTREAM_DIR\" add . && git -C \"\$UPSTREAM_DIR\" commit -q -m 'init'
    git -C \"\$UPSTREAM_DIR\" push -q -u origin main

    git clone -q --single-branch -b main \"\$REMOTE_DIR\" \"\$WORK_DIR\"
    git -C \"\$WORK_DIR\" config user.name 'Tester'
    git -C \"\$WORK_DIR\" config user.email 'tester@example.com'
    git -C \"\$WORK_DIR\" checkout -q -b feat/idem-branch

    # Attempt 1: verify.sh fails; PR_DESCRIPTION.md and workspace must be preserved
    echo 'Summary of changes' > \"\$WORK_DIR/PR_DESCRIPTION.md\"
    echo 'v1' > \"\$WORK_DIR/feature.txt\"
    touch \"\$WORK_DIR/.fail_verify\"

    export GITHUB_PAT='mock_pat'
    export AERIAL_PR_REGISTER_URL='http://mock-router/internal/pr/register'
    (
        REPO_TEST_MODE=1
        source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
        submit_scratch \"\$WORK_DIR\" 'feat: initial attempt' >/dev/null 2>&1
    ) || true

    if [ -d \"\$WORK_DIR\" ] && [ -s \"\$WORK_DIR/PR_DESCRIPTION.md\" ]; then
        echo 'retry_preserved_desc:ok'
    else
        echo 'retry_preserved_desc:fail'
    fi

    # Attempt 2: remove .fail_verify and re-run submit_scratch WITHOUT re-creating PR_DESCRIPTION.md
    rm -f \"\$WORK_DIR/.fail_verify\" \"\$WORK_DIR/.git/index.lock\"
    git -C \"\$WORK_DIR\" rm --cached -f .fail_verify >/dev/null 2>&1 || true
    out2=\$(
        REPO_TEST_MODE=1
        source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
        curl() {
            if [[ \"\$*\" == *'/pulls'* ]] && [[ \"\$*\" == *'-X POST'* ]]; then
                printf '{\"number\":77,\"html_url\":\"https://github.com/azylman/aerial/pull/77\",\"node_id\":\"PR_kwDO77\"}\n201'
            elif [[ \"\$*\" == *'graphql'* ]]; then
                printf '{\"data\":{\"enablePullRequestAutoMerge\":{\"pullRequest\":{\"autoMergeRequest\":{\"enabledAt\":\"now\"}}}}}'
            elif [[ \"\$*\" == *'/internal/pr/register'* ]]; then
                printf '{\"status\":\"registered\"}'
            fi
        }
        export -f curl
        submit_scratch \"\$WORK_DIR\" 'feat: initial attempt' 2>/dev/null
    )
    if echo \"\$out2\" | jq -e '.status == \"submitted\" and .pr_number == 77' >/dev/null 2>&1 && [ ! -d \"\$WORK_DIR\" ]; then
        echo 'retry_submit_success:ok'
    else
        echo \"retry_submit_success:fail:\$out2\"
    fi

    # Attempt 3: Remediation checkout of existing PR branch with pre-committed fix and NO PR_DESCRIPTION.md
    REMED_DIR=\$(mktemp -d /tmp/aerial-pr-idem-remed.XXXXXX)
    git clone -q --single-branch -b main \"\$REMOTE_DIR\" \"\$REMED_DIR\"
    git -C \"\$REMED_DIR\" config user.name 'Tester'
    git -C \"\$REMED_DIR\" config user.email 'tester@example.com'
    git -C \"\$REMED_DIR\" fetch -q origin feat/idem-branch
    git -C \"\$REMED_DIR\" checkout -q -b feat/idem-branch FETCH_HEAD

    # Advance upstream main while remediation worker commits a fix locally
    echo 'upstream-advance' > \"\$UPSTREAM_DIR/upstream.txt\"
    git -C \"\$UPSTREAM_DIR\" add . && git -C \"\$UPSTREAM_DIR\" commit -q -m 'upstream advance'
    git -C \"\$UPSTREAM_DIR\" push -q origin main

    echo 'v2-remediated' > \"\$REMED_DIR/feature.txt\"
    git -C \"\$REMED_DIR\" add feature.txt
    git -C \"\$REMED_DIR\" commit -q -m 'fix: remediate lint'

    out3=\$(
        REPO_TEST_MODE=1
        source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
        curl() {
            if [[ \"\$*\" == *'/pulls?state=open'* ]]; then
                printf '[{\"number\":77,\"title\":\"feat: initial attempt\",\"body\":\"Existing PR body\",\"html_url\":\"https://github.com/azylman/aerial/pull/77\",\"node_id\":\"PR_kwDO77\"}]'
            elif [[ \"\$*\" == *'graphql'* ]]; then
                printf '{\"data\":{\"enablePullRequestAutoMerge\":{\"pullRequest\":{\"autoMergeRequest\":{\"enabledAt\":\"now\"}}}}}'
            elif [[ \"\$*\" == *'/internal/pr/register'* ]]; then
                printf '{\"status\":\"registered\"}'
            fi
        }
        export -f curl
        submit_scratch \"\$REMED_DIR\" 2>/dev/null
    )
    if echo \"\$out3\" | jq -e '.status == \"submitted\" and .pr_number == 77' >/dev/null 2>&1 && [ ! -d \"\$REMED_DIR\" ]; then
        echo 'remed_existing_pr:ok'
    else
        echo \"remed_existing_pr:fail:\$out3\"
    fi
    rm -rf \"\$REMED_DIR\"
")
assert_eq "true" "$(echo "$SUBMIT_IDEMPOTENCY_CHECK" | grep -q 'retry_preserved_desc:ok' && echo true || echo false)" "submit_scratch restores PR_DESCRIPTION.md and preserves workspace on verify failure"
assert_eq "true" "$(echo "$SUBMIT_IDEMPOTENCY_CHECK" | grep -q 'retry_submit_success:ok' && echo true || echo false)" "submit_scratch succeeds on second attempt without re-creating PR_DESCRIPTION.md"
assert_eq "true" "$(echo "$SUBMIT_IDEMPOTENCY_CHECK" | grep -q 'remed_existing_pr:ok' && echo true || echo false)" "submit_scratch succeeds idempotently on existing PR branch with pre-committed changes and no local PR_DESCRIPTION.md"

# Case 16: Tree-stamped pre-flight verification cache and init_scratch --branch
TREE_CACHE_AND_BRANCH_CHECK=$(bash -c "
    REPO_TEST_MODE=1
    source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    CACHE_DIR=\$(mktemp -d /tmp/aerial-pr-tree-cache.XXXXXX)
    REMOTE_DIR=\$(mktemp -d /tmp/aerial-pr-branch-remote.XXXXXX)
    SEED_DIR=\$(mktemp -d /tmp/aerial-pr-branch-seed.XXXXXX)
    trap 'rm -rf \"\$CACHE_DIR\" \"\$REMOTE_DIR\" \"\$SEED_DIR\"' EXIT

    git init -q \"\$CACHE_DIR\"
    git -C \"\$CACHE_DIR\" config user.name 'Tester'
    git -C \"\$CACHE_DIR\" config user.email 'tester@example.com'
    mkdir -p \"\$CACHE_DIR/scripts\"
    cat << 'EOF' > \"\$CACHE_DIR/scripts/verify.sh\"
#!/bin/sh
echo \"ran_verify_script\" >> .verify_runs
printf '%s\n' \"\$(git write-tree)\" > \"\$(git rev-parse --git-dir)/.aerial-verified-tree\"
exit 0
EOF
    chmod +x \"\$CACHE_DIR/scripts/verify.sh\"
    echo 'v1' > \"\$CACHE_DIR/app.txt\"
    git -C \"\$CACHE_DIR\" add scripts/verify.sh app.txt

    # First run executes verify.sh and stamps .aerial-verified-tree
    out1=\$(run_preflight_verification \"\$CACHE_DIR\" 2>&1)
    # Second run with identical staged tree and clean working diff skips verify.sh
    out2=\$(run_preflight_verification \"\$CACHE_DIR\" 2>&1)
    if echo \"\$out2\" | grep -q 'Skipping duplicate pre-flight verification' && [ \"\$(wc -l < \"\$CACHE_DIR/.verify_runs\" | tr -d '[:space:]')\" = \"1\" ]; then
        echo 'tree_cache_hit:ok'
    else
        echo \"tree_cache_hit:fail:\$out2\"
    fi

    # Unstaged modification invalidates cache and re-runs verify.sh
    echo 'v2-unstaged' >> \"\$CACHE_DIR/app.txt\"
    out3=\$(run_preflight_verification \"\$CACHE_DIR\" 2>&1)
    if echo \"\$out3\" | grep -q 'Running pre-flight verification' && [ \"\$(wc -l < \"\$CACHE_DIR/.verify_runs\" | tr -d '[:space:]')\" = \"2\" ]; then
        echo 'tree_cache_invalidated:ok'
    else
        echo \"tree_cache_invalidated:fail:\$out3\"
    fi

    # Test init_scratch --branch against local bare repo (including origin/main refspec + submit_scratch round-trip)
    git init -q --bare \"\$REMOTE_DIR\"
    git clone -q \"\$REMOTE_DIR\" \"\$SEED_DIR\"
    git -C \"\$SEED_DIR\" config user.name 'Tester'
    git -C \"\$SEED_DIR\" config user.email 'tester@example.com'
    git -C \"\$SEED_DIR\" checkout -q -b main
    echo 'base-content' > \"\$SEED_DIR/base.txt\"
    git -C \"\$SEED_DIR\" add base.txt && git -C \"\$SEED_DIR\" commit -q -m 'base commit'
    git -C \"\$SEED_DIR\" push -q -u origin main
    git -C \"\$SEED_DIR\" checkout -q -b feat/existing-ci-branch
    echo 'branch-content' > \"\$SEED_DIR/file.txt\"
    git -C \"\$SEED_DIR\" add file.txt && git -C \"\$SEED_DIR\" commit -q -m 'branch commit'
    git -C \"\$SEED_DIR\" push -q -u origin feat/existing-ci-branch

    export GITHUB_PAT='mock_pat'
    export AERIAL_REPO_URL_OVERRIDE=\"\$REMOTE_DIR\"
    export AERIAL_PR_REGISTER_URL='http://mock-router/internal/pr/register'
    init_json=\$(init_scratch aerial --branch feat/existing-ci-branch)
    cloned_dir=\$(echo \"\$init_json\" | jq -r '.scratch_dir')
    cloned_branch=\$(echo \"\$init_json\" | jq -r '.branch')
    actual_head=\$(git -C \"\$cloned_dir\" rev-parse --abbrev-ref HEAD)
    origin_main_sha=\$(git -C \"\$cloned_dir\" rev-parse --verify -q refs/remotes/origin/main || echo '')
    if [ \"\$cloned_branch\" = \"feat/existing-ci-branch\" ] && [ \"\$actual_head\" = \"feat/existing-ci-branch\" ] && [ -n \"\$origin_main_sha\" ]; then
        echo 'init_branch:ok'
    else
        echo \"init_branch:fail:\$cloned_branch:\$actual_head:\$origin_main_sha\"
    fi

    # Advance origin/main and submit a remediation fix directly from the init_scratch --branch checkout
    echo 'main-v2' > \"\$SEED_DIR/main_v2.txt\"
    git -C \"\$SEED_DIR\" checkout -q main
    git -C \"\$SEED_DIR\" add main_v2.txt && git -C \"\$SEED_DIR\" commit -q -m 'main advance'
    git -C \"\$SEED_DIR\" push -q origin main

    echo 'branch-content-fixed' > \"\$cloned_dir/file.txt\"
    remed_submit_out=\$(
        curl() {
            if [[ \"\$*\" == *'/pulls?state=open'* ]]; then
                printf '[{\"number\":88,\"title\":\"feat: existing branch\",\"body\":\"Existing PR body\",\"html_url\":\"https://github.com/azylman/aerial/pull/88\",\"node_id\":\"PR_kwDO88\"}]'
            elif [[ \"\$*\" == *'graphql'* ]]; then
                printf '{\"data\":{\"enablePullRequestAutoMerge\":{\"pullRequest\":{\"autoMergeRequest\":{\"enabledAt\":\"now\"}}}}}'
            elif [[ \"\$*\" == *'/internal/pr/register'* ]]; then
                printf '{\"status\":\"registered\"}'
            fi
        }
        export -f curl
        submit_scratch \"\$cloned_dir\" 'fix: address roborev finding' 2>/dev/null
    )
    if echo \"\$remed_submit_out\" | jq -e '.status == \"submitted\" and .pr_number == 88' >/dev/null 2>&1 && [ ! -d \"\$cloned_dir\" ]; then
        echo 'init_branch_submit:ok'
    else
        echo \"init_branch_submit:fail:\$remed_submit_out\"
    fi
    rm -rf \"\$cloned_dir\"
")
assert_eq "true" "$(echo "$TREE_CACHE_AND_BRANCH_CHECK" | grep -q 'tree_cache_hit:ok' && echo true || echo false)" "run_preflight_verification skips duplicate verify.sh when .aerial-verified-tree matches"
assert_eq "true" "$(echo "$TREE_CACHE_AND_BRANCH_CHECK" | grep -q 'tree_cache_invalidated:ok' && echo true || echo false)" "run_preflight_verification re-runs verify.sh when unstaged changes exist"
assert_eq "true" "$(echo "$TREE_CACHE_AND_BRANCH_CHECK" | grep -q 'init_branch:ok' && echo true || echo false)" "init_scratch --branch clones, tracks origin/main, and stays on existing PR branch"
assert_eq "true" "$(echo "$TREE_CACHE_AND_BRANCH_CHECK" | grep -q 'init_branch_submit:ok' && echo true || echo false)" "submit_scratch rebases and updates PR cleanly from init_scratch --branch checkout"

echo "--------------------------------------------------------"
echo "Test results: $PASSED passed, $FAILED failed"
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
echo "🎉 All tests passed!"
