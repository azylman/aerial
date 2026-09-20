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

echo "--------------------------------------------------------"
echo "Test results: $PASSED passed, $FAILED failed"
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
echo "🎉 All tests passed!"
