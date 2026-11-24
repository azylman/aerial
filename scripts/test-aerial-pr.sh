#!/usr/bin/env bash
set -euo pipefail

# scripts/test-aerial-pr.sh - Focused unit tests for slimmed aerial-pr.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AERIAL_PR="${SCRIPT_DIR}/aerial-pr.sh"

unset AERIAL_VANADIS_CHUNK_ID VANADIS_CHUNK_ID AERIAL_TARGET_ID DISCORD_THREAD_ID DISCORD_CHANNEL_ID

PASSED=0
FAILED=0

assert_eq() {
    local expected="$1" actual="$2" msg="$3"
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

assert_match() {
    local pattern="$1" actual="$2" msg="$3"
    if [[ "$actual" =~ $pattern ]]; then
        echo "✅ PASS: $msg"
        PASSED=$((PASSED + 1))
    else
        echo "❌ FAIL: $msg"
        echo "   Pattern: '$pattern'"
        echo "   Actual:  '$actual'"
        FAILED=$((FAILED + 1))
    fi
}

echo "🧪 Running unit tests for slim aerial-pr.sh..."

# 1. normalize_repo
TEST_NORM=$(bash -c "
    REPO_TEST_MODE=1; source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
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
assert_eq "aerial" "$(echo "$TEST_NORM" | grep '^core:' | cut -d: -f2)" "normalize_repo core -> aerial"
assert_eq "aerial" "$(echo "$TEST_NORM" | grep '^aerial:' | cut -d: -f2)" "normalize_repo aerial -> aerial"
assert_eq "aerial-config" "$(echo "$TEST_NORM" | grep '^config:' | cut -d: -f2)" "normalize_repo config -> aerial-config"
assert_eq "aerial-config" "$(echo "$TEST_NORM" | grep '^aerial-config:' | cut -d: -f2)" "normalize_repo aerial-config -> aerial-config"
assert_eq "aerial-sidecars" "$(echo "$TEST_NORM" | grep '^sidecars:' | cut -d: -f2)" "normalize_repo sidecars -> aerial-sidecars"
assert_eq "aerial-sidecars" "$(echo "$TEST_NORM" | grep '^aerial-sidecars:' | cut -d: -f2)" "normalize_repo aerial-sidecars -> aerial-sidecars"
assert_eq "aerial" "$(echo "$TEST_NORM" | grep '^azylman/aerial:' | cut -d: -f2)" "normalize_repo azylman/aerial -> aerial"
assert_eq "aerial-config" "$(echo "$TEST_NORM" | grep '^azylman/aerial-config.git:' | cut -d: -f2)" "normalize_repo azylman/aerial-config.git -> aerial-config"
assert_eq "aerial" "$(echo "$TEST_NORM" | grep '^aerial.git:' | cut -d: -f2)" "normalize_repo aerial.git -> aerial"
assert_eq "custom-repo" "$(echo "$TEST_NORM" | grep '^custom-repo:' | cut -d: -f2)" "normalize_repo custom-repo -> custom-repo"

# 2. extract_remote_repo
TEST_REMOTE=$(bash -c "
    REPO_TEST_MODE=1; source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    extract_remote_repo \"https://github.com/azylman/aerial-sidecars.git\"; echo \"r1:\${PARSED_OWNER}/\${PARSED_REPO}\"
    extract_remote_repo \"git@github.com:azylman/aerial-config.git\"; echo \"r2:\${PARSED_OWNER}/\${PARSED_REPO}\"
    extract_remote_repo \"http://127.0.0.1:4028/internal/github-git/azylman/aerial.git\"; echo \"r3:\${PARSED_OWNER}/\${PARSED_REPO}\"
    extract_remote_repo \"https://github.com/myorg/myrepo\"; echo \"r4:\${PARSED_OWNER}/\${PARSED_REPO}\"
")
assert_eq "azylman/aerial-sidecars" "$(echo "$TEST_REMOTE" | grep '^r1:' | cut -d: -f2)" "extract_remote_repo https"
assert_eq "azylman/aerial-config" "$(echo "$TEST_REMOTE" | grep '^r2:' | cut -d: -f2)" "extract_remote_repo git@"
assert_eq "azylman/aerial" "$(echo "$TEST_REMOTE" | grep '^r3:' | cut -d: -f2)" "extract_remote_repo loopback git"
assert_eq "myorg/myrepo" "$(echo "$TEST_REMOTE" | grep '^r4:' | cut -d: -f2)" "extract_remote_repo custom"

# 3. init_scratch branch naming & directory initialization
TMP_REMOTE=$(mktemp -d /tmp/aerial-pr-remote.XXXXXX)
git init --bare "$TMP_REMOTE" >/dev/null
git -C "$TMP_REMOTE" symbolic-ref HEAD refs/heads/main
TMP_WORK=$(mktemp -d /tmp/aerial-pr-work.XXXXXX)
git clone "$TMP_REMOTE" "$TMP_WORK" >/dev/null 2>&1
git -C "$TMP_WORK" config user.name "Tester"
git -C "$TMP_WORK" config user.email "tester@example.com"
git -C "$TMP_WORK" checkout -b main >/dev/null 2>&1
echo "init" > "$TMP_WORK/README.md"
git -C "$TMP_WORK" add README.md
git -C "$TMP_WORK" commit -m "init" >/dev/null
git -C "$TMP_WORK" push -u origin main >/dev/null 2>&1
rm -rf "$TMP_WORK"

SCRATCH_BASE=$(mktemp -d /tmp/aerial-pr-scratch.XXXXXX)
trap 'rm -rf "$TMP_REMOTE" "$SCRATCH_BASE"' EXIT

# Standalone without target_id
OUT_INIT_1=$(AERIAL_SCRATCH_BASE_DIR="$SCRATCH_BASE" AERIAL_REPO_URL_OVERRIDE="$TMP_REMOTE" bash "${AERIAL_PR}" init aerial)
BRANCH_1=$(echo "$OUT_INIT_1" | jq -r '.branch')
assert_match '^feat/aerial-pr-[0-9]{14}-[a-f0-9]{6}$' "$BRANCH_1" "init standalone branch without target_id"

# Standalone with target_id (18-digit snowflake)
OUT_INIT_2=$(AERIAL_SCRATCH_BASE_DIR="$SCRATCH_BASE" AERIAL_REPO_URL_OVERRIDE="$TMP_REMOTE" DISCORD_THREAD_ID="123456789012345678" bash "${AERIAL_PR}" init aerial)
BRANCH_2=$(echo "$OUT_INIT_2" | jq -r '.branch')
assert_match '^feat/aerial-pr-t123456789012345678-[a-f0-9]{6}$' "$BRANCH_2" "init standalone branch with target_id"

# Vanadis chunk without target_id
OUT_INIT_3=$(AERIAL_SCRATCH_BASE_DIR="$SCRATCH_BASE" AERIAL_REPO_URL_OVERRIDE="$TMP_REMOTE" bash "${AERIAL_PR}" init aerial --vanadis-chunk chunk.alpha)
BRANCH_3=$(echo "$OUT_INIT_3" | jq -r '.branch')
DIR_3=$(echo "$OUT_INIT_3" | jq -r '.scratch_dir')
assert_match '^feat/aerial-pr-vanadis-chunk_alpha-[a-f0-9]{6}$' "$BRANCH_3" "init vanadis chunk branch without target_id"
assert_match '/vanadis-chunk_alpha-aerial$' "$DIR_3" "init vanadis chunk creates deterministic dir"

# Vanadis chunk with target_id
OUT_INIT_4=$(AERIAL_SCRATCH_BASE_DIR="$SCRATCH_BASE" AERIAL_REPO_URL_OVERRIDE="$TMP_REMOTE" AERIAL_TARGET_ID="987654321098765432" bash "${AERIAL_PR}" init aerial --vanadis-chunk chunk.beta)
BRANCH_4=$(echo "$OUT_INIT_4" | jq -r '.branch')
assert_match '^feat/aerial-pr-vanadis-chunk_beta-t987654321098765432-[a-f0-9]{6}$' "$BRANCH_4" "init vanadis chunk branch with target_id"

# Non-existent explicit AERIAL_SCRATCH_BASE_DIR is created and honored
NONEXISTENT_BASE="${SCRATCH_BASE}/custom-scratch-dir"
OUT_INIT_5=$(AERIAL_SCRATCH_BASE_DIR="$NONEXISTENT_BASE" AERIAL_REPO_URL_OVERRIDE="$TMP_REMOTE" bash "${AERIAL_PR}" init aerial)
DIR_5=$(echo "$OUT_INIT_5" | jq -r '.scratch_dir')
assert_eq "true" "$([ -d "$NONEXISTENT_BASE" ] && echo true || echo false)" "init creates explicit nonexistent AERIAL_SCRATCH_BASE_DIR"
assert_match "^${NONEXISTENT_BASE}/" "$DIR_5" "init places scratch dir under explicit AERIAL_SCRATCH_BASE_DIR"

# 4. submit_scratch: preflight verification, PR_DESCRIPTION.md cleanup, PR creation, branch reuse
# Case 4A: Preflight verification failure stops submit
SUBMIT_DIR_A=$(mktemp -d /tmp/aerial-pr-submit-a.XXXXXX)
git clone "$TMP_REMOTE" "$SUBMIT_DIR_A" >/dev/null 2>&1
git -C "$SUBMIT_DIR_A" checkout -b feat/test-fail >/dev/null 2>&1
mkdir -p "$SUBMIT_DIR_A/scripts"
cat << 'EOF' > "$SUBMIT_DIR_A/scripts/verify.sh"
#!/bin/sh
echo "Simulated verify failure" >&2
exit 1
EOF
chmod +x "$SUBMIT_DIR_A/scripts/verify.sh"
echo "data" > "$SUBMIT_DIR_A/file.txt"
echo "description" > "$SUBMIT_DIR_A/PR_DESCRIPTION.md"
FAIL_OUT=$(bash "${AERIAL_PR}" submit "$SUBMIT_DIR_A" "test commit" 2>&1 || true)
assert_match "Pre-flight verification failed" "$FAIL_OUT" "submit stops on verify.sh failure"
assert_eq "true" "$([ -d "$SUBMIT_DIR_A" ] && echo true || echo false)" "submit preserves scratch dir on failure"
assert_eq "true" "$([ -f "$SUBMIT_DIR_A/PR_DESCRIPTION.md" ] && echo true || echo false)" "submit restores PR_DESCRIPTION.md on failure"
rm -rf "$SUBMIT_DIR_A"

# Case 4B: PR_DESCRIPTION.md is consumed for body, deleted before commit, and PR is created
SUBMIT_DIR_B=$(mktemp -d /tmp/aerial-pr-submit-b.XXXXXX)
git clone "$TMP_REMOTE" "$SUBMIT_DIR_B" >/dev/null 2>&1
git -C "$SUBMIT_DIR_B" checkout -b feat/test-create >/dev/null 2>&1
echo "hello world" > "$SUBMIT_DIR_B/data.txt"
echo "Test PR Body Content" > "$SUBMIT_DIR_B/PR_DESCRIPTION.md"

CURL_PAYLOAD_FILE=$(mktemp /tmp/aerial-pr-curl-payload.XXXXXX)
SUBMIT_OUT=$(bash -c "
    export GITHUB_PAT=mock_pat
    curl() {
        local url='' data=''
        while [ \$# -gt 0 ]; do
            case \"\$1\" in
                -d) data=\"\$2\"; shift 2 ;;
                http://*|https://*) url=\"\$1\"; shift ;;
                *) shift ;;
            esac
        done
        if [[ \"\$url\" =~ /pulls$ ]]; then
            printf \"%s\" \"\$data\" > \"${CURL_PAYLOAD_FILE}\"
            echo '{\"number\":123,\"html_url\":\"https://github.com/azylman/aerial/pull/123\"}'
            echo '201'
        fi
    }
    export -f curl
    bash \"${AERIAL_PR}\" submit \"${SUBMIT_DIR_B}\" 'feat: my feature'
")

assert_eq "submitted" "$(echo "$SUBMIT_OUT" | jq -r '.status')" "submit succeeds and returns submitted status"
assert_eq "end_turn" "$(echo "$SUBMIT_OUT" | jq -r '.turn_action')" "submit sets turn_action: end_turn"
assert_eq "123" "$(echo "$SUBMIT_OUT" | jq -r '.pr_number')" "submit returns created PR number"
assert_eq "false" "$([ -d "$SUBMIT_DIR_B" ] && echo true || echo false)" "submit cleans up scratch dir on success"
PAYLOAD_BODY=$(jq -r '.body' "$CURL_PAYLOAD_FILE")
assert_eq "Test PR Body Content" "$PAYLOAD_BODY" "submit passes PR_DESCRIPTION.md as PR body"
rm -f "$CURL_PAYLOAD_FILE"

# Case 4C: Branch reuse on HTTP 422
SUBMIT_DIR_C=$(mktemp -d /tmp/aerial-pr-submit-c.XXXXXX)
git clone "$TMP_REMOTE" "$SUBMIT_DIR_C" >/dev/null 2>&1
git -C "$SUBMIT_DIR_C" checkout -b feat/test-reuse >/dev/null 2>&1
echo "more changes" > "$SUBMIT_DIR_C/data.txt"

REUSE_OUT=$(bash -c "
    export GITHUB_PAT=mock_pat
    curl() {
        local url=''
        while [ \$# -gt 0 ]; do
            case \"\$1\" in
                http://*|https://*) url=\"\$1\"; shift ;;
                *) shift ;;
            esac
        done
        if [[ \"\$url\" =~ /pulls\? ]]; then
            echo '[{\"number\":456,\"html_url\":\"https://github.com/azylman/aerial/pull/456\",\"title\":\"Existing PR\"}]'
        elif [[ \"\$url\" =~ /pulls$ ]]; then
            echo '{\"message\":\"A pull request already exists for this branch.\"}'
            echo '422'
        fi
    }
    export -f curl
    bash \"${AERIAL_PR}\" submit -b 'Updated description' \"${SUBMIT_DIR_C}\" 'feat: updated commit'
")
assert_eq "submitted" "$(echo "$REUSE_OUT" | jq -r '.status')" "submit handles 422 by reusing existing PR"
assert_eq "456" "$(echo "$REUSE_OUT" | jq -r '.pr_number')" "submit returns existing PR number on reuse"
rm -rf "$SUBMIT_DIR_C"

# 5. override_roborev
# Case 5A: Closed PR rejection
CLOSED_OUT=$(bash -c "
    export GITHUB_PAT=mock_pat
    curl() {
        echo '{\"state\":\"closed\",\"head\":{\"sha\":\"abc1234\"},\"html_url\":\"https://github.com/azylman/aerial/pull/42\"}'
        echo '200'
    }
    export -f curl
    bash \"${AERIAL_PR}\" override-roborev 42 'valid rebuttal' 2>&1 || true
")
assert_match "Pull Request #42 is closed" "$CLOSED_OUT" "override-roborev rejects closed PR"

# Case 5B: In-progress workflow run: immediate return without sleeping or calling rerun
IN_PROGRESS_CALLED=$(mktemp /tmp/aerial-pr-inprog.XXXXXX)
IN_PROG_OUT=$(bash -c "
    export GITHUB_PAT=mock_pat
    curl() {
        local url='' method='GET'
        while [ \$# -gt 0 ]; do
            case \"\$1\" in
                -X) method=\"\$2\"; shift 2 ;;
                http://*|https://*) url=\"\$1\"; shift ;;
                *) shift ;;
            esac
        done
        if [[ \"\$url\" =~ /pulls/42$ ]]; then
            echo '{\"state\":\"open\",\"head\":{\"sha\":\"1234567890abcdef1234567890abcdef12345678\"},\"html_url\":\"https://github.com/azylman/aerial/pull/42\"}'
            echo '200'
        elif [[ \"\$url\" =~ /issues/42/comments ]]; then
            echo '{\"id\":101}'
            echo '201'
        elif [[ \"\$url\" =~ /actions/runs\? ]]; then
            echo '{\"workflow_runs\":[{\"id\":9988,\"name\":\"RoboRev Code Review\",\"status\":\"in_progress\",\"path\":\".github/workflows/roborev.yml\"}]}'
            echo '200'
        elif [[ \"\$url\" =~ /actions/runs/.*rerun ]]; then
            echo 'rerun-called' > \"${IN_PROGRESS_CALLED}\"
            echo '201'
        fi
    }
    export -f curl
    bash \"${AERIAL_PR}\" override-roborev 42 'valid technical rebuttal'
")
assert_eq "overridden" "$(echo "$IN_PROG_OUT" | jq -r '.status')" "in-progress run returns overridden immediately"
assert_eq "end_turn" "$(echo "$IN_PROG_OUT" | jq -r '.turn_action')" "in-progress run returns turn_action: end_turn"
assert_eq "9988" "$(echo "$IN_PROG_OUT" | jq -r '.run_id')" "in-progress run returns run_id"
assert_eq "false" "$([ -s "$IN_PROGRESS_CALLED" ] && echo true || echo false)" "in-progress run does not call rerun endpoint"
rm -f "$IN_PROGRESS_CALLED"

# Case 5C: Completed workflow run triggers rerun-failed-jobs
COMPLETED_CALLED=$(mktemp /tmp/aerial-pr-completed.XXXXXX)
COMMENT_BODY_FILE=$(mktemp /tmp/aerial-pr-comment.XXXXXX)
COMP_OUT=$(bash -c "
    export GITHUB_PAT=mock_pat
    curl() {
        local url='' method='GET' data=''
        while [ \$# -gt 0 ]; do
            case \"\$1\" in
                -X) method=\"\$2\"; shift 2 ;;
                -d) data=\"\$2\"; shift 2 ;;
                http://*|https://*) url=\"\$1\"; shift ;;
                *) shift ;;
            esac
        done
        if [[ \"\$url\" =~ /pulls/42$ ]]; then
            echo '{\"state\":\"open\",\"head\":{\"sha\":\"fedcba9876543210fedcba9876543210fedcba98\"},\"html_url\":\"https://github.com/azylman/aerial/pull/42\"}'
            echo '200'
        elif [[ \"\$url\" =~ /issues/42/comments$ ]] && [ \"\$method\" = 'POST' ]; then
            printf \"%s\" \"\$data\" > \"${COMMENT_BODY_FILE}\"
            echo '{\"id\":202}'
            echo '201'
        elif [[ \"\$url\" =~ /issues/42/comments ]]; then
            echo '[]'
            echo '200'
        elif [[ \"\$url\" =~ /actions/runs\? ]]; then
            echo '{\"workflow_runs\":[{\"id\":7766,\"name\":\"RoboRev Code Review\",\"status\":\"completed\",\"path\":\".github/workflows/roborev.yml\"}]}'
            echo '200'
        elif [[ \"\$url\" =~ /actions/runs/7766/rerun-failed-jobs ]]; then
            echo 'rerun-failed-jobs-called' > \"${COMPLETED_CALLED}\"
            echo '{\"status\":\"queued\"}'
            echo '201'
        fi
    }
    export -f curl
    bash \"${AERIAL_PR}\" override-roborev 42 'Technical rebuttal explanation'
")
assert_eq "overridden" "$(echo "$COMP_OUT" | jq -r '.status')" "completed run returns overridden"
assert_eq "7766" "$(echo "$COMP_OUT" | jq -r '.run_id')" "completed run returns run_id"
assert_eq "rerun-failed-jobs-called" "$(cat "$COMPLETED_CALLED" 2>/dev/null || echo "")" "completed run calls rerun-failed-jobs"
assert_match "## roborev-override \(\`fedcba9\`\)" "$(jq -r '.body' "$COMMENT_BODY_FILE" 2>/dev/null || echo "")" "override comment includes formatted markdown header"
rm -f "$COMPLETED_CALLED" "$COMMENT_BODY_FILE"

# 6. rebase_staged_changes & push_scratch_branch: ancestor-preserving stacked commits for stateful RoboRev
ANCESTRY_CHECK=$(bash -c "
    REPO_TEST_MODE=1; source \"${AERIAL_PR}\" --source-only 2>/dev/null || true
    REMOTE_DIR=\$(mktemp -d /tmp/aerial-pr-anc-remote.XXXXXX)
    WORK_DIR=\$(mktemp -d /tmp/aerial-pr-anc-work.XXXXXX)
    UPSTREAM_DIR=\$(mktemp -d /tmp/aerial-pr-anc-up.XXXXXX)
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
    git -C \"\$WORK_DIR\" checkout -q -b feat/stateful-pr
    echo 'v1' > \"\$WORK_DIR/feat.txt\"
    git -C \"\$WORK_DIR\" add . && git -C \"\$WORK_DIR\" commit -q -m 'feat v1'
    git -C \"\$WORK_DIR\" push -q -u origin feat/stateful-pr
    V1_SHA=\$(git -C \"\$WORK_DIR\" rev-parse HEAD)

    # Round 2 WITHOUT upstream main advancing: V1_SHA must remain ancestor of V2_SHA
    cd \"\$WORK_DIR\"
    echo 'v2' > feat.txt
    git add feat.txt
    rebase_staged_changes main 'fix: round 2' 2>/dev/null
    git commit -q -m 'fix: round 2'
    V2_SHA=\$(git rev-parse HEAD)
    if push_scratch_branch feat/stateful-pr 2>/dev/null && git merge-base --is-ancestor \"\$V1_SHA\" \"\$V2_SHA\" && [ \"\$V1_SHA\" != \"\$V2_SHA\" ]; then
        echo 'stack_same_main:ok'
    else
        echo 'stack_same_main:fail'
    fi

    # Round 3 WITH upstream main advancing: both V2_SHA and UP_SHA must be ancestors of V3_SHA
    echo 'upstream-v2' > \"\$UPSTREAM_DIR/up.txt\"
    git -C \"\$UPSTREAM_DIR\" add . && git -C \"\$UPSTREAM_DIR\" commit -q -m 'upstream v2'
    git -C \"\$UPSTREAM_DIR\" push -q origin main
    UP_SHA=\$(git -C \"\$UPSTREAM_DIR\" rev-parse HEAD)

    echo 'v3' > feat.txt
    git add feat.txt
    rebase_staged_changes main 'fix: round 3' 2>/dev/null
    git commit -q -m 'fix: round 3'
    V3_SHA=\$(git rev-parse HEAD)
    if push_scratch_branch feat/stateful-pr 2>/dev/null && git merge-base --is-ancestor \"\$V2_SHA\" \"\$V3_SHA\" && git merge-base --is-ancestor \"\$UP_SHA\" \"\$V3_SHA\"; then
        echo 'stack_advanced_main:ok'
    else
        echo 'stack_advanced_main:fail'
    fi

    # Round 4: Remote branch advances independently while local checkout has diverged changes
    SINGLE_DIR=\$(mktemp -d /tmp/aerial-pr-anc-single.XXXXXX)
    git clone -q -b feat/stateful-pr \"\$REMOTE_DIR\" \"\$SINGLE_DIR\"
    git -C \"\$SINGLE_DIR\" config user.name 'Tester'
    git -C \"\$SINGLE_DIR\" config user.email 'tester@example.com'

    echo 'remote-c4' > \"\$WORK_DIR/remote4.txt\"
    git -C \"\$WORK_DIR\" add . && git -C \"\$WORK_DIR\" commit -q -m 'remote c4'
    git -C \"\$WORK_DIR\" push -q origin feat/stateful-pr
    V4_REMOTE_SHA=\$(git -C \"\$WORK_DIR\" rev-parse HEAD)

    cd \"\$SINGLE_DIR\"
    echo 'local-c5' > local5.txt
    git add local5.txt && git commit -q -m 'local c5'
    rebase_staged_changes main 'fix: round 5' 2>/dev/null
    git commit -q -m 'fix: round 5'
    V5_SHA=\$(git rev-parse HEAD)
    if push_scratch_branch feat/stateful-pr 2>/dev/null && git merge-base --is-ancestor \"\$V4_REMOTE_SHA\" \"\$V5_SHA\" && [ -f remote4.txt ]; then
        echo 'stack_diverged_remote:ok'
    else
        echo 'stack_diverged_remote:fail'
    fi

    # Un-incorporated remote advance is rejected by push_scratch_branch
    echo 'remote-c6' > \"\$WORK_DIR/remote6.txt\"
    git -C \"\$WORK_DIR\" pull -q origin feat/stateful-pr
    git -C \"\$WORK_DIR\" add . && git -C \"\$WORK_DIR\" commit -q -m 'remote c6'
    git -C \"\$WORK_DIR\" push -q origin feat/stateful-pr

    echo 'local-c7' > local7.txt
    git add local7.txt && git commit -q -m 'local c7 diverged'
    if ! push_scratch_branch feat/stateful-pr 2>/dev/null; then
        echo 'reject_unincorporated:ok'
    else
        echo 'reject_unincorporated:fail'
    fi
    rm -rf \"\$SINGLE_DIR\"
")
assert_eq "true" "$(echo "$ANCESTRY_CHECK" | grep -q 'stack_same_main:ok' && echo true || echo false)" "rebase_staged_changes stacks commit on existing remote branch when main has not advanced"
assert_eq "true" "$(echo "$ANCESTRY_CHECK" | grep -q 'stack_advanced_main:ok' && echo true || echo false)" "rebase_staged_changes merges advanced main while keeping remote branch SHA as ancestor"
assert_eq "true" "$(echo "$ANCESTRY_CHECK" | grep -q 'stack_diverged_remote:ok' && echo true || echo false)" "rebase_staged_changes preserves intermediate remote commits when remote branch advances"
assert_eq "true" "$(echo "$ANCESTRY_CHECK" | grep -q 'reject_unincorporated:ok' && echo true || echo false)" "push_scratch_branch rejects pushing over un-incorporated remote advances"

# 7. submit_scratch remediation commit on existing open PR preserves ancestry and clean commit message
REMED_SUBMIT_CHECK=$(bash -c "
    REMOTE_DIR=\$(mktemp -d /tmp/aerial-pr-rem-remote.XXXXXX)
    WORK_DIR=\$(mktemp -d /tmp/aerial-pr-rem-work.XXXXXX)
    trap 'rm -rf \"\$REMOTE_DIR\" \"\$WORK_DIR\"' EXIT

    git init -q --bare \"\$REMOTE_DIR\"
    git clone -q \"\$REMOTE_DIR\" \"\$WORK_DIR\"
    git -C \"\$WORK_DIR\" config user.name 'Tester'
    git -C \"\$WORK_DIR\" config user.email 'tester@example.com'
    git -C \"\$WORK_DIR\" checkout -q -b main
    echo 'base' > \"\$WORK_DIR/file.txt\"
    git -C \"\$WORK_DIR\" add . && git -C \"\$WORK_DIR\" commit -q -m 'init'
    git -C \"\$WORK_DIR\" push -q -u origin main

    git -C \"\$WORK_DIR\" checkout -q -b feat/remed-branch
    echo 'round 1' > \"\$WORK_DIR/file.txt\"
    git -C \"\$WORK_DIR\" add . && git -C \"\$WORK_DIR\" commit -q -m 'feat: round 1'
    git -C \"\$WORK_DIR\" push -q -u origin feat/remed-branch
    R1_SHA=\$(git -C \"\$WORK_DIR\" rev-parse HEAD)

    echo 'round 2 fix' > \"\$WORK_DIR/file.txt\"
    export GITHUB_PAT=mock_pat
    curl() {
        local url=''
        while [ \$# -gt 0 ]; do
            case \"\$1\" in
                http://*|https://*) url=\"\$1\"; shift ;;
                *) shift ;;
            esac
        done
        if [[ \"\$url\" =~ /pulls\? ]]; then
            echo '[{\"number\":789,\"html_url\":\"https://github.com/azylman/aerial/pull/789\",\"title\":\"feat: round 1\",\"body\":\"Original Round 1 PR Description\"}]'
        fi
    }
    export -f curl
    bash \"${AERIAL_PR}\" submit \"\$WORK_DIR\" 'fix: round 2 remediation' >/dev/null 2>&1
    R2_SHA=\$(git -C \"\$REMOTE_DIR\" rev-parse refs/heads/feat/remed-branch)
    R2_MSG=\$(git -C \"\$REMOTE_DIR\" log -1 --format=%B \"\$R2_SHA\")
    if git -C \"\$REMOTE_DIR\" merge-base --is-ancestor \"\$R1_SHA\" \"\$R2_SHA\" && [ \"\$R1_SHA\" != \"\$R2_SHA\" ]; then
        echo 'remed_ancestor:ok'
    fi
    if [ \"\$R2_MSG\" = 'fix: round 2 remediation' ]; then
        echo 'remed_clean_msg:ok'
    else
        echo \"remed_clean_msg:fail:\$R2_MSG\"
    fi
")
assert_eq "true" "$(echo "$REMED_SUBMIT_CHECK" | grep -q 'remed_ancestor:ok' && echo true || echo false)" "submit_scratch stacks remediation commit onto existing remote PR branch"
assert_eq "true" "$(echo "$REMED_SUBMIT_CHECK" | grep -q 'remed_clean_msg:ok' && echo true || echo false)" "submit_scratch does not pollute remediation commit message with original PR body"

echo "--------------------------------------------------------"
echo "Test results: $PASSED passed, $FAILED failed"
if [ "$FAILED" -gt 0 ]; then
    echo "❌ Some tests failed!"
    exit 1
fi
echo "🎉 All tests passed cleanly!"
