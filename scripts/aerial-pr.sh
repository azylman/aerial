#!/usr/bin/env bash
set -euo pipefail

# scripts/aerial-pr.sh - Unified Monorepo PR Automation & Continuous Integration Verifier
# Safely clones repositories into ephemeral scratch space, enforces the universal
# verification contract (scripts/verify.sh --staged), creates Pull Requests with auto-merge,
# schedules follow-up checks via scheduler-mcp, and provides single-shot merge verification.

REPO_OWNER="azylman"
DEFAULT_REPO="aerial"
REPO_NAME="${AERIAL_REPO_NAME:-$DEFAULT_REPO}"
REPO_NAME_EXPLICIT=0
DEFAULT_BRANCH="main"
SIDE_SYNC_URL="${AERIAL_HANGAR_URL:-${AERIAL_GITSYNC_URL:-http://aerial-hangar:8080/sync}}"
DEFAULT_PR_CHECK_DELAY="2m"

normalize_repo() {
    local r="${1:-}"
    # Strip any leading owner/ prefix (e.g. azylman/aerial-config -> aerial-config)
    r="${r##*/}"
    # Strip .git suffix if present
    r="${r%.git}"
    case "$r" in
        core|aerial)
            echo "aerial"
            ;;
        config|aerial-config)
            echo "aerial-config"
            ;;
        sidecars|aerial-sidecars)
            echo "aerial-sidecars"
            ;;
        *)
            echo "$r"
            ;;
    esac
}

extract_remote_repo() {
    local remote_url="${1:-}"
    PARSED_OWNER=""
    PARSED_REPO=""
    local clean_url="${remote_url%/}"
    clean_url="${clean_url%.git}"
    if [[ "$clean_url" =~ github\.com[:/]([^/]+)/([^/]+)$ ]]; then
        PARSED_OWNER="${BASH_REMATCH[1]}"
        PARSED_REPO="${BASH_REMATCH[2]}"
    fi
}

run_preflight_verification() {
    local target_dir="${1:-.}"
    local max_rule_bytes=23040 # 22.5 KB (leaves 512B buffer for SyncRules frontmatter wrapper)
    for rf in "${target_dir}/GEMINI.md" "${target_dir}/AGENTS.md"; do
        if [ -f "$rf" ]; then
            local size
            size=$(wc -c < "$rf" 2>/dev/null | tr -d '[:space:]')
            if [ -n "$size" ] && [ "$size" -gt "$max_rule_bytes" ]; then
                echo "ERROR: Rule file '$(basename "$rf")' in scratch workspace ($size bytes) exceeds 22.5 KB ceiling ($max_rule_bytes bytes)." >&2
                echo "       Antigravity truncates prompt context when compiled rules exceed 23 KB." >&2
                if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
                    echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
                    SCRATCH_DIR_CLEANUP=""
                fi
                return 1
            fi
        fi
    done

    if [ -f "${target_dir}/scripts/verify.sh" ]; then
        echo "⚡ Running pre-flight verification checks in scratch checkout..." >&2
        chmod +x "${target_dir}/scripts/verify.sh" 2>/dev/null || true
        if ! (cd "${target_dir}" && ./scripts/verify.sh --staged) >&2; then
            echo "ERROR: Pre-flight verification failed in scratch workspace. Aborting submit." >&2
            if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
                echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
                SCRATCH_DIR_CLEANUP=""
            fi
            return 1
        fi
    fi
    return 0
}

# Testing hook: allow unit tests to source helper functions without executing command dispatch
if [ "${REPO_TEST_MODE:-0}" = "1" ] || [ "${1:-}" = "--source-only" ]; then
    return 0 2>/dev/null || exit 0
fi

# Pre-dispatch global flag parsing (allows --repo before or after command)
PRE_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --repo|-r)
            if [ $# -lt 2 ]; then
                echo "ERROR: $1 requires a repository name." >&2
                exit 1
            fi
            REPO_NAME="$(normalize_repo "$2")"
            REPO_NAME_EXPLICIT=1
            shift 2
            ;;
        --repo=*)
            REPO_NAME="$(normalize_repo "${1#*=}")"
            REPO_NAME_EXPLICIT=1
            shift 1
            ;;
        *)
            PRE_ARGS+=("$1")
            shift 1
            ;;
    esac
done
set -- "${PRE_ARGS[@]}"

cmd="${1:-}"
if [ -n "$cmd" ]; then
    shift
fi

build_auth_header() {
    local pat="${GITHUB_PAT:-}"
    if [ -z "$pat" ]; then
        echo "ERROR: GITHUB_PAT environment variable is required." >&2
        return 1
    fi
    printf "x-access-token:%s" "$pat" | base64 | tr -d '\r\n'
}

init_scratch() {
    # If first positional arg is given and not an option, treat as target repo
    if [ $# -gt 0 ] && [[ "$1" != -* ]]; then
        REPO_NAME="$(normalize_repo "$1")"
        REPO_NAME_EXPLICIT=1
        shift
    fi

    # Allow trailing --repo inside init
    while [ $# -gt 0 ]; do
        case "$1" in
            --repo|-r)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a repository name." >&2
                    exit 1
                fi
                REPO_NAME="$(normalize_repo "$2")"
                REPO_NAME_EXPLICIT=1
                shift 2
                ;;
            --repo=*)
                REPO_NAME="$(normalize_repo "${1#*=}")"
                REPO_NAME_EXPLICIT=1
                shift 1
                ;;
            *)
                echo "ERROR: Unexpected argument to init: $1" >&2
                exit 1
                ;;
        esac
    done

    REPO_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}.git"

    local auth_header
    auth_header=$(build_auth_header)

    local base_dir="/data/scratch"
    if [ ! -d "/data" ] || [ ! -w "/data" ]; then
        base_dir="/dev/shm"
        if [ ! -d "$base_dir" ] || [ ! -w "$base_dir" ]; then
            base_dir="${TMPDIR:-/tmp}"
        fi
    else
        mkdir -p "$base_dir"
    fi

    local scratch_prefix="aerial-code-scratch"
    local branch_prefix="feat/aerial-pr"
    case "$REPO_NAME" in
        aerial)
            scratch_prefix="aerial-code-scratch"
            branch_prefix="feat/aerial-pr"
            ;;
        aerial-config)
            scratch_prefix="aerial-config-scratch"
            branch_prefix="update/config"
            ;;
        aerial-sidecars)
            scratch_prefix="aerial-sidecars-scratch"
            branch_prefix="feat/sidecars-pr"
            ;;
        *)
            scratch_prefix="${REPO_NAME}-scratch"
            branch_prefix="feat/${REPO_NAME}-pr"
            ;;
    esac

    local scratch_dir
    scratch_dir=$(mktemp -d "${base_dir}/${scratch_prefix}.XXXXXX")
    chmod 700 "$scratch_dir"

    local rand_suffix
    rand_suffix=$(head -c 16 /dev/urandom | md5sum | head -c 6)
    local branch_name="${branch_prefix}-$(date +%Y%m%d%H%M%S)-${rand_suffix}"

    export GIT_TERMINAL_PROMPT=0
    export GIT_CONFIG_COUNT=2
    export GIT_CONFIG_KEY_0="http.extraHeader"
    export GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}"
    export GIT_CONFIG_KEY_1="http.version"
    export GIT_CONFIG_VALUE_1="HTTP/1.1"

    local clone_out=""
    if ! clone_out=$(git clone --depth 1 --single-branch -b "$DEFAULT_BRANCH" "$REPO_URL" "$scratch_dir" 2>&1); then
        echo "ERROR: Failed to clone ${REPO_URL} into scratch directory: ${clone_out}" >&2
        rm -rf "$scratch_dir"
        exit 1
    fi

    cd "$scratch_dir"
    git checkout -b "$branch_name" >/dev/null 2>&1
    git config user.name "Aerial"
    git config user.email "aerial@noreply.github.com"

    echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${branch_name}\",\"repo\":\"${REPO_NAME}\"}"
}

SCRATCH_DIR_CLEANUP=""

get_deploy_status() {
    local target="${1:-}"
    local commit_sha=""

    # 1. If target is numeric, treat as PR number and resolve merged/head SHA
    if [[ "$target" =~ ^[0-9]+$ ]]; then
        local pr_raw=""
        if [ -n "${GITHUB_PAT:-}" ]; then
            pr_raw=$(curl -s --connect-timeout 3 -m 10 -H "Authorization: token ${GITHUB_PAT}" \
                -H "Accept: application/vnd.github.v3+json" \
                "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${target}" 2>/dev/null || true)
        fi
        if [ -n "$pr_raw" ] && echo "$pr_raw" | jq -e . >/dev/null 2>&1; then
            local is_merged
            is_merged=$(echo "$pr_raw" | jq -r '.merged // false' 2>/dev/null || echo "false")
            if [ "$is_merged" = "true" ]; then
                commit_sha=$(echo "$pr_raw" | jq -r '.merge_commit_sha // empty' 2>/dev/null || true)
            else
                echo '{"deploy_state":"not_started","stage":"not_started","details":"PR is not merged yet; deployment starts upon merge"}'
                return 0
            fi
        fi
    else
        commit_sha="$target"
    fi

    local short_sha=""
    if [ -n "$commit_sha" ]; then
        short_sha="${commit_sha:0:7}"
    fi

    local dashboard_url="${AERIAL_DASHBOARD_URL:-http://aerial-dashboard:8080/api/status}"

    # Branch deployment tracking for aerial-config (git-sync driven)
    if [ "$REPO_NAME" = "aerial-config" ]; then
        local dash_resp=""
        dash_resp=$(curl -s --connect-timeout 3 -m 10 "$dashboard_url" 2>/dev/null || true)
        if [ -n "$dash_resp" ] && echo "$dash_resp" | jq -e '.git_sync.repos | objects' >/dev/null 2>&1; then
            local cfg_repo
            cfg_repo=$(echo "$dash_resp" | jq '.git_sync.repos["/share/aerial-config"] // empty' 2>/dev/null || true)
            if [ -n "$cfg_repo" ] && [ "$cfg_repo" != "null" ]; then
                local sync_status disk_commit remote_commit
                sync_status=$(echo "$cfg_repo" | jq -r '.sync_status // "unknown"' 2>/dev/null || echo "unknown")
                disk_commit=$(echo "$cfg_repo" | jq -r '.disk_commit // empty' 2>/dev/null || true)
                remote_commit=$(echo "$cfg_repo" | jq -r '.remote_commit // empty' 2>/dev/null || true)

                local matches_commit=0
                if [ -n "$short_sha" ]; then
                    if [[ "$disk_commit" == "$short_sha"* ]] || [[ "$remote_commit" == "$short_sha"* ]]; then
                        matches_commit=1
                    fi
                else
                    matches_commit=1
                fi

                if [ "$matches_commit" -eq 1 ] && [ "$sync_status" = "synced" ]; then
                    echo '{"deploy_state":"done","stage":"done","details":"Configuration synced and hot-reloaded via gitsync"}'
                    return 0
                elif [ "$sync_status" = "synced" ]; then
                    echo '{"deploy_state":"ongoing","stage":"syncing","details":"Configuration sync in progress for merged commit"}'
                    return 0
                elif [ "$sync_status" = "error" ]; then
                    echo '{"deploy_state":"failed","stage":"failed","details":"Configuration git-sync error"}'
                    return 0
                else
                    echo "{\"deploy_state\":\"ongoing\",\"stage\":\"${sync_status}\",\"details\":\"Configuration git-sync status: ${sync_status}\"}"
                    return 0
                fi
            fi
        fi
        echo '{"deploy_state":"unknown","stage":"unknown","details":"Telemetry unavailable"}'
        return 0
    fi

    # 2. Query Dashboard API (primary real-time source with strict SHA matching)
    local dash_resp=""
    dash_resp=$(curl -s --connect-timeout 3 -m 10 "$dashboard_url" 2>/dev/null || true)

    if [ -n "$dash_resp" ] && echo "$dash_resp" | jq -e '.deployments | arrays' >/dev/null 2>&1; then
        local matching_dep=""
        if [ -n "$short_sha" ]; then
            matching_dep=$(echo "$dash_resp" | jq --arg sha "$short_sha" '[.deployments[]? | select(.commit != null and ((.commit | startswith($sha)) or ($sha | startswith(.commit))))] | first // empty' 2>/dev/null || true)
        fi

        if [ -n "$matching_dep" ] && [ "$matching_dep" != "null" ]; then
            local stage progress matrix_summary
            stage=$(echo "$matching_dep" | jq -r '.stage // "unknown"' 2>/dev/null || echo "unknown")
            progress=$(echo "$matching_dep" | jq -r '.progress // 0' 2>/dev/null || echo 0)
            matrix_summary=$(echo "$matching_dep" | jq -r '[.matrix_jobs[]? | "\(.name): \(.status)"] | join(", ")' 2>/dev/null || true)

            case "$stage" in
                live)
                    echo "{\"deploy_state\":\"done\",\"stage\":\"done\",\"progress\":100,\"details\":\"Stack is live and healthy\"}"
                    return 0
                    ;;
                queued)
                    echo "{\"deploy_state\":\"ongoing\",\"stage\":\"queued\",\"progress\":${progress},\"details\":\"Continuous Delivery workflow queued\"}"
                    return 0
                    ;;
                building)
                    local detail_msg="CI Build & GHCR in progress"
                    if [ -n "$matrix_summary" ]; then
                        detail_msg="CI Build & GHCR (${matrix_summary})"
                    fi
                    echo "{\"deploy_state\":\"ongoing\",\"stage\":\"building\",\"progress\":${progress},\"details\":\"${detail_msg}\"}"
                    return 0
                    ;;
                awaiting_pull)
                    echo "{\"deploy_state\":\"ongoing\",\"stage\":\"awaiting_pull\",\"progress\":${progress},\"details\":\"Images published to GHCR; awaiting Hangar pull\"}"
                    return 0
                    ;;
                swapping)
                    echo "{\"deploy_state\":\"ongoing\",\"stage\":\"swapping\",\"progress\":${progress},\"details\":\"Containers restarting / swapping\"}"
                    return 0
                    ;;
                failed)
                    echo "{\"deploy_state\":\"failed\",\"stage\":\"failed\",\"progress\":${progress},\"details\":\"Deployment failed\"}"
                    return 0
                    ;;
                degraded)
                    echo "{\"deploy_state\":\"degraded\",\"stage\":\"degraded\",\"progress\":${progress},\"details\":\"One or more containers unhealthy\"}"
                    return 0
                    ;;
                *)
                    echo "{\"deploy_state\":\"ongoing\",\"stage\":\"${stage}\",\"progress\":${progress},\"details\":\"Deployment in stage ${stage}\"}"
                    return 0
                    ;;
            esac
        fi
    fi

    # 3. Direct GitHub Actions API check (fallback & registration grace guard)
    if [ -n "$commit_sha" ] && [ -n "${GITHUB_PAT:-}" ]; then
        local gh_runs=""
        gh_runs=$(curl -s --connect-timeout 3 -m 10 -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs?head_sha=${commit_sha}&event=push" 2>/dev/null || true)

        if [ -n "$gh_runs" ] && echo "$gh_runs" | jq -e '.workflow_runs | arrays' >/dev/null 2>&1; then
            # Filter specifically to Continuous Delivery / docker-publish workflow
            local cd_run
            cd_run=$(echo "$gh_runs" | jq '[.workflow_runs[]? | select(.name == "Continuous Delivery" or (.path? | endswith("docker-publish.yml")))] | first // empty' 2>/dev/null || true)

            if [ -z "$cd_run" ] || [ "$cd_run" = "null" ]; then
                echo '{"deploy_state":"ongoing","stage":"pending_registration","details":"Continuous Delivery workflow pending registration in GitHub Actions"}'
                return 0
            fi

            local run_status run_conclusion run_html
            run_status=$(echo "$cd_run" | jq -r '.status // empty' 2>/dev/null || true)
            run_conclusion=$(echo "$cd_run" | jq -r '.conclusion // empty' 2>/dev/null || true)
            run_html=$(echo "$cd_run" | jq -r '.html_url // empty' 2>/dev/null || true)

            if [ "$run_status" = "queued" ]; then
                echo "{\"deploy_state\":\"ongoing\",\"stage\":\"queued\",\"details\":\"Continuous Delivery workflow queued\",\"html_url\":\"${run_html}\"}"
                return 0
            elif [ "$run_status" = "in_progress" ]; then
                echo "{\"deploy_state\":\"ongoing\",\"stage\":\"building\",\"details\":\"Continuous Delivery workflow building in GitHub Actions\",\"html_url\":\"${run_html}\"}"
                return 0
            elif [ "$run_conclusion" = "failure" ]; then
                echo "{\"deploy_state\":\"failed\",\"stage\":\"failed\",\"details\":\"Continuous Delivery workflow failed in GitHub Actions\",\"html_url\":\"${run_html}\"}"
                return 0
            elif [ "$run_conclusion" = "success" ]; then
                echo "{\"deploy_state\":\"done\",\"stage\":\"done\",\"details\":\"Continuous Delivery completed successfully\",\"html_url\":\"${run_html}\"}"
                return 0
            fi
        fi
    fi

    echo '{"deploy_state":"unknown","stage":"unknown","details":"Telemetry unavailable"}'
    return 0
}

merge_pr() {
    local pr_num=""
    local branch=""
    local commit_sha=""

    while [ $# -gt 0 ]; do
        case "$1" in
            --repo|-r)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a repository name." >&2
                    exit 1
                fi
                REPO_NAME="$(normalize_repo "$2")"
                REPO_NAME_EXPLICIT=1
                shift 2
                ;;
            --repo=*)
                REPO_NAME="$(normalize_repo "${1#*=}")"
                REPO_NAME_EXPLICIT=1
                shift 1
                ;;
            *)
                if [ -z "$pr_num" ]; then
                    pr_num="$1"
                elif [ -z "$branch" ]; then
                    branch="$1"
                elif [ -z "$commit_sha" ]; then
                    commit_sha="$1"
                fi
                shift
                ;;
        esac
    done

    if [ -z "$pr_num" ] || ! [[ "$pr_num" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Valid numeric pr_num required for merge." >&2
        exit 1
    fi

    local pr_url="https://github.com/${REPO_OWNER}/${REPO_NAME}/pull/${pr_num}"

    # 1. Fetch Pull Request details to check state and resolve branch/sha
    local pr_raw
    pr_raw=$(curl -s --connect-timeout 3 -m 10 --retry 3 --retry-delay 2 --retry-connrefused -w "\n%{http_code}" -X GET \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}")

    local pr_code
    pr_code=$(echo "$pr_raw" | tail -n1)
    local pr_resp
    pr_resp=$(echo "$pr_raw" | sed '$d')

    if [ "$pr_code" -lt 200 ] || [ "$pr_code" -ge 300 ]; then
        echo "{\"status\":\"error\",\"error_code\":${pr_code},\"message\":\"Failed to query PR #${pr_num}\"}" >&2
        exit 1
    fi

    local is_merged
    is_merged=$(echo "$pr_resp" | jq -r '.merged // false')
    if [ "$is_merged" = "true" ]; then
        if [ -z "$branch" ]; then
            branch=$(echo "$pr_resp" | jq -r '.head.ref // empty')
        fi
        if [ -n "$branch" ]; then
            curl -s --connect-timeout 3 -m 10 -X DELETE \
                -H "Authorization: token ${GITHUB_PAT}" \
                -H "Accept: application/vnd.github.v3+json" \
                "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
        fi
        local sync_status="skipped"
        if curl -s --connect-timeout 3 -m 10 -f -X POST "$SIDE_SYNC_URL" >/dev/null 2>&1; then
            sync_status="synced"
        fi
        local recheck_sha
        recheck_sha=$(echo "$pr_resp" | jq -r '.merge_commit_sha // empty')
        if [ -z "$recheck_sha" ]; then
            recheck_sha="$commit_sha"
        fi
        local deploy_json
        deploy_json=$(get_deploy_status "$recheck_sha" 2>/dev/null || echo '{"deploy_state":"unknown","stage":"unknown","details":"Telemetry error"}')
        echo "{\"status\":\"already_merged\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"merged_sha\":\"${recheck_sha}\",\"sync_status\":\"${sync_status}\",\"deployment\":${deploy_json}}"
        exit 0
    fi

    local state
    state=$(echo "$pr_resp" | jq -r '.state')
    if [ "$state" = "closed" ]; then
        echo "{\"status\":\"closed\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"message\":\"Pull Request is closed without being merged\"}"
        exit 1
    fi

    if [ -z "$branch" ]; then
        branch=$(echo "$pr_resp" | jq -r '.head.ref // empty')
    fi
    if [ -z "$commit_sha" ]; then
        commit_sha=$(echo "$pr_resp" | jq -r '.head.sha // empty')
    fi

    if [ -z "$branch" ] || [ -z "$commit_sha" ]; then
        echo "ERROR: Could not resolve branch or head commit SHA for PR #${pr_num}." >&2
        exit 1
    fi

    # Check for merge conflicts
    local mergeable
    mergeable=$(echo "$pr_resp" | jq -r '.mergeable')
    local mergeable_state
    mergeable_state=$(echo "$pr_resp" | jq -r '.mergeable_state // empty')
    if [ "$mergeable" = "false" ] || [ "$mergeable_state" = "dirty" ]; then
        echo "{\"status\":\"conflict\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"error\":\"Pull request has merge conflicts with base branch\",\"deployment\":{\"deploy_state\":\"not_started\",\"stage\":\"not_started\",\"details\":\"Pull request has merge conflicts; deployment blocked\"}}"
        exit 1
    fi

    # 2. Check commit status / check runs
    local check_raw
    check_raw=$(curl -s --connect-timeout 3 -m 10 --retry 3 --retry-delay 2 --retry-connrefused -w "\n%{http_code}" -X GET \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/commits/${commit_sha}/check-runs")

    local check_code
    check_code=$(echo "$check_raw" | tail -n1)
    local check_resp
    check_resp=$(echo "$check_raw" | sed '$d')

    local total_count=0
    local pending_count=0
    local failure_count=0
    local check_details=""

    if [ "$check_code" -ge 200 ] && [ "$check_code" -lt 300 ]; then
        total_count=$(echo "$check_resp" | jq -r '.total_count // 0')
        pending_count=$(echo "$check_resp" | jq '[.check_runs[]? | select(.status != "completed")] | length')
        failure_count=$(echo "$check_resp" | jq '[.check_runs[]? | select(.conclusion != null and .conclusion != "success" and .conclusion != "neutral" and .conclusion != "skipped")] | length')
        check_details=$(echo "$check_resp" | jq -r '[.check_runs[]? | "\(.name): status=\(.status) conclusion=\(.conclusion // "none")"] | join(", ")')
    fi

    # Check runs haven't registered yet or are still running
    if [ "$total_count" -eq 0 ] || [ "$pending_count" -gt 0 ]; then
        echo "{\"status\":\"pending\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"pending_checks\":${pending_count},\"total_checks\":${total_count},\"details\":\"${check_details}\",\"deployment\":{\"deploy_state\":\"not_started\",\"stage\":\"not_started\",\"details\":\"CI checks are pending; deployment starts upon merge\"}}"
        exit 0
    fi

    # If any check runs failed, report failure and do not merge
    if [ "$failure_count" -gt 0 ]; then
        local failing_runs
        failing_runs=$(echo "$check_resp" | jq -r '[.check_runs[]? | select(.conclusion != null and .conclusion != "success" and .conclusion != "neutral" and .conclusion != "skipped") | "\(.name) (\(.conclusion))"] | join(", ")')
        echo "{\"status\":\"failed\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"failed_checks\":${failure_count},\"failing_runs\":\"${failing_runs}\",\"details\":\"${check_details}\",\"deployment\":{\"deploy_state\":\"not_started\",\"stage\":\"not_started\",\"details\":\"CI failed; merge blocked\"}}"
        exit 1
    fi

    # 3. All checks passed: Squash-merge PR into DEFAULT_BRANCH
    echo "🔀 [aerial-pr] Squash-merging PR #${pr_num} into ${DEFAULT_BRANCH}..." >&2

    local merge_raw
    merge_raw=$(curl -s --connect-timeout 3 -m 10 -w "\n%{http_code}" -X PUT \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}/merge" \
        -d '{"merge_method":"squash"}')

    local merge_code
    merge_code=$(echo "$merge_raw" | tail -n1)
    local merge_resp
    merge_resp=$(echo "$merge_raw" | sed '$d')

    if [ "$merge_code" -lt 200 ] || [ "$merge_code" -ge 300 ]; then
        # Check if it was already merged concurrently by native auto-merge
        local recheck_raw
        recheck_raw=$(curl -s --connect-timeout 3 -m 10 -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}")
        local recheck_merged
        recheck_merged=$(echo "$recheck_raw" | jq -r '.merged // false' 2>/dev/null || echo "false")
        if [ "$recheck_merged" = "true" ]; then
            local recheck_sha
            recheck_sha=$(echo "$recheck_raw" | jq -r '.merge_commit_sha // empty')
            # Clean up branch if still present
            curl -s --connect-timeout 3 -m 10 -X DELETE \
                -H "Authorization: token ${GITHUB_PAT}" \
                -H "Accept: application/vnd.github.v3+json" \
                "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
            curl -s --connect-timeout 3 -m 10 -f -X POST "$SIDE_SYNC_URL" >/dev/null 2>&1 || true
            local deploy_json
            deploy_json=$(get_deploy_status "$recheck_sha" 2>/dev/null || echo '{"deploy_state":"unknown","stage":"unknown","details":"Telemetry error"}')
            echo "{\"status\":\"already_merged\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"merged_sha\":\"${recheck_sha}\",\"deployment\":${deploy_json}}"
            exit 0
        fi
        echo "{\"status\":\"error\",\"error_code\":${merge_code},\"message\":\"Failed to merge PR #${pr_num}: ${merge_resp}\"}" >&2
        exit 1
    fi

    local merged
    merged=$(echo "$merge_resp" | jq -r '.merged // false')
    if [ "$merged" != "true" ]; then
        echo "{\"status\":\"error\",\"message\":\"Pull Request #${pr_num} merge rejected: ${merge_resp}\"}" >&2
        exit 1
    fi

    local merged_sha
    merged_sha=$(echo "$merge_resp" | jq -r '.sha // empty')
    if [ -z "$merged_sha" ]; then
        merged_sha="$commit_sha"
    fi

    echo "🎉 [aerial-pr] Successfully merged PR #${pr_num} (commit ${merged_sha})." >&2

    # Delete remote ephemeral feature branch
    curl -s --connect-timeout 3 -m 10 -X DELETE \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true

    # Trigger fast-path sync on aerial-hangar sidecar
    local sync_status="skipped"
    if curl -s --connect-timeout 3 -m 10 -f -X POST "$SIDE_SYNC_URL" >/dev/null 2>&1; then
        sync_status="synced"
    fi

    local deploy_json
    deploy_json=$(get_deploy_status "$merged_sha" 2>/dev/null || echo '{"deploy_state":"unknown","stage":"unknown","details":"Telemetry error"}')

    echo "{\"status\":\"merged\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"merged_sha\":\"${merged_sha}\",\"sync_status\":\"${sync_status}\",\"deployment\":${deploy_json}}"
    exit 0
}

schedule_pr_followup() {
    local pr_num="${1:-}"
    local pr_url="${2:-}"
    local target_id="${3:-}"
    local delay="${4:-}"
    local pr_title="${5:-}"

    # Default to active thread/channel if target_id was not explicitly specified
    if [ -z "$target_id" ]; then
        target_id="${AERIAL_TARGET_ID:-${DISCORD_THREAD_ID:-${DISCORD_CHANNEL_ID:-1542423172400291873}}}"
    fi

    if [ -z "$delay" ]; then
        delay="${AERIAL_PR_CHECK_DELAY:-$DEFAULT_PR_CHECK_DELAY}"
    fi

    local script_name
    script_name="$(basename "$0")"
    local scheduler_url="${SCHEDULER_MCP_URL:-http://scheduler-mcp:8080/mcp}"
    local prompt
    prompt=$(cat <<EOF
Check on the status of Pull Request #${pr_num} on ${REPO_OWNER}/${REPO_NAME} (${pr_url}) for commit: "${pr_title}".
1. Inspect CI status, PR state, and deployment state by running:
   scripts/${script_name} --repo ${REPO_NAME} merge ${pr_num}
2. If status is "pending":
   Quietly reschedule a ${delay} follow-up check via schedule_once with target_id "${target_id}" and suppress response output (empty response) to avoid channel noise while CI is running.
3. If status is "merged" or "already_merged":
   Report status in plain prose (maximum two sentences). Do NOT output markdown bullet lists, task checklists, or forward-looking promises.
   - If deployment state is "done": confirm merge and completed deployment in 1–2 sentences.
   - If deployment state is "ongoing" (e.g. stage: queued, building, awaiting_pull, swapping) or "pending_registration": state the current deployment stage in 1–2 sentences, and reschedule a ${delay} follow-up check via schedule_once with target_id "${target_id}" to track deployment to completion. If status was already merged, report only the deployment stage update.
   - If deployment state is "failed": the two-sentence limit does NOT apply; report full failure details, error logs, and diagnostic context immediately.
4. If status is "failed" or "conflict", or if the merge command errors:
   The two-sentence limit does NOT apply; report full PR/CI failure details, failing check names, and error logs to the user.
EOF
)

    # Build JSON-RPC payload safely via jq
    local rpc_payload
    rpc_payload=$(jq -n \
        --arg tool "schedule_once" \
        --arg target_id "$target_id" \
        --arg run_at "$delay" \
        --arg prompt "$prompt" \
        '{
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: {
                name: $tool,
                arguments: {
                    target_id: $target_id,
                    run_at: $run_at,
                    prompt: $prompt
                }
            }
        }')

    # Execute curl with bounded network timeout and error suppression
    local mcp_resp=""
    mcp_resp=$(curl -s --connect-timeout 3 -m 10 -X POST \
        -H "Content-Type: application/json" \
        -d "$rpc_payload" \
        "$scheduler_url" 2>/dev/null) || {
        echo "⚠️ [aerial-pr] Warning: Unable to contact scheduler-mcp (${scheduler_url}). Follow-up event not scheduled." >&2
        return 0
    }

    # Verify response is valid JSON before parsing
    if ! echo "$mcp_resp" | jq -e . >/dev/null 2>&1; then
        echo "⚠️ [aerial-pr] Warning: Invalid non-JSON response from scheduler-mcp. Skipping schedule registration." >&2
        return 0
    fi

    # Check for tool-level error in JSON-RPC result
    local is_err
    is_err=$(echo "$mcp_resp" | jq -r '.result.isError // false' 2>/dev/null || true)
    if [ "$is_err" = "true" ]; then
        local err_msg
        err_msg=$(echo "$mcp_resp" | jq -r '.result.content[0].text // "Unknown error"' 2>/dev/null || true)
        echo "⚠️ [aerial-pr] Warning: scheduler-mcp returned tool error: ${err_msg}" >&2
        return 0
    fi

    # Unpack nested MCP text content
    local sched_id
    sched_id=$(echo "$mcp_resp" | jq -r '(.result.content[0].text | fromjson? | .schedule_id) // empty' 2>/dev/null || true)

    if [ -n "$sched_id" ]; then
        echo "⏰ [aerial-pr] Scheduled one-shot follow-up check in ${delay} (Schedule ID: ${sched_id}, Target: ${target_id})." >&2
        echo "$sched_id"
    else
        echo "⚠️ [aerial-pr] Warning: Scheduler MCP did not return a valid schedule ID." >&2
    fi
    return 0
}

enable_github_auto_merge() {
    local node_id="$1"
    local pr_num="${2:-}"
    if [ -z "$node_id" ] || [ -z "${GITHUB_PAT:-}" ]; then
        return 0
    fi

    local query='mutation($input: EnablePullRequestAutoMergeInput!) {
        enablePullRequestAutoMerge(input: $input) {
            pullRequest {
                autoMergeRequest {
                    enabledAt
                    mergeMethod
                }
            }
        }
    }'

    local payload
    payload=$(jq -n \
        --arg query "$query" \
        --arg node_id "$node_id" \
        '{query: $query, variables: {input: {pullRequestId: $node_id, mergeMethod: "SQUASH"}}}')

    local resp
    resp=$(curl -s --connect-timeout 3 -m 10 -X POST \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Content-Type: application/json" \
        "https://api.github.com/graphql" \
        -d "$payload" 2>/dev/null || true)

    if echo "$resp" | jq -e '.data.enablePullRequestAutoMerge.pullRequest.autoMergeRequest != null' >/dev/null 2>&1; then
        echo "🤖 [aerial-pr] Native GitHub auto-merge enabled for PR #${pr_num} (SQUASH)." >&2
    else
        local err_msg
        err_msg=$(echo "$resp" | jq -r '.errors[0].message // "unknown error"' 2>/dev/null || echo "failed")
        echo "⚠️ [aerial-pr] Warning: Could not enable native GitHub auto-merge on PR #${pr_num}: ${err_msg}" >&2
    fi
}

submit_scratch() {
    local target_id=""
    local check_delay=""
    local no_schedule=0
    local scratch_dir=""
    local commit_msg=""
    local pr_body=""
    local pr_body_file=""

    while [ $# -gt 0 ]; do
        case "$1" in
            --repo|-r)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a repository argument." >&2
                    exit 1
                fi
                REPO_NAME="$(normalize_repo "$2")"
                REPO_NAME_EXPLICIT=1
                shift 2
                ;;
            --repo=*)
                REPO_NAME="$(normalize_repo "${1#*=}")"
                REPO_NAME_EXPLICIT=1
                shift 1
                ;;
            --async|-a)
                # Accepted as no-op for backwards compatibility; async is now the only mode
                shift
                ;;
            --sync)
                echo "ERROR: Synchronous submit mode has been eliminated. Submissions are strictly asynchronous." >&2
                exit 1
                ;;
            --target-id|--target|-t)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a target ID argument." >&2
                    exit 1
                fi
                target_id="$2"
                shift 2
                ;;
            --target-id=*|--target=*)
                target_id="${1#*=}"
                shift 1
                ;;
            --delay|--run-at|-d)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a delay duration argument." >&2
                    exit 1
                fi
                check_delay="$2"
                shift 2
                ;;
            --delay=*|--run-at=*)
                check_delay="${1#*=}"
                shift 1
                ;;
            --no-schedule)
                no_schedule=1
                shift
                ;;
            --body|-b)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a body string argument." >&2
                    exit 1
                fi
                pr_body="$2"
                shift 2
                ;;
            --body=*|-b=*)
                pr_body="${1#*=}"
                shift 1
                ;;
            --body-file|-f|-F)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a file path argument." >&2
                    exit 1
                fi
                pr_body_file="$2"
                shift 2
                ;;
            --body-file=*|-f=*|-F=*)
                pr_body_file="${1#*=}"
                shift 1
                ;;
            --)
                shift
                while [ $# -gt 0 ]; do
                    if [ -z "$scratch_dir" ]; then
                        scratch_dir="$1"
                    elif [ -z "$commit_msg" ]; then
                        commit_msg="$1"
                    else
                        echo "ERROR: Unexpected extra argument: $1" >&2
                        exit 1
                    fi
                    shift
                done
                break
                ;;
            -*)
                echo "ERROR: Unknown option: $1" >&2
                exit 1
                ;;
            *)
                if [ -z "$scratch_dir" ]; then
                    scratch_dir="$1"
                elif [ -z "$commit_msg" ]; then
                    commit_msg="$1"
                else
                    echo "ERROR: Unexpected extra argument: $1" >&2
                    exit 1
                fi
                shift
                ;;
        esac
    done

    # Validation: Mutual exclusivity of body sources
    if [ -n "$pr_body" ] && [ -n "$pr_body_file" ]; then
        echo "ERROR: Cannot specify both --body and --body-file." >&2
        exit 1
    fi

    if [ -z "$scratch_dir" ] || [ ! -d "$scratch_dir" ]; then
        echo "ERROR: Valid scratch directory path required for submit." >&2
        exit 1
    fi

    # Resolve Tier 1 file if specified
    if [ -n "$pr_body_file" ]; then
        local resolved_body_file=""
        if [ -f "$pr_body_file" ]; then
            resolved_body_file="$pr_body_file"
        elif [ -f "${scratch_dir}/${pr_body_file}" ]; then
            resolved_body_file="${scratch_dir}/${pr_body_file}"
        else
            echo "ERROR: Specified body file does not exist: $pr_body_file" >&2
            exit 1
        fi
        if [ ! -r "$resolved_body_file" ]; then
            echo "ERROR: Specified body file is not readable: $resolved_body_file" >&2
            exit 1
        fi
        pr_body=$(cat "$resolved_body_file")
    fi

    SCRATCH_DIR_CLEANUP="$scratch_dir"
    # Ensure cleanup of scratch directory on exit
    trap 'if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then rm -rf "${SCRATCH_DIR_CLEANUP}"; fi' EXIT INT TERM

    cd "$scratch_dir"

    # Auto-detect remote repository from origin URL if not explicitly set via --repo
    local remote_url
    remote_url=$(git remote get-url origin 2>/dev/null || echo "")
    extract_remote_repo "$remote_url"
    if [ -n "${PARSED_REPO:-}" ]; then
        REPO_OWNER="${PARSED_OWNER:-$REPO_OWNER}"
        if [ "$REPO_NAME_EXPLICIT" -eq 0 ]; then
            REPO_NAME="$(normalize_repo "$PARSED_REPO")"
        fi
    fi

    local default_title="feat(core): automated update by Aerial"
    local default_delay="2m"
    case "$REPO_NAME" in
        aerial)
            default_title="feat(core): automated update by Aerial"
            default_delay="2m"
            ;;
        aerial-config)
            default_title="chore(config): automated configuration update by Aerial"
            default_delay="1m"
            ;;
        aerial-sidecars)
            default_title="feat(sidecars): automated sidecars update by Aerial"
            default_delay="2m"
            ;;
        *)
            default_title="feat(${REPO_NAME}): automated update by Aerial"
            default_delay="2m"
            ;;
    esac

    if [ -z "$commit_msg" ]; then
        commit_msg="$default_title"
    fi

    # Resolve PR description from convention file in scratch workspace if not explicitly passed
    if [ -z "$pr_body" ]; then
        if [ -s "${scratch_dir}/PR_DESCRIPTION.md" ]; then
            pr_body=$(cat "${scratch_dir}/PR_DESCRIPTION.md")
        elif [ -s "${scratch_dir}/.pr_description.md" ]; then
            pr_body=$(cat "${scratch_dir}/.pr_description.md")
        fi
    fi

    # CRITICAL WORKSPACE HYGIENE: Always remove convention files from scratch workspace before staging
    # so they are NEVER staged by git add -A or permanently committed into git history on main!
    rm -f "${scratch_dir}/PR_DESCRIPTION.md" "${scratch_dir}/.pr_description.md"

    # Title Hygiene: Extract first non-empty line, strip \r, \n, and whitespace, clamp to 256 chars
    local clean_title
    clean_title=$(printf "%s\n" "$commit_msg" | awk 'NF {print; exit}' | tr -d '\r\n' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | cut -c 1-256)
    if [ -z "$clean_title" ]; then
        clean_title="$default_title"
    fi

    # Mandatory PR Description Invariant: PR description must not be empty
    local trimmed_body
    trimmed_body=$(printf "%s" "$pr_body" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    if [ -z "$trimmed_body" ]; then
        echo "ERROR: Pull Request description is mandatory. Provide a description via PR_DESCRIPTION.md in the workspace, or via --body-file / -b." >&2
        exit 1
    fi

    # Defensively clamp body length to 60,000 characters (GitHub API limit is 65,536)
    local max_body_len=60000
    if [ "${#pr_body}" -gt "$max_body_len" ]; then
        pr_body="${pr_body:0:$max_body_len}

> ⚠️ **[Aerial Notice]**: PR description was truncated because it exceeded 60,000 characters."
    fi

    # 1. Check for modifications
    if git diff --quiet && git diff --staged --quiet; then
        echo "{\"status\":\"no_changes\",\"message\":\"No modifications detected in scratch directory.\"}"
        exit 0
    fi

    # 2. Stage all modifications (defensively unstage any convention files if previously cached)
    git add -A
    git rm --cached -f PR_DESCRIPTION.md .pr_description.md 2>/dev/null || true

    # 3. Pre-flight verification (universal contract: run repo-defined staged verify suite on staged changes)
    if ! run_preflight_verification "."; then
        exit 1
    fi

    # 4. Commit (preserves complete multi-line commit message in git history)
    git commit -m "$commit_msg" >/dev/null

    local branch
    branch=$(git rev-parse --abbrev-ref HEAD)
    local commit_sha
    commit_sha=$(git rev-parse HEAD)

    local auth_header
    auth_header=$(build_auth_header)

    export GIT_TERMINAL_PROMPT=0
    export GIT_CONFIG_COUNT=2
    export GIT_CONFIG_KEY_0="http.extraHeader"
    export GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}"
    export GIT_CONFIG_KEY_1="http.version"
    export GIT_CONFIG_VALUE_1="HTTP/1.1"

    # 5. Push branch to remote
    echo "📤 [aerial-pr] Pushing feature branch ${branch} to origin (${REPO_OWNER}/${REPO_NAME})..." >&2
    git push -u origin "$branch" >/dev/null 2>&1 || {
        echo "ERROR: Failed to push branch ${branch} to GitHub." >&2
        if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
            SCRATCH_DIR_CLEANUP=""
        fi
        exit 1
    }

    # 6. Open Pull Request via GitHub REST API safely using jq
    local pr_payload
    pr_payload=$(jq -n \
        --arg title "$clean_title" \
        --arg head "$branch" \
        --arg base "$DEFAULT_BRANCH" \
        --arg body "$pr_body" \
        '{title: $title, head: $head, base: $base, body: $body}')

    local pr_raw
    pr_raw=$(curl -s --connect-timeout 3 -m 10 -w "\n%{http_code}" -X POST \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls" \
        -d "$pr_payload")

    local pr_code
    pr_code=$(echo "$pr_raw" | tail -n1)
    local pr_resp
    pr_resp=$(echo "$pr_raw" | sed '$d')

    if [ "$pr_code" -lt 200 ] || [ "$pr_code" -ge 300 ]; then
        echo "ERROR: Failed to create Pull Request (HTTP ${pr_code}): ${pr_resp}" >&2
        # Prune remote branch
        curl -s --connect-timeout 3 -m 10 -X DELETE \
            -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
        # Prevent trap from nuking the local workspace so code can be recovered
        if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
            SCRATCH_DIR_CLEANUP=""
        fi
        exit 1
    fi

    local pr_num
    pr_num=$(echo "$pr_resp" | jq -r '.number')
    local pr_url
    pr_url=$(echo "$pr_resp" | jq -r '.html_url')
    local pr_node_id
    pr_node_id=$(echo "$pr_resp" | jq -r '.node_id // empty')

    # Enable native GitHub auto-merge (SQUASH)
    enable_github_auto_merge "$pr_node_id" "$pr_num"

    # Asynchronous Submission: Disarm cleanup trap and remove scratch workspace immediately
    if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
        rm -rf "${SCRATCH_DIR_CLEANUP}"
        SCRATCH_DIR_CLEANUP=""
    fi

    local sched_id=""
    local effective_delay="${check_delay:-${AERIAL_PR_CHECK_DELAY:-$default_delay}}"
    if [ "$no_schedule" -eq 0 ]; then
        sched_id=$(schedule_pr_followup "$pr_num" "$pr_url" "$target_id" "$effective_delay" "$clean_title" | tail -n 1)
    fi

    cat <<EOF >&2
================================================================================
📝 [aerial-pr] Created Pull Request #${pr_num}: ${pr_url}
⚡ [aerial-pr] Asynchronous submission active. Follow-up check scheduled via scheduler-mcp (${effective_delay}).
🛑 MANDATORY TURN ACTION: Do not poll CI in the foreground. End active execution turn now.
================================================================================
EOF

    echo "{\"status\":\"submitted\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"branch\":\"${branch}\",\"commit_sha\":\"${commit_sha}\",\"async\":true,\"scheduled_check\":\"${effective_delay}\",\"schedule_id\":\"${sched_id}\",\"repo\":\"${REPO_NAME}\",\"turn_action\":\"end_turn\",\"message\":\"PR #${pr_num} submitted on ${REPO_OWNER}/${REPO_NAME}. Follow-up check scheduled. Do not poll CI in foreground; end active turn now.\"}"
    return 0
}

case "$cmd" in
    init)
        init_scratch "$@"
        ;;
    submit)
        submit_scratch "$@"
        ;;
    merge|monitor)
        merge_pr "$@"
        ;;
    deploy-status|deployment-status|deployment)
        get_deploy_status "$@"
        ;;
    *)
        echo "Usage: $0 [--repo <name>] {init [repo]|submit [-t <target_id>] [-d <delay>] [--no-schedule] [-b <body>|--body <body>|-f <file>|--body-file <file>] <scratch_dir> [commit_msg]|merge <pr_num> [branch] [commit_sha]|deploy-status <pr_num|commit_sha>}" >&2
        exit 1
        ;;
esac
