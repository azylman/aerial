#!/usr/bin/env bash
set -euo pipefail

# scripts/aerial-config-pr.sh - Configuration PR Automation & GitOps Verifier
# Safely clones azylman/aerial-config into ephemeral scratch space, verifies YAML syntax,
# creates Pull Requests, schedules follow-up checks via scheduler-mcp, and provides single-shot merge verification.

REPO_OWNER="azylman"
REPO_NAME="aerial-config"
REPO_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}.git"
DEFAULT_BRANCH="main"
SIDE_SYNC_URL="${AERIAL_GITSYNC_URL:-http://aerial-gitsync:8080/sync}"
DEFAULT_PR_CHECK_DELAY="1m"

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
    local auth_header
    auth_header=$(build_auth_header)

    local base_dir="/dev/shm"
    if [ ! -d "$base_dir" ] || [ ! -w "$base_dir" ]; then
        base_dir="${TMPDIR:-/tmp}"
    fi

    local scratch_dir
    scratch_dir=$(mktemp -d "${base_dir}/aerial-scratch.XXXXXX")
    chmod 700 "$scratch_dir"

    local rand_suffix
    rand_suffix=$(head -c 16 /dev/urandom | md5sum | head -c 6)
    local branch_name="update/config-$(date +%Y%m%d%H%M%S)-${rand_suffix}"

    export GIT_TERMINAL_PROMPT=0
    export GIT_CONFIG_COUNT=2
    export GIT_CONFIG_KEY_0="http.extraHeader"
    export GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}"
    export GIT_CONFIG_KEY_1="http.version"
    export GIT_CONFIG_VALUE_1="HTTP/1.1"

    if ! git clone --depth 1 --single-branch -b "$DEFAULT_BRANCH" "$REPO_URL" "$scratch_dir" 2>&1; then
        echo "ERROR: Failed to clone ${REPO_URL} into scratch directory." >&2
        rm -rf "$scratch_dir"
        exit 1
    fi
    
    cd "$scratch_dir"
    git checkout -b "$branch_name" >/dev/null 2>&1
    git config user.name "Aerial"
    git config user.email "aerial@noreply.github.com"

    echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${branch_name}\"}"
}

merge_pr() {
    local pr_num="${1:-}"
    local branch="${2:-}"
    local commit_sha="${3:-}"

    if [ -z "$pr_num" ] || ! [[ "$pr_num" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Valid numeric pr_num required for merge." >&2
        exit 1
    fi

    local pr_url="https://github.com/${REPO_OWNER}/${REPO_NAME}/pull/${pr_num}"

    # 1. Fetch Pull Request details to check state and resolve branch/sha
    local pr_raw
    pr_raw=$(curl -s --retry 3 --retry-delay 2 --retry-connrefused -w "\n%{http_code}" -X GET \
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
        local merged_sha
        merged_sha=$(echo "$pr_resp" | jq -r '.merge_commit_sha // empty')
        echo "{\"status\":\"already_merged\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"merged_sha\":\"${merged_sha}\"}"
        exit 0
    fi

    local state
    state=$(echo "$pr_resp" | jq -r '.state // empty')
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
        echo "{\"status\":\"conflict\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"error\":\"Pull request has merge conflicts with base branch\"}"
        exit 1
    fi

    # 2. Check GitHub Actions CI runs for head_sha
    local check_raw
    check_raw=$(curl -s --retry 3 --retry-delay 2 --retry-connrefused -w "\n%{http_code}" -X GET \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs?head_sha=${commit_sha}")

    local check_code
    check_code=$(echo "$check_raw" | tail -n1)
    local check_resp
    check_resp=$(echo "$check_raw" | sed '$d')

    if [ "$check_code" -lt 200 ] || [ "$check_code" -ge 300 ]; then
        echo "{\"status\":\"error\",\"error_code\":${check_code},\"message\":\"Failed to query CI runs for commit ${commit_sha}\"}" >&2
        exit 1
    fi

    local total_count
    total_count=$(echo "$check_resp" | jq -r '.total_count // 0')

    # Guard: CI Registration Grace Period
    # If no runs registered yet, check PR age. If PR was created < 60s ago, consider it pending registration.
    local created_at
    created_at=$(echo "$pr_resp" | jq -r '.created_at // empty')
    local pr_created_epoch=0
    if [ -n "$created_at" ]; then
        pr_created_epoch=$(date -d "$created_at" +%s 2>/dev/null || date -jf "%Y-%m-%dT%H:%M:%SZ" "$created_at" +%s 2>/dev/null || echo 0)
    fi
    local now_epoch
    now_epoch=$(date +%s)
    local pr_age=$((now_epoch - pr_created_epoch))

    if [ "$total_count" -eq 0 ]; then
        if [ $pr_age -lt 60 ]; then
            echo "{\"status\":\"pending\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"pending_count\":1,\"failure_count\":0,\"total_runs\":0,\"message\":\"CI checks pending registration (PR age ${pr_age}s)\"}"
            exit 2
        fi
        echo "{\"status\":\"pending\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"pending_count\":0,\"failure_count\":0,\"total_runs\":0,\"message\":\"No CI runs registered yet after ${pr_age}s\"}"
        exit 2
    fi

    local pending_count
    pending_count=$(echo "$check_resp" | jq '[.workflow_runs[]? | select(.status != "completed")] | length')
    local failure_count
    failure_count=$(echo "$check_resp" | jq '[.workflow_runs[]? | select(.conclusion != null and .conclusion != "success" and .conclusion != "neutral" and .conclusion != "skipped")] | length')

    if [ "$failure_count" -gt 0 ]; then
        local failed_runs
        failed_runs=$(echo "$check_resp" | jq '[.workflow_runs[]? | select(.conclusion != null and .conclusion != "success" and .conclusion != "neutral" and .conclusion != "skipped") | {name: .name, conclusion: .conclusion, html_url: .html_url}]')
        echo "{\"status\":\"failed\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"failure_count\":${failure_count},\"failed_runs\":${failed_runs}}"
        exit 1
    fi

    if [ "$pending_count" -gt 0 ]; then
        echo "{\"status\":\"pending\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"pending_count\":${pending_count},\"failure_count\":0,\"total_runs\":${total_count}}"
        exit 2
    fi

    # All CI runs are green (pending_count == 0 && failure_count == 0)
    echo "✅ [aerial-config-pr] All GitHub Actions CI checks passed for PR #${pr_num}." >&2
    echo "🔀 [aerial-config-pr] Squash-merging PR #${pr_num} into ${DEFAULT_BRANCH}..." >&2

    local merge_payload='{"merge_method":"squash"}'
    local merge_raw
    merge_raw=$(curl -s --retry 3 --retry-delay 2 --retry-connrefused -w "\n%{http_code}" -X PUT \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}/merge" \
        -d "$merge_payload")

    local merge_code
    merge_code=$(echo "$merge_raw" | tail -n1)
    local merge_resp
    merge_resp=$(echo "$merge_raw" | sed '$d')

    if [ "$merge_code" -lt 200 ] || [ "$merge_code" -ge 300 ]; then
        local recheck_merged
        recheck_merged=$(curl -s -H "Authorization: token ${GITHUB_PAT}" "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}" | jq -r '.merged // false')
        if [ "$recheck_merged" = "true" ]; then
            echo "{\"status\":\"already_merged\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num}}"
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

    echo "🎉 [aerial-config-pr] Successfully merged PR #${pr_num} (commit ${merged_sha})." >&2

    # Delete remote ephemeral feature branch
    curl -s -X DELETE \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true

    # Trigger fast-path sync on aerial-gitsync sidecar
    local sync_status="skipped"
    if curl -s -f -X POST "$SIDE_SYNC_URL" >/dev/null 2>&1; then
        sync_status="synced"
    fi

    echo "{\"status\":\"merged\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"merged_sha\":\"${merged_sha}\",\"sync_status\":\"${sync_status}\"}"
    exit 0
}

schedule_pr_followup() {
    local pr_num="${1:-}"
    local pr_url="${2:-}"
    local target_id="${3:-}"
    local delay="${4:-}"
    local pr_title="${5:-}"

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
1. Inspect CI status and PR state by running:
   scripts/${script_name} merge ${pr_num}
2. If status is "merged" or "already_merged", confirm the successful merge and deployment to the user.
3. If status is "pending", quietly reschedule a ${delay} follow-up check via scheduler_schedule_once with target_id "${target_id}" and suppress response output (empty stdout).
4. If status is "failed" or "conflict", report the failure details to the user.
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
    mcp_resp=$(curl -s --connect-timeout 2 -m 5 -X POST \
        -H "Content-Type: application/json" \
        -d "$rpc_payload" \
        "$scheduler_url" 2>/dev/null) || {
        echo "⚠️ [aerial-config-pr] Warning: Unable to contact scheduler-mcp (${scheduler_url}). Follow-up event not scheduled." >&2
        return 0
    }

    # Verify response is valid JSON before parsing
    if ! echo "$mcp_resp" | jq -e . >/dev/null 2>&1; then
        echo "⚠️ [aerial-config-pr] Warning: Invalid non-JSON response from scheduler-mcp. Skipping schedule registration." >&2
        return 0
    fi

    # Check for tool-level error in JSON-RPC result
    local is_err
    is_err=$(echo "$mcp_resp" | jq -r '.result.isError // false' 2>/dev/null || true)
    if [ "$is_err" = "true" ]; then
        local err_msg
        err_msg=$(echo "$mcp_resp" | jq -r '.result.content[0].text // "Unknown error"' 2>/dev/null || true)
        echo "⚠️ [aerial-config-pr] Warning: scheduler-mcp returned tool error: ${err_msg}" >&2
        return 0
    fi

    # Unpack nested MCP text content
    local sched_id
    sched_id=$(echo "$mcp_resp" | jq -r '(.result.content[0].text | fromjson? | .schedule_id) // empty' 2>/dev/null || true)

    if [ -n "$sched_id" ]; then
        echo "⏰ [aerial-config-pr] Scheduled one-shot follow-up check in ${delay} (Schedule ID: ${sched_id}, Target: ${target_id})."
        echo "$sched_id"
    else
        echo "⚠️ [aerial-config-pr] Warning: Scheduler MCP did not return a valid schedule ID." >&2
    fi
    return 0
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

    if [ -z "$commit_msg" ]; then
        commit_msg="chore(config): update configuration via Aerial self-improvement"
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
        clean_title="chore(config): update configuration via Aerial self-improvement"
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

    SCRATCH_DIR_CLEANUP="$scratch_dir"
    # Ensure cleanup of scratch directory on exit
    trap 'if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then rm -rf "${SCRATCH_DIR_CLEANUP}"; fi' EXIT INT TERM

    cd "$scratch_dir"

    # 1. Pre-flight verification (syntax and basic schema checks)
    if command -v python3 >/dev/null 2>&1; then
        if [ -f "config.yaml" ]; then
            python3 -c "
import yaml
with open('config.yaml') as f:
    cfg = yaml.safe_load(f)
assert isinstance(cfg, dict), 'config.yaml must be a YAML mapping'
assert 'channels' in cfg, 'channels key is required'
assert 'default' in cfg['channels'], 'channels.default is required'
" || {
                echo "ERROR: config.yaml failed pre-flight syntax/schema validation." >&2
                exit 1
            }
        fi

        for cf in docker-compose.override.yml docker-compose.override.yaml; do
            if [ -f "$cf" ]; then
                python3 -c "
import yaml
with open('$cf') as f:
    data = yaml.safe_load(f)
if data is not None:
    assert isinstance(data, dict), '$cf must be a YAML mapping'
" || {
                    echo "ERROR: $cf failed pre-flight YAML validation." >&2
                    exit 1
                }
            fi
        done
    fi

    # 2. Check for modifications
    if git diff --quiet && git diff --staged --quiet; then
        echo "{\"status\":\"no_changes\",\"message\":\"No modifications detected in scratch directory.\"}"
        exit 0
    fi

    # 3. Stage all modifications (defensively unstage any convention files if previously cached)
    git add -A
    git rm --cached -f PR_DESCRIPTION.md .pr_description.md 2>/dev/null || true
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

    # 4. Push branch to remote
    git push -u origin "$branch" >/dev/null 2>&1 || {
        echo "ERROR: Failed to push branch ${branch} to GitHub." >&2
        exit 1
    }

    # 5. Open Pull Request via GitHub REST API without silent -f safely using jq
    local pr_payload
    pr_payload=$(jq -n \
        --arg title "$clean_title" \
        --arg head "$branch" \
        --arg base "$DEFAULT_BRANCH" \
        --arg body "$pr_body" \
        '{title: $title, head: $head, base: $base, body: $body}')

    local pr_raw
    pr_raw=$(curl -s -w "\n%{http_code}" -X POST \
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
        curl -s -X DELETE \
            -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
        # Prevent trap from nuking the local workspace so code can be recovered
        if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            echo "💾 [aerial-config-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
            SCRATCH_DIR_CLEANUP=""
        fi
        exit 1
    fi

    local pr_num
    pr_num=$(echo "$pr_resp" | jq -r '.number')
    local pr_url
    pr_url=$(echo "$pr_resp" | jq -r '.html_url')
    echo "📝 [aerial-config-pr] Created Pull Request #${pr_num}: ${pr_url}"

    # Asynchronous Submission: Disarm cleanup trap and remove scratch workspace immediately
    if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
        rm -rf "${SCRATCH_DIR_CLEANUP}"
        SCRATCH_DIR_CLEANUP=""
    fi

    echo "⚡ [aerial-config-pr] Asynchronous submission active. Follow-up check scheduled via scheduler-mcp."

    local sched_id=""
    local effective_delay="${check_delay:-${AERIAL_PR_CHECK_DELAY:-$DEFAULT_PR_CHECK_DELAY}}"
    if [ "$no_schedule" -eq 0 ]; then
        sched_id=$(schedule_pr_followup "$pr_num" "$pr_url" "$target_id" "$effective_delay" "$clean_title" | tail -n 1)
    fi

    echo "{\"status\":\"submitted\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"branch\":\"${branch}\",\"commit_sha\":\"${commit_sha}\",\"async\":true,\"scheduled_check\":\"${effective_delay}\",\"schedule_id\":\"${sched_id}\"}"
    return 0
}

case "$cmd" in
    init)
        init_scratch
        ;;
    submit)
        submit_scratch "$@"
        ;;
    merge|monitor)
        merge_pr "$@"
        ;;
    *)
        echo "Usage: $0 {init|submit [-t <target_id>] [-d <delay>] [--no-schedule] [-b <body>|--body <body>|-f <file>|--body-file <file>] <scratch_dir> [commit_msg]|merge <pr_num> [branch] [commit_sha]}" >&2
        exit 1
        ;;
esac
