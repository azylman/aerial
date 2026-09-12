#!/usr/bin/env bash
set -euo pipefail

REPO_OWNER="azylman"
REPO_NAME="aerial-config"
REPO_URL="https://github.com/${REPO_OWNER}/${REPO_NAME}.git"
DEFAULT_BRANCH="main"
SIDE_SYNC_URL="${AERIAL_GITSYNC_URL:-http://aerial-gitsync:8080/sync}"
DEFAULT_PR_CHECK_DELAY="2m"

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

monitor_and_merge_pr() {
    local pr_num="${1:-}"
    local branch="${2:-}"
    local commit_sha="${3:-}"

    if [ -z "$pr_num" ] || [ -z "$branch" ] || [ -z "$commit_sha" ]; then
        echo "ERROR: pr_num, branch, and commit_sha are required for monitor." >&2
        exit 1
    fi

    local pr_url="https://github.com/${REPO_OWNER}/${REPO_NAME}/pull/${pr_num}"

    # 6. Poll CI workflow runs (bounded timeout: 180s)
    local max_wait=180
    local elapsed=0
    local poll_interval=5
    local has_registered_checks=0
    local all_green=0

    while [ $elapsed -lt $max_wait ]; do
        local check_raw
        check_raw=$(curl -s -w "\n%{http_code}" -X GET \
            -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs?head_sha=${commit_sha}")

        local check_code
        check_code=$(echo "$check_raw" | tail -n1)
        local check_resp
        check_resp=$(echo "$check_raw" | sed '$d')

        if [ "$check_code" -ge 200 ] && [ "$check_code" -lt 300 ]; then
            local total_count
            total_count=$(echo "$check_resp" | jq -r '.total_count // 0')

            if [ "$total_count" -gt 0 ]; then
                has_registered_checks=1

                local pending_count
                pending_count=$(echo "$check_resp" | jq '[.workflow_runs[]? | select(.status != "completed")] | length')
                local failure_count
                failure_count=$(echo "$check_resp" | jq '[.workflow_runs[]? | select(.conclusion != null and .conclusion != "success" and .conclusion != "neutral" and .conclusion != "skipped")] | length')

                if [ "$failure_count" -gt 0 ]; then
                    echo "ERROR: GitHub Actions workflow runs failed on PR #${pr_num}." >&2
                    # Post comment to issues endpoint (general PR comments)
                    curl -s -X POST \
                        -H "Authorization: token ${GITHUB_PAT}" \
                        -H "Accept: application/vnd.github.v3+json" \
                        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/issues/${pr_num}/comments" \
                        -d '{"body":"Automated validation failed in CI. Closing PR and discarding scratch workspace."}' >/dev/null 2>&1 || true
                    # Close PR
                    curl -s -X PATCH \
                        -H "Authorization: token ${GITHUB_PAT}" \
                        -H "Accept: application/vnd.github.v3+json" \
                        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}" \
                        -d '{"state":"closed"}' >/dev/null 2>&1 || true
                    # Delete remote branch
                    curl -s -X DELETE \
                        -H "Authorization: token ${GITHUB_PAT}" \
                        -H "Accept: application/vnd.github.v3+json" \
                        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
                    exit 1
                fi

                if [ "$pending_count" -eq 0 ]; then
                    all_green=1
                    break
                fi
            else
                # If after 30 seconds no checks have ever registered, assume repo has no CI checks configured
                if [ $has_registered_checks -eq 0 ] && [ $elapsed -ge 30 ]; then
                    all_green=1
                    break
                fi
            fi
        fi

        sleep $poll_interval
        elapsed=$((elapsed + poll_interval))
    done

    # CRITICAL CHECK: Verify all_green was actually attained before merging!
    if [ "$all_green" -ne 1 ]; then
        echo "ERROR: CI checks timed out or never completed successfully after ${max_wait}s. Aborting merge." >&2
        # Close PR
        curl -s -X PATCH \
            -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}" \
            -d '{"state":"closed"}' >/dev/null 2>&1 || true
        # Delete remote branch
        curl -s -X DELETE \
            -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
        exit 1
    fi

    # 7. Squash Merge Pull Request
    local merge_payload='{"merge_method":"squash"}'
    local merge_raw
    merge_raw=$(curl -s -w "\n%{http_code}" -X PUT \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}/merge" \
        -d "$merge_payload")

    local merge_code
    merge_code=$(echo "$merge_raw" | tail -n1)
    local merge_resp
    merge_resp=$(echo "$merge_raw" | sed '$d')

    if [ "$merge_code" -lt 200 ] || [ "$merge_code" -ge 300 ]; then
        echo "ERROR: Failed to merge Pull Request #${pr_num} (HTTP ${merge_code}): ${merge_resp}" >&2
        exit 1
    fi

    local merged
    merged=$(echo "$merge_resp" | jq -r '.merged // false')
    if [ "$merged" != "true" ]; then
        echo "ERROR: Pull Request #${pr_num} merge rejected: ${merge_resp}" >&2
        exit 1
    fi

    # Extract the actual new squash merge SHA on main!
    local merged_sha
    merged_sha=$(echo "$merge_resp" | jq -r '.sha // empty')
    if [ -z "$merged_sha" ]; then
        merged_sha="$commit_sha"
    fi

    # 8. Delete remote ephemeral feature branch
    curl -s -X DELETE \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true

    # 9. Trigger fast-path sync on aerial-gitsync sidecar
    local sync_status="skipped"
    if curl -s -f -X POST "$SIDE_SYNC_URL" >/dev/null 2>&1; then
        sync_status="synced"
    fi

    echo "{\"status\":\"merged\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"merged_sha\":\"${merged_sha}\",\"sync_status\":\"${sync_status}\"}"
}

schedule_pr_followup() {
    local pr_num="${1:-}"
    local pr_url="${2:-}"
    local target_id="${3:-}"
    local delay="${4:-}"
    local commit_msg="${5:-}"

    if [ -z "$target_id" ]; then
        target_id="${AERIAL_TARGET_ID:-${DISCORD_THREAD_ID:-${DISCORD_CHANNEL_ID:-1542423172400291873}}}"
    fi

    if [ -z "$delay" ]; then
        delay="${AERIAL_PR_CHECK_DELAY:-$DEFAULT_PR_CHECK_DELAY}"
    fi

    local scheduler_url="${SCHEDULER_MCP_URL:-http://scheduler-mcp:8080/mcp}"
    local prompt
    prompt=$(cat <<EOF
Check on the status of Pull Request #${pr_num} on ${REPO_OWNER}/${REPO_NAME} (${pr_url}) for commit: "${commit_msg}".
1. If the PR is already merged, confirm the successful merge and deployment to the user.
2. If all CI checks passed and the PR is still open (e.g. background monitor terminated), immediately complete the squash-merge, delete the remote branch, trigger gitsync, and confirm deployment.
3. If CI failed or was blocked, report the exact failure details to the user.
4. If CI is still in progress, do not emit noisy chatter; quietly reschedule a 3m follow-up check via scheduler_schedule_once with target_id "${target_id}" and suppress response output.
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
    local async_mode=0
    local target_id=""
    local check_delay=""
    local no_schedule=0
    local scratch_dir=""
    local commit_msg=""

    if [ "${AERIAL_PR_ASYNC:-0}" = "1" ]; then
        async_mode=1
    fi

    while [ $# -gt 0 ]; do
        case "$1" in
            --async|-a)
                async_mode=1
                shift
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
            --)
                shift
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

    if [ -z "$scratch_dir" ] || [ ! -d "$scratch_dir" ]; then
        echo "ERROR: Valid scratch directory path required for submit." >&2
        exit 1
    fi

    if [ -z "$commit_msg" ]; then
        commit_msg="chore(config): update configuration via Aerial self-improvement"
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

    # 3. Commit
    git add -A
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

    # 5. Open Pull Request via GitHub REST API without silent -f
    local pr_payload
    pr_payload=$(cat <<EOF
{
  "title": $(echo "$commit_msg" | jq -Rs .),
  "head": "${branch}",
  "base": "${DEFAULT_BRANCH}",
  "body": "Automated configuration update by Aerial from scratch workspace."
}
EOF
)

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
        exit 1
    fi

    local pr_num
    pr_num=$(echo "$pr_resp" | jq -r '.number')
    local pr_url
    pr_url=$(echo "$pr_resp" | jq -r '.html_url')

    if [ "$async_mode" -eq 1 ]; then
        # Disarm cleanup trap and remove scratch workspace immediately since changes are pushed
        if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            rm -rf "${SCRATCH_DIR_CLEANUP}"
            SCRATCH_DIR_CLEANUP=""
        fi

        local script_path
        script_path="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
        local log_file="/tmp/aerial-config-pr-monitor-${pr_num}.log"

        nohup bash "$script_path" monitor "$pr_num" "$branch" "$commit_sha" < /dev/null > "$log_file" 2>&1 &
        local monitor_pid=$!

        echo "⚡ [aerial-config-pr] Asynchronous submission active. CI monitoring delegated to background PID ${monitor_pid} (log: ${log_file})."

        local sched_id=""
        local effective_delay="${check_delay:-${AERIAL_PR_CHECK_DELAY:-$DEFAULT_PR_CHECK_DELAY}}"
        if [ "$no_schedule" -eq 0 ]; then
            sched_id=$(schedule_pr_followup "$pr_num" "$pr_url" "$target_id" "$effective_delay" "$commit_msg" | tail -n 1)
        fi

        echo "{\"status\":\"submitted\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"branch\":\"${branch}\",\"commit_sha\":\"${commit_sha}\",\"async\":true,\"monitor_pid\":${monitor_pid},\"scheduled_check\":\"${effective_delay}\",\"schedule_id\":\"${sched_id}\"}"
        return 0
    fi

    # Synchronous mode: monitor and merge inline
    monitor_and_merge_pr "$pr_num" "$branch" "$commit_sha"
}

case "$cmd" in
    init)
        init_scratch
        ;;
    submit)
        submit_scratch "$@"
        ;;
    monitor)
        monitor_and_merge_pr "$@"
        ;;
    *)
        echo "Usage: $0 {init|submit [--async] [-t <target_id>] [-d <delay>] [--no-schedule] <scratch_dir> [commit_msg]|monitor <pr_num> <branch> <commit_sha>}" >&2
        exit 1
        ;;
esac
