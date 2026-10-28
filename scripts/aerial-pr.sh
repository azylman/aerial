#!/usr/bin/env bash
set -euo pipefail

# scripts/aerial-pr.sh - Unified Monorepo PR Automation & Continuous Integration Verifier
# Safely clones repositories into ephemeral scratch space, enforces the universal
# verification contract (scripts/verify.sh --staged), creates Pull Requests with auto-merge,
# registers PRs for event-driven continuous delivery, and supports RoboRev overrides.

REPO_OWNER="azylman"
DEFAULT_REPO="aerial"
REPO_NAME="${AERIAL_REPO_NAME:-$DEFAULT_REPO}"
REPO_NAME_EXPLICIT=0
DEFAULT_BRANCH="main"

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
        if (
            cd "${target_dir}" &&
            git_dir=$(git rev-parse --git-dir 2>/dev/null) &&
            [ -f "${git_dir}/.aerial-verified-tree" ] &&
            git diff --quiet 2>/dev/null &&
            verified_tree=$(cat "${git_dir}/.aerial-verified-tree" 2>/dev/null) &&
            [ -n "$verified_tree" ] &&
            [ "$(git write-tree 2>/dev/null)" = "$verified_tree" ]
        ); then
            echo "⚡ Skipping duplicate pre-flight verification (staged tree already verified)." >&2
            return 0
        fi
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

# Pre-dispatch global flag parsing (allows --repo and --vanadis-chunk before or after command)
VANADIS_CHUNK_ID="${AERIAL_VANADIS_CHUNK_ID:-${VANADIS_CHUNK_ID:-}}"
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
        --vanadis-chunk)
            if [ $# -lt 2 ]; then
                echo "ERROR: $1 requires a chunk ID argument." >&2
                exit 1
            fi
            VANADIS_CHUNK_ID="$2"
            shift 2
            ;;
        --vanadis-chunk=*)
            VANADIS_CHUNK_ID="${1#*=}"
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
    local init_branch=""
    local positional_repo_set=0

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
            --branch|-b)
                if [ $# -lt 2 ] || [ -z "${2:-}" ]; then
                    echo "ERROR: $1 requires a branch name." >&2
                    exit 1
                fi
                init_branch="$2"
                shift 2
                ;;
            --branch=*)
                init_branch="${1#*=}"
                if [ -z "$init_branch" ]; then
                    echo "ERROR: --branch requires a branch name." >&2
                    exit 1
                fi
                shift 1
                ;;
            -*)
                echo "ERROR: Unexpected argument to init: $1" >&2
                exit 1
                ;;
            *)
                if [ "$positional_repo_set" -eq 0 ]; then
                    REPO_NAME="$(normalize_repo "$1")"
                    REPO_NAME_EXPLICIT=1
                    positional_repo_set=1
                    shift 1
                else
                    echo "ERROR: Unexpected argument to init: $1" >&2
                    exit 1
                fi
                ;;
        esac
    done

    REPO_URL="${AERIAL_REPO_URL_OVERRIDE:-https://github.com/${REPO_OWNER}/${REPO_NAME}.git}"

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
    local clone_branch="$DEFAULT_BRANCH"
    if [ -n "$init_branch" ]; then
        clone_branch="$init_branch"
        branch_name="$init_branch"
    fi

    export GIT_TERMINAL_PROMPT=0
    export GIT_CONFIG_COUNT=2
    export GIT_CONFIG_KEY_0="http.extraHeader"
    export GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}"
    export GIT_CONFIG_KEY_1="http.version"
    export GIT_CONFIG_VALUE_1="HTTP/1.1"

    local clone_out=""
    if ! clone_out=$(git clone --depth 20 --single-branch -b "$clone_branch" "$REPO_URL" "$scratch_dir" 2>&1); then
        echo "ERROR: Failed to clone ${REPO_URL} into scratch directory: ${clone_out}" >&2
        rm -rf "$scratch_dir"
        exit 1
    fi

    cd "$scratch_dir"
    if [ -z "$init_branch" ]; then
        git checkout -b "$branch_name" >/dev/null 2>&1
    fi
    git config user.name "Aerial"
    git config user.email "aerial@noreply.github.com"

    echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${branch_name}\",\"repo\":\"${REPO_NAME}\"}"
}

SCRATCH_DIR_CLEANUP=""

resolve_target_id() {
    if [ -n "${1:-}" ]; then echo "${1#discord-}"; return; fi
    if [ -n "${AERIAL_TARGET_ID:-}" ] && [[ "${AERIAL_TARGET_ID}" != *:* ]] && [[ "${AERIAL_TARGET_ID}" != heartbeat-* ]] && [[ "${AERIAL_TARGET_ID}" != dispatch-* ]]; then
        echo "${AERIAL_TARGET_ID#discord-}"
        return
    fi
    if [ -n "${DISCORD_THREAD_ID:-}" ]; then echo "${DISCORD_THREAD_ID#discord-}"; return; fi
    if [ -n "${DISCORD_CHANNEL_ID:-}" ]; then echo "${DISCORD_CHANNEL_ID#discord-}"; return; fi
    local tfile="${ANTIGRAVITY_APP_DATA_DIR:-}/brain/${ANTIGRAVITY_CONVERSATION_ID:-}/.system_generated/logs/transcript.jsonl"
    if [ -f "$tfile" ]; then
        local tid
        tid=$(head -n 1 "$tfile" 2>/dev/null | grep -oE 'thread_id: [0-9]{17,20}' | head -n 1 | awk '{print $2}' || true)
        [ -z "$tid" ] && tid=$(head -n 1 "$tfile" 2>/dev/null | grep -oE 'channel_id: [0-9]{17,20}' | head -n 1 | awk '{print $2}' || true)
        if [ -n "$tid" ]; then echo "$tid"; return; fi
    fi
    echo ""
}

resolve_vanadis_chunk_id() {
    if [ -n "${1:-}" ]; then echo "$1"; return; fi
    if [ -n "${VANADIS_CHUNK_ID:-}" ]; then echo "${VANADIS_CHUNK_ID}"; return; fi
    if [ -n "${AERIAL_VANADIS_CHUNK_ID:-}" ]; then echo "${AERIAL_VANADIS_CHUNK_ID}"; return; fi
    local tfile="${ANTIGRAVITY_APP_DATA_DIR:-}/brain/${ANTIGRAVITY_CONVERSATION_ID:-}/.system_generated/logs/transcript.jsonl"
    if [ -f "$tfile" ]; then
        local cid
        cid=$(grep -E '"type":[[:space:]]*"USER_INPUT"' "$tfile" 2>/dev/null | tail -n 1 | sed 's/.*<\/retrieved_memory>//' | grep -oE -- '(Chunk ID:|--vanadis-chunk(=|[[:space:]]+)|- vanadis_chunk_id:)[[:space:]]*[^\\"[:space:]]+' | tail -n 1 | sed -E 's/^(Chunk ID:|--vanadis-chunk=|--vanadis-chunk|- vanadis_chunk_id:)[[:space:]]*//' || true)
        if [ -n "$cid" ]; then echo "$cid"; return; fi
    fi
    echo ""
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

register_pr_in_brain() {
    local brain_reg_url="$1"
    local reg_payload="$2"
    local pr_num="$3"
    local target_id="$4"
    local max_attempts="${AERIAL_PR_REGISTER_RETRIES:-3}"
    local delay="${AERIAL_PR_REGISTER_RETRY_DELAY:-1}"
    local attempt=1
    local reg_resp=""

    while [ "$attempt" -le "$max_attempts" ]; do
        reg_resp=$(curl -s --connect-timeout 2 -m 5 -X POST \
            -H "Content-Type: application/json" \
            -d "$reg_payload" \
            "$brain_reg_url" 2>/dev/null || true)
        if [ -n "$reg_resp" ] && echo "$reg_resp" | jq -e '.status == "registered"' >/dev/null 2>&1; then
            local resolved_target
            resolved_target=$(echo "$reg_resp" | jq -r '.target_id // empty' 2>/dev/null || true)
            echo "🗄️ [aerial-pr] Registered PR #${pr_num} in PostgreSQL pr_registry (target: ${resolved_target:-${target_id:-headless}})." >&2
            return 0
        fi
        if [ "$attempt" -lt "$max_attempts" ]; then
            echo "⚠️ [aerial-pr] Warning: PR #${pr_num} registration attempt ${attempt}/${max_attempts} failed (${brain_reg_url}): ${reg_resp:-unreachable}. Retrying in ${delay}s..." >&2
            if [ "$delay" -gt 0 ] 2>/dev/null; then
                sleep "$delay"
                delay=$((delay * 2))
            fi
        fi
        attempt=$((attempt + 1))
    done

    echo "❌ [aerial-pr] ERROR: Failed to register PR #${pr_num} in brain pr_registry (${brain_reg_url}) after ${max_attempts} attempts: ${reg_resp:-unreachable}" >&2
    return 1
}

PUSH_UPDATED_EXISTING=0

push_scratch_branch() {
    local branch="$1"
    PUSH_UPDATED_EXISTING=0
    if git push -u origin "$branch" >/dev/null 2>&1; then
        return 0
    fi
    git fetch origin "+refs/heads/${branch}:refs/remotes/origin/${branch}" >/dev/null 2>&1 || true
    local remote_sha
    remote_sha=$(git rev-parse "refs/remotes/origin/${branch}" 2>/dev/null || echo "")
    if { [ -n "$remote_sha" ] && git push --force-with-lease="refs/heads/${branch}:${remote_sha}" -u origin "$branch" >/dev/null 2>&1; } || \
       git push --force-with-lease -u origin "$branch" >/dev/null 2>&1; then
        PUSH_UPDATED_EXISTING=1
        echo "🔄 [aerial-pr] Updated existing remote branch ${branch} with --force-with-lease." >&2
        return 0
    fi
    return 1
}

lookup_existing_open_pr() {
    local branch="$1"
    curl -s --connect-timeout 3 -m 10 \
        -H "Authorization: token ${GITHUB_PAT}" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls?state=open&head=${REPO_OWNER}:${branch}" 2>/dev/null | jq -c '.[0] // empty' 2>/dev/null || true
}

format_aerial_pr_trailer() {
    local t_id="${1:-}" v_id="${2:-}"
    [ -z "$t_id" ] && [ -z "$v_id" ] && return 0
    printf "<!-- aerial-pr: %s -->" "$(jq -c -n --arg target_id "$t_id" --arg vanadis_chunk_id "$v_id" '{target_id: $target_id, vanadis_chunk_id: $vanadis_chunk_id}')"
}

rebase_staged_changes() {
    local target_branch="${1:-$DEFAULT_BRANCH}"
    local msg="${2:-chore: temporary rebase commit}"
    if ! git fetch origin "$target_branch" >/dev/null 2>&1; then
        echo "⚠️ [aerial-pr] Warning: Could not fetch origin/${target_branch}; proceeding with current base." >&2
        return 0
    fi
    local head_sha upstream_sha
    head_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
    upstream_sha=$(git rev-parse "origin/${target_branch}" 2>/dev/null || echo "")
    if [ -z "$upstream_sha" ] || [ "$head_sha" = "$upstream_sha" ]; then
        return 0
    fi
    local had_staged=0
    if ! git diff --cached --quiet; then
        had_staged=1
        git commit -q -m "$msg" >/dev/null 2>&1
    fi
    if ! git rebase "origin/${target_branch}" >/dev/null 2>&1; then
        git rebase --abort >/dev/null 2>&1 || true
        if [ "$had_staged" -eq 1 ]; then
            git reset --soft HEAD~1 >/dev/null 2>&1 || true
        fi
        echo "ERROR: Failed to rebase scratch branch onto origin/${target_branch} due to merge conflicts. Aborting submit." >&2
        if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
            SCRATCH_DIR_CLEANUP=""
        fi
        return 1
    fi
    git reset --soft "origin/${target_branch}" >/dev/null 2>&1
    echo "🔄 [aerial-pr] Rebased staged changes onto latest origin/${target_branch} (${upstream_sha:0:7})." >&2
    return 0
}

submit_scratch() {
    local target_id="$(resolve_target_id)"
    local scratch_dir=""
    local commit_msg=""
    local commit_msg_explicit=0
    local pr_body=""
    local pr_body_file=""
    local vanadis_chunk_id="$(resolve_vanadis_chunk_id)"

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
            --vanadis-chunk)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a chunk ID argument." >&2
                    exit 1
                fi
                vanadis_chunk_id="$2"
                shift 2
                ;;
            --vanadis-chunk=*)
                vanadis_chunk_id="${1#*=}"
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
            --target-id|--target|-t|--target-id=*|--target=*)
                echo "ERROR: Target ID cannot be specified manually ($1 is removed). Target ID is strictly inherited from the active Discord environment (AERIAL_TARGET_ID / DISCORD_THREAD_ID)." >&2
                exit 1
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
                        commit_msg_explicit=1
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
                    commit_msg_explicit=1
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

    local saved_pr_desc_file=""
    local saved_pr_desc_content=""
    if [ -s "${scratch_dir}/PR_DESCRIPTION.md" ]; then
        saved_pr_desc_file="${scratch_dir}/PR_DESCRIPTION.md"
        saved_pr_desc_content=$(cat "${scratch_dir}/PR_DESCRIPTION.md")
    elif [ -s "${scratch_dir}/.pr_description.md" ]; then
        saved_pr_desc_file="${scratch_dir}/.pr_description.md"
        saved_pr_desc_content=$(cat "${scratch_dir}/.pr_description.md")
    fi

    SCRATCH_DIR_CLEANUP="$scratch_dir"
    SAVED_PR_DESC_FILE="$saved_pr_desc_file"
    SAVED_PR_DESC_CONTENT="$saved_pr_desc_content"
    # On non-zero exit, restore convention file and preserve scratch workspace; only remove on exit 0
    trap '
        exit_status=$?
        if [ "$exit_status" -ne 0 ]; then
            if [ -n "${SAVED_PR_DESC_FILE:-}" ] && [ -d "${scratch_dir}" ] && [ ! -f "${SAVED_PR_DESC_FILE}" ]; then
                printf "%s\n" "${SAVED_PR_DESC_CONTENT}" > "${SAVED_PR_DESC_FILE}" 2>/dev/null || true
            fi
            if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
                echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
                SCRATCH_DIR_CLEANUP=""
            fi
        elif [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            rm -rf "${SCRATCH_DIR_CLEANUP}"
        fi
    ' EXIT INT TERM

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
    case "$REPO_NAME" in
        aerial)
            default_title="feat(core): automated update by Aerial"
            ;;
        aerial-config)
            default_title="chore(config): automated configuration update by Aerial"
            ;;
        aerial-sidecars)
            default_title="feat(sidecars): automated sidecars update by Aerial"
            ;;
        *)
            default_title="feat(${REPO_NAME}): automated update by Aerial"
            ;;
    esac

    # Resolve PR description from convention file in scratch workspace if not explicitly passed
    if [ -z "$pr_body" ] && [ -n "$saved_pr_desc_content" ]; then
        pr_body="$saved_pr_desc_content"
    fi

    # CRITICAL WORKSPACE HYGIENE: Always remove convention files from scratch workspace before staging
    # so they are NEVER staged by git add -A or permanently committed into git history on main!
    rm -f "${scratch_dir}/PR_DESCRIPTION.md" "${scratch_dir}/.pr_description.md"

    # If submitting on an existing PR branch without a local description, inherit metadata from the open PR
    local current_branch existing_pr=""
    current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    if [ -z "${pr_body//[[:space:]]/}" ] && [ -n "${GITHUB_PAT:-}" ] && [ -n "$current_branch" ] && [ "$current_branch" != "$DEFAULT_BRANCH" ] && [ "$current_branch" != "HEAD" ]; then
        existing_pr=$(lookup_existing_open_pr "$current_branch")
        if [ -n "$existing_pr" ]; then
            pr_body=$(echo "$existing_pr" | jq -r '.body // empty')
            if [ -z "${pr_body//[[:space:]]/}" ]; then
                pr_body=$(echo "$existing_pr" | jq -r '.title // "Automated update"')
            fi
            if [ "$commit_msg_explicit" -eq 0 ]; then
                local existing_title
                existing_title=$(echo "$existing_pr" | jq -r '.title // empty')
                if [ -n "$existing_title" ]; then
                    commit_msg="$existing_title"
                fi
            fi
        fi
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

    # 1. Stage all modifications (defensively unstage any convention files if previously cached)
    git add -A
    git rm --cached -f PR_DESCRIPTION.md .pr_description.md 2>/dev/null || true

    local has_committed_changes=0
    if git rev-parse --verify "origin/${DEFAULT_BRANCH}" >/dev/null 2>&1; then
        if [ "$(git rev-parse HEAD 2>/dev/null || echo "")" != "$(git rev-parse "origin/${DEFAULT_BRANCH}" 2>/dev/null || echo "")" ]; then
            has_committed_changes=1
            if [ "$commit_msg_explicit" -eq 0 ] && [ -z "$commit_msg" ]; then
                local prior_msg
                prior_msg=$(git log -1 --format=%B HEAD 2>/dev/null || true)
                if [ -n "${prior_msg//[[:space:]]/}" ]; then
                    commit_msg="$prior_msg"
                fi
            fi
        fi
    fi

    if [ -z "$commit_msg" ]; then
        commit_msg="$default_title"
    fi

    # Title Hygiene: Extract first non-empty line, strip \r, \n, and whitespace, clamp to 256 chars
    local clean_title
    clean_title=$(printf "%s\n" "$commit_msg" | awk 'NF {print; exit}' | tr -d '\r\n' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | cut -c 1-256)
    if [ -z "$clean_title" ]; then
        clean_title="$default_title"
    fi

    # 2. Check for modifications (either staged in index or already committed ahead of origin/DEFAULT_BRANCH)
    if git diff --cached --quiet && [ "$has_committed_changes" -eq 0 ]; then
        echo "{\"status\":\"no_changes\",\"message\":\"No modifications detected in scratch directory.\"}"
        exit 0
    fi

    local auth_header
    auth_header=$(build_auth_header)

    export GIT_TERMINAL_PROMPT=0
    export GIT_CONFIG_COUNT=2
    export GIT_CONFIG_KEY_0="http.extraHeader"
    export GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}"
    export GIT_CONFIG_KEY_1="http.version"
    export GIT_CONFIG_VALUE_1="HTTP/1.1"

    # 3. Rebase staged/committed branch changes onto latest origin/DEFAULT_BRANCH before pre-flight verification
    if ! rebase_staged_changes "$DEFAULT_BRANCH" "$commit_msg"; then
        exit 1
    fi
    if git diff --cached --quiet; then
        echo "{\"status\":\"no_changes\",\"message\":\"No modifications remaining after rebasing onto origin/${DEFAULT_BRANCH}.\"}"
        exit 0
    fi

    # 4. Pre-flight verification (universal contract: run repo-defined staged verify suite on combined tree)
    if ! run_preflight_verification "."; then
        exit 1
    fi

    # 5. Commit (preserves complete multi-line commit message in git history)
    git commit -m "$commit_msg" >/dev/null

    local branch
    branch=$(git rev-parse --abbrev-ref HEAD)
    local commit_sha
    commit_sha=$(git rev-parse HEAD)

    local pr_trailer
    pr_trailer=$(format_aerial_pr_trailer "$target_id" "$vanadis_chunk_id")
    if [ -n "$pr_trailer" ] && [[ "$pr_body" != *"<!-- aerial-pr:"* ]]; then
        pr_body="${pr_body}"$'\n\n'"${pr_trailer}"
    fi

    # 6. Push branch to remote
    echo "📤 [aerial-pr] Pushing feature branch ${branch} to origin (${REPO_OWNER}/${REPO_NAME})..." >&2
    if ! push_scratch_branch "$branch"; then
        echo "ERROR: Failed to push branch ${branch} to GitHub." >&2
        if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
            SCRATCH_DIR_CLEANUP=""
        fi
        exit 1
    fi

    # 7. Open Pull Request via GitHub REST API safely using jq (or reuse existing open PR for branch)
    local pr_code="" pr_resp=""
    if [ -n "$existing_pr" ]; then
        pr_resp="$existing_pr"
        pr_code="200"
        echo "🔄 [aerial-pr] Reusing existing open Pull Request for branch ${branch}." >&2
    else
        local pr_payload pr_raw
        pr_payload=$(jq -n \
            --arg title "$clean_title" \
            --arg head "$branch" \
            --arg base "$DEFAULT_BRANCH" \
            --arg body "$pr_body" \
            '{title: $title, head: $head, base: $base, body: $body}')

        pr_raw=$(curl -s --connect-timeout 3 -m 10 -w "\n%{http_code}" -X POST \
            -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" \
            "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls" \
            -d "$pr_payload")

        pr_code=$(echo "$pr_raw" | tail -n1)
        pr_resp=$(echo "$pr_raw" | sed '$d')

        if [ "$pr_code" = "422" ]; then
            existing_pr=$(lookup_existing_open_pr "$branch")
            if [ -n "$existing_pr" ]; then
                pr_resp="$existing_pr"
                pr_code="200"
                echo "🔄 [aerial-pr] Reusing existing open Pull Request for branch ${branch}." >&2
            fi
        fi
    fi

    if [ "$pr_code" -lt 200 ] || [ "$pr_code" -ge 300 ]; then
        echo "ERROR: Failed to create Pull Request (HTTP ${pr_code}): ${pr_resp}" >&2
        # Prune remote branch only when it was newly created (never delete an existing remote PR branch)
        if [ "${PUSH_UPDATED_EXISTING:-0}" -eq 0 ] && [ -z "${existing_pr:-}" ]; then
            curl -s --connect-timeout 3 -m 10 -X DELETE \
                -H "Authorization: token ${GITHUB_PAT}" \
                -H "Accept: application/vnd.github.v3+json" \
                "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
        fi
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

    # Register PR in PostgreSQL pr_registry via internal endpoint
    local brain_reg_url=""
    if [ -n "${AERIAL_PR_REGISTER_URL:-}" ]; then
        if [[ "${AERIAL_PR_REGISTER_URL}" == */internal/pr/register ]]; then
            brain_reg_url="${AERIAL_PR_REGISTER_URL}"
        else
            brain_reg_url="${AERIAL_PR_REGISTER_URL%/}/internal/pr/register"
        fi
    else
        # Probe candidate endpoints for webhooks-router or Eri/Brain's /internal/pr/register handler
        for cand in "http://webhooks-router.aerial" "http://127.0.0.1:4020" "http://127.0.0.1:8088" "http://eri.aerial" "http://brain.aerial" "http://host.docker.internal:8088" "http://127.0.0.1:8080" "http://aerial-brain:8080"; do
            local probe_code
            probe_code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 1 -m 2 -X POST "${cand}/internal/pr/register" 2>/dev/null || true)
            if [ "$probe_code" = "400" ] || [ "$probe_code" = "200" ]; then
                brain_reg_url="${cand}/internal/pr/register"
                break
            fi
        done
        if [ -z "$brain_reg_url" ]; then
            brain_reg_url="http://webhooks-router.aerial/internal/pr/register"
        fi
    fi

    local reg_payload
    reg_payload=$(jq -n \
        --arg repo "${REPO_NAME}" \
        --argjson pr_num "$pr_num" \
        --arg branch "$branch" \
        --arg head_sha "$commit_sha" \
        --arg target_id "$target_id" \
        --arg title "$clean_title" \
        --arg vanadis_chunk_id "$vanadis_chunk_id" \
        '{repo: $repo, pr_number: $pr_num, branch: $branch, head_sha: $head_sha, target_id: $target_id, title: $title, metadata: "{}", vanadis_chunk_id: $vanadis_chunk_id}')

    if ! register_pr_in_brain "$brain_reg_url" "$reg_payload" "$pr_num" "$target_id"; then
        if [ -n "$pr_trailer" ]; then
            echo "⚠️ [aerial-pr] Warning: Brain registration unreachable; relying on webhooks-router PR trailer self-healing for PR #${pr_num}." >&2
        else
            if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
                echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
                SCRATCH_DIR_CLEANUP=""
            fi
            exit 1
        fi
    fi

    # Asynchronous Submission: Disarm cleanup trap and remove scratch workspace immediately
    if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
        rm -rf "${SCRATCH_DIR_CLEANUP}"
        SCRATCH_DIR_CLEANUP=""
    fi

    cat <<EOF >&2
================================================================================
📝 [aerial-pr] Created Pull Request #${pr_num}: ${pr_url}
⚡ [aerial-pr] Asynchronous submission active. Continuous delivery automated via GitHub push events.
🛑 MANDATORY TURN ACTION: Do not poll CI in the foreground. End active execution turn now.
================================================================================
EOF

    echo "{\"status\":\"submitted\",\"pr_url\":\"${pr_url}\",\"pr_number\":${pr_num},\"branch\":\"${branch}\",\"commit_sha\":\"${commit_sha}\",\"async\":true,\"repo\":\"${REPO_NAME}\",\"turn_action\":\"end_turn\",\"message\":\"PR #${pr_num} submitted on ${REPO_OWNER}/${REPO_NAME}. Continuous delivery automated via push events. End active turn now.\"}"
    return 0
}

override_roborev() {
    local pr_num=""
    local rebuttal=""
    local reason_file=""

    while [ $# -gt 0 ]; do
        case "$1" in
            --repo|-r)
                [ $# -ge 2 ] || { echo "ERROR: $1 requires a repository argument." >&2; exit 1; }
                REPO_NAME="$(normalize_repo "$2")"
                REPO_NAME_EXPLICIT=1
                shift 2
                ;;
            --repo=*)
                REPO_NAME="$(normalize_repo "${1#*=}")"
                REPO_NAME_EXPLICIT=1
                shift 1
                ;;
            -m|--reason)
                [ $# -ge 2 ] || { echo "ERROR: $1 requires a rebuttal argument." >&2; exit 1; }
                rebuttal="$2"
                shift 2
                ;;
            --reason=*)
                rebuttal="${1#*=}"
                shift 1
                ;;
            -f|--reason-file)
                [ $# -ge 2 ] || { echo "ERROR: $1 requires a file path argument." >&2; exit 1; }
                reason_file="$2"
                shift 2
                ;;
            --reason-file=*)
                reason_file="${1#*=}"
                shift 1
                ;;
            *)
                if [ -z "$pr_num" ]; then
                    pr_num="$1"
                elif [ -z "$rebuttal" ] && [ -z "$reason_file" ]; then
                    rebuttal="$1"
                else
                    echo "ERROR: Unexpected argument: $1" >&2
                    exit 1
                fi
                shift 1
                ;;
        esac
    done

    if [ -z "$pr_num" ] || ! [[ "$pr_num" =~ ^[0-9]+$ ]]; then
        echo "ERROR: Valid numeric pr_num required for roborev override." >&2
        exit 1
    fi

    if [ -n "$reason_file" ]; then
        [ -f "$reason_file" ] || { echo "ERROR: Rebuttal file not found: $reason_file" >&2; exit 1; }
        rebuttal="$(cat "$reason_file")"
    fi

    if [ -z "${rebuttal//[[:space:]]/}" ]; then
        echo "ERROR: Technical rebuttal cannot be empty." >&2
        exit 1
    fi

    if [ -z "${GITHUB_PAT:-}" ]; then
        echo "ERROR: GITHUB_PAT environment variable must be set." >&2
        exit 1
    fi

    local remote_url
    remote_url=$(git remote get-url origin 2>/dev/null || echo "")
    extract_remote_repo "$remote_url"
    if [ -n "${PARSED_REPO:-}" ]; then
        REPO_OWNER="${PARSED_OWNER:-$REPO_OWNER}"
        if [ "$REPO_NAME_EXPLICIT" -eq 0 ]; then
            REPO_NAME="$(normalize_repo "$PARSED_REPO")"
        fi
    fi

    gh_api_call() {
        curl -s --connect-timeout 3 -m 15 --retry 3 --retry-delay 2 --retry-connrefused -w "\n%{http_code}" \
            -H "Authorization: token ${GITHUB_PAT}" \
            -H "Accept: application/vnd.github.v3+json" "$@"
    }

    # 1. Fetch Pull Request details
    local pr_raw pr_code pr_resp
    pr_raw=$(gh_api_call -X GET "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}")
    pr_code=$(echo "$pr_raw" | tail -n1)
    pr_resp=$(echo "$pr_raw" | sed '$d')

    if [ -z "$pr_code" ] || [ "$pr_code" -lt 200 ] || [ "$pr_code" -ge 300 ]; then
        echo "ERROR: Failed to query PR #${pr_num} on ${REPO_OWNER}/${REPO_NAME} (HTTP ${pr_code}): ${pr_resp}" >&2
        exit 1
    fi

    local pr_state
    pr_state=$(echo "$pr_resp" | jq -r '.state // empty')
    if [ "$pr_state" = "closed" ]; then
        echo "ERROR: Pull Request #${pr_num} is closed. Cannot override RoboRev on closed PR." >&2
        exit 1
    fi

    local head_sha short_sha pr_url
    head_sha=$(echo "$pr_resp" | jq -r '.head.sha // empty')
    short_sha="${head_sha:0:7}"
    pr_url=$(echo "$pr_resp" | jq -r '.html_url // empty')

    if [ -z "$head_sha" ]; then
        echo "ERROR: Could not resolve head commit SHA for PR #${pr_num}." >&2
        exit 1
    fi

    # 2. Post override comment on PR issue
    local comment_body comment_payload comment_raw comment_code
    comment_body=$(cat <<EOF
## roborev-override (\`${short_sha}\`)

**Commit:** \`${head_sha}\`
**Technical Rebuttal:**
${rebuttal}
EOF
)
    comment_payload=$(jq -n --arg b "$comment_body" '{body: $b}')
    comment_raw=$(gh_api_call -H "Content-Type: application/json" -X POST -d "$comment_payload" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/issues/${pr_num}/comments")
    comment_code=$(echo "$comment_raw" | tail -n1)

    if [ -z "$comment_code" ] || [ "$comment_code" -lt 200 ] || [ "$comment_code" -ge 300 ]; then
        local comment_resp
        comment_resp=$(echo "$comment_raw" | sed '$d')
        echo "ERROR: Failed to post override comment on PR #${pr_num} (HTTP ${comment_code}): ${comment_resp}" >&2
        exit 1
    fi

    # 3. Locate single latest RoboRev workflow run for head_sha
    local runs_raw runs_code runs_resp run_id
    runs_raw=$(gh_api_call -X GET "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs?head_sha=${head_sha}")
    runs_code=$(echo "$runs_raw" | tail -n1)
    runs_resp=$(echo "$runs_raw" | sed '$d')

    if [ -z "$runs_code" ] || [ "$runs_code" -lt 200 ] || [ "$runs_code" -ge 300 ]; then
        echo "ERROR: Failed to query workflow runs for commit ${short_sha} (HTTP ${runs_code}): ${runs_resp}" >&2
        exit 1
    fi

    run_id=$(echo "$runs_resp" | jq -r '[.workflow_runs[]? | select(.name == "RoboRev Code Review" or (.path // "" | endswith("roborev.yml"))) | .id] | first // empty')
    if [ -z "$run_id" ]; then
        echo "ERROR: No RoboRev workflow run found for commit ${short_sha} on PR #${pr_num}." >&2
        exit 1
    fi

    # 4. Trigger workflow re-run (rerun-failed-jobs falling back to rerun)
    local rerun_raw rerun_code
    rerun_raw=$(gh_api_call -X POST "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun-failed-jobs")
    rerun_code=$(echo "$rerun_raw" | tail -n1)

    if [ -z "$rerun_code" ] || [ "$rerun_code" -lt 200 ] || [ "$rerun_code" -ge 300 ]; then
        echo "⚠️ rerun-failed-jobs returned HTTP ${rerun_code}; attempting fallback to full workflow rerun..." >&2
        rerun_raw=$(gh_api_call -X POST "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun")
        rerun_code=$(echo "$rerun_raw" | tail -n1)
        if [ -z "$rerun_code" ] || [ "$rerun_code" -lt 200 ] || [ "$rerun_code" -ge 300 ]; then
            local rerun_resp
            rerun_resp=$(echo "$rerun_raw" | sed '$d')
            echo "ERROR: Failed to rerun RoboRev workflow run ${run_id} (both rerun-failed-jobs and rerun failed, HTTP ${rerun_code}): ${rerun_resp}" >&2
            exit 1
        fi
    fi

    # 5. Output JSON deliverable
    local msg="RoboRev gate overridden for PR #${pr_num} (${short_sha}) on ${REPO_OWNER}/${REPO_NAME}. Re-run triggered."
    jq -n \
        --arg pr_url "$pr_url" \
        --argjson pr_number "$pr_num" \
        --arg commit_sha "$head_sha" \
        --argjson run_id "$run_id" \
        --arg repo "$REPO_NAME" \
        --arg message "$msg" \
        '{status: "overridden", pr_url: $pr_url, pr_number: $pr_number, commit_sha: $commit_sha, run_id: $run_id, repo: $repo, turn_action: "end_turn", message: $message}'
    return 0
}

# Testing hook: allow unit tests to source all functions without executing command dispatch
if [ "${REPO_TEST_MODE:-0}" = "1" ] || [ "${cmd:-}" = "--source-only" ]; then
    return 0 2>/dev/null || exit 0
fi

case "$cmd" in
    init)
        init_scratch "$@"
        ;;
    submit)
        submit_scratch "$@"
        ;;
    override-roborev|dispute-roborev)
        override_roborev "$@"
        ;;
    *)
        echo "Usage: $0 [--repo <name>] {init [repo] [-b|--branch <branch>]|submit [-b <body>|--body <body>|-f <file>|--body-file <file>] <scratch_dir> [commit_msg]|override-roborev [-m <rebuttal>|--reason <rebuttal>|-f <path>|--reason-file <path>] <pr_num> [rebuttal]}" >&2
        exit 1
        ;;
esac
