#!/usr/bin/env bash
set -euo pipefail

# scripts/aerial-pr.sh - Unified Monorepo PR Automation & Continuous Integration Verifier
# Safely clones repositories into ephemeral scratch space, enforces the universal
# verification contract (scripts/verify.sh --staged), creates Pull Requests,
# and supports RoboRev overrides. PR registration, attachment, context enrichment,
# auto-merge, and RoboRev rerun gates are handled event-driven by Synapse and Vanadis-Restate.

REPO_OWNER="azylman"
DEFAULT_REPO="aerial"
REPO_NAME="${AERIAL_REPO_NAME:-$DEFAULT_REPO}"
REPO_NAME_EXPLICIT=0
DEFAULT_BRANCH="main"

GITHUB_API_BASE="${GITHUB_API_URL:-https://api.github.com}"
GITHUB_PROXY_BASE="http://127.0.0.1:4028"
USE_LOOPBACK_GITHUB=0
if [ -z "${GITHUB_PAT:-}" ]; then
    if [ "${AERIAL_USE_LOOPBACK_GITHUB:-0}" = "1" ] || { [ "${REPO_TEST_MODE:-0}" != "1" ] && [ "$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 1 -m 2 "${GITHUB_PROXY_BASE}/healthz" 2>/dev/null || true)" = "200" ]; }; then
        USE_LOOPBACK_GITHUB=1
        GITHUB_API_BASE="${GITHUB_PROXY_BASE}/internal/github-api"
    fi
fi

normalize_repo() {
    local r="${1:-}"
    r="${r##*/}"
    r="${r%.git}"
    case "$r" in
        core|aerial) echo "aerial" ;;
        config|aerial-config) echo "aerial-config" ;;
        sidecars|aerial-sidecars) echo "aerial-sidecars" ;;
        *) echo "$r" ;;
    esac
}

extract_remote_repo() {
    local remote_url="${1:-}"
    PARSED_OWNER=""
    PARSED_REPO=""
    local clean_url="${remote_url%/}"
    clean_url="${clean_url%.git}"
    if [[ "$clean_url" =~ (github\.com|/internal/github-git)[:/]([^/]+)/([^/]+)$ ]]; then
        PARSED_OWNER="${BASH_REMATCH[2]}"
        PARSED_REPO="${BASH_REMATCH[3]}"
    fi
}

run_preflight_verification() {
    local target_dir="${1:-.}"
    local max_rule_bytes=23040 # 22.5 KB
    for rf in "${target_dir}/GEMINI.md" "${target_dir}/AGENTS.md"; do
        if [ -f "$rf" ]; then
            local size
            size=$(wc -c < "$rf" 2>/dev/null | tr -d '[:space:]')
            if [ -n "$size" ] && [ "$size" -gt "$max_rule_bytes" ]; then
                echo "ERROR: Rule file '$(basename "$rf")' in scratch workspace ($size bytes) exceeds 22.5 KB ceiling ($max_rule_bytes bytes)." >&2
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
            return 1
        fi
    fi
    return 0
}

build_auth_header() {
    [ "${USE_LOOPBACK_GITHUB:-0}" = "1" ] && { echo "loopback"; return 0; }
    local pat="${GITHUB_PAT:-}"
    [ -z "$pat" ] && { echo "ERROR: GITHUB_PAT environment variable is required." >&2; return 1; }
    printf "x-access-token:%s" "$pat" | base64 | tr -d '\r\n'
}

gh_api_call() {
    local auth_args=()
    [ -n "${GITHUB_PAT:-}" ] && auth_args=(-H "Authorization: token ${GITHUB_PAT}")
    curl -s --connect-timeout 3 -m 15 -w "\n%{http_code}" "${auth_args[@]}" -H "Accept: application/vnd.github.v3+json" "$@"
}

resolve_target_id() {
    local raw="${1:-${AERIAL_TARGET_ID:-${DISCORD_THREAD_ID:-${DISCORD_CHANNEL_ID:-}}}}"
    [[ "$raw" =~ ^[0-9]{17,20}$ ]] && echo "$raw" || true
}

resolve_vanadis_chunk_id() {
    if [ -n "${1:-}" ]; then echo "$1"; return; fi
    if [ -n "${VANADIS_CHUNK_ID_EXPLICIT:-}" ]; then echo "${VANADIS_CHUNK_ID_EXPLICIT}"; return; fi
    if [ "${AERIAL_NO_VANADIS_CHUNK:-0}" = "1" ]; then echo ""; return; fi
    if [ -n "${VANADIS_CHUNK_ID:-}" ]; then echo "${VANADIS_CHUNK_ID}"; return; fi
    if [ -n "${AERIAL_VANADIS_CHUNK_ID:-}" ]; then echo "${AERIAL_VANADIS_CHUNK_ID}"; return; fi
    echo ""
}

prune_stale_scratch_dirs() {
    find "$1" -maxdepth 1 -mindepth 1 -type d -name '*-scratch.*' -mmin +1440 -exec rm -rf {} + 2>/dev/null || true
}

VANADIS_CHUNK_ID_EXPLICIT=""
[ "${AERIAL_NO_VANADIS_CHUNK:-0}" = "1" ] && VANADIS_CHUNK_ID="" || VANADIS_CHUNK_ID="${AERIAL_VANADIS_CHUNK_ID:-${VANADIS_CHUNK_ID:-}}"
PRE_ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --repo|-r) [ $# -ge 2 ] || { echo "ERROR: $1 requires a repo name." >&2; exit 1; }; REPO_NAME="$(normalize_repo "$2")"; REPO_NAME_EXPLICIT=1; shift 2 ;;
        --repo=*) REPO_NAME="$(normalize_repo "${1#*=}")"; REPO_NAME_EXPLICIT=1; shift 1 ;;
        --vanadis-chunk) [ $# -ge 2 ] || { echo "ERROR: $1 requires a chunk ID." >&2; exit 1; }; VANADIS_CHUNK_ID="$2"; VANADIS_CHUNK_ID_EXPLICIT="$2"; shift 2 ;;
        --vanadis-chunk=*) VANADIS_CHUNK_ID="${1#*=}"; VANADIS_CHUNK_ID_EXPLICIT="${1#*=}"; shift 1 ;;
        *) PRE_ARGS+=("$1"); shift 1 ;;
    esac
done
set -- "${PRE_ARGS[@]}"

cmd="${1:-}"
[ -n "$cmd" ] && shift

init_scratch() {
    local init_branch="" positional_repo_set=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --repo|-r) [ $# -ge 2 ] || { echo "ERROR: $1 requires a repo name." >&2; exit 1; }; REPO_NAME="$(normalize_repo "$2")"; REPO_NAME_EXPLICIT=1; shift 2 ;;
            --repo=*) REPO_NAME="$(normalize_repo "${1#*=}")"; REPO_NAME_EXPLICIT=1; shift 1 ;;
            --branch|-b) [ $# -ge 2 ] && [ -n "${2:-}" ] || { echo "ERROR: $1 requires a branch name." >&2; exit 1; }; init_branch="$2"; shift 2 ;;
            --branch=*) init_branch="${1#*=}"; [ -n "$init_branch" ] || { echo "ERROR: --branch requires a branch name." >&2; exit 1; }; shift 1 ;;
            --vanadis-chunk) [ $# -ge 2 ] || { echo "ERROR: $1 requires a chunk ID." >&2; exit 1; }; VANADIS_CHUNK_ID="$2"; VANADIS_CHUNK_ID_EXPLICIT="$2"; shift 2 ;;
            --vanadis-chunk=*) VANADIS_CHUNK_ID="${1#*=}"; VANADIS_CHUNK_ID_EXPLICIT="${1#*=}"; shift 1 ;;
            -*) echo "ERROR: Unexpected argument to init: $1" >&2; exit 1 ;;
            *)
                if [ "$positional_repo_set" -eq 0 ]; then
                    REPO_NAME="$(normalize_repo "$1")"; REPO_NAME_EXPLICIT=1; positional_repo_set=1; shift 1
                else
                    echo "ERROR: Unexpected argument to init: $1" >&2; exit 1
                fi
                ;;
        esac
    done

    local default_repo_url="https://github.com/${REPO_OWNER}/${REPO_NAME}.git"
    [ "${USE_LOOPBACK_GITHUB:-0}" = "1" ] && default_repo_url="${GITHUB_PROXY_BASE}/internal/github-git/${REPO_OWNER}/${REPO_NAME}.git"
    local repo_url="${AERIAL_REPO_URL_OVERRIDE:-$default_repo_url}" auth_header; auth_header=$(build_auth_header)

    local base_dir="${AERIAL_SCRATCH_BASE_DIR:-}"
    if [ -z "$base_dir" ]; then
        base_dir="/data/scratch"
        if [ ! -d "/data" ] || [ ! -w "/data" ]; then
            base_dir="/dev/shm"
            [ ! -d "$base_dir" ] || [ ! -w "$base_dir" ] && base_dir="${TMPDIR:-/tmp}"
        fi
    fi
    mkdir -p "$base_dir" && prune_stale_scratch_dirs "$base_dir"

    local vanadis_chunk_id target_id rand_suffix deterministic_dir="" branch_name="" clone_branch="${init_branch:-$DEFAULT_BRANCH}"
    vanadis_chunk_id="$(resolve_vanadis_chunk_id "${VANADIS_CHUNK_ID_EXPLICIT:-}")"
    target_id="$(resolve_target_id)"
    rand_suffix=$(head -c 16 /dev/urandom | md5sum | head -c 6)

    if [ -n "$init_branch" ]; then
        branch_name="$init_branch"
    elif [ -n "$vanadis_chunk_id" ]; then
        local safe_chunk_id="$(printf '%s' "$vanadis_chunk_id" | sed 's/[^a-zA-Z0-9_-]/_/g')"
        deterministic_dir="${base_dir}/vanadis-${safe_chunk_id}-${REPO_NAME}"
        [ -n "$target_id" ] && branch_name="feat/${REPO_NAME}-pr-vanadis-${safe_chunk_id}-t${target_id}-${rand_suffix}" || branch_name="feat/${REPO_NAME}-pr-vanadis-${safe_chunk_id}-${rand_suffix}"
    else
        [ -n "$target_id" ] && branch_name="feat/${REPO_NAME}-pr-t${target_id}-${rand_suffix}" || branch_name="feat/${REPO_NAME}-pr-$(date +%Y%m%d%H%M%S)-${rand_suffix}"
    fi

    local scratch_dir
    if [ -n "$deterministic_dir" ]; then
        scratch_dir="$deterministic_dir"
        if [ -d "${scratch_dir}/.git" ] && git -C "$scratch_dir" rev-parse --verify HEAD >/dev/null 2>&1; then
            rm -f "${scratch_dir}/.git/index.lock"
            local existing_branch="$(git -C "$scratch_dir" symbolic-ref --short HEAD 2>/dev/null || git -C "$scratch_dir" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")"
            if [ -n "$existing_branch" ] && [ "$existing_branch" != "HEAD" ] && [ "$existing_branch" != "$DEFAULT_BRANCH" ]; then
                echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${existing_branch}\",\"repo\":\"${REPO_NAME}\",\"reused\":true}"
                return 0
            fi
            git -C "$scratch_dir" checkout -b "$branch_name" >/dev/null 2>&1
            echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${branch_name}\",\"repo\":\"${REPO_NAME}\"}"
            return 0
        fi
        rm -rf "$scratch_dir"
    else
        scratch_dir=$(mktemp -d "${base_dir}/${REPO_NAME}-scratch.XXXXXX")
        chmod 700 "$scratch_dir"
    fi

    export GIT_TERMINAL_PROMPT=0 GIT_CONFIG_COUNT=2 GIT_CONFIG_KEY_0="http.extraHeader" GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}" GIT_CONFIG_KEY_1="http.version" GIT_CONFIG_VALUE_1="HTTP/1.1"
    local clone_out
    if ! clone_out=$(git clone --depth 20 --single-branch -b "$clone_branch" "$repo_url" "$scratch_dir" 2>&1); then
        echo "ERROR: Failed to clone ${repo_url} into scratch directory: ${clone_out}" >&2; rm -rf "$scratch_dir"; exit 1
    fi
    chmod 700 "$scratch_dir"
    git -C "$scratch_dir" config user.name "Aerial" && git -C "$scratch_dir" config user.email "aerial@noreply.github.com"
    if [ -z "$init_branch" ]; then
        git -C "$scratch_dir" checkout -b "$branch_name" >/dev/null 2>&1
    elif [ "$init_branch" != "$DEFAULT_BRANCH" ]; then
        git -C "$scratch_dir" config --add remote.origin.fetch "+refs/heads/${DEFAULT_BRANCH}:refs/remotes/origin/${DEFAULT_BRANCH}"
        git -C "$scratch_dir" fetch --depth 20 origin "+refs/heads/${DEFAULT_BRANCH}:refs/remotes/origin/${DEFAULT_BRANCH}" >/dev/null 2>&1 || true
    fi
    echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${branch_name}\",\"repo\":\"${REPO_NAME}\"}"
}

SCRATCH_DIR_CLEANUP=""
SAVED_PR_DESC_FILE=""
SAVED_PR_DESC_CONTENT=""
PUSH_UPDATED_EXISTING=0
REBASE_INCORPORATED_REMOTE_SHA=""

push_scratch_branch() {
    local branch="$1"
    PUSH_UPDATED_EXISTING=0
    git fetch origin "+refs/heads/${branch}:refs/remotes/origin/${branch}" >/dev/null 2>&1 && PUSH_UPDATED_EXISTING=1 || true
    git push -u origin "$branch" >/dev/null 2>&1 && return 0
    local remote_sha; remote_sha=$(git rev-parse --verify -q "refs/remotes/origin/${branch}" 2>/dev/null || echo "")
    if [ -n "$remote_sha" ]; then
        if ! git merge-base --is-ancestor "$remote_sha" HEAD 2>/dev/null && [ "$remote_sha" != "${REBASE_INCORPORATED_REMOTE_SHA:-}" ]; then
            echo "ERROR: Remote branch ${branch} has newer commits that are not present in local HEAD. Re-run aerial-pr.sh submit or rebase onto the updated remote branch before pushing." >&2
            return 1
        fi
        git push --force-with-lease="refs/heads/${branch}:${remote_sha}" -u origin "$branch" >/dev/null 2>&1 && { PUSH_UPDATED_EXISTING=1; return 0; }
    elif git push --force-with-lease -u origin "$branch" >/dev/null 2>&1; then
        PUSH_UPDATED_EXISTING=1
        return 0
    fi
    return 1
}

lookup_existing_open_pr() {
    local branch="$1" auth_args=()
    [ -n "${GITHUB_PAT:-}" ] && auth_args=(-H "Authorization: token ${GITHUB_PAT}")
    curl -s --connect-timeout 3 -m 10 "${auth_args[@]}" \
        -H "Accept: application/vnd.github.v3+json" \
        "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/pulls?state=open&head=${REPO_OWNER}:${branch}" 2>/dev/null | jq -c '.[0] // empty' 2>/dev/null || true
}

rebase_staged_changes() {
    local target_branch="${1:-$DEFAULT_BRANCH}" msg="${2:-chore: temporary rebase commit}"
    REBASE_INCORPORATED_REMOTE_SHA=""
    if ! git fetch origin "+refs/heads/${target_branch}:refs/remotes/origin/${target_branch}" >/dev/null 2>&1; then
        echo "⚠️ [aerial-pr] Warning: Could not fetch origin/${target_branch}; proceeding with current base." >&2
        return 0
    fi
    local head_sha upstream_sha current_branch remote_branch_sha=""
    head_sha=$(git rev-parse --verify -q HEAD 2>/dev/null || echo "")
    upstream_sha=$(git rev-parse --verify -q "refs/remotes/origin/${target_branch}" 2>/dev/null || echo "")
    [ -z "$upstream_sha" ] && return 0
    current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    if [ -n "$current_branch" ] && [ "$current_branch" != "$target_branch" ] && [ "$current_branch" != "HEAD" ]; then
        git fetch origin "+refs/heads/${current_branch}:refs/remotes/origin/${current_branch}" >/dev/null 2>&1 || true
        remote_branch_sha=$(git rev-parse --verify -q "refs/remotes/origin/${current_branch}" 2>/dev/null || echo "")
    fi
    if [ -n "$remote_branch_sha" ]; then
        if [ "$head_sha" = "$remote_branch_sha" ] && git diff --cached --quiet; then
            REBASE_INCORPORATED_REMOTE_SHA="$remote_branch_sha"
            return 0
        fi
        if git merge-base --is-ancestor "$remote_branch_sha" "$head_sha" 2>/dev/null; then
            [ "$head_sha" != "$remote_branch_sha" ] && git reset --soft "$remote_branch_sha" >/dev/null 2>&1
            if git merge-base --is-ancestor "$upstream_sha" "$remote_branch_sha" 2>/dev/null; then
                REBASE_INCORPORATED_REMOTE_SHA="$remote_branch_sha"
                return 0
            fi
            local temp_sha=""
            if ! git diff --cached --quiet; then
                git commit -q -m "$msg" >/dev/null 2>&1 && temp_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
                git reset --hard "$remote_branch_sha" >/dev/null 2>&1
            fi
            if git merge --no-edit -q "origin/${target_branch}" >/dev/null 2>&1 && \
               { [ -z "$temp_sha" ] || git cherry-pick -n "$temp_sha" >/dev/null 2>&1; }; then
                REBASE_INCORPORATED_REMOTE_SHA="$remote_branch_sha"
                echo "🔄 [aerial-pr] Merged latest origin/${target_branch} (${upstream_sha:0:7}) into ${current_branch}." >&2
                return 0
            fi
            git cherry-pick --abort >/dev/null 2>&1 || true
            git merge --abort >/dev/null 2>&1 || true
            [ -n "$temp_sha" ] && git reset --hard "$temp_sha" >/dev/null 2>&1 && git reset --soft "$remote_branch_sha" >/dev/null 2>&1
            echo "ERROR: Failed to merge origin/${target_branch} into ${current_branch} due to merge conflicts. Aborting submit." >&2
            return 1
        fi
        local fork_point="" temp_sha=""
        fork_point=$(git merge-base "$head_sha" "$remote_branch_sha" 2>/dev/null || echo "")
        [ -n "$fork_point" ] && [ "$head_sha" != "$fork_point" ] && git reset --soft "$fork_point" >/dev/null 2>&1
        if ! git diff --cached --quiet; then
            git commit -q -m "$msg" >/dev/null 2>&1 && temp_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
        fi
        git reset --hard "$remote_branch_sha" >/dev/null 2>&1
        if ! git merge-base --is-ancestor "$upstream_sha" HEAD 2>/dev/null; then
            if ! git merge --no-edit -q "origin/${target_branch}" >/dev/null 2>&1; then
                git merge --abort >/dev/null 2>&1 || true
                [ -n "$temp_sha" ] && git reset --hard "$temp_sha" >/dev/null 2>&1 && git reset --soft "$head_sha" >/dev/null 2>&1
                echo "ERROR: Failed to merge origin/${target_branch} into ${current_branch} due to merge conflicts. Aborting submit." >&2
                return 1
            fi
            echo "🔄 [aerial-pr] Merged latest origin/${target_branch} (${upstream_sha:0:7}) into ${current_branch}." >&2
        fi
        if [ -n "$temp_sha" ] && ! git cherry-pick -n "$temp_sha" >/dev/null 2>&1; then
            git cherry-pick --abort >/dev/null 2>&1 || true
            git reset --hard "$temp_sha" >/dev/null 2>&1 && git reset --soft "$head_sha" >/dev/null 2>&1
            echo "ERROR: Failed to apply staged changes onto updated remote branch ${current_branch} due to merge conflicts. Aborting submit." >&2
            return 1
        fi
        REBASE_INCORPORATED_REMOTE_SHA="$remote_branch_sha"
        return 0
    fi
    [ "$head_sha" = "$upstream_sha" ] && return 0
    local had_staged=0
    if ! git diff --cached --quiet; then
        had_staged=1
        git commit -q -m "$msg" >/dev/null 2>&1
    fi
    if ! git rebase "origin/${target_branch}" >/dev/null 2>&1; then
        git rebase --abort >/dev/null 2>&1 || true
        [ "$had_staged" -eq 1 ] && git reset --soft HEAD~1 >/dev/null 2>&1 || true
        echo "ERROR: Failed to rebase scratch branch onto origin/${target_branch} due to merge conflicts. Aborting submit." >&2
        return 1
    fi
    git reset --soft "origin/${target_branch}" >/dev/null 2>&1
    echo "🔄 [aerial-pr] Rebased staged changes onto latest origin/${target_branch} (${upstream_sha:0:7})." >&2
    return 0
}

submit_scratch() {
    local scratch_dir="" commit_msg="" commit_msg_explicit=0 pr_body="" pr_body_file=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --repo|-r) [ $# -ge 2 ] || { echo "ERROR: $1 requires a repo argument." >&2; exit 1; }; REPO_NAME="$(normalize_repo "$2")"; REPO_NAME_EXPLICIT=1; shift 2 ;;
            --repo=*) REPO_NAME="$(normalize_repo "${1#*=}")"; REPO_NAME_EXPLICIT=1; shift 1 ;;
            --vanadis-chunk) [ $# -ge 2 ] || { echo "ERROR: $1 requires a chunk ID." >&2; exit 1; }; shift 2 ;;
            --vanadis-chunk=*) shift 1 ;;
            --async|-a) shift ;;
            --sync) echo "ERROR: Synchronous submit mode has been eliminated. Submissions are strictly asynchronous." >&2; exit 1 ;;
            --target-id|--target|-t|--target-id=*|--target=*) echo "ERROR: Target ID cannot be specified manually ($1 is removed). Target ID is strictly inherited from the active Discord environment (AERIAL_TARGET_ID / DISCORD_THREAD_ID)." >&2; exit 1 ;;
            --body|-b) [ $# -ge 2 ] || { echo "ERROR: $1 requires a body string argument." >&2; exit 1; }; pr_body="$2"; shift 2 ;;
            --body=*|-b=*) pr_body="${1#*=}"; shift 1 ;;
            --body-file|-f|-F) [ $# -ge 2 ] || { echo "ERROR: $1 requires a file path argument." >&2; exit 1; }; pr_body_file="$2"; shift 2 ;;
            --body-file=*|-f=*|-F=*) pr_body_file="${1#*=}"; shift 1 ;;
            --)
                shift
                while [ $# -gt 0 ]; do
                    [ -z "$scratch_dir" ] && scratch_dir="$1" || { [ -z "$commit_msg" ] && commit_msg="$1" && commit_msg_explicit=1 || { echo "ERROR: Unexpected extra argument: $1" >&2; exit 1; }; }
                    shift
                done; break ;;
            -*) echo "ERROR: Unknown option: $1" >&2; exit 1 ;;
            *)
                [ -z "$scratch_dir" ] && scratch_dir="$1" || { [ -z "$commit_msg" ] && commit_msg="$1" && commit_msg_explicit=1 || { echo "ERROR: Unexpected extra argument: $1" >&2; exit 1; }; }
                shift ;;
        esac
    done

    [ -n "$pr_body" ] && [ -n "$pr_body_file" ] && { echo "ERROR: Cannot specify both --body and --body-file." >&2; exit 1; }
    [ -n "$scratch_dir" ] && [ -d "$scratch_dir" ] || { echo "ERROR: Valid scratch directory path required for submit." >&2; exit 1; }

    if [ -n "$pr_body_file" ]; then
        local r_file="$pr_body_file"
        [ ! -f "$r_file" ] && [ -f "${scratch_dir}/${r_file}" ] && r_file="${scratch_dir}/${r_file}"
        [ -r "$r_file" ] || { echo "ERROR: Specified body file does not exist or is not readable: $pr_body_file" >&2; exit 1; }
        pr_body=$(cat "$r_file")
    fi

    local saved_desc="" saved_content=""
    if [ -s "${scratch_dir}/PR_DESCRIPTION.md" ]; then
        saved_desc="${scratch_dir}/PR_DESCRIPTION.md"; saved_content=$(cat "$saved_desc")
    elif [ -s "${scratch_dir}/.pr_description.md" ]; then
        saved_desc="${scratch_dir}/.pr_description.md"; saved_content=$(cat "$saved_desc")
    fi

    SCRATCH_DIR_CLEANUP="$scratch_dir"; SAVED_PR_DESC_FILE="$saved_desc"; SAVED_PR_DESC_CONTENT="$saved_content"
    trap 'exit_status=$?; if [ "$exit_status" -ne 0 ]; then [ -n "${SAVED_PR_DESC_FILE:-}" ] && [ -d "${SCRATCH_DIR_CLEANUP:-}" ] && [ ! -f "${SAVED_PR_DESC_FILE}" ] && printf "%s\n" "${SAVED_PR_DESC_CONTENT}" > "${SAVED_PR_DESC_FILE}" 2>/dev/null || true; elif [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then rm -rf "${SCRATCH_DIR_CLEANUP}"; fi' EXIT INT TERM

    cd "$scratch_dir"
    [ -z "$(git config user.name 2>/dev/null || true)" ] && git config user.name "Aerial"
    [ -z "$(git config user.email 2>/dev/null || true)" ] && git config user.email "aerial@noreply.github.com"
    local remote_url; remote_url=$(git remote get-url origin 2>/dev/null || echo ""); extract_remote_repo "$remote_url"
    [ -n "${PARSED_REPO:-}" ] && REPO_OWNER="${PARSED_OWNER:-$REPO_OWNER}" && [ "$REPO_NAME_EXPLICIT" -eq 0 ] && REPO_NAME="$(normalize_repo "$PARSED_REPO")"
    local default_title="feat(${REPO_NAME}): automated update by Aerial"
    [ -z "$pr_body" ] && [ -n "$saved_content" ] && pr_body="$saved_content"
    rm -f "${scratch_dir}/PR_DESCRIPTION.md" "${scratch_dir}/.pr_description.md"

    local current_branch existing_pr="" pr_body_from_existing=0
    current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    if [ -z "${pr_body//[[:space:]]/}" ] && { [ -n "${GITHUB_PAT:-}" ] || [ "${USE_LOOPBACK_GITHUB:-0}" -eq 1 ]; } && [ -n "$current_branch" ] && [ "$current_branch" != "$DEFAULT_BRANCH" ] && [ "$current_branch" != "HEAD" ]; then
        existing_pr=$(lookup_existing_open_pr "$current_branch")
        if [ -n "$existing_pr" ]; then
            pr_body_from_existing=1
            pr_body=$(echo "$existing_pr" | jq -r '.body // empty'); [ -z "${pr_body//[[:space:]]/}" ] && pr_body=$(echo "$existing_pr" | jq -r '.title // "Automated update"')
            [ "$commit_msg_explicit" -eq 0 ] && local ex_title && ex_title=$(echo "$existing_pr" | jq -r '.title // empty') && [ -n "$ex_title" ] && commit_msg="$ex_title"
        fi
    fi

    local trimmed_body; trimmed_body=$(printf "%s" "$pr_body" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    [ -z "$trimmed_body" ] && { echo "ERROR: Pull Request description is mandatory. Provide a description via PR_DESCRIPTION.md in the workspace, or via --body-file / -b." >&2; exit 1; }
    [ "${#pr_body}" -gt 60000 ] && pr_body="${pr_body:0:60000}"$'\n\n> ⚠️ **[Aerial Notice]**: PR description was truncated because it exceeded 60,000 characters.'

    git add -A && git rm --cached -f PR_DESCRIPTION.md .pr_description.md 2>/dev/null || true
    local has_committed=0
    if git rev-parse --verify "origin/${DEFAULT_BRANCH}" >/dev/null 2>&1 && [ "$(git rev-parse HEAD 2>/dev/null || echo "")" != "$(git rev-parse "origin/${DEFAULT_BRANCH}" 2>/dev/null || echo "")" ]; then
        has_committed=1
        local base_ref="origin/${DEFAULT_BRANCH}"
        if [ -n "$current_branch" ] && git rev-parse --verify -q "refs/remotes/origin/${current_branch}" >/dev/null 2>&1; then
            base_ref="refs/remotes/origin/${current_branch}"
        fi
        if [ "$commit_msg_explicit" -eq 0 ] && [ "$(git rev-parse HEAD 2>/dev/null || echo "")" != "$(git rev-parse "$base_ref" 2>/dev/null || echo "")" ]; then
            local prior_msg; prior_msg=$(git log -1 --format=%B HEAD 2>/dev/null || true)
            [ -n "${prior_msg//[[:space:]]/}" ] && commit_msg="$prior_msg"
        fi
    fi

    [ -z "$commit_msg" ] && commit_msg="$default_title"
    local clean_title; clean_title=$(printf "%s\n" "$commit_msg" | awk 'NF {print; exit}' | tr -d '\r\n' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | cut -c 1-256)
    [ -z "$clean_title" ] && clean_title="$default_title"

    if git diff --cached --quiet && [ "$has_committed" -eq 0 ]; then
        echo "{\"status\":\"no_changes\",\"message\":\"No modifications detected in scratch directory.\"}"
        exit 0
    fi

    local auth_header; auth_header=$(build_auth_header)
    export GIT_TERMINAL_PROMPT=0 GIT_CONFIG_COUNT=2 GIT_CONFIG_KEY_0="http.extraHeader" GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}" GIT_CONFIG_KEY_1="http.version" GIT_CONFIG_VALUE_1="HTTP/1.1"

    rebase_staged_changes "$DEFAULT_BRANCH" "$commit_msg" || exit 1
    git diff --cached --quiet && { echo "{\"status\":\"no_changes\",\"message\":\"No modifications remaining after rebasing onto origin/${DEFAULT_BRANCH}.\"}"; exit 0; }
    run_preflight_verification "." || exit 1

    local full_commit_msg="$commit_msg"
    [ "$pr_body_from_existing" -eq 0 ] && [ -n "$pr_body" ] && [[ "$commit_msg" != *"$pr_body"* ]] && full_commit_msg="${clean_title}"$'\n\n'"${pr_body}"
    git commit -m "$full_commit_msg" >/dev/null

    local branch commit_sha; branch=$(git rev-parse --abbrev-ref HEAD); commit_sha=$(git rev-parse HEAD)
    echo "📤 [aerial-pr] Pushing feature branch ${branch} to origin (${REPO_OWNER}/${REPO_NAME})..." >&2
    push_scratch_branch "$branch" || { echo "ERROR: Failed to push branch ${branch} to GitHub." >&2; exit 1; }

    local pr_code="" pr_resp=""
    if [ -n "$existing_pr" ]; then
        pr_resp="$existing_pr"; pr_code="200"
        echo "🔄 [aerial-pr] Reusing existing open Pull Request for branch ${branch}." >&2
    else
        local pr_payload pr_raw auth_args=()
        pr_payload=$(jq -n --arg title "$clean_title" --arg head "$branch" --arg base "$DEFAULT_BRANCH" --arg body "$pr_body" '{title: $title, head: $head, base: $base, body: $body}')
        [ -n "${GITHUB_PAT:-}" ] && auth_args=(-H "Authorization: token ${GITHUB_PAT}")
        pr_raw=$(curl -s --connect-timeout 3 -m 10 -w "\n%{http_code}" -X POST "${auth_args[@]}" -H "Accept: application/vnd.github.v3+json" "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/pulls" -d "$pr_payload")
        pr_code=$(echo "$pr_raw" | tail -n1); pr_resp=$(echo "$pr_raw" | sed '$d')
        if [ "$pr_code" = "422" ]; then
            existing_pr=$(lookup_existing_open_pr "$branch")
            [ -n "$existing_pr" ] && pr_resp="$existing_pr" && pr_code="200" && echo "🔄 [aerial-pr] Reusing existing open Pull Request for branch ${branch}." >&2
        fi
    fi

    if [ "$pr_code" -lt 200 ] || [ "$pr_code" -ge 300 ]; then
        echo "ERROR: Failed to create Pull Request (HTTP ${pr_code}): ${pr_resp}" >&2
        if [ "${PUSH_UPDATED_EXISTING:-0}" -eq 0 ] && [ -z "${existing_pr:-}" ]; then
            local del_auth_args=()
            [ -n "${GITHUB_PAT:-}" ] && del_auth_args=(-H "Authorization: token ${GITHUB_PAT}")
            curl -s --connect-timeout 3 -m 10 -X DELETE "${del_auth_args[@]}" -H "Accept: application/vnd.github.v3+json" "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
        fi
        exit 1
    fi

    local pr_num pr_url; pr_num=$(echo "$pr_resp" | jq -r '.number'); pr_url=$(echo "$pr_resp" | jq -r '.html_url')
    [ -n "${SCRATCH_DIR_CLEANUP:-}" ] && rm -rf "${SCRATCH_DIR_CLEANUP}" && SCRATCH_DIR_CLEANUP=""

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
    local pr_num="" rebuttal="" reason_file=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --repo|-r) [ $# -ge 2 ] || { echo "ERROR: $1 requires a repo argument." >&2; exit 1; }; REPO_NAME="$(normalize_repo "$2")"; REPO_NAME_EXPLICIT=1; shift 2 ;;
            --repo=*) REPO_NAME="$(normalize_repo "${1#*=}")"; REPO_NAME_EXPLICIT=1; shift 1 ;;
            -m|--reason) [ $# -ge 2 ] || { echo "ERROR: $1 requires a rebuttal." >&2; exit 1; }; rebuttal="$2"; shift 2 ;;
            --reason=*) rebuttal="${1#*=}"; shift 1 ;;
            -f|--reason-file) [ $# -ge 2 ] || { echo "ERROR: $1 requires a file path." >&2; exit 1; }; reason_file="$2"; shift 2 ;;
            --reason-file=*) reason_file="${1#*=}"; shift 1 ;;
            *)
                if [ -z "$pr_num" ]; then pr_num="$1"
                elif [ -z "$rebuttal" ] && [ -z "$reason_file" ]; then rebuttal="$1"
                else echo "ERROR: Unexpected argument: $1" >&2; exit 1; fi
                shift 1 ;;
        esac
    done

    [ -z "$pr_num" ] || ! [[ "$pr_num" =~ ^[0-9]+$ ]] && { echo "ERROR: Valid numeric pr_num required for roborev override." >&2; exit 1; }
    [ -n "$reason_file" ] && { [ -f "$reason_file" ] || { echo "ERROR: Rebuttal file not found: $reason_file" >&2; exit 1; }; rebuttal="$(cat "$reason_file")"; }
    [ -z "${rebuttal//[[:space:]]/}" ] && { echo "ERROR: Technical rebuttal cannot be empty." >&2; exit 1; }
    [ -z "${GITHUB_PAT:-}" ] && [ "${USE_LOOPBACK_GITHUB:-0}" -ne 1 ] && { echo "ERROR: GITHUB_PAT environment variable must be set (or loopback proxy available)." >&2; exit 1; }

    local remote_url; remote_url=$(git remote get-url origin 2>/dev/null || echo ""); extract_remote_repo "$remote_url"
    [ -n "${PARSED_REPO:-}" ] && REPO_OWNER="${PARSED_OWNER:-$REPO_OWNER}" && [ "$REPO_NAME_EXPLICIT" -eq 0 ] && REPO_NAME="$(normalize_repo "$PARSED_REPO")"

    local pr_raw pr_code pr_resp; pr_raw=$(gh_api_call -X GET "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}")
    pr_code=$(echo "$pr_raw" | tail -n1); pr_resp=$(echo "$pr_raw" | sed '$d')
    [ -z "$pr_code" ] || [ "$pr_code" -lt 200 ] || [ "$pr_code" -ge 300 ] && { echo "ERROR: Failed to query PR #${pr_num} on ${REPO_OWNER}/${REPO_NAME} (HTTP ${pr_code}): ${pr_resp}" >&2; exit 1; }
    [ "$(echo "$pr_resp" | jq -r '.state // empty')" = "closed" ] && { echo "ERROR: Pull Request #${pr_num} is closed. Cannot override RoboRev on closed PR." >&2; exit 1; }

    local head_sha short_sha pr_url; head_sha=$(echo "$pr_resp" | jq -r '.head.sha // empty'); short_sha="${head_sha:0:7}"; pr_url=$(echo "$pr_resp" | jq -r '.html_url // empty')
    [ -z "$head_sha" ] && { echo "ERROR: Could not resolve head commit SHA for PR #${pr_num}." >&2; exit 1; }

    local comments_raw comments_code comments_resp existing_comment_id=""
    comments_raw=$(gh_api_call -X GET "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/issues/${pr_num}/comments?per_page=100")
    comments_code=$(echo "$comments_raw" | tail -n1); comments_resp=$(echo "$comments_raw" | sed '$d')
    [ -n "$comments_code" ] && [ "$comments_code" -ge 200 ] && [ "$comments_code" -lt 300 ] && \
        existing_comment_id=$(echo "$comments_resp" | jq -r --arg sha "${short_sha}" '[if type == "array" then .[] else empty end | objects | select((.body // "") | (contains("## roborev-override") and contains($sha))) | .id] | first // empty')

    if [ -z "$existing_comment_id" ]; then
        local comment_body comment_payload comment_raw comment_code
        comment_body=$(printf "## roborev-override (\`%s\`)\n\n**Commit:** \`%s\`\n**Technical Rebuttal:**\n%s\n" "$short_sha" "$head_sha" "$rebuttal")
        comment_payload=$(jq -n --arg b "$comment_body" '{body: $b}')
        comment_raw=$(gh_api_call -H "Content-Type: application/json" -X POST -d "$comment_payload" "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/issues/${pr_num}/comments")
        comment_code=$(echo "$comment_raw" | tail -n1)
        [ -z "$comment_code" ] || [ "$comment_code" -lt 200 ] || [ "$comment_code" -ge 300 ] && { echo "ERROR: Failed to post override comment on PR #${pr_num} (HTTP ${comment_code}): $(echo "$comment_raw" | sed '$d')" >&2; exit 1; }
    fi

    local runs_raw runs_code runs_resp target_run run_id run_status
    runs_raw=$(gh_api_call -X GET "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs?head_sha=${head_sha}")
    runs_code=$(echo "$runs_raw" | tail -n1); runs_resp=$(echo "$runs_raw" | sed '$d')
    [ -z "$runs_code" ] || [ "$runs_code" -lt 200 ] || [ "$runs_code" -ge 300 ] && { echo "ERROR: Failed to query workflow runs for commit ${short_sha} (HTTP ${runs_code}): ${runs_resp}" >&2; exit 1; }

    target_run=$(echo "$runs_resp" | jq -c '
        ([.workflow_runs[]? | select(.name == "RoboRev Code Review" or (.path // "" | endswith("roborev.yml")))] | first) //
        ([.workflow_runs[]? | select(.name == "Continuous Delivery" or .name == "Validate Configuration" or (.path // "" | (endswith("docker-publish.yml") or endswith("ci.yml") or endswith("validate.yml"))))] | first) //
        empty
    ')
    run_id=$(echo "$target_run" | jq -r '.id // empty'); run_status=$(echo "$target_run" | jq -r '.status // empty')
    [ -z "$run_id" ] && { echo "ERROR: No RoboRev workflow run found for commit ${short_sha} on PR #${pr_num}." >&2; exit 1; }

    if [ "$run_status" = "completed" ]; then
        local rerun_raw rerun_code
        rerun_raw=$(gh_api_call -X POST "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun-failed-jobs")
        rerun_code=$(echo "$rerun_raw" | tail -n1)
        if [ "$rerun_code" != "403" ] && [ "$rerun_code" != "409" ] && { [ -z "$rerun_code" ] || [ "$rerun_code" -lt 200 ] || [ "$rerun_code" -ge 300 ]; }; then
            rerun_raw=$(gh_api_call -X POST "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun")
            rerun_code=$(echo "$rerun_raw" | tail -n1)
            [ "$rerun_code" != "403" ] && [ "$rerun_code" != "409" ] && { [ -z "$rerun_code" ] || [ "$rerun_code" -lt 200 ] || [ "$rerun_code" -ge 300 ]; } && {
                echo "ERROR: Failed to rerun RoboRev workflow run ${run_id} (HTTP ${rerun_code}): $(echo "$rerun_raw" | sed '$d')" >&2; exit 1
            }
        fi
    fi

    local msg="RoboRev gate overridden for PR #${pr_num} (${short_sha}) on ${REPO_OWNER}/${REPO_NAME}. Re-run triggered."
    jq -n --arg pr_url "$pr_url" --argjson pr_number "$pr_num" --arg commit_sha "$head_sha" --argjson run_id "$run_id" --arg repo "$REPO_NAME" --arg message "$msg" \
        '{status: "overridden", pr_url: $pr_url, pr_number: $pr_number, commit_sha: $commit_sha, run_id: $run_id, repo: $repo, turn_action: "end_turn", message: $message}'
    return 0
}

if [ "${REPO_TEST_MODE:-0}" = "1" ] || [ "${cmd:-}" = "--source-only" ]; then
    return 0 2>/dev/null || exit 0
fi

case "$cmd" in
    init) init_scratch "$@" ;;
    submit) submit_scratch "$@" ;;
    override-roborev|dispute-roborev) override_roborev "$@" ;;
    *)
        echo "Usage: $0 [--repo <name>] {init [repo] [-b|--branch <branch>]|submit [-b <body>|--body <body>|-f <file>|--body-file <file>] <scratch_dir> [commit_msg]|override-roborev [-m <rebuttal>|--reason <rebuttal>|-f <path>|--reason-file <path>] <pr_num> [rebuttal]}" >&2
        exit 1
        ;;
esac
