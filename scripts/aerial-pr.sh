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

GITHUB_API_BASE="${GITHUB_API_URL:-https://api.github.com}"
USE_LOOPBACK_GITHUB=0
if [ -z "${GITHUB_PAT:-}" ]; then
    if [ "${AERIAL_USE_LOOPBACK_GITHUB:-0}" = "1" ] || { [ "${REPO_TEST_MODE:-0}" != "1" ] && [ "$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 1 -m 2 "${AERIAL_INTERNAL_ERI_URL:-http://127.0.0.1:8088}/healthz" 2>/dev/null || true)" = "200" ]; }; then
        USE_LOOPBACK_GITHUB=1
        GITHUB_API_BASE="${AERIAL_INTERNAL_ERI_URL:-http://127.0.0.1:8088}/internal/github-api"
    fi
fi

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
    if [[ "$clean_url" =~ (github\.com|/internal/github-git)[:/]([^/]+)/([^/]+)$ ]]; then
        PARSED_OWNER="${BASH_REMATCH[2]}"
        PARSED_REPO="${BASH_REMATCH[3]}"
    fi
}

preserve_scratch_workspace() {
    if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
        if [ -d "${SCRATCH_DIR_CLEANUP}/.git" ]; then
            touch "${SCRATCH_DIR_CLEANUP}/.git/.aerial-preserved" 2>/dev/null || true
        elif [ -d "${SCRATCH_DIR_CLEANUP}" ]; then
            touch "${SCRATCH_DIR_CLEANUP}.preserved" 2>/dev/null || true
        fi
        echo "💾 [aerial-pr] Scratch workspace preserved at: ${SCRATCH_DIR_CLEANUP}" >&2
        SCRATCH_DIR_CLEANUP=""
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
                preserve_scratch_workspace
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
            preserve_scratch_workspace
            return 1
        fi
    fi
    return 0
}

# Pre-dispatch global flag parsing (allows --repo and --vanadis-chunk before or after command)
VANADIS_CHUNK_ID_EXPLICIT=""
if [ "${AERIAL_NO_VANADIS_CHUNK:-0}" = "1" ]; then
    VANADIS_CHUNK_ID=""
else
    VANADIS_CHUNK_ID="${AERIAL_VANADIS_CHUNK_ID:-${VANADIS_CHUNK_ID:-}}"
fi
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
            VANADIS_CHUNK_ID_EXPLICIT="$2"
            shift 2
            ;;
        --vanadis-chunk=*)
            VANADIS_CHUNK_ID="${1#*=}"
            VANADIS_CHUNK_ID_EXPLICIT="${1#*=}"
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
    if [ "${USE_LOOPBACK_GITHUB:-0}" = "1" ]; then
        echo "loopback"
        return 0
    fi
    local pat="${GITHUB_PAT:-}"
    if [ -z "$pat" ]; then
        echo "ERROR: GITHUB_PAT environment variable is required." >&2
        return 1
    fi
    printf "x-access-token:%s" "$pat" | base64 | tr -d '\r\n'
}

is_scratch_dir_abandoned() {
    local d="$1"
    [ -d "$d" ] || return 1
    [ ! -f "${d}/.git/.aerial-preserved" ] && [ ! -f "${d}.preserved" ] && [ ! -f "${d}/.aerial-preserved" ] || return 1
    if [ -n "$(find "$d" -mmin -1440 -print -quit 2>/dev/null)" ]; then
        return 1
    fi
    if [ -d "${d}/.git" ]; then
        if [ -n "$(git -C "$d" status --porcelain 2>/dev/null)" ]; then
            return 1
        fi
        if ! git -C "$d" rev-parse --verify HEAD >/dev/null 2>&1; then
            return 0
        fi
        local head_sha base_sha
        head_sha="$(git -C "$d" rev-parse HEAD 2>/dev/null || echo "")"
        base_sha="$(git -C "$d" rev-parse "origin/${DEFAULT_BRANCH}" 2>/dev/null || echo "")"
        if [ -n "$head_sha" ] && [ -n "$base_sha" ] && [ "$head_sha" != "$base_sha" ]; then
            return 1
        fi
    fi
    return 0
}

is_git_lock_active() {
    local repo_dir="$1"
    local lock_file="$2"
    local real_dir real_lock
    real_dir="$(readlink -f "$repo_dir" 2>/dev/null || printf '%s' "$repo_dir")"
    real_lock="$(readlink -f "$lock_file" 2>/dev/null || printf '%s' "$lock_file")"

    if [ -n "$(find /proc/[0-9]*/fd -maxdepth 1 \( -lname "$real_lock" -o -lname "$lock_file" \) -print -quit 2>/dev/null)" ]; then
        return 0
    fi

    local first_line=""
    first_line="$(head -n 1 "$lock_file" 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ "$first_line" =~ ^[0-9]+$ ]] && kill -0 "$first_line" 2>/dev/null; then
        return 0
    fi

    local pid proc_cwd cmdline
    while IFS= read -r pid; do
        [ -n "$pid" ] || continue
        [ "$pid" = "$$" ] || [ "$pid" = "${BASHPID:-}" ] && continue
        proc_cwd="$(readlink -f "/proc/${pid}/cwd" 2>/dev/null || true)"
        if [ "$proc_cwd" = "$real_dir" ] || [[ "$proc_cwd" == "${real_dir}/"* ]]; then
            return 0
        fi
        cmdline="$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null || true)"
        if [[ "$cmdline" == *"$real_dir"* ]] || [[ "$cmdline" == *"$repo_dir"* ]]; then
            return 0
        fi
    done < <(pgrep -f '^git([-[:space:]]|$)|/git([-[:space:]]|$)' 2>/dev/null || true)

    return 1
}

recover_stale_git_locks() {
    local repo_dir="$1"
    [ -d "${repo_dir}/.git" ] || return 0

    local lock_file removed_any=0 active_any=0
    while IFS= read -r -d '' lock_file; do
        if is_git_lock_active "$repo_dir" "$lock_file"; then
            active_any=1
        else
            rm -f "$lock_file" 2>/dev/null || true
            removed_any=1
        fi
    done < <(find "${repo_dir}/.git" -name '*.lock' -type f -print0 2>/dev/null)

    if [ "$removed_any" -eq 1 ] && [ "$active_any" -eq 0 ]; then
        if ! git -C "$repo_dir" status --porcelain >/dev/null 2>&1; then
            rm -f "${repo_dir}/.git/index" 2>/dev/null || true
            git -C "$repo_dir" read-tree HEAD >/dev/null 2>&1 || true
        fi
    fi
}

recover_interrupted_publication() {
    local target_dir="$1"
    local backup_dir="${target_dir}.bak"
    local publish_marker="${target_dir}.publish"
    local pending_stage=""

    if [ -f "$publish_marker" ]; then
        pending_stage="$(cat "$publish_marker" 2>/dev/null || echo "")"
    fi

    if [ ! -d "$target_dir" ]; then
        local active_backup=""
        if [ -d "$backup_dir" ]; then
            active_backup="$backup_dir"
        else
            local b
            for b in "${target_dir}.bak."*; do
                if [ -d "$b" ]; then
                    active_backup="$b"
                    break
                fi
            done
        fi

        if [ -n "$pending_stage" ] && [ -d "${pending_stage}/.git" ] && git -C "$pending_stage" rev-parse --verify HEAD >/dev/null 2>&1; then
            local copy_ok=1
            if [ -n "$active_backup" ]; then
                local item
                while IFS= read -r -d '' item; do
                    if ! cp -a "$item" "$pending_stage/" 2>/dev/null; then
                        copy_ok=0
                        break
                    fi
                done < <(find "$active_backup" -mindepth 1 -maxdepth 1 ! -name '.git' -print0)
            fi
            if [ "$copy_ok" -eq 1 ] && mv "$pending_stage" "$target_dir" 2>/dev/null; then
                rm -rf "$backup_dir" "${target_dir}.bak."* "$publish_marker" 2>/dev/null || true
                return 0
            fi
        fi

        if [ -n "$active_backup" ]; then
            mv "$active_backup" "$target_dir" 2>/dev/null || true
        fi
    fi

    if [ -d "$target_dir" ]; then
        rm -rf "$backup_dir" "${target_dir}.bak."* "$publish_marker" 2>/dev/null || true
        if [ -n "$pending_stage" ] && [ "$pending_stage" != "$target_dir" ] && [ -d "$pending_stage" ]; then
            rm -rf "$pending_stage" 2>/dev/null || true
        fi
    fi
}

prune_stale_scratch_dirs() {
    local base_dir="$1"
    local d
    for d in "${base_dir}"/*-scratch.*; do
        [[ "$d" == *.lock || "$d" == *.preserved ]] && continue
        [ -d "$d" ] || continue
        (
            exec 8>"${d}.lock"
            if flock -n 8; then
                if is_scratch_dir_abandoned "$d"; then
                    rm -rf "$d" "${d}.lock" "${d}.preserved" 2>/dev/null || true
                fi
            fi
        ) 2>/dev/null || true
    done
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
            --vanadis-chunk)
                if [ $# -lt 2 ]; then
                    echo "ERROR: $1 requires a chunk ID argument." >&2
                    exit 1
                fi
                VANADIS_CHUNK_ID="$2"
                VANADIS_CHUNK_ID_EXPLICIT="$2"
                shift 2
                ;;
            --vanadis-chunk=*)
                VANADIS_CHUNK_ID="${1#*=}"
                VANADIS_CHUNK_ID_EXPLICIT="${1#*=}"
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

    local default_repo_url="https://github.com/${REPO_OWNER}/${REPO_NAME}.git"
    if [ "${USE_LOOPBACK_GITHUB:-0}" = "1" ]; then
        default_repo_url="${AERIAL_INTERNAL_ERI_URL:-http://127.0.0.1:8088}/internal/github-git/${REPO_OWNER}/${REPO_NAME}.git"
    fi
    REPO_URL="${AERIAL_REPO_URL_OVERRIDE:-$default_repo_url}"

    local auth_header
    auth_header=$(build_auth_header)

    local base_dir="${AERIAL_SCRATCH_BASE_DIR:-/data/scratch}"
    if [ -z "${AERIAL_SCRATCH_BASE_DIR:-}" ]; then
        if [ ! -d "/data" ] || [ ! -w "/data" ]; then
            base_dir="/dev/shm"
            if [ ! -d "$base_dir" ] || [ ! -w "$base_dir" ]; then
                base_dir="${TMPDIR:-/tmp}"
            fi
        fi
    fi
    mkdir -p "$base_dir"

    prune_stale_scratch_dirs "$base_dir"

    local branch_prefix="feat/${REPO_NAME}-pr"
    local rand_suffix
    rand_suffix=$(head -c 16 /dev/urandom | md5sum | head -c 6)
    local branch_name="${branch_prefix}-$(date +%Y%m%d%H%M%S)-${rand_suffix}"
    local clone_branch="$DEFAULT_BRANCH"
    if [ -n "$init_branch" ]; then
        clone_branch="$init_branch"
        branch_name="$init_branch"
    fi

    local vanadis_chunk_id="$(resolve_vanadis_chunk_id "${VANADIS_CHUNK_ID_EXPLICIT:-}")"
    local deterministic_dir=""
    local safe_chunk_id=""
    if [ -n "$vanadis_chunk_id" ] && [ -z "$init_branch" ]; then
        safe_chunk_id="$(printf '%s' "$vanadis_chunk_id" | sed 's/[^a-zA-Z0-9_-]/_/g')"
        deterministic_dir="${base_dir}/vanadis-${safe_chunk_id}-${REPO_NAME}"
    fi

    local scratch_dir
    local clone_target=""
    if [ -n "$deterministic_dir" ]; then
        scratch_dir="$deterministic_dir"
        exec 9>"${scratch_dir}.lock"
        flock 9
        recover_interrupted_publication "$scratch_dir"
        if [ -d "${scratch_dir}/.git" ]; then
            recover_stale_git_locks "$scratch_dir"
        fi
        if [ -d "${scratch_dir}/.git" ] && git -C "$scratch_dir" rev-parse --verify HEAD >/dev/null 2>&1; then
            cd "$scratch_dir"
            local existing_branch=""
            existing_branch="$(git symbolic-ref --short HEAD 2>/dev/null || git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")"
            if [ -f "${scratch_dir}/.git/.aerial-init-complete" ] && [ -n "$existing_branch" ] && [ "$existing_branch" != "HEAD" ] && [ "$existing_branch" != "$DEFAULT_BRANCH" ]; then
                git config user.name "Aerial"
                git config user.email "aerial@noreply.github.com"
                chmod 700 "$scratch_dir"
                exec 9>&- 2>/dev/null || true
                echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${existing_branch}\",\"repo\":\"${REPO_NAME}\",\"reused\":true}"
                return 0
            fi
            if [ -n "$existing_branch" ] && [ "$existing_branch" != "HEAD" ] && [ "$existing_branch" != "$DEFAULT_BRANCH" ]; then
                branch_name="$existing_branch"
            else
                git checkout -b "$branch_name" >/dev/null 2>&1
            fi
            git config user.name "Aerial"
            git config user.email "aerial@noreply.github.com"
            printf '%s\n' "$branch_name" > "${scratch_dir}/.git/.aerial-init-complete"
            chmod 700 "$scratch_dir"
            exec 9>&- 2>/dev/null || true
            echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${branch_name}\",\"repo\":\"${REPO_NAME}\"}"
            return 0
        fi
        rm -rf "${base_dir}/.init-${safe_chunk_id}-${REPO_NAME}."* 2>/dev/null || true
        clone_target=$(mktemp -d "${base_dir}/.init-${safe_chunk_id}-${REPO_NAME}.XXXXXX")
        chmod 700 "$clone_target"
    else
        local scratch_prefix="${REPO_NAME}-scratch"
        scratch_dir=$(mktemp -d "${base_dir}/${scratch_prefix}.XXXXXX")
        chmod 700 "$scratch_dir"
        clone_target="$scratch_dir"
    fi

    export GIT_TERMINAL_PROMPT=0
    export GIT_CONFIG_COUNT=2
    export GIT_CONFIG_KEY_0="http.extraHeader"
    export GIT_CONFIG_VALUE_0="AUTHORIZATION: basic ${auth_header}"
    export GIT_CONFIG_KEY_1="http.version"
    export GIT_CONFIG_VALUE_1="HTTP/1.1"

    local clone_out=""
    if ! clone_out=$(git clone --depth 20 --single-branch -b "$clone_branch" "$REPO_URL" "$clone_target" 2>&1); then
        echo "ERROR: Failed to clone ${REPO_URL} into scratch directory: ${clone_out}" >&2
        rm -rf "$clone_target"
        exec 9>&- 2>/dev/null || true
        exit 1
    fi

    cd "$clone_target"
    if [ -z "$init_branch" ]; then
        git checkout -b "$branch_name" >/dev/null 2>&1
    elif [ "$init_branch" != "$DEFAULT_BRANCH" ]; then
        git config --add remote.origin.fetch "+refs/heads/${DEFAULT_BRANCH}:refs/remotes/origin/${DEFAULT_BRANCH}"
        git fetch --depth 20 origin "+refs/heads/${DEFAULT_BRANCH}:refs/remotes/origin/${DEFAULT_BRANCH}" >/dev/null 2>&1 || true
    fi
    git config user.name "Aerial"
    git config user.email "aerial@noreply.github.com"

    if [ "$clone_target" != "$scratch_dir" ]; then
        local backup_dir="${scratch_dir}.bak"
        local publish_marker="${scratch_dir}.publish"
        rm -rf "$backup_dir" "$publish_marker" 2>/dev/null || true
        if [ -d "$scratch_dir" ]; then
            while IFS= read -r -d '' item; do
                if ! cp -a "$item" "$clone_target/"; then
                    echo "ERROR: Failed to copy existing workspace contents from ${scratch_dir} into initialized checkout." >&2
                    rm -rf "$clone_target"
                    exec 9>&- 2>/dev/null || true
                    exit 1
                fi
            done < <(find "$scratch_dir" -mindepth 1 -maxdepth 1 ! -name '.git' -print0)
            printf '%s\n' "$clone_target" > "$publish_marker"
            if ! mv "$scratch_dir" "$backup_dir"; then
                echo "ERROR: Failed to stage existing directory ${scratch_dir} for replacement." >&2
                rm -rf "$clone_target" "$publish_marker"
                exec 9>&- 2>/dev/null || true
                exit 1
            fi
        else
            printf '%s\n' "$clone_target" > "$publish_marker"
        fi
        if ! mv "$clone_target" "$scratch_dir"; then
            [ -d "$backup_dir" ] && mv "$backup_dir" "$scratch_dir" 2>/dev/null || true
            echo "ERROR: Failed to publish initialized checkout to ${scratch_dir}." >&2
            rm -rf "$clone_target" "$publish_marker"
            exec 9>&- 2>/dev/null || true
            exit 1
        fi
        rm -rf "$backup_dir" "$publish_marker"
        chmod 700 "$scratch_dir"
        cd "$scratch_dir"
    fi
    printf '%s\n' "$branch_name" > "${scratch_dir}/.git/.aerial-init-complete"
    exec 9>&- 2>/dev/null || true

    echo "{\"status\":\"initialized\",\"scratch_dir\":\"${scratch_dir}\",\"branch\":\"${branch_name}\",\"repo\":\"${REPO_NAME}\"}"
}

SCRATCH_DIR_CLEANUP=""

read_config_yaml_val() {
    local key="$1"
    local config_files=()
    if [ -n "${CONFIG_PATH:-}" ]; then
        config_files+=("${CONFIG_PATH}")
    else
        config_files+=("/local/config.yaml" "/share/aerial-config/config.yaml")
    fi

    for f in "${config_files[@]}"; do
        if [ -f "$f" ] && [ -r "$f" ]; then
            local val
            val=$(awk -v k="$key" '
                $0 ~ "^" k ":[ \t]*" {
                    sub("^" k ":[ \t]*", "")
                    sub(/[ \t]*#.*$/, "")
                    gsub(/^["\x27]|["\x27]$/, "")
                    print
                    exit
                }
            ' "$f" 2>/dev/null || true)
            if [ -n "$val" ]; then
                echo "$val"
                return 0
            fi
        fi
    done
    echo ""
}

resolve_target_id() {
    if [ -n "${1:-}" ]; then echo "$1"; return; fi
    if [ -n "${AERIAL_TARGET_ID:-}" ]; then echo "${AERIAL_TARGET_ID}"; return; fi
    if [ -n "${DISCORD_THREAD_ID:-}" ]; then echo "${DISCORD_THREAD_ID}"; return; fi
    if [ -n "${DISCORD_CHANNEL_ID:-}" ]; then echo "${DISCORD_CHANNEL_ID}"; return; fi
    echo ""
}

resolve_vanadis_chunk_id() {
    if [ -n "${1:-}" ]; then echo "$1"; return; fi
    if [ -n "${VANADIS_CHUNK_ID_EXPLICIT:-}" ]; then echo "${VANADIS_CHUNK_ID_EXPLICIT}"; return; fi
    if [ "${AERIAL_NO_VANADIS_CHUNK:-0}" = "1" ]; then echo ""; return; fi
    if [ -n "${VANADIS_CHUNK_ID:-}" ]; then echo "${VANADIS_CHUNK_ID}"; return; fi
    if [ -n "${AERIAL_VANADIS_CHUNK_ID:-}" ]; then echo "${AERIAL_VANADIS_CHUNK_ID}"; return; fi
    echo ""
}

resolve_vanadis_url() {
    local vanadis_base="${VANADIS_URL:-$(read_config_yaml_val vanadis_url)}"
    if [ -n "$vanadis_base" ]; then
        echo "${vanadis_base%/}"
        return 0
    fi
    return 1
}

attach_pr_in_vanadis() {
    local vanadis_url="$1"
    local attach_payload="$2"
    local pr_num="$3"
    local max_attempts="${AERIAL_VANADIS_ATTACH_RETRIES:-3}"
    local delay="${AERIAL_VANADIS_ATTACH_RETRY_DELAY:-1}"
    local attempt=1
    local attach_resp=""

    while [ "$attempt" -le "$max_attempts" ]; do
        attach_resp=$(curl -s --connect-timeout 2 -m 5 -X POST \
            -H "Content-Type: application/json" \
            -d "$attach_payload" \
            "${vanadis_url%/}/chunks/attach-pr" 2>/dev/null || true)
        if [ -n "$attach_resp" ] && echo "$attach_resp" | jq -e '.status == "attached"' >/dev/null 2>&1; then
            local resolved_thread
            resolved_thread=$(echo "$attach_resp" | jq -r '.thread_id // empty' 2>/dev/null || true)
            echo "🔗 [aerial-pr] Attached PR #${pr_num} to Vanadis (thread: ${resolved_thread:-none})." >&2
            echo "$attach_resp"
            return 0
        fi
        if [ "$attempt" -lt "$max_attempts" ]; then
            echo "⚠️ [aerial-pr] Warning: PR #${pr_num} Vanadis attachment attempt ${attempt}/${max_attempts} failed (${vanadis_url}): ${attach_resp:-unreachable}. Retrying in ${delay}s..." >&2
            if [ "$delay" -gt 0 ] 2>/dev/null; then
                sleep "$delay"
                delay=$((delay * 2))
            fi
        fi
        attempt=$((attempt + 1))
    done

    echo "❌ [aerial-pr] ERROR: Failed to attach PR #${pr_num} to Vanadis (${vanadis_url}) after ${max_attempts} attempts: ${attach_resp:-unreachable}" >&2
    return 1
}

verify_vanadis_chunk_active() {
    local chunk_id="${1:-}"
    if [ -z "$chunk_id" ]; then
        return 0
    fi
    local vanadis_base
    vanadis_base=$(resolve_vanadis_url 2>/dev/null || true)
    if [ -z "$vanadis_base" ]; then
        return 0
    fi
    local resp=""
    if resp=$(curl -sS --connect-timeout 2 --max-time 4 "${vanadis_base}/chunks/${chunk_id}" 2>/dev/null); then
        if [ -n "$resp" ] && echo "$resp" | jq -e '(.chunk.status == "cancelled" or .chunk.status == "merged" or .chunk.status == "deploying" or .chunk.status == "completed" or .build.status == "cancelled" or .build.status == "completed")' >/dev/null 2>&1; then
            echo "ERROR: Vanadis chunk ${chunk_id} (or parent build) is cancelled; aborting PR submission." >&2
            exit 1
        fi
    fi
    return 0
}

format_vanadis_chunk_context() {
    local chunk_id="${1:-}"
    if [ -z "$chunk_id" ]; then
        return 0
    fi

    local vanadis_base
    vanadis_base="$(resolve_vanadis_url 2>/dev/null || true)"
    if [ -z "$vanadis_base" ] && [ -n "${VANADIS_FALLBACK_URL:-}" ]; then
        vanadis_base="${VANADIS_FALLBACK_URL%/}"
    fi
    if [ -z "$vanadis_base" ]; then
        return 0
    fi

    local chunk_resp=""
    chunk_resp="$(curl -sS --connect-timeout 3 --max-time 5 "${vanadis_base}/chunks/${chunk_id}" 2>/dev/null || true)"
    if [ -z "$chunk_resp" ] && [ -n "${VANADIS_FALLBACK_URL:-}" ] && [ "$vanadis_base" != "${VANADIS_FALLBACK_URL%/}" ]; then
        chunk_resp="$(curl -sS --connect-timeout 3 --max-time 5 "${VANADIS_FALLBACK_URL%/}/chunks/${chunk_id}" 2>/dev/null || true)"
        if [ -n "$chunk_resp" ]; then
            vanadis_base="${VANADIS_FALLBACK_URL%/}"
        fi
    fi

    if [ -z "$chunk_resp" ] || ! echo "$chunk_resp" | jq -e '.chunk.id != null and .chunk.id != ""' >/dev/null 2>&1; then
        return 0
    fi

    local build_id
    build_id="$(echo "$chunk_resp" | jq -r '.chunk.build_id // .build.id // empty' 2>/dev/null || true)"

    local build_resp=""
    if [ -n "$build_id" ]; then
        build_resp="$(curl -sS --connect-timeout 3 --max-time 5 "${vanadis_base}/builds/${build_id}" 2>/dev/null || true)"
        if [ -z "$build_resp" ] && [ -n "${VANADIS_FALLBACK_URL:-}" ] && [ "$vanadis_base" != "${VANADIS_FALLBACK_URL%/}" ]; then
            build_resp="$(curl -sS --connect-timeout 3 --max-time 5 "${VANADIS_FALLBACK_URL%/}/builds/${build_id}" 2>/dev/null || true)"
        fi
    fi

    local chunks_json=""
    if [ -n "$build_resp" ] && echo "$build_resp" | jq -e '.chunks | type == "array" and length > 0' >/dev/null 2>&1; then
        chunks_json="$(echo "$build_resp" | jq -c '.chunks')"
    else
        chunks_json="$(echo "$chunk_resp" | jq -c '[.chunk]')"
    fi

    jq -r -n \
        --argjson chunk "$(echo "$chunk_resp" | jq -c '.chunk')" \
        --argjson build "$(echo "$chunk_resp" | jq -c '.build // {}')" \
        --argjson chunks "$chunks_json" '
        $chunk as $c |
        $build as $b |
        $chunks as $all_chunks |
        ($all_chunks | length) as $total |
        (($c.chunk_index // 0) + 1) as $idx |
        ([$c.target_files[]?] | join(", ")) as $tf |
        [
          "## Vanadis Build Context",
          "- **Build**: \($b.id // $c.build_id // "unknown") — \($b.title // "Untitled Build")",
          (if ($b.objective != null and ($b.objective | test("\\S"))) then "- **Build Objective**: \($b.objective | gsub("\r?\n"; " ") | gsub("^[[:space:]]+|[[:space:]]+$"; ""))" else empty end),
          "- **Active Chunk (\($idx) of \($total))**: \($c.id) — \($c.title // "Untitled Chunk")",
          (if ($tf != "") then "- **Active Chunk Target Files**: \($tf)" else empty end),
          "- **Build Chunks Roadmap**:",
          ($all_chunks[] | "- [Chunk \((.chunk_index // 0) + 1)/\($total)] \(.id): \(.title // "Untitled") (\(.mode // "unknown"), \(.status // "unknown"))"),
          "- **Scope Boundary (RoboRev & Review Guidance)**: This PR implements ONLY the active chunk listed above. Caller cutover, downstream lifecycle wiring, and legacy code removal assigned to downstream chunks in the roadmap are intentionally deferred to their respective chunks and must not be flagged as incomplete or dead wiring in this PR."
        ] | join("\n")
    ' 2>/dev/null || return 0
}

enable_github_auto_merge() {
    local node_id="$1"
    local pr_num="${2:-}"
    if [ -z "$node_id" ] || { [ -z "${GITHUB_PAT:-}" ] && [ "${USE_LOOPBACK_GITHUB:-0}" -ne 1 ]; }; then
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

    local auth_args=()
    if [ -n "${GITHUB_PAT:-}" ]; then
        auth_args=(-H "Authorization: token ${GITHUB_PAT}")
    fi

    local resp
    resp=$(curl -s --connect-timeout 3 -m 10 -X POST \
        "${auth_args[@]}" \
        -H "Content-Type: application/json" \
        "${GITHUB_API_BASE}/graphql" \
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
REBASE_INCORPORATED_REMOTE_SHA=""

push_scratch_branch() {
    local branch="$1"
    PUSH_UPDATED_EXISTING=0
    if git fetch origin "+refs/heads/${branch}:refs/remotes/origin/${branch}" >/dev/null 2>&1; then
        PUSH_UPDATED_EXISTING=1
    fi
    if git push -u origin "$branch" >/dev/null 2>&1; then
        return 0
    fi
    local remote_sha
    remote_sha=$(git rev-parse --verify -q "refs/remotes/origin/${branch}" 2>/dev/null || echo "")
    if [ -n "$remote_sha" ]; then
        if ! git merge-base --is-ancestor "$remote_sha" HEAD 2>/dev/null && [ "$remote_sha" != "${REBASE_INCORPORATED_REMOTE_SHA:-}" ]; then
            echo "ERROR: Remote branch ${branch} has newer commits that are not present in local HEAD. Re-run aerial-pr.sh submit or rebase onto the updated remote branch before pushing." >&2
            return 1
        fi
        if git push --force-with-lease="refs/heads/${branch}:${remote_sha}" -u origin "$branch" >/dev/null 2>&1; then
            PUSH_UPDATED_EXISTING=1
            echo "🔄 [aerial-pr] Updated existing remote branch ${branch} with --force-with-lease." >&2
            return 0
        fi
    elif git push --force-with-lease -u origin "$branch" >/dev/null 2>&1; then
        PUSH_UPDATED_EXISTING=1
        echo "🔄 [aerial-pr] Updated existing remote branch ${branch} with --force-with-lease." >&2
        return 0
    fi
    return 1
}

lookup_existing_open_pr() {
    local branch="$1"
    local auth_args=()
    if [ -n "${GITHUB_PAT:-}" ]; then
        auth_args=(-H "Authorization: token ${GITHUB_PAT}")
    fi
    curl -s --connect-timeout 3 -m 10 \
        "${auth_args[@]}" \
        -H "Accept: application/vnd.github.v3+json" \
        "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/pulls?state=open&head=${REPO_OWNER}:${branch}" 2>/dev/null | jq -c '.[0] // empty' 2>/dev/null || true
}

format_aerial_pr_trailer() {
    local t_id="${1:-}" v_id="${2:-}"
    local b_id="${3:-${AERIAL_BOT_ID:-$(read_config_yaml_val bot_id)}}"
    [ -z "$t_id" ] && [ -z "$v_id" ] && return 0
    if [ -n "$b_id" ]; then
        printf "<!-- aerial-pr: %s -->" "$(jq -c -n --arg target_id "$t_id" --arg vanadis_chunk_id "$v_id" --arg bot_id "$b_id" '{target_id: $target_id, vanadis_chunk_id: $vanadis_chunk_id, bot_id: $bot_id}')"
    else
        printf "<!-- aerial-pr: %s -->" "$(jq -c -n --arg target_id "$t_id" --arg vanadis_chunk_id "$v_id" '{target_id: $target_id, vanadis_chunk_id: $vanadis_chunk_id}')"
    fi
}

rebase_staged_changes() {
    local target_branch="${1:-$DEFAULT_BRANCH}"
    local msg="${2:-chore: temporary rebase commit}"
    if ! git fetch origin "+refs/heads/${target_branch}:refs/remotes/origin/${target_branch}" >/dev/null 2>&1; then
        echo "⚠️ [aerial-pr] Warning: Could not fetch origin/${target_branch}; proceeding with current base." >&2
        return 0
    fi
    local head_sha upstream_sha current_branch remote_branch_sha=""
    head_sha=$(git rev-parse --verify -q HEAD 2>/dev/null || echo "")
    upstream_sha=$(git rev-parse --verify -q "refs/remotes/origin/${target_branch}" 2>/dev/null || echo "")
    if [ -z "$upstream_sha" ] || [ "$head_sha" = "$upstream_sha" ]; then
        return 0
    fi
    current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    if [ -n "$current_branch" ] && [ "$current_branch" != "$target_branch" ] && [ "$current_branch" != "HEAD" ]; then
        git fetch origin "+refs/heads/${current_branch}:refs/remotes/origin/${current_branch}" >/dev/null 2>&1 || true
        remote_branch_sha=$(git rev-parse --verify -q "refs/remotes/origin/${current_branch}" 2>/dev/null || echo "")
    fi
    if [ -n "$remote_branch_sha" ]; then
        if git merge-base --is-ancestor "$upstream_sha" "$head_sha" 2>/dev/null && git merge-base --is-ancestor "$remote_branch_sha" "$head_sha" 2>/dev/null; then
            REBASE_INCORPORATED_REMOTE_SHA="$remote_branch_sha"
            git reset --soft "origin/${target_branch}" >/dev/null 2>&1
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
            preserve_scratch_workspace
            return 1
        else
            local fork_point=""
            fork_point=$(git merge-base "$head_sha" "$remote_branch_sha" 2>/dev/null || echo "")
            if [ -n "$fork_point" ] && [ "$head_sha" != "$fork_point" ]; then
                git reset --soft "$fork_point" >/dev/null 2>&1
            fi
            local temp_sha=""
            if ! git diff --cached --quiet; then
                git commit -q -m "temp: staged changes" >/dev/null 2>&1
                temp_sha=$(git rev-parse HEAD 2>/dev/null || echo "")
            fi
            git reset --hard "$remote_branch_sha" >/dev/null 2>&1
            if ! git merge-base --is-ancestor "$upstream_sha" HEAD 2>/dev/null; then
                if ! git merge --no-edit -q "origin/${target_branch}" >/dev/null 2>&1; then
                    git merge --abort >/dev/null 2>&1 || true
                    [ -n "$temp_sha" ] && git reset --hard "$temp_sha" >/dev/null 2>&1 && git reset --soft "$head_sha" >/dev/null 2>&1
                    echo "ERROR: Failed to merge origin/${target_branch} into ${current_branch} due to merge conflicts. Aborting submit." >&2
                    preserve_scratch_workspace
                    return 1
                fi
                echo "🔄 [aerial-pr] Merged latest origin/${target_branch} (${upstream_sha:0:7}) into ${current_branch}." >&2
            fi
            if [ -n "$temp_sha" ]; then
                if ! git cherry-pick -n "$temp_sha" >/dev/null 2>&1; then
                    git cherry-pick --abort >/dev/null 2>&1 || true
                    git reset --hard "$temp_sha" >/dev/null 2>&1 && git reset --soft "$head_sha" >/dev/null 2>&1
                    echo "ERROR: Failed to apply staged changes onto updated remote branch ${current_branch} due to merge conflicts. Aborting submit." >&2
                    preserve_scratch_workspace
                    return 1
                fi
            fi
            REBASE_INCORPORATED_REMOTE_SHA="$remote_branch_sha"
            return 0
        fi
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
        preserve_scratch_workspace
        return 1
    fi
    git reset --soft "origin/${target_branch}" >/dev/null 2>&1
    echo "🔄 [aerial-pr] Rebased staged changes onto latest origin/${target_branch} (${upstream_sha:0:7})." >&2
    return 0
}

resolve_pr_register_url() {
    if [ -n "${AERIAL_PR_REGISTER_URL:-}" ]; then
        if [[ "${AERIAL_PR_REGISTER_URL}" == */internal/pr/register ]]; then
            echo "${AERIAL_PR_REGISTER_URL}"
        else
            echo "${AERIAL_PR_REGISTER_URL%/}/internal/pr/register"
        fi
        return 0
    fi
    local syn_url="${SYNAPSE_URL:-$(read_config_yaml_val synapse_url)}"
    if [ -n "$syn_url" ]; then
        echo "${syn_url%/}/internal/pr/register"
        return 0
    fi
    return 1
}

submit_scratch() {
    local target_id="$(resolve_target_id)"
    local scratch_dir=""
    local commit_msg=""
    local commit_msg_explicit=0
    local pr_body=""
    local pr_body_file=""
    local vanadis_chunk_id="$(resolve_vanadis_chunk_id "${VANADIS_CHUNK_ID_EXPLICIT:-}")"

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
                VANADIS_CHUNK_ID_EXPLICIT="$2"
                shift 2
                ;;
            --vanadis-chunk=*)
                vanadis_chunk_id="${1#*=}"
                VANADIS_CHUNK_ID_EXPLICIT="${1#*=}"
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

    verify_vanadis_chunk_active "${vanadis_chunk_id}"

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
            preserve_scratch_workspace
        elif [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
            rm -rf "${SCRATCH_DIR_CLEANUP}" "${SCRATCH_DIR_CLEANUP}.lock" "${SCRATCH_DIR_CLEANUP}.preserved"
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

    local default_title="feat(${REPO_NAME}): automated update by Aerial"

    # Resolve PR description from convention file in scratch workspace if not explicitly passed
    if [ -z "$pr_body" ] && [ -n "$saved_pr_desc_content" ]; then
        pr_body="$saved_pr_desc_content"
    fi

    # CRITICAL WORKSPACE HYGIENE: Always remove convention files from scratch workspace before staging
    # so they are NEVER staged by git add -A or permanently committed into git history on main!
    if [ -f "${scratch_dir}/.aerial-preserved" ]; then
        [ -d "${scratch_dir}/.git" ] && touch "${scratch_dir}/.git/.aerial-preserved" 2>/dev/null || true
        rm -f "${scratch_dir}/.aerial-preserved"
    fi
    rm -f "${scratch_dir}/PR_DESCRIPTION.md" "${scratch_dir}/.pr_description.md"

    # If submitting on an existing PR branch without a local description, inherit metadata from the open PR
    local current_branch existing_pr=""
    current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
    if [ -z "${pr_body//[[:space:]]/}" ] && { [ -n "${GITHUB_PAT:-}" ] || [ "${USE_LOOPBACK_GITHUB:-0}" -eq 1 ]; } && [ -n "$current_branch" ] && [ "$current_branch" != "$DEFAULT_BRANCH" ] && [ "$current_branch" != "HEAD" ]; then
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

    # Append Vanadis build/chunk context to PR description if chunk ID is set and not already present
    if [ -n "$vanadis_chunk_id" ] && [[ "$pr_body" != *"## Vanadis Build Context"* ]]; then
        local vanadis_ctx=""
        vanadis_ctx="$(format_vanadis_chunk_context "$vanadis_chunk_id" || true)"
        if [ -n "$vanadis_ctx" ]; then
            if [ -n "$pr_body" ]; then
                pr_body="${pr_body}"$'\n\n'"${vanadis_ctx}"
            else
                pr_body="$vanadis_ctx"
            fi
        fi
    fi

    # Defensively clamp body length to 60,000 characters (GitHub API limit is 65,536)
    local max_body_len=60000
    if [ "${#pr_body}" -gt "$max_body_len" ]; then
        pr_body="${pr_body:0:$max_body_len}

> ⚠️ **[Aerial Notice]**: PR description was truncated because it exceeded 60,000 characters."
    fi

    # 1. Stage all modifications (defensively unstage any convention files if previously cached)
    git add -A
    git rm --cached -f PR_DESCRIPTION.md .pr_description.md .aerial-preserved 2>/dev/null || true

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

    verify_vanadis_chunk_active "${vanadis_chunk_id}"

    # 5. Commit (preserves complete multi-line commit message in git history)
    local full_commit_msg="$commit_msg"
    local commit_body
    commit_body="$(printf '%s\n' "$pr_body" | grep -v '^<!-- aerial-pr: .* -->$' || true)"
    if [ -n "$commit_body" ] && [[ "$commit_msg" != *"$commit_body"* ]]; then
        full_commit_msg="${clean_title}"$'\n\n'"${commit_body}"
    fi
    git commit -m "$full_commit_msg" >/dev/null

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
        preserve_scratch_workspace
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

        local auth_args=()
        if [ -n "${GITHUB_PAT:-}" ]; then
            auth_args=(-H "Authorization: token ${GITHUB_PAT}")
        fi

        pr_raw=$(curl -s --connect-timeout 3 -m 10 -w "\n%{http_code}" -X POST \
            "${auth_args[@]}" \
            -H "Accept: application/vnd.github.v3+json" \
            "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/pulls" \
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
            local del_auth_args=()
            if [ -n "${GITHUB_PAT:-}" ]; then
                del_auth_args=(-H "Authorization: token ${GITHUB_PAT}")
            fi
            curl -s --connect-timeout 3 -m 10 -X DELETE \
                "${del_auth_args[@]}" \
                -H "Accept: application/vnd.github.v3+json" \
                "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/git/refs/heads/${branch}" >/dev/null 2>&1 || true
        fi
        # Prevent trap from nuking the local workspace so code can be recovered
        preserve_scratch_workspace
        exit 1
    fi

    local pr_num
    pr_num=$(echo "$pr_resp" | jq -r '.number')
    local pr_url
    pr_url=$(echo "$pr_resp" | jq -r '.html_url')
    local pr_node_id
    pr_node_id=$(echo "$pr_resp" | jq -r '.node_id // empty')

    # Attach PR directly to Vanadis (for both multi-chunk and standalone Tier 0 PRs)
    local vanadis_url
    vanadis_url=$(resolve_vanadis_url 2>/dev/null || true)
    if [ -n "$vanadis_url" ]; then
        local attach_payload
        attach_payload=$(jq -n \
            --arg chunk_id "$vanadis_chunk_id" \
            --argjson pr_number "$pr_num" \
            --arg repo "${REPO_NAME}" \
            --arg branch "$branch" \
            --arg title "$clean_title" \
            --arg target_id "$target_id" \
            '{chunk_id: $chunk_id, pr_number: $pr_number, repo: $repo, branch: $branch, title: $title, target_id: $target_id}')
        local vanadis_attach_out
        if vanadis_attach_out=$(attach_pr_in_vanadis "$vanadis_url" "$attach_payload" "$pr_num"); then
            local returned_thread
            returned_thread=$(echo "$vanadis_attach_out" | jq -r '.thread_id // empty' 2>/dev/null || true)
            if [ -n "$returned_thread" ]; then
                target_id="$returned_thread"
            fi
        else
            echo "ERROR: Failed to attach PR #${pr_num} to Vanadis (chunk: '${vanadis_chunk_id:-standalone}')." >&2
            preserve_scratch_workspace
            exit 1
        fi
    elif [ -n "$vanadis_chunk_id" ]; then
        echo "ERROR: Cannot attach PR #${pr_num} to Vanadis chunk '${vanadis_chunk_id}': Vanadis URL could not be resolved." >&2
        preserve_scratch_workspace
        exit 1
    fi

    # Register PR in PostgreSQL pr_registry via internal endpoint
    local brain_reg_url
    brain_reg_url=$(resolve_pr_register_url 2>/dev/null || true)

    local reg_payload
    reg_payload=$(jq -n \
        --arg repo "${REPO_NAME}" \
        --argjson pr_num "$pr_num" \
        --arg branch "$branch" \
        --arg head_sha "$commit_sha" \
        --arg target_id "$target_id" \
        --arg title "$clean_title" \
        '{repo: $repo, pr_number: $pr_num, branch: $branch, head_sha: $head_sha, target_id: $target_id, title: $title, metadata: "{}"}')

    if [ -n "$brain_reg_url" ]; then
        if ! register_pr_in_brain "$brain_reg_url" "$reg_payload" "$pr_num" "$target_id"; then
            echo "ERROR: Failed to register PR #${pr_num} in PostgreSQL pr_registry (${brain_reg_url})." >&2
            preserve_scratch_workspace
            exit 1
        fi
    else
        echo "ERROR: No PR registration endpoint available (AERIAL_PR_REGISTER_URL, SYNAPSE_URL, or synapse_url in config.yaml must be set)." >&2
        preserve_scratch_workspace
        exit 1
    fi

    # Enable native GitHub auto-merge (SQUASH) only after PR attach and registration succeed
    enable_github_auto_merge "$pr_node_id" "$pr_num"

    # Asynchronous Submission: Disarm cleanup trap and remove scratch workspace immediately
    if [ -n "${SCRATCH_DIR_CLEANUP:-}" ]; then
        rm -rf "${SCRATCH_DIR_CLEANUP}" "${SCRATCH_DIR_CLEANUP}.lock" "${SCRATCH_DIR_CLEANUP}.preserved"
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

    if [ -z "${GITHUB_PAT:-}" ] && [ "${USE_LOOPBACK_GITHUB:-0}" -ne 1 ]; then
        echo "ERROR: GITHUB_PAT environment variable must be set (or loopback proxy available)." >&2
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
        local auth_args=()
        if [ -n "${GITHUB_PAT:-}" ]; then
            auth_args=(-H "Authorization: token ${GITHUB_PAT}")
        fi
        curl -s --connect-timeout 3 -m 15 --retry 3 --retry-delay 2 --retry-connrefused -w "\n%{http_code}" \
            "${auth_args[@]}" \
            -H "Accept: application/vnd.github.v3+json" "$@"
    }

    # 1. Fetch Pull Request details
    local pr_raw pr_code pr_resp
    pr_raw=$(gh_api_call -X GET "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/pulls/${pr_num}")
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

    # 2. Check for existing override comment or post override comment on PR issue
    local comments_raw comments_code comments_resp existing_comment_id=""
    comments_raw=$(gh_api_call -X GET "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/issues/${pr_num}/comments?per_page=100")
    comments_code=$(echo "$comments_raw" | tail -n1)
    comments_resp=$(echo "$comments_raw" | sed '$d')

    if [ -n "$comments_code" ] && [ "$comments_code" -ge 200 ] && [ "$comments_code" -lt 300 ]; then
        existing_comment_id=$(echo "$comments_resp" | jq -r --arg sha "${short_sha}" '[if type == "array" then .[] else empty end | objects | select((.body // "") | (contains("## roborev-override") and contains($sha))) | .id] | first // empty')
    fi

    local comment_id=""
    if [ -n "$existing_comment_id" ]; then
        comment_id="$existing_comment_id"
    else
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
            "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/issues/${pr_num}/comments")
        comment_code=$(echo "$comment_raw" | tail -n1)

        if [ -z "$comment_code" ] || [ "$comment_code" -lt 200 ] || [ "$comment_code" -ge 300 ]; then
            local comment_resp
            comment_resp=$(echo "$comment_raw" | sed '$d')
            echo "ERROR: Failed to post override comment on PR #${pr_num} (HTTP ${comment_code}): ${comment_resp}" >&2
            exit 1
        fi
        comment_id=$(echo "$comment_raw" | sed '$d' | jq -r '.id // empty')
    fi

    # 3. Locate single latest RoboRev workflow run for head_sha
    local runs_raw runs_code runs_resp target_run run_id run_status
    runs_raw=$(gh_api_call -X GET "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs?head_sha=${head_sha}")
    runs_code=$(echo "$runs_raw" | tail -n1)
    runs_resp=$(echo "$runs_raw" | sed '$d')

    if [ -z "$runs_code" ] || [ "$runs_code" -lt 200 ] || [ "$runs_code" -ge 300 ]; then
        echo "ERROR: Failed to query workflow runs for commit ${short_sha} (HTTP ${runs_code}): ${runs_resp}" >&2
        exit 1
    fi

    target_run=$(echo "$runs_resp" | jq -c '
        ([.workflow_runs[]? | select(.name == "RoboRev Code Review" or (.path // "" | endswith("roborev.yml")))] | first) //
        ([.workflow_runs[]? | select(.name == "Continuous Delivery" or .name == "Validate Configuration" or (.path // "" | (endswith("docker-publish.yml") or endswith("ci.yml") or endswith("validate.yml"))))] | first) //
        empty
    ')
    run_id=$(echo "$target_run" | jq -r '.id // empty')
    run_status=$(echo "$target_run" | jq -r '.status // empty')

    if [ -z "$run_id" ]; then
        echo "ERROR: No RoboRev workflow run found for commit ${short_sha} on PR #${pr_num}." >&2
        exit 1
    fi

    # 4. Trigger workflow re-run (rerun-failed-jobs falling back to rerun)
    wait_for_workflow_completed() {
        local target_run_id="$1"
        local max_attempts="${AERIAL_RERUN_WAIT_ATTEMPTS:-60}"
        local wait_delay="${AERIAL_RERUN_WAIT_DELAY:-5}"
        local attempt=0
        local poll_raw poll_code poll_resp cur_status=""

        while [ "$attempt" -lt "$max_attempts" ]; do
            attempt=$((attempt + 1))
            poll_raw=$(gh_api_call -X GET "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${target_run_id}")
            poll_code=$(echo "$poll_raw" | tail -n1)
            poll_resp=$(echo "$poll_raw" | sed '$d')
            if [ -n "$poll_code" ] && [ "$poll_code" -ge 200 ] && [ "$poll_code" -lt 300 ]; then
                cur_status=$(echo "$poll_resp" | jq -r '.status // empty')
                if [ "$cur_status" = "completed" ]; then
                    return 0
                fi
            fi
            if [ "$attempt" -lt "$max_attempts" ]; then
                [ "${wait_delay}" -gt 0 ] && sleep "${wait_delay}"
            fi
        done
        return 1
    }

    if [ -n "$run_status" ] && [ "$run_status" != "completed" ]; then
        wait_for_workflow_completed "$run_id" || true
    fi

    local rerun_raw rerun_code
    rerun_raw=$(gh_api_call -X POST "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun-failed-jobs")
    rerun_code=$(echo "$rerun_raw" | tail -n1)

    if [ "$rerun_code" = "403" ] || [ "$rerun_code" = "409" ]; then
        wait_for_workflow_completed "$run_id" || true
        rerun_raw=$(gh_api_call -X POST "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun-failed-jobs")
        rerun_code=$(echo "$rerun_raw" | tail -n1)
    fi

    if [ -z "$rerun_code" ] || [ "$rerun_code" -lt 200 ] || [ "$rerun_code" -ge 300 ]; then
        echo "⚠️ rerun-failed-jobs returned HTTP ${rerun_code}; attempting fallback to full workflow rerun..." >&2
        rerun_raw=$(gh_api_call -X POST "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun")
        rerun_code=$(echo "$rerun_raw" | tail -n1)
        if [ "$rerun_code" = "403" ] || [ "$rerun_code" = "409" ]; then
            wait_for_workflow_completed "$run_id" || true
            rerun_raw=$(gh_api_call -X POST "${GITHUB_API_BASE}/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${run_id}/rerun")
            rerun_code=$(echo "$rerun_raw" | tail -n1)
        fi
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
