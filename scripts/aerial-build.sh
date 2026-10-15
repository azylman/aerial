#!/usr/bin/env bash
set -euo pipefail

# scripts/aerial-build.sh - CLI client for Vanadis Autonomous Build Queue Orchestrator

VANADIS_URL="${VANADIS_URL:-http://vanadis.aerial:4025}"
VANADIS_URL="${VANADIS_URL%/}"
VANADIS_FALLBACK_URL="${VANADIS_FALLBACK_URL:-http://127.0.0.1:4025}"
VANADIS_FALLBACK_URL="${VANADIS_FALLBACK_URL%/}"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [args]

Commands:
  enqueue [--file|-f] <plan.json>   Validate and enqueue a multi-chunk build plan in Vanadis
  status <build_id>                 Fetch build and chunk status from Vanadis
  cancel <build_id>                 Cancel an active Vanadis build run

Environment:
  VANADIS_URL          Vanadis base URL (default: http://vanadis.aerial:4025, fallback: http://127.0.0.1:4025)
  DISCORD_THREAD_ID    Automatically injected as thread_id when omitted from plan.json
  DISCORD_CHANNEL_ID   Fallback thread_id when DISCORD_THREAD_ID is unset
EOF
}

resolve_thread_id() {
    if [ -n "${DISCORD_THREAD_ID:-}" ]; then
        echo "${DISCORD_THREAD_ID}"
    elif [ -n "${DISCORD_CHANNEL_ID:-}" ]; then
        echo "${DISCORD_CHANNEL_ID}"
    elif [ -n "${AERIAL_TARGET_ID:-}" ]; then
        echo "${AERIAL_TARGET_ID}"
    else
        echo ""
    fi
}

do_curl_request() {
    local method="$1"
    local url="$2"
    local body="${3:-}"

    if [ -n "$body" ]; then
        curl -sS --connect-timeout 5 -w "\n%{http_code}" -X "$method" \
            -H "Content-Type: application/json" \
            -d "$body" \
            "$url"
    else
        curl -sS --connect-timeout 5 -w "\n%{http_code}" -X "$method" \
            "$url"
    fi
}

vanadis_request() {
    local method="$1"
    local path="$2"
    local body="${3:-}"

    local raw_resp=""
    if ! raw_resp="$(do_curl_request "$method" "${VANADIS_URL}${path}" "$body" 2>/dev/null)"; then
        if [ "$VANADIS_URL" != "$VANADIS_FALLBACK_URL" ]; then
            if ! raw_resp="$(do_curl_request "$method" "${VANADIS_FALLBACK_URL}${path}" "$body" 2>&1)"; then
                echo "ERROR: Failed to connect to Vanadis at ${VANADIS_URL}${path} and fallback ${VANADIS_FALLBACK_URL}${path}: ${raw_resp}" >&2
                return 1
            fi
        else
            echo "ERROR: Failed to connect to Vanadis at ${VANADIS_URL}${path}" >&2
            return 1
        fi
    fi

    local http_code
    http_code="$(printf '%s\n' "$raw_resp" | tail -n 1)"
    local resp_body
    resp_body="$(printf '%s\n' "$raw_resp" | sed '$d')"

    if [[ ! "$http_code" =~ ^[0-9]{3}$ ]] || [ "$http_code" -lt 200 ] || [ "$http_code" -ge 300 ]; then
        echo "ERROR: Vanadis request ${method} ${path} failed with HTTP ${http_code}: ${resp_body}" >&2
        return 1
    fi

    printf '%s\n' "$resp_body"
}

cmd_enqueue() {
    local plan_file=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --file|-f)
                if [ $# -lt 2 ]; then
                    echo "ERROR: --file requires a path argument" >&2
                    exit 1
                fi
                plan_file="$2"
                shift 2
                ;;
            --file=*)
                plan_file="${1#*=}"
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            -*)
                echo "ERROR: Unknown option for enqueue: $1" >&2
                exit 1
                ;;
            *)
                if [ -z "$plan_file" ]; then
                    plan_file="$1"
                else
                    echo "ERROR: Unexpected extra argument: $1" >&2
                    exit 1
                fi
                shift
                ;;
        esac
    done

    if [ -z "$plan_file" ]; then
        echo "ERROR: Plan JSON file path is required (Usage: aerial-build.sh enqueue <plan.json>)" >&2
        exit 1
    fi

    if [ ! -f "$plan_file" ]; then
        echo "ERROR: Plan file not found: $plan_file" >&2
        exit 1
    fi

    if ! jq empty "$plan_file" >/dev/null 2>&1; then
        echo "ERROR: Invalid JSON in plan file: $plan_file" >&2
        exit 1
    fi

    local payload
    payload="$(cat "$plan_file")"

    local existing_tid
    existing_tid="$(printf '%s' "$payload" | jq -r '.thread_id // ""' 2>/dev/null || true)"
    if [ -z "$existing_tid" ]; then
        local env_tid
        env_tid="$(resolve_thread_id)"
        if [ -n "$env_tid" ]; then
            payload="$(printf '%s' "$payload" | jq -c --arg tid "$env_tid" '.thread_id = $tid')"
        fi
    fi

    local resp
    resp="$(vanadis_request "POST" "/builds" "$payload")"

    if printf '%s' "$resp" | jq empty >/dev/null 2>&1; then
        printf '%s\n' "$resp" | jq '. + {"turn_action": "end_turn"}'
    else
        printf '%s\n' "$resp"
    fi
}

cmd_status() {
    local build_id="${1:-}"
    if [ -z "$build_id" ]; then
        echo "ERROR: build_id is required (Usage: aerial-build.sh status <build_id>)" >&2
        exit 1
    fi
    vanadis_request "GET" "/builds/${build_id}"
}

cmd_cancel() {
    local build_id="${1:-}"
    if [ -z "$build_id" ]; then
        echo "ERROR: build_id is required (Usage: aerial-build.sh cancel <build_id>)" >&2
        exit 1
    fi
    vanadis_request "POST" "/builds/${build_id}/cancel"
}

if [ $# -eq 0 ]; then
    usage >&2
    exit 1
fi

SUBCOMMAND="$1"
shift

case "$SUBCOMMAND" in
    enqueue)
        cmd_enqueue "$@"
        ;;
    status)
        cmd_status "$@"
        ;;
    cancel)
        cmd_cancel "$@"
        ;;
    -h|--help|help)
        usage
        exit 0
        ;;
    *)
        echo "ERROR: Unknown command: $SUBCOMMAND" >&2
        usage >&2
        exit 1
        ;;
esac
