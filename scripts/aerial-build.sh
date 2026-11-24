#!/usr/bin/env bash
set -euo pipefail
read_cfg() {
    for f in "${CONFIG_PATH:-}" "${AERIAL_LOCAL_CONFIG_PATH:-/local/config.yaml}" "${AERIAL_SHARED_CONFIG_PATH:-/share/aerial-config/config.yaml}"; do
        [ -r "$f" ] || continue
        local v; v=$(awk -v k="$1" '$0 ~ "^" k ":[ \t]*" { sub("^" k ":[ \t]*", ""); sub(/[ \t]*#.*$/, ""); gsub(/^["\x27]|["\x27]$/, ""); print; exit }' "$f" 2>/dev/null || true)
        [ -n "$v" ] && { echo "$v"; return 0; }
    done
    echo ""
}
VANADIS_URL="${VANADIS_URL:-$(read_cfg vanadis_url)}"; VANADIS_URL="${VANADIS_URL%/}"
VANADIS_FALLBACK_URL="${VANADIS_FALLBACK_URL:-}"; VANADIS_FALLBACK_URL="${VANADIS_FALLBACK_URL%/}"
resolve_tid() { echo "${DISCORD_THREAD_ID:-${DISCORD_CHANNEL_ID:-${AERIAL_TARGET_ID:-}}}"; }
do_curl() {
    local m="$1" u="$2" b="${3:-}" sa="" auth=()
    for tok in "${AERIAL_SA_TOKEN_PATH:-/var/run/secrets/kubernetes.io/serviceaccount/token}" "${HOME:-}/.gemini/turn_env/sa-token"; do
        [ -r "$tok" ] && sa="$(tr -d '[:space:]' < "$tok" 2>/dev/null || true)" && [ -n "$sa" ] && { auth=(-H "Authorization: Bearer $sa"); break; }
    done
    if [ -n "$b" ]; then
        printf '%s' "$b" | curl -sS --connect-timeout 5 --max-time 20 -w "\n%{http_code}" -X "$m" ${auth[@]+"${auth[@]}"} -H "Content-Type: application/json" --data-binary @- "$u"
    else
        curl -sS --connect-timeout 5 --max-time 20 -w "\n%{http_code}" -X "$m" ${auth[@]+"${auth[@]}"} "$u"
    fi
}
vanadis_req() {
    local m="$1" p="$2" b="${3:-}" resp="" code="" body="" st=0 err diag
    [ -n "$VANADIS_URL" ] || { echo "ERROR: VANADIS_URL is not configured" >&2; return 1; }
    err="$(mktemp)"; resp="$(do_curl "$m" "${VANADIS_URL}${p}" "$b" 2>"$err")" || st=$?
    if [ "$st" -ne 0 ] && { [ "$m" = GET ] || [ "$st" -eq 6 ] || [ "$st" -eq 7 ]; } && [ -n "$VANADIS_FALLBACK_URL" ] && [ "$VANADIS_URL" != "$VANADIS_FALLBACK_URL" ]; then
        st=0; resp="$(do_curl "$m" "${VANADIS_FALLBACK_URL}${p}" "$b" 2>"$err")" || st=$?
    fi
    rm -f "$err"
    if [ "$st" -ne 0 ]; then
        diag="Transport failure (curl exit $st)"
        case "$st" in
            6) diag="Could not resolve host" ;;
            7) diag="Failed to connect" ;;
            28) diag="Request timed out" ;;
            52) diag="Empty reply from server" ;;
            35|60) diag="TLS connection failed" ;;
        esac
        echo "ERROR: ${diag}; acceptance unknown; check build status before resubmitting" >&2
        return 1
    fi
    code="$(printf '%s\n' "$resp" | tail -n 1)"; body="$(printf '%s\n' "$resp" | sed '$d')"
    if [[ ! "$code" =~ ^[0-9]{3}$ ]] || [ "$code" = "000" ]; then
        echo "ERROR: No valid HTTP response; acceptance unknown; check build status before resubmitting" >&2
        return 1
    fi
    [ "$code" -ge 200 ] && [ "$code" -lt 300 ] || { echo "ERROR: Vanadis request $m $p failed with HTTP $code: $body" >&2; return 1; }
    printf '%s\n' "$body"
}
usage() { echo "Usage: $(basename "$0") {enqueue <plan.json>|status <build_id>|cancel <build_id>}"; }
case "${1:-}" in
    enqueue)
        shift; pf=""
        while [ $# -gt 0 ]; do
            case "$1" in
                --file|-f) [ $# -ge 2 ] || { echo "ERROR: --file requires an argument" >&2; exit 1; }; pf="$2"; shift 2 ;;
                --file=*) pf="${1#*=}"; shift ;;
                -h|--help) usage; exit 0 ;;
                -*) echo "ERROR: Unknown option $1" >&2; exit 1 ;;
                *) [ -z "$pf" ] && pf="$1" || { echo "ERROR: Extra arg $1" >&2; exit 1; }; shift ;;
            esac
        done
        [ -n "$pf" ] && [ -f "$pf" ] || { echo "ERROR: Plan JSON file required" >&2; exit 1; }
        jq empty "$pf" >/dev/null 2>&1 || { echo "ERROR: Invalid JSON in $pf" >&2; exit 1; }
        jq -e '(.chunks | type == "array" and length > 0) and all(.chunks[]; (.depends_on | type) == "array")' "$pf" >/dev/null 2>&1 || { echo "ERROR: depends_on array required for every chunk" >&2; exit 1; }
        payload="$(cat "$pf")"; tid="$(jq -r '.thread_id // ""' "$pf" 2>/dev/null || true)"
        [ -n "$tid" ] || { env_tid="$(resolve_tid)"; [ -n "$env_tid" ] && payload="$(printf '%s' "$payload" | jq -c --arg t "$env_tid" '.thread_id = $t')"; }
        resp="$(vanadis_req "POST" "/builds" "$payload")"
        if printf '%s' "$resp" | jq empty >/dev/null 2>&1; then
            printf '%s\n' "$resp" | jq 'if .chunks then .chunks |= map({id, chunk_index, mode, repo, title, effort, status, depends_on}) else . end + {"turn_action": "end_turn"}'
        else printf '%s\n' "$resp"; fi
        ;;
    status)
        [ -n "${2:-}" ] || { echo "ERROR: build_id is required" >&2; exit 1; }
        vanadis_req "GET" "/builds/$2"
        ;;
    cancel)
        [ -n "${2:-}" ] || { echo "ERROR: build_id is required" >&2; exit 1; }
        vanadis_req "POST" "/builds/$2/cancel"
        ;;
    -h|--help|help)
        usage; exit 0
        ;;
    *)
        usage >&2; exit 1
        ;;
esac
