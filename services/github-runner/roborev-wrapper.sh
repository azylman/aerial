#!/usr/bin/env bash
set -euo pipefail
unset OPENAI_API_KEY || true
for p in /root/.roborev/claude-oauth-token /local/claude-oauth-token /secrets/claude-oauth-token; do
  [ -s "$p" ] && export CLAUDE_CODE_OAUTH_TOKEN="$(tr -d '\r\n' < "$p")" && break
done
run_real() { env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "$@"; }
if [[ "${1:-}" == "ci" && "${2:-}" == "review" ]]; then
  shift 2
  min_sev="" ref=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --min-severity) min_sev="${2:-}"; shift 2 ;;
      --min-severity=*) min_sev="${1#*=}"; shift ;;
      --ref) ref="${2:-}"; shift 2 ;;
      --ref=*) ref="${1#*=}"; shift ;;
      *) shift ;;
    esac
  done
  branch="${GITHUB_HEAD_REF:-}" base_sha="" head_sha=""
  if [[ -n "${GITHUB_EVENT_PATH:-}" && -f "${GITHUB_EVENT_PATH}" ]]; then
    branch="${branch:-$(jq -r '.pull_request.head.ref // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)}"
    base_sha=$(jq -r '.pull_request.base.sha // .before // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)
    head_sha=$(jq -r '.pull_request.head.sha // .after // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)
  fi
  head_sha="${head_sha:-$(git rev-parse HEAD 2>/dev/null || true)}"
  branch="${branch:-$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)}"
  [ "${branch}" = "HEAD" ] && branch=""
  [ -z "${ref}" ] && [ -n "${base_sha}" ] && [ -n "${head_sha}" ] && ref="$(git merge-base "${base_sha}" "${head_sha}" 2>/dev/null || echo "${base_sha}")..${head_sha}"
  ROBOREV_SYNC_BRANCH="${branch:-__none__}" run_real daemon start >/dev/null 2>&1 || true
  if [ -s "/root/.roborev/postgres-password" ]; then
    for _ in $(seq 1 10); do ROBOREV_SYNC_BRANCH="${branch:-__none__}" run_real sync now >/dev/null 2>&1 && break || sleep 1; done
  fi
  args=(review --wait)
  (git show origin/main:.roborev.toml 2>/dev/null || cat .roborev.toml 2>/dev/null || true) | grep -qE 'review_types[[:space:]]*=[[:space:]]*\[[[:space:]]*"default"[[:space:]]*\]' && args+=(--panel none)
  [ -n "${ref}" ] && args+=("${ref}")
  [ -n "${min_sev}" ] && args+=(--min-severity "${min_sev}")
  raw=$(mktemp) body=$(mktemp)
  trap 'rm -f "${raw}" "${body}"' EXIT
  set +e; run_real "${args[@]}" > "${raw}" 2>&1; rc=$?
  [ -s "/root/.roborev/postgres-password" ] && run_real sync now >/dev/null 2>&1 || true
  set -e
  if grep -q "^Review (by " "${raw}"; then
    awk 'found{print}/^------------------------------------------------------------$/{if(!found){found=1;next}}' "${raw}" > "${body}"
    short="${head_sha:0:7}"
    short="${short:-$(git rev-parse --short=7 HEAD 2>/dev/null || echo unknown)}"
    [ "$rc" -eq 0 ] && [ -s "${body}" ] && echo "## roborev: Review Passed (\`${short}\`)" || echo "## roborev: Review Complete (\`${short}\`)"
    echo ""
    cat "${body}"
  else
    cat "${raw}"
  fi
  exit "$rc"
fi
exec env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "$@"
