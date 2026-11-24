#!/usr/bin/env bash
set -euo pipefail
REPO_OWNER="azylman"; REPO_NAME="${AERIAL_REPO_NAME:-aerial}"; REPO_EXP=0; DEFAULT_BRANCH="main"; GITHUB_API="${GITHUB_API_URL:-https://api.github.com}"; PROXY="http://127.0.0.1:4028"; LOOP=0
if [ -z "${GITHUB_PAT:-}" ] && { [ "${AERIAL_USE_LOOPBACK_GITHUB:-0}" = "1" ] || [ "$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 1 -m 2 "$PROXY/healthz" || true)" = "200" ]; }; then LOOP=1; GITHUB_API="$PROXY/internal/github-api"; fi
norm_repo() { case "${1##*/}" in core|aerial|"") echo aerial;; config|aerial-config) echo aerial-config;; sidecars|aerial-sidecars) echo aerial-sidecars;; *) echo "${1%.git}";; esac; }
parse_remote() { local u; u=$(git remote get-url origin 2>/dev/null || true); u="${u%.git}"; if [[ "$u" =~ (github\.com|/internal/github-git)[:/]([^/]+)/([^/]+)$ ]]; then REPO_OWNER="${BASH_REMATCH[2]}"; [ "$REPO_EXP" = "0" ] && REPO_NAME="$(norm_repo "${BASH_REMATCH[3]}")"; fi; }
auth_hdr() { [ "$LOOP" = "1" ] && echo loopback || { [ -n "${GITHUB_PAT:-}" ] && printf "x-access-token:%s" "$GITHUB_PAT" | base64 | tr -d '\r\n' || { echo "ERROR: GITHUB_PAT required" >&2; exit 1; }; }; }
gh_call() { local h=(); [ -n "${GITHUB_PAT:-}" ] && h=(-H "Authorization: token $GITHUB_PAT"); curl -s --connect-timeout 3 -m 15 -w "\n%{http_code}" "${h[@]}" -H "Accept: application/vnd.github.v3+json" "$@"; }
git_auth() { export GIT_TERMINAL_PROMPT=0 GIT_CONFIG_COUNT=2 GIT_CONFIG_KEY_0="http.extraHeader" GIT_CONFIG_VALUE_0="AUTHORIZATION: basic $(auth_hdr)" GIT_CONFIG_KEY_1="http.version" GIT_CONFIG_VALUE_1="HTTP/1.1"; }
get_v() { if [ "$1" = "$2" ]; then [ $3 -ge 2 ] || { echo "ERROR: $1 requires value" >&2; exit 1; }; echo "$4"; else echo "${1#*=}"; fi; }
is_ok() { [[ "${1:-}" =~ ^[0-9]+$ ]] && [ "$1" -ge 200 ] && [ "$1" -lt 300 ]; }
anc() { git merge-base --is-ancestor "$1" "$2" 2>/dev/null || { [ -f .git/shallow ] && git fetch --unshallow origin >/dev/null 2>&1 && git merge-base --is-ancestor "$1" "$2" 2>/dev/null; }; }
if [ "${1:-}" = "--source-only" ] || [ "${REPO_TEST_MODE:-0}" = "1" ]; then return 0 2>/dev/null || exit 0; fi
CMD=""; BR=""; BODY=""; BFILE=""; RFILE=""; REB=""; CHUNK="${AERIAL_VANADIS_CHUNK_ID:-${VANADIS_CHUNK_ID:-}}"; ARGS=()
[ "${AERIAL_NO_VANADIS_CHUNK:-0}" = "1" ] && CHUNK=""
for a in "$@"; do case "$a" in init|submit|override-roborev|dispute-roborev) CMD="$a"; break;; esac; done
while [ $# -gt 0 ]; do
  case "$1" in
    init|submit|override-roborev|dispute-roborev) shift;;
    --source-only) return 0 2>/dev/null || exit 0;;
    --repo|-r|--repo=*) REPO_EXP=1; REPO_NAME="$(norm_repo "$(get_v "$1" "${1%%=*}" $# "${2:-}")")"; [ "$1" = "${1%%=*}" ] && shift 2 || shift;;
    --branch|--branch=*) [ "$CMD" = "init" ] || { echo "ERROR: --branch only for init" >&2; exit 1; }; BR="$(get_v "$1" "${1%%=*}" $# "${2:-}")"; [ "$1" = "${1%%=*}" ] && shift 2 || shift;;
    --body|--body=*) [ "$CMD" = "submit" ] || { echo "ERROR: --body only for submit" >&2; exit 1; }; BODY="$(get_v "$1" "${1%%=*}" $# "${2:-}")"; [ "$1" = "${1%%=*}" ] && shift 2 || shift;;
    -b|-b=*) val="$(get_v "$1" "${1%%=*}" $# "${2:-}")"; [ "$CMD" = "init" ] && BR="$val" || BODY="$val"; [ "$1" = "${1%%=*}" ] && shift 2 || shift;;
    --body-file|-f|-F|--reason-file|--body-file=*|-f=*|-F=*|--reason-file=*) val="$(get_v "$1" "${1%%=*}" $# "${2:-}")"; [ "$CMD" = "submit" ] && BFILE="$val" || RFILE="$val"; [ "$1" = "${1%%=*}" ] && shift 2 || shift;;
    --vanadis-chunk|--vanadis-chunk=*) CHUNK="$(get_v "$1" "${1%%=*}" $# "${2:-}")"; [ "$1" = "${1%%=*}" ] && shift 2 || shift;;
    -m|--reason|-m=*|--reason=*) REB="$(get_v "$1" "${1%%=*}" $# "${2:-}")"; [ "$1" = "${1%%=*}" ] && shift 2 || shift;;
    --async|-a) shift;;
    --sync|--target*|-*) echo "ERROR: $1 not supported" >&2; exit 1;;
    *) ARGS+=("$1"); shift;;
  esac
done
if [ "$CMD" = "init" ]; then
  [ -n "${ARGS[0]:-}" ] && [ "$REPO_EXP" = "0" ] && REPO_NAME="$(norm_repo "${ARGS[0]}")"; r_url="${AERIAL_REPO_URL_OVERRIDE:-https://github.com/${REPO_OWNER}/${REPO_NAME}.git}"
  [ "$LOOP" = "1" ] && [ -z "${AERIAL_REPO_URL_OVERRIDE:-}" ] && r_url="${PROXY}/internal/github-git/${REPO_OWNER}/${REPO_NAME}.git"
  base="${AERIAL_SCRATCH_BASE_DIR:-/data/scratch}"; [ ! -w "$base" ] && base="${TMPDIR:-/tmp}"; mkdir -p "$base"; tid="${AERIAL_TARGET_ID:-${DISCORD_THREAD_ID:-${DISCORD_CHANNEL_ID:-}}}"; [[ ! "$tid" =~ ^[0-9]{17,20}$ ]] && tid=""
  rnd=$(head -c 8 /dev/urandom | md5sum | head -c 6); s_dir=""; b_name="$BR"; [ -z "$b_name" ] && [ -n "$CHUNK" ] && { sc="$(printf '%s' "$CHUNK" | sed 's/[^a-zA-Z0-9_-]/_/g')"; s_dir="${base}/vanadis-${sc}-${REPO_NAME}"; b_name="feat/${REPO_NAME}-pr-vanadis-${sc}${tid:+-t$tid}-${rnd}"; }; [ -z "$b_name" ] && b_name="feat/${REPO_NAME}-pr${tid:+-t$tid}-$(date +%Y%m%d%H%M%S)-${rnd}"
  if [ -n "$s_dir" ] && [ -d "$s_dir/.git" ] && git -C "$s_dir" rev-parse --verify HEAD >/dev/null 2>&1; then
    rm -f "$s_dir/.git/index.lock"; ex_b="$(git -C "$s_dir" symbolic-ref --short HEAD 2>/dev/null || true)"
    [ -n "$ex_b" ] && [ "$ex_b" != "HEAD" ] && [ "$ex_b" != "$DEFAULT_BRANCH" ] && { jq -n --arg s "initialized" --arg sd "$s_dir" --arg b "$ex_b" --arg r "$REPO_NAME" --argjson u true '{status:$s,scratch_dir:$sd,branch:$b,repo:$r,reused:$u}'; exit 0; }
    git -C "$s_dir" checkout -b "$b_name" >/dev/null 2>&1; jq -n --arg s "initialized" --arg sd "$s_dir" --arg b "$b_name" --arg r "$REPO_NAME" '{status:$s,scratch_dir:$sd,branch:$b,repo:$r}'; exit 0
  fi
  [ -n "$s_dir" ] && rm -rf "$s_dir" || s_dir=$(mktemp -d "${base}/${REPO_NAME}-scratch.XXXXXX"); git_auth
  if ! git clone --depth 20 --single-branch -b "${BR:-$DEFAULT_BRANCH}" "$r_url" "$s_dir" >&2; then rm -rf "$s_dir"; echo "ERROR: clone failed" >&2; exit 1; fi
  chmod 700 "$s_dir"; git -C "$s_dir" config user.name "Aerial" && git -C "$s_dir" config user.email "aerial@noreply.github.com"
  if [ -z "$BR" ]; then git -C "$s_dir" checkout -b "$b_name" >/dev/null 2>&1; elif [ "$BR" != "$DEFAULT_BRANCH" ]; then git -C "$s_dir" fetch --depth 20 origin "$DEFAULT_BRANCH" >/dev/null 2>&1 || true; fi
  jq -n --arg s "initialized" --arg sd "$s_dir" --arg b "$b_name" --arg r "$REPO_NAME" '{status:$s,scratch_dir:$sd,branch:$b,repo:$r}'
elif [ "$CMD" = "submit" ]; then
  s_dir="${ARGS[0]:-}"; msg="${ARGS[1]:-}"; s_desc=""; s_cont=""; [ -d "$s_dir" ] || { echo "ERROR: valid scratch dir required" >&2; exit 1; }
  [ -n "$BFILE" ] && { r_file="$BFILE"; [ ! -f "$r_file" ] && [ -f "$s_dir/$r_file" ] && r_file="$s_dir/$r_file"; [ -r "$r_file" ] || { echo "ERROR: body file not found: $BFILE" >&2; exit 1; }; BODY="$(cat "$r_file")"; }
  for df in "$s_dir/PR_DESCRIPTION.md" "$s_dir/.pr_description.md"; do [ -s "$df" ] && s_desc="$df" && s_cont="$(cat "$df")" && break; done; [ -z "$BODY" ] && BODY="$s_cont"; rm -f "$s_dir/PR_DESCRIPTION.md" "$s_dir/.pr_description.md"
  trap 'rc=$?; if [ $rc -ne 0 ]; then [ -n "$s_desc" ] && printf "%s\n" "$s_cont" > "$s_desc"; else rm -rf "$s_dir"; fi' EXIT INT TERM
  cd "$s_dir"; git config user.name "Aerial"; git config user.email "aerial@noreply.github.com"; parse_remote; git_auth
  cur_b=$(git rev-parse --abbrev-ref HEAD); enc_b=$(jq -rn --arg b "$cur_b" '$b|@uri'); ex_pr=""; [ -n "$cur_b" ] && [ "$cur_b" != "$DEFAULT_BRANCH" ] && ex_pr=$(gh_call -X GET "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/pulls?state=open&head=$REPO_OWNER:$enc_b" | sed '$d' | jq -c '.[0] // empty' 2>/dev/null || true); [ -n "$ex_pr" ] && { [ -z "$BODY" ] && BODY=$(echo "$ex_pr" | jq -r '.body // .title // "Automated update"'); [ -z "$msg" ] && msg=$(echo "$ex_pr" | jq -r '.title // empty'); }
  [ "${#BODY}" -gt 60000 ] && BODY="${BODY:0:60000}"$'\n\n> ⚠️ **[Aerial Notice]**: PR description was truncated because it exceeded 60,000 characters.'; [ -z "${BODY//[[:space:]]/}" ] && { echo "ERROR: PR description mandatory" >&2; exit 1; }
  for rf in GEMINI.md AGENTS.md; do [ -f "$rf" ] && [ $(wc -c < "$rf") -gt 23040 ] && { echo "ERROR: $rf exceeds 22.5 KB ceiling" >&2; exit 1; }; done
  git add -A && git rm --cached -f PR_DESCRIPTION.md .pr_description.md 2>/dev/null || true
  git fetch origin "+refs/heads/$DEFAULT_BRANCH:refs/remotes/origin/$DEFAULT_BRANCH" >/dev/null 2>&1 || true; git fetch origin "+refs/heads/$cur_b:refs/remotes/origin/$cur_b" >/dev/null 2>&1 || true
  r_sha=$(git rev-parse --verify -q "refs/remotes/origin/$cur_b" 2>/dev/null || echo ""); t_sha=$(git rev-parse --verify -q "refs/remotes/origin/$DEFAULT_BRANCH" 2>/dev/null || echo ""); o_head=$(git rev-parse HEAD); temp_sha=""; r_base=""
  [ -n "$r_sha" ] && [ "$r_sha" != "$o_head" ] && { anc "$r_sha" "$o_head" || { anc "$o_head" "$r_sha" && r_base="$r_sha" || { echo "ERROR: Remote branch has diverged from local HEAD" >&2; exit 1; }; }; }
  if [ -n "$r_base" ] || { [ -n "$t_sha" ] && ! anc "$t_sha" "$o_head"; }; then
    ! git diff --cached --quiet && { git commit -q -m "chore: temp" >/dev/null 2>&1 || { echo "ERROR: temp commit failed" >&2; exit 1; }; temp_sha=$(git rev-parse HEAD); }
    git reset --hard "${r_base:-$o_head}" >/dev/null 2>&1
    [ -n "$t_sha" ] && ! anc "$t_sha" HEAD && { git merge --no-edit -q "$t_sha" >/dev/null 2>&1 || { git merge --abort >/dev/null 2>&1 || true; git reset --hard "${temp_sha:-$o_head}" >/dev/null 2>&1; git reset --soft "$o_head" >/dev/null 2>&1; echo "ERROR: Merge conflict with origin/$DEFAULT_BRANCH" >&2; exit 1; }; }
    [ -n "$temp_sha" ] && { git cherry-pick -n "$temp_sha" >/dev/null 2>&1 || { git cherry-pick --abort >/dev/null 2>&1 || true; git reset --hard "$temp_sha" >/dev/null 2>&1; git reset --soft "$o_head" >/dev/null 2>&1; echo "ERROR: Merge conflict applying staged changes" >&2; exit 1; }; }
  fi
  has_c=0; [ -n "$t_sha" ] && ! git diff --quiet "$t_sha" HEAD 2>/dev/null && has_c=1; [ -z "$t_sha" ] && [ "$(git rev-parse HEAD 2>/dev/null)" != "$(git rev-parse "$DEFAULT_BRANCH" 2>/dev/null || true)" ] && has_c=1; git diff --cached --quiet && [ "$has_c" -eq 0 ] && { jq -n --arg s "no_changes" --arg m "No modifications detected in scratch directory." '{status:$s,message:$m}'; exit 0; }
  if [ -f .git/.aerial-verified-tree ] && [ "$(git write-tree 2>/dev/null)" = "$(cat .git/.aerial-verified-tree 2>/dev/null)" ]; then echo "⚡ Skipping duplicate pre-flight verification (staged tree already verified)." >&2; elif [ -f scripts/verify.sh ]; then chmod +x scripts/verify.sh && ./scripts/verify.sh --staged >&2 || { [ -n "$temp_sha" ] && { git reset --hard "$temp_sha" >/dev/null 2>&1; git reset --soft "$o_head" >/dev/null 2>&1; } || { [ "$(git rev-parse HEAD)" != "$o_head" ] && git reset --hard "$o_head" >/dev/null 2>&1 || true; }; echo "ERROR: Pre-flight verification failed" >&2; exit 1; }; fi
  title="${msg:-feat($REPO_NAME): automated update by Aerial}"; title=$(printf "%s\n" "$title" | awk 'NF{print;exit}' | cut -c 1-256); ! git diff --cached --quiet && git commit -m "$(printf "%s\n\n%s" "$title" "$BODY")" >/dev/null
  [ -n "$r_sha" ] && ! anc "$r_sha" HEAD && { echo "ERROR: Remote branch has commits not in local HEAD" >&2; exit 1; }; if ! p_err=$(git push -u origin "$cur_b" 2>&1); then echo "ERROR: push failed: $p_err" >&2; exit 1; fi
  if [ -z "$ex_pr" ]; then
    raw=$(gh_call -X POST "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/pulls" -d "$(jq -n --arg t "$title" --arg h "$cur_b" --arg b "$DEFAULT_BRANCH" --arg bd "$BODY" '{title:$t,head:$h,base:$b,body:$bd}')")
    p_code=$(echo "$raw" | tail -n1); p_resp=$(echo "$raw" | sed '$d')
    if is_ok "$p_code"; then ex_pr="$p_resp"; elif [ "$p_code" = "422" ]; then raw_get=$(gh_call -X GET "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/pulls?state=open&head=$REPO_OWNER:$enc_b"); is_ok "$(echo "$raw_get" | tail -n1)" && ex_pr=$(echo "$raw_get" | sed '$d' | jq -c '.[0] // empty' 2>/dev/null || true); fi; [ -z "$ex_pr" ] && { echo "ERROR: Failed to create Pull Request (HTTP $p_code): $p_resp" >&2; exit 1; }
  fi
  p_num=$(echo "$ex_pr" | jq -r '.number // empty'); p_url=$(echo "$ex_pr" | jq -r '.html_url // empty'); c_sha=$(git rev-parse HEAD); [ -z "$p_num" ] && { echo "ERROR: Failed PR: $ex_pr" >&2; exit 1; }
  jq -n --arg s "submitted" --arg u "$p_url" --argjson n "$p_num" --arg b "$cur_b" --arg c "$c_sha" --arg r "$REPO_NAME" --arg m "PR #$p_num submitted on $REPO_OWNER/$REPO_NAME. Continuous delivery automated via push events. End active turn now." '{status:$s,pr_url:$u,pr_number:$n,branch:$b,commit_sha:$c,async:true,repo:$r,turn_action:"end_turn",message:$m}'
elif [ "$CMD" = "override-roborev" ] || [ "$CMD" = "dispute-roborev" ]; then
  pr="${ARGS[0]:-}"; [ -z "$REB" ] && REB="${ARGS[1]:-}"; [ -n "$RFILE" ] && { [ -f "$RFILE" ] || { echo "ERROR: rebuttal file not found: $RFILE" >&2; exit 1; }; REB="$(cat "$RFILE")"; }
  [[ "$pr" =~ ^[0-9]+$ ]] && [ -n "${REB//[[:space:]]/}" ] || { echo "ERROR: numeric PR number and rebuttal required" >&2; exit 1; }
  parse_remote; raw=$(gh_call -X GET "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/pulls/$pr"); code=$(echo "$raw" | tail -n1); resp=$(echo "$raw" | sed '$d')
  is_ok "$code" || { echo "ERROR: Query PR failed (HTTP $code)" >&2; exit 1; }
  [ "$(echo "$resp" | jq -r .state)" = "closed" ] && { echo "ERROR: PR #$pr is closed" >&2; exit 1; }; h_sha=$(echo "$resp" | jq -r '.head.sha // empty'); short_sha="${h_sha:0:7}"; pr_url=$(echo "$resp" | jq -r '.html_url // empty'); [ -n "$h_sha" ] || { echo "ERROR: Could not resolve head SHA" >&2; exit 1; }
  comments=$(gh_call -X GET "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/issues/$pr/comments?per_page=100" | sed '$d'); exists=$(echo "$comments" | jq -r --arg s "$short_sha" '[.[]? | select((.body//"") | contains("## roborev-override") and contains($s))] | first // empty')
  if [ -z "$exists" ]; then
    raw=$(gh_call -H "Content-Type: application/json" -X POST -d "$(jq -n --arg b "$(printf '## roborev-override (`%s`)\n\n**Commit:** `%s`\n**Technical Rebuttal:**\n%s\n' "$short_sha" "$h_sha" "$REB")" '{body: $b}')" "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/issues/$pr/comments")
    is_ok "$(echo "$raw" | tail -n1)" || { echo "ERROR: Comment failed" >&2; exit 1; }
  fi
  runs=$(gh_call -X GET "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/actions/runs?head_sha=$h_sha" | sed '$d'); target_run=$(echo "$runs" | jq -c '([.workflow_runs[]? | select((.name // "") == "RoboRev Code Review" or (.path // "" | endswith("roborev.yml")))] | first) // ([.workflow_runs[]? | select((.name // "") == "Continuous Delivery" or (.name // "") == "Validate Configuration" or (.path // "" | test("(docker-publish|ci|validate)\\.yml$")))] | first) // empty'); run_id=$(echo "$target_run" | jq -r '.id // empty'); run_stat=$(echo "$target_run" | jq -r '.status // empty'); [ -n "$run_id" ] || { echo "ERROR: No workflow run found" >&2; exit 1; }
  rerun_call() { local c; c=$(gh_call -X POST "$GITHUB_API/repos/$REPO_OWNER/$REPO_NAME/actions/runs/$run_id/$1" | tail -n1); [ "$c" = "403" ] || [ "$c" = "409" ] || is_ok "$c"; }; [ "$run_stat" = "completed" ] && { rerun_call rerun-failed-jobs || rerun_call rerun || { echo "ERROR: Rerun failed" >&2; exit 1; }; }
  jq -n --arg pr_url "$pr_url" --argjson pr_number "$pr" --arg commit_sha "$h_sha" --argjson run_id "$run_id" --arg repo "$REPO_NAME" --arg message "RoboRev gate overridden for PR #$pr ($short_sha) on $REPO_OWNER/$REPO_NAME. Re-run triggered." \
    '{status: "overridden", pr_url: $pr_url, pr_number: $pr_number, commit_sha: $commit_sha, run_id: $run_id, repo: $repo, turn_action: "end_turn", message: $message}'
else
  echo "Usage: $0 {init|submit|override-roborev}" >&2; exit 1
fi
