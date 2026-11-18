#!/usr/bin/env bash
set -euo pipefail

unset OPENAI_API_KEY || true
/usr/local/bin/sync-gemini-settings || true

if [ -s "/root/.roborev/claude-oauth-token" ]; then
  export CLAUDE_CODE_OAUTH_TOKEN="$(tr -d '\r\n' < /root/.roborev/claude-oauth-token)"
else
  unset CLAUDE_CODE_OAUTH_TOKEN
fi

if [ -d "/cache" ]; then
  export GOCACHE="/data/cache/go-build"
  export GOPATH="/data/cache/go"
  export GOMODCACHE="/data/cache/go/pkg/mod"
  export GOLANGCI_LINT_CACHE="/data/cache/golangci-lint"
  export GOFLAGS="-trimpath"
  export PIP_CACHE_DIR="/cache/pip"
  export CARGO_TARGET_DIR="/cache/cargo-target"
  export NPM_CONFIG_CACHE="/cache/npm"
fi

if [[ "${1:-}" == "ci" && "${2:-}" == "review" ]]; then
  orig_args=("$@")
  shift 2

  agent=""
  reasoning=""
  min_severity=""
  explicit_ref=""
  do_comment=0
  upsert=0
  gh_repo=""
  pr_num=""
  has_review_types=0

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --agent)
        agent="${2:-}"
        shift 2
        ;;
      --agent=*)
        agent="${1#*=}"
        shift
        ;;
      --reasoning)
        reasoning="${2:-}"
        shift 2
        ;;
      --reasoning=*)
        reasoning="${1#*=}"
        shift
        ;;
      --min-severity)
        min_severity="${2:-}"
        shift 2
        ;;
      --min-severity=*)
        min_severity="${1#*=}"
        shift
        ;;
      --ref)
        explicit_ref="${2:-}"
        shift 2
        ;;
      --ref=*)
        explicit_ref="${1#*=}"
        shift
        ;;
      --comment)
        do_comment=1
        shift
        ;;
      --upsert-comments)
        upsert=1
        shift
        ;;
      --gh-repo)
        gh_repo="${2:-}"
        shift 2
        ;;
      --gh-repo=*)
        gh_repo="${1#*=}"
        shift
        ;;
      --pr)
        pr_num="${2:-}"
        shift 2
        ;;
      --pr=*)
        pr_num="${1#*=}"
        shift
        ;;
      --review-types)
        has_review_types=1
        shift 2
        ;;
      --review-types=*)
        has_review_types=1
        shift
        ;;
      *)
        shift
        ;;
    esac
  done

  if [ -s "/root/.codex/auth.json" ] && command -v codex >/dev/null 2>&1; then
    agent="codex"
    reasoning="low"
    for i in "${!orig_args[@]}"; do
      case "${orig_args[i]}" in
        --agent=*|--agent)
          if [[ "${orig_args[i]}" == --agent=* ]]; then
            orig_args[i]="--agent=codex"
          elif [[ "${orig_args[i]}" == "--agent" && $((i+1)) -lt ${#orig_args[@]} ]]; then
            orig_args[i+1]="codex"
          fi
          ;;
        --reasoning=*|--reasoning)
          if [[ "${orig_args[i]}" == --reasoning=* ]]; then
            orig_args[i]="--reasoning=low"
          elif [[ "${orig_args[i]}" == "--reasoning" && $((i+1)) -lt ${#orig_args[@]} ]]; then
            orig_args[i+1]="low"
          fi
          ;;
      esac
    done
  elif [[ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ]] && command -v claude >/dev/null 2>&1; then
    if [[ -z "${agent}" || "${agent}" == "gemini" || "${agent}" == "codex" ]]; then
      agent="claude-code"
    fi
    reasoning="low"
    for i in "${!orig_args[@]}"; do
      if [[ "${orig_args[i]}" == "--agent=gemini" || "${orig_args[i]}" == "--agent=codex" ]]; then
        orig_args[i]="--agent=claude-code"
      elif [[ "${orig_args[i]}" == "--agent" && ( "${orig_args[i+1]:-}" == "gemini" || "${orig_args[i+1]:-}" == "codex" ) ]]; then
        orig_args[i+1]="claude-code"
      elif [[ "${orig_args[i]}" == --reasoning=* ]]; then
        orig_args[i]="--reasoning=low"
      elif [[ "${orig_args[i]}" == "--reasoning" && $((i+1)) -lt ${#orig_args[@]} ]]; then
        orig_args[i+1]="low"
      fi
    done
  else
    if [[ "${agent}" == "codex" || "${agent}" == "claude-code" || "${agent}" == "claude" ]]; then
      agent="gemini"
    fi
    reasoning="low"
    for i in "${!orig_args[@]}"; do
      if [[ "${orig_args[i]}" == "--agent=codex" || "${orig_args[i]}" == "--agent=claude-code" || "${orig_args[i]}" == "--agent=claude" ]]; then
        orig_args[i]="--agent=gemini"
      elif [[ "${orig_args[i]}" == "--agent" && ( "${orig_args[i+1]:-}" == "codex" || "${orig_args[i+1]:-}" == "claude-code" || "${orig_args[i+1]:-}" == "claude" ) ]]; then
        orig_args[i+1]="gemini"
      elif [[ "${orig_args[i]}" == --reasoning=* ]]; then
        orig_args[i]="--reasoning=low"
      elif [[ "${orig_args[i]}" == "--reasoning" && $((i+1)) -lt ${#orig_args[@]} ]]; then
        orig_args[i+1]="low"
      fi
    done
  fi

  branch="${GITHUB_HEAD_REF:-}"
  base_sha=""
  head_sha=""

  if [[ -n "${GITHUB_EVENT_PATH:-}" && -f "${GITHUB_EVENT_PATH}" ]]; then
    if [[ -z "${gh_repo}" ]]; then
      gh_repo="${GITHUB_REPOSITORY:-$(jq -r '.repository.full_name // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)}"
    fi
    if [[ -z "${pr_num}" ]]; then
      pr_num=$(jq -r '.pull_request.number // .number // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)
    fi
    if [[ -z "${branch}" ]]; then
      branch=$(jq -r '.pull_request.head.ref // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)
    fi
    base_sha=$(jq -r '.pull_request.base.sha // .before // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)
    head_sha=$(jq -r '.pull_request.head.sha // .after // empty' "${GITHUB_EVENT_PATH}" 2>/dev/null || true)
  fi

  if [[ -z "${head_sha}" ]]; then
    head_sha=$(git rev-parse HEAD 2>/dev/null || true)
  fi
  if [[ -z "${branch}" ]]; then
    cur_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
    if [[ -n "${cur_branch}" && "${cur_branch}" != "HEAD" ]]; then
      branch="${cur_branch}"
    fi
  fi

  ref="${explicit_ref}"
  if [[ -z "${ref}" && -n "${base_sha}" && -n "${head_sha}" ]]; then
    merge_base=$(git merge-base "${base_sha}" "${head_sha}" 2>/dev/null || echo "${base_sha}")
    ref="${merge_base}..${head_sha}"
  fi

  if [[ -n "${ref}" ]]; then
    if [[ -n "${branch}" && -n "${head_sha}" ]]; then
      git update-ref "refs/heads/${branch}" "${head_sha}" 2>/dev/null || true
    fi

    cmd_args=(review --wait)
    if [[ -z "${explicit_ref}" && -n "${branch}" && -n "${base_sha}" ]]; then
      cmd_args+=(--branch="${branch}" --base="${base_sha}")
    else
      cmd_args+=("${ref}")
    fi
    [[ -n "${agent}" ]] && cmd_args+=(--agent "${agent}")
    [[ -n "${reasoning}" ]] && cmd_args+=(--reasoning "${reasoning}")
    [[ -n "${min_severity}" ]] && cmd_args+=(--min-severity "${min_severity}")

    raw_out=$(mktemp)
    body_out=$(mktemp)
    formatted_out=$(mktemp)
    trap 'rm -f "${raw_out}" "${body_out}" "${formatted_out}"' EXIT

    set +e
    ROBOREV_SYNC_BRANCH="${branch:-__none__}" env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real daemon start >/dev/null 2>&1 || true

    if [ -s "/root/.roborev/postgres-password" ]; then
      for _ in 1 2 3 4 5 6 7 8 9 10; do
        if ROBOREV_SYNC_BRANCH="${branch:-__none__}" env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real sync now >/dev/null 2>&1; then
          break
        fi
        sleep 1
      done
    fi

    # Backfill ## roborev-override PR comments into native roborev responses and close overridden reviews.
    gh_token="${GH_TOKEN:-${GITHUB_TOKEN:-${GITHUB_PAT:-}}}"
    if [[ -n "${gh_token}" && -n "${gh_repo}" && -n "${pr_num}" ]]; then
      pr_comments_json=$(curl -fsSL -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${gh_token}" \
        -H "X-GitHub-Api-Version: 2022-11-28" "https://api.github.com/repos/${gh_repo}/issues/${pr_num}/comments?per_page=100" 2>/dev/null || echo "[]")
      overrides_ndjson=$(printf '%s' "${pr_comments_json}" | jq -c --arg repo "${gh_repo}" '
        .[]? | select(((.body // "") | contains("## roborev-override")) and (.user.login == ($repo | split("/")[0]) or ([.author_association] | inside(["OWNER","MEMBER","COLLABORATOR"]))))
        | {
            commenter: (.user.login // "override"),
            sha: ((try (.body | capture("\\*\\*Commit:\\*\\* `(?<s>[0-9a-fA-F]{7,40})`").s) catch null) // (try (.body | capture("## roborev-override \\(`(?<s>[0-9a-fA-F]{7,40})`\\)").s) catch "")),
            rebuttal: (if (.body | contains("**Technical Rebuttal:**")) then (.body | split("**Technical Rebuttal:**")[1] | gsub("^[[:space:]]+|[[:space:]]+$"; "")) else (.body | gsub("^[[:space:]]+|[[:space:]]+$"; "")) end),
            body: .body
          } | select(.sha != "" and .rebuttal != "")' 2>/dev/null || true)

      if [[ -n "${overrides_ndjson}" ]]; then
        jobs_json=$(env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real list --json --all-branches --limit 100 2>/dev/null || echo "[]")
        head_override_body=""
        override_synced_any=0
        while IFS= read -r ovr_row; do
          [[ -z "${ovr_row}" ]] && continue
          ovr_commenter=$(printf '%s' "${ovr_row}" | jq -r '.commenter')
          ovr_sha=$(printf '%s' "${ovr_row}" | jq -r '.sha')
          ovr_rebuttal=$(printf '%s' "${ovr_row}" | jq -r '.rebuttal')
          ovr_body=$(printf '%s' "${ovr_row}" | jq -r '.body')
          match_job_id=$(printf '%s' "${jobs_json}" | jq -r --arg sha "${ovr_sha}" '
            [.[]? | select((.panel_role // "") != "member" and .status == "done")
              | select((.git_ref == $sha) or (.git_ref | startswith($sha)) or (.git_ref | endswith(".." + $sha)) or (($sha | length) >= 7 and (((.git_ref | split("..") | last) | startswith($sha)) or ($sha | startswith(.git_ref | split("..") | last)))))]
            | first | .id // empty' 2>/dev/null || true)
          if [[ -n "${match_job_id}" ]]; then
            already_recorded=$(env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real show --json --job "${match_job_id}" 2>/dev/null \
              | jq -r --arg reb "${ovr_rebuttal}" 'if ([.comments[]? | select(.response == $reb)] | length) > 0 then "1" else "0" end' 2>/dev/null || echo "0")
            if [[ "${already_recorded}" != "1" ]]; then
              env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real comment --commenter "${ovr_commenter}" --job "${match_job_id}" "${ovr_rebuttal}" >/dev/null 2>&1 || true
              override_synced_any=1
            fi
            if env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real close "${match_job_id}" >/dev/null 2>&1; then
              override_synced_any=1
            fi
          fi
          if [[ -n "${head_sha}" && ( "${head_sha}" == "${ovr_sha}"* || "${ovr_sha}" == "${head_sha:0:7}"* ) ]]; then
            head_override_body="${ovr_body}"
          fi
        done <<< "${overrides_ndjson}"

        if [[ "${override_synced_any}" -eq 1 && -s "/root/.roborev/postgres-password" ]]; then
          env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real sync now >/dev/null 2>&1 || true
        fi
        if [[ -n "${head_override_body}" ]]; then
          short_sha="${head_sha:0:7}"
          [[ -z "${short_sha}" ]] && short_sha=$(git rev-parse --short=7 HEAD 2>/dev/null || echo "unknown")
          printf '## roborev: Review Passed (`%s`)\n\n%s\n' "${short_sha}" "${head_override_body}"
          exit 0
        fi
      fi
    fi

    env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "${cmd_args[@]}" > "${raw_out}" 2>&1
    rc=$?
    if [ -s "/root/.roborev/postgres-password" ]; then
      env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real sync now >/dev/null 2>&1 || true
    fi
    set -e

    if grep -q "^Review (by " "${raw_out}"; then
      awk 'found { print } /^------------------------------------------------------------$/ { if (!found) { found=1; next } }' "${raw_out}" > "${body_out}"
      short_sha="${head_sha:0:7}"
      if [[ -z "${short_sha}" ]]; then
        short_sha=$(git rev-parse --short=7 HEAD 2>/dev/null || echo "unknown")
      fi

      # RoboRev review --wait exits 0 when the review command finishes without crashing,
      # regardless of whether findings are present in body_out. The CI gate (.github/workflows/docker-publish.yml)
      # and Synapse webhook router identify passes strictly by the '^## roborev: Review Passed' header.
      # Only emit 'Review Passed' when rc==0, body_out is non-empty, and no actionable findings or failures exist.
      has_findings=0
      if [[ "${rc}" -ne 0 || ! -s "${body_out}" ]] || \
         grep -qiE "(\*\*Agent assessment:\*\*\s*Fail|^## Findings|\*\*Severity:\*\*\s*(critical|high|medium)|^### [0-9]+\.\s*(critical|high|medium)|Synthesis unavailable)" "${body_out}"; then
        has_findings=1
      fi

      if [[ "${has_findings}" -eq 0 ]]; then
        header="## roborev: Review Passed (\`${short_sha}\`)"
      else
        header="## roborev: Review Complete (\`${short_sha}\`)"
      fi

      {
        echo "${header}"
        echo ""
        cat "${body_out}"
      } > "${formatted_out}"

      cat "${formatted_out}"

      if [[ "${do_comment}" -eq 1 && "${has_findings}" -eq 1 ]]; then
        gh_token="${GH_TOKEN:-${GITHUB_TOKEN:-${GITHUB_PAT:-}}}"
        if [[ -n "${gh_token}" && -n "${gh_repo}" && -n "${pr_num}" ]]; then
          marker="<!-- roborev-ci-review -->"
          comment_payload=$(jq -n --arg marker "${marker}" --rawfile body "${formatted_out}" '{body: ($marker + "\n" + $body)}')
          existing_id=""
          if [[ "${upsert}" -eq 1 ]]; then
            existing_id=$(curl -fsSL \
              -H "Accept: application/vnd.github+json" \
              -H "Authorization: Bearer ${gh_token}" \
              -H "X-GitHub-Api-Version: 2022-11-28" \
              "https://api.github.com/repos/${gh_repo}/issues/${pr_num}/comments?per_page=100" 2>/dev/null \
              | jq -r '[.[] | select(.body | contains("<!-- roborev-ci-review -->")) | .id] | last // empty' 2>/dev/null || true)
          fi
          if [[ -n "${existing_id}" ]]; then
            curl -fsSL -X PATCH \
              -H "Accept: application/vnd.github+json" \
              -H "Authorization: Bearer ${gh_token}" \
              -H "X-GitHub-Api-Version: 2022-11-28" \
              -d "${comment_payload}" \
              "https://api.github.com/repos/${gh_repo}/issues/comments/${existing_id}" >/dev/null 2>&1 || true
          else
            curl -fsSL -X POST \
              -H "Accept: application/vnd.github+json" \
              -H "Authorization: Bearer ${gh_token}" \
              -H "X-GitHub-Api-Version: 2022-11-28" \
              -d "${comment_payload}" \
              "https://api.github.com/repos/${gh_repo}/issues/${pr_num}/comments" >/dev/null 2>&1 || true
          fi
        fi
      fi

      exit 0
    elif grep -q "^Skipped:" "${raw_out}"; then
      cat "${raw_out}"
      exit 0
    elif grep -qi "quota" "${raw_out}"; then
      # The stateless ci review below has no agent failover, so after a quota
      # failure it can only post a misleading "Review Skipped (quota)" comment
      # that hides member reviews the backup agent already completed.
      cat "${raw_out}" >&2
      echo "ERROR: roborev daemon review failed on agent quota (exit ${rc}); not falling back to stateless ci review" >&2
      exit 1
    else
      cat "${raw_out}" >&2
      echo "WARN: roborev daemon review did not complete (exit ${rc}); falling back to stateless ci review" >&2
    fi
  fi

  if [[ "${has_review_types}" -eq 0 ]]; then
    exec env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "${orig_args[@]}" --review-types default,test-value
  fi
  exec env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "${orig_args[@]}"
fi

exec env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "$@"
