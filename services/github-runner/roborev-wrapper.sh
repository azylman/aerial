#!/usr/bin/env bash
set -euo pipefail

unset OPENAI_API_KEY || true
/usr/local/bin/sync-gemini-settings || true

if [ -s "/root/.roborev/claude-oauth-token" ]; then
  export CLAUDE_CODE_OAUTH_TOKEN="$(tr -d '\r\n' < /root/.roborev/claude-oauth-token)"
else
  unset CLAUDE_CODE_OAUTH_TOKEN
fi

# Canonical workflow-style roborev-override matcher, shared by the pre-review
# check, the quota branch, and the stateless fallback below. Matches only when
# the comment author is the repo owner and the body contains ## roborev-override
# plus a hex token exactly equal to the full head SHA or exactly its 7-char
# prefix (tokens are bounded by non-hex characters, so a short prefix embedded
# in a longer hex string written for another commit never matches).
roborev_workflow_override_match() {
  local comments_json="$1" repo="$2" head="$3"
  local owner short body
  owner="${repo%%/*}"
  short="${head:0:7}"
  [[ -n "${owner}" && -n "${head}" ]] || return 1
  body=$(printf '%s' "${comments_json}" | jq -r --arg owner "${owner}" --arg full "${head}" --arg short "${short}" '
    [ .[]?
      | select(((.body // "") | contains("## roborev-override"))
        and (.user.login == $owner)
        and (((.body | [scan("[0-9a-fA-F]+")] | any(. == $full)) or ((($short != "") and ((.body | [scan("[0-9a-fA-F]+")] | any(. == $short))))))))]
    | last | .body // empty' 2>/dev/null || true)
  [[ -n "${body}" ]] || return 1
  printf '%s' "${body}"
}

roborev_fetch_workflow_override_body() {
  local repo="$1" pr="$2" head="$3"
  local token comments page page_json count all
  token="${GH_TOKEN:-${GITHUB_TOKEN:-${GITHUB_PAT:-}}}"
  [[ -n "${token}" && -n "${repo}" && -n "${pr}" && -n "${head}" ]] || return 1
  # GitHub lists issue comments in ascending order at most 100 per page, so a
  # late override past comment 100 would otherwise be missed. Walk pages until
  # a short page arrives (capped to bound pathological threads). Any fetch
  # failure fails closed.
  all="[]"
  page=1
  while [[ "${page}" -le 10 ]]; do
    page_json=$(curl -fsSL -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${token}" \
      -H "X-GitHub-Api-Version: 2022-11-28" "https://api.github.com/repos/${repo}/issues/${pr}/comments?per_page=100&page=${page}" 2>/dev/null || true)
    [[ -n "${page_json}" ]] || return 1
    count=$(printf '%s' "${page_json}" | jq 'length' 2>/dev/null || echo 0)
    all=$(jq -c -n --argjson a "${all}" --argjson b "${page_json}" '$a + $b' 2>/dev/null || echo "[]")
    [[ "${count}" -lt 100 ]] && break
    page=$((page + 1))
  done
  comments="${all}"
  roborev_workflow_override_match "${comments}" "${repo}" "${head}"
}

roborev_emit_override_pass() {
  local body="$1" head="$2"
  local short
  short="${head:0:7}"
  [[ -z "${short}" ]] && short=$(git rev-parse --short=7 HEAD 2>/dev/null || echo "unknown")
  printf '## roborev: Review Passed (`%s`)\n\n%s\n' "${short}" "${body}"
}

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
  shift 2

  ci_args=(ci review)
  min_severity=""
  explicit_ref=""
  do_comment=0
  upsert=0
  gh_repo=""
  pr_num=""
  has_review_types=0
  review_types_arg=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --agent)
        shift 2
        ;;
      --agent=*)
        shift
        ;;
      --reasoning)
        shift 2
        ;;
      --reasoning=*)
        shift
        ;;
      --min-severity)
        min_severity="${2:-}"
        ci_args+=("$1" "${2:-}")
        shift 2
        ;;
      --min-severity=*)
        min_severity="${1#*=}"
        ci_args+=("$1")
        shift
        ;;
      --ref)
        explicit_ref="${2:-}"
        ci_args+=("$1" "${2:-}")
        shift 2
        ;;
      --ref=*)
        explicit_ref="${1#*=}"
        ci_args+=("$1")
        shift
        ;;
      --comment)
        do_comment=1
        ci_args+=("$1")
        shift
        ;;
      --upsert-comments)
        upsert=1
        ci_args+=("$1")
        shift
        ;;
      --gh-repo)
        gh_repo="${2:-}"
        ci_args+=("$1" "${2:-}")
        shift 2
        ;;
      --gh-repo=*)
        gh_repo="${1#*=}"
        ci_args+=("$1")
        shift
        ;;
      --pr)
        pr_num="${2:-}"
        ci_args+=("$1" "${2:-}")
        shift 2
        ;;
      --pr=*)
        pr_num="${1#*=}"
        ci_args+=("$1")
        shift
        ;;
      --review-types)
        has_review_types=1
        review_types_arg="${2:-}"
        ci_args+=("$1" "${2:-}")
        shift 2
        ;;
      --review-types=*)
        has_review_types=1
        review_types_arg="${1#*=}"
        ci_args+=("$1")
        shift
        ;;
      *)
        ci_args+=("$1")
        shift
        ;;
    esac
  done

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

  if [[ -z "${review_types_arg}" ]]; then
    repo_roborev_toml=$(git show origin/main:.roborev.toml 2>/dev/null || cat .roborev.toml 2>/dev/null || true)
    if [[ -n "${repo_roborev_toml}" ]]; then
      if echo "${repo_roborev_toml}" | grep -qE 'review_types[[:space:]]*=[[:space:]]*\[[[:space:]]*"default"[[:space:]]*\]'; then
        review_types_arg="default"
      fi
    fi
  fi

  if [[ -n "${ref}" ]]; then
    if [[ -n "${branch}" && -n "${head_sha}" ]]; then
      git update-ref "refs/heads/${branch}" "${head_sha}" 2>/dev/null || true
    fi

    cmd_args=(review --wait)
    if [[ "${review_types_arg}" == "default" ]]; then
      cmd_args+=(--panel none)
    fi
    if [[ -z "${explicit_ref}" && -n "${branch}" && -n "${base_sha}" ]]; then
      cmd_args+=(--branch="${branch}" --base="${base_sha}")
    else
      cmd_args+=("${ref}")
    fi
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
      # Workflow-style override (canonical aerial-config treatment): an owner
      # comment containing ## roborev-override plus the full or 7-char head
      # SHA. Freshly fetched, so overrides posted while the review was running
      # still apply even when the strict Commit:/override-header extraction
      # above found nothing.
      if [[ -z "${head_override_body:-}" ]]; then
        head_override_body=$(roborev_fetch_workflow_override_body "${gh_repo}" "${pr_num}" "${head_sha}" 2>/dev/null || true)
      fi
      if [[ -n "${head_override_body:-}" ]]; then
        roborev_emit_override_pass "${head_override_body}" "${head_sha}"
        exit 0
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

      # Upstream roborev.real review --wait computes the canonical structured verdict
      # from Document.Passed(minSeverity) and exits 0 on a PASS verdict and non-zero (1)
      # on a FAIL verdict or job error. Only emit 'Review Passed' when rc==0 and body_out is non-empty.
      has_findings=0
      if [[ "${rc}" -ne 0 || ! -s "${body_out}" ]]; then
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
      # An override posted while the review was running still honours the
      # canonical treatment: re-fetch and exit 0 without burning another agent
      # call. Fail-closed behaviour for non-overridden PRs is unchanged.
      if override_body=$(roborev_fetch_workflow_override_body "${gh_repo}" "${pr_num}" "${head_sha}" 2>/dev/null); [[ -n "${override_body}" ]]; then
        roborev_emit_override_pass "${override_body}" "${head_sha}"
        exit 0
      fi
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

  # No daemon ref was derivable: honour a workflow-style override before the
  # stateless fallback so it never runs an agent for an overridden head.
  if override_body=$(roborev_fetch_workflow_override_body "${gh_repo}" "${pr_num}" "${head_sha}" 2>/dev/null); [[ -n "${override_body}" ]]; then
    roborev_emit_override_pass "${override_body}" "${head_sha}"
    exit 0
  fi
  if [[ "${has_review_types}" -eq 0 ]]; then
    exec env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "${ci_args[@]}" --review-types "${review_types_arg:-default,test-value}"
  fi
  exec env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "${ci_args[@]}"
fi

exec env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real "$@"
