#!/usr/bin/env bash
set -euo pipefail

export RUNNER_ALLOW_RUNASROOT=1

# Create isolated directories with 0700 permissions
mkdir -p /root/.gemini/antigravity-cli /root/.roborev
chmod 0700 /root/.gemini/antigravity-cli /root/.roborev

# Pre-populate global /root/.roborev/config.toml with cluster-wide review guidelines,
# global exclude_patterns (honored even by security specialist reviews), engine defaults,
# and a 2-member default+test-value review panel for daemon-backed CI reviews.
cat > /root/.roborev/config.toml <<'EOF'
default_agent = "gemini"
review_agent = "gemini"
review_reasoning = "thorough"
review_min_severity = "medium"
agent_quota_cooldown = "2m"
review_context_count = 5

exclude_patterns = [
  "*.sum",
  "*.lock",
  "package-lock.json",
  "pnpm-lock.yaml",
  "web/static/ui/**"
]

[ci]
review_types = ["default", "test-value"]

[review]
default_panel = "ci"

[review.subagents.default]
review_type = "default"

[review.subagents.test-value]
review_type = "test-value"

[review.panels.ci]
members = ["default", "test-value"]

review_guidelines = """
Global Aerial Stack & Cluster Invariants (applies across all repositories):
1. Security & Secrets: Zero plaintext tokens, API keys, passwords, or GitHub PATs in code, configs, logs, or git configs. Zero imperative SQL DDL (ALTER TABLE, DROP TABLE) in application code (must use declarative schema.sql + Atlas migrations). Prevent command injection, SSRF, XSS, and path traversal on untrusted inputs.
2. Concurrency & Reliability: Zero data races, unclosed HTTP response bodies, or leaked goroutines/tickers/timers. Zero arbitrary time.Sleep calls in tests or production plumbing.
3. Architecture & Testing: Functional Core / Imperative Shell. Go unit tests must use homogeneous table-driven loops without closure fields on table structs (run, check, setup, runnerFn, opts, etc.) or catch-all Test*Coverage* names.
4. Internal Cluster & Runtime Coverage Topology: All services run inside a private NixOS k3s Kubernetes cluster (*.aerial, *.lan, postgres, synapse, eri, vanadis, haro, coven) behind NGINX Gateway Fabric (lan-gateway and edge-gateway) that never exposes /debug/*. Outbound internal cluster DNS names are trusted private mesh endpoints, not public internet URLs. Compiling Go binaries directly with -cover -covermode=atomic in Dockerfiles and unconditionally registering GET/POST /debug/coverage/flush (calling coverage.WriteMetaDir and coverage.WriteCountersDir into an ephemeral os.MkdirTemp directory and streaming an application/x-tar archive) are intentional cluster-wide invariants. Do NOT flag /debug/coverage/flush, WriteMetaDir, WriteCountersDir, or -cover in Dockerfiles as security, DoS, or disk-I/O findings. In services/github-runner/entrypoint.sh and k8s/operators-and-storage.yaml, retaining GITHUB_PAT for ./run.sh in roborev-runner-* runner deployments/scale sets (RUNNER_LABELS=self-hosted,roborev) while stripping GITHUB_PAT/GH_TOKEN/GITHUB_TOKEN inside the /usr/local/bin/roborev wrapper is intentional so allowlisted fork PRs can post review comments via gh pr comment without exposing tokens to agy review subprocesses. Mounting /var/run/docker.sock and /cache in ci-runner-* runner deployments/scale sets (RUNNER_LABELS=self-hosted,ci, which strip GITHUB_PAT via env -u GITHUB_PAT ./run.sh) while roborev-runner-* scale sets use isolated per-pod /root/.roborev storage synced to in-cluster PostgreSQL via /local/postgres-password without docker.sock is an intentional cluster invariant.
5. Prior Review Consistency (<prior-range-reviews>): When <prior-range-reviews> is present in the prompt, treat any file, function, or code block that was already reviewed in an ancestor commit on this branch and NOT modified between the prior reviewed commit and HEAD as previously approved. Do NOT raise new findings on untouched code that passed an earlier review on the same branch unless a newly modified line directly broke a caller/callee contract with that code. If a prior finding was fixed, verify the fix and do not re-flag it or invent tangential style/nitpick findings in the same area.
"""
EOF

if [ -s "/local/postgres-password" ]; then
  cp -f "/local/postgres-password" "/root/.roborev/postgres-password"
  chmod 0600 "/root/.roborev/postgres-password"
elif [ -s "/secrets/postgres-password" ]; then
  cp -f "/secrets/postgres-password" "/root/.roborev/postgres-password"
  chmod 0600 "/root/.roborev/postgres-password"
fi

if [ -s "/root/.roborev/postgres-password" ]; then
  cat >> /root/.roborev/config.toml <<'EOF'

[sync]
enabled = true
postgres_url = "postgres://aerial:${file:/root/.roborev/postgres-password}@postgres:5432/aerial?sslmode=disable"
interval = "1h"
EOF
fi
chmod 0600 /root/.roborev/config.toml

# Seed antigravity-oauth-token as a writable 0600 file from /local/antigravity-oauth-token (or fallback /secrets/antigravity-oauth-token)
if [ -s "/local/antigravity-oauth-token" ]; then
  cp -f "/local/antigravity-oauth-token" "/root/.gemini/antigravity-cli/antigravity-oauth-token"
  chmod 0600 "/root/.gemini/antigravity-cli/antigravity-oauth-token"
elif [ -s "/secrets/antigravity-oauth-token" ]; then
  cp -f "/secrets/antigravity-oauth-token" "/root/.gemini/antigravity-cli/antigravity-oauth-token"
  chmod 0600 "/root/.gemini/antigravity-cli/antigravity-oauth-token"
fi

# Generate installation_id if not already present
if [ ! -s "/root/.gemini/antigravity-cli/installation_id" ]; then
  cat /proc/sys/kernel/random/uuid > "/root/.gemini/antigravity-cli/installation_id"
  chmod 0600 "/root/.gemini/antigravity-cli/installation_id"
fi
INSTALLATION_ID=$(tr -d '\r\n' < "/root/.gemini/antigravity-cli/installation_id")

# Pre-populate jetski_state.pbtxt with completed onboarding steps and migrations
cat > "/root/.gemini/antigravity-cli/jetski_state.pbtxt" <<EOF
post_onboarding:  {
  completed_steps:  POST_ONBOARDING_STEP_TYPE_MANAGER_WELCOME
  completed_steps:  POST_ONBOARDING_STEP_TYPE_USAGE_MODE
  completed_steps:  POST_ONBOARDING_STEP_TYPE_AGENT_CONFIGURATION
  completed_steps:  POST_ONBOARDING_STEP_TYPE_ADD_WORKSPACE
}
installation_uuid:  "${INSTALLATION_ID}"
migrations:  {
  key:  3
  value:  MIGRATION_STATUS_COMPLETED
}
migrations:  {
  key:  4
  value:  MIGRATION_STATUS_COMPLETED
}
migrations:  {
  key:  5
  value:  MIGRATION_STATUS_COMPLETED
}
EOF
chmod 0600 "/root/.gemini/antigravity-cli/jetski_state.pbtxt"

# Pre-populate /root/.gemini/antigravity-cli/permissions.json with read-only review tool permissions
# and merge with base defaults into /root/.gemini/antigravity-cli/settings.json
# so concurrent agy review goroutines inherit settings without racing on updates.
cat > /root/.gemini/antigravity-cli/permissions.json <<'EOF'
{
  "permissions": {
    "allow": [
      "read_file(*)",
      "command(*)",
      "unsandboxed(*)",
      "command(pwd)",
      "command(wc)",
      "command(ls)",
      "command(cat)",
      "command(head)",
      "command(tail)",
      "command(stat)",
      "command(file)",
      "command(git)",
      "command(rg)",
      "command(grep)",
      "command(egrep)",
      "command(fgrep)",
      "command(find)",
      "command(echo)",
      "command(printf)",
      "command(sort)",
      "command(uniq)",
      "command(awk)",
      "command(sed)",
      "command(cut)",
      "command(tr)",
      "command(diff)",
      "command(jq)",
      "command(dirname)",
      "command(basename)",
      "command(realpath)",
      "command(readlink)",
      "unsandboxed(pwd)",
      "unsandboxed(wc)",
      "unsandboxed(ls)",
      "unsandboxed(cat)",
      "unsandboxed(head)",
      "unsandboxed(tail)",
      "unsandboxed(stat)",
      "unsandboxed(file)",
      "unsandboxed(git)",
      "unsandboxed(rg)",
      "unsandboxed(grep)",
      "unsandboxed(egrep)",
      "unsandboxed(fgrep)",
      "unsandboxed(find)",
      "unsandboxed(echo)",
      "unsandboxed(printf)",
      "unsandboxed(sort)",
      "unsandboxed(uniq)",
      "unsandboxed(awk)",
      "unsandboxed(sed)",
      "unsandboxed(cut)",
      "unsandboxed(tr)",
      "unsandboxed(diff)",
      "unsandboxed(jq)",
      "unsandboxed(dirname)",
      "unsandboxed(basename)",
      "unsandboxed(realpath)",
      "unsandboxed(readlink)"
    ],
    "deny": [
      "read_file(/local/*)",
      "read_file(/secrets/*)",
      "read_file(/root/.roborev/*)",
      "command(git push)",
      "command(git commit)",
      "command(git reset)",
      "command(git clean)",
      "command(git checkout)",
      "command(sed -i)",
      "unsandboxed(git push)",
      "unsandboxed(git commit)",
      "unsandboxed(git reset)",
      "unsandboxed(git clean)",
      "unsandboxed(git checkout)",
      "unsandboxed(sed -i)"
    ]
  }
}
EOF
chmod 0600 /root/.gemini/antigravity-cli/permissions.json

cat > /usr/local/bin/sync-gemini-settings <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

BASE_DEFAULTS='{"model": "gemini-4-argon-high", "trustedWorkspaces": ["/app", "/actions-runner"], "useG1Credits": true}'
PERMS_FILE="/root/.gemini/antigravity-cli/permissions.json"
TARGET_SETTINGS="/root/.gemini/antigravity-cli/settings.json"
TMP_SETTINGS="/root/.gemini/antigravity-cli/.settings.json.tmp.$$"

trap 'rm -f "${TMP_SETTINGS}"' EXIT

if [ -f "${PERMS_FILE}" ] && jq -e 'type == "object"' "${PERMS_FILE}" >/dev/null 2>&1; then
  jq -n --argjson base "${BASE_DEFAULTS}" --slurpfile perms "${PERMS_FILE}" '$base * $perms[0]' > "${TMP_SETTINGS}"
else
  echo "${BASE_DEFAULTS}" | jq '.' > "${TMP_SETTINGS}"
fi

chmod 0600 "${TMP_SETTINGS}"
if [ ! -f "${TARGET_SETTINGS}" ] || ! cmp -s "${TMP_SETTINGS}" "${TARGET_SETTINGS}"; then
  mv -f "${TMP_SETTINGS}" "${TARGET_SETTINGS}"
fi
EOF
chmod 0755 /usr/local/bin/sync-gemini-settings
/usr/local/bin/sync-gemini-settings

if [ -x /usr/local/bin/roborev ] && [ ! -e /usr/local/bin/roborev.real ]; then
  mv /usr/local/bin/roborev /usr/local/bin/roborev.real
  cat > /usr/local/bin/roborev <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

/usr/local/bin/sync-gemini-settings || true

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
    if [ -s "/root/.roborev/postgres-password" ]; then
      env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real sync now >/dev/null 2>&1 || true
    fi
    env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real daemon start >/dev/null 2>&1 || true
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
EOF
  chmod 0755 /usr/local/bin/roborev
fi

# Define a local helper mint_runner_token() that reads /secrets/github_pat
# (falling back to ${GITHUB_PAT:-} only if /secrets/github_pat is absent) into a local pat variable
# and calls POST https://api.github.com/repos/${TARGET_REPO}/actions/runners/$1.
mint_runner_token() {
  local token_type="$1"
  local pat=""
  if [ -f "/secrets/github_pat" ]; then
    pat=$(tr -d '\r\n' < /secrets/github_pat)
  else
    pat="${GITHUB_PAT:-}"
  fi

  if [ -z "${pat}" ]; then
    echo "ERROR: No GitHub PAT found in /secrets/github_pat or GITHUB_PAT environment variable" >&2
    return 1
  fi

  local resp
  resp=$(curl -fsSL -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${pat}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/repos/${TARGET_REPO}/actions/runners/${token_type}")

  echo "${resp}" | jq -r .token
}

TARGET_REPO="${TARGET_REPO:-azylman/aerial}"
RUNNER_NAME="${RUNNER_NAME:-github-runner-$(hostname)}"
RUNNER_LABELS="${RUNNER_LABELS:-self-hosted,roborev}"

echo "${PATH}" > /actions-runner/.path

if [ -d "/cache" ]; then
  mkdir -p /cache/go-build /cache/go-mod /cache/pip /cache/golangci-lint /cache/cargo-target
  export GOCACHE="/cache/go-build"
  export GOMODCACHE="/cache/go-mod"
  export PIP_CACHE_DIR="/cache/pip"
  export GOLANGCI_LINT_CACHE="/cache/golangci-lint"
  export RUSTUP_HOME="/usr/local/rustup"
  export CARGO_HOME="/usr/local/cargo"
  export CARGO_TARGET_DIR="/cache/cargo-target"

  cat >> /actions-runner/.env <<EOF
GOCACHE=/cache/go-build
GOMODCACHE=/cache/go-mod
PIP_CACHE_DIR=/cache/pip
GOLANGCI_LINT_CACHE=/cache/golangci-lint
RUSTUP_HOME=/usr/local/rustup
CARGO_HOME=/usr/local/cargo
CARGO_TARGET_DIR=/cache/cargo-target
EOF
fi

cd /actions-runner

if [ -n "${ACTIONS_RUNNER_INPUT_JITCONFIG:-}" ]; then
  echo "Starting ephemeral ARC runner with JIT configuration..."
else
  echo "Minting runner registration token for ${TARGET_REPO}..."
  REG_TOKEN=$(mint_runner_token "registration-token")

  echo "Configuring GitHub Actions Runner..."
  ./config.sh --unattended \
    --url "https://github.com/${TARGET_REPO}" \
    --token "${REG_TOKEN}" \
    --name "${RUNNER_NAME}" \
    --labels "${RUNNER_LABELS}" \
    --work "_work" \
    --replace
fi

cleanup() {
  echo "Caught signal, deregistering runner..."
  if [ -n "${RUNNER_PID:-}" ]; then
    kill "${RUNNER_PID}" 2>/dev/null || true
  fi

  if [[ ",${RUNNER_LABELS}," == *",roborev,"* ]] && [ -x /usr/local/bin/roborev.real ]; then
    env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real daemon stop 2>/dev/null || true
  fi

  if [ -z "${ACTIONS_RUNNER_INPUT_JITCONFIG:-}" ]; then
    local rem_token
    rem_token=$(mint_runner_token "remove-token" 2>/dev/null || true)
    if [ -n "${rem_token}" ]; then
      ./config.sh remove --token "${rem_token}" || true
    fi
  fi
  exit 0
}

trap cleanup SIGTERM SIGINT

echo "Starting runner..."
if [[ ",${RUNNER_LABELS}," == *",ci,"* ]]; then
  env -u GITHUB_PAT ./run.sh &
else
  ./run.sh &
fi
RUNNER_PID=$!
wait "${RUNNER_PID}"
