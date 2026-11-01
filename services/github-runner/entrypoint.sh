#!/usr/bin/env bash
set -euo pipefail

export RUNNER_ALLOW_RUNASROOT=1

# Create isolated directories with 0700 permissions
mkdir -p /root/.gemini/antigravity-cli /root/.roborev
chmod 0700 /root/.gemini/antigravity-cli /root/.roborev

# Pre-populate global /root/.roborev/config.toml with cluster-wide review guidelines,
# global exclude_patterns (honored even by security specialist reviews), and engine defaults.
cat > /root/.roborev/config.toml <<'EOF'
default_agent = "gemini"
review_agent = "gemini"
review_reasoning = "thorough"
review_min_severity = "medium"

exclude_patterns = [
  "*.sum",
  "*.lock",
  "package-lock.json",
  "pnpm-lock.yaml",
  "web/static/ui/**"
]

review_guidelines = """
Global Aerial Stack & Cluster Invariants (applies across all repositories):
1. Security & Secrets: Zero plaintext tokens, API keys, passwords, or GitHub PATs in code, configs, logs, or git configs. Zero imperative SQL DDL (ALTER TABLE, DROP TABLE) in application code (must use declarative schema.sql + Atlas migrations). Prevent command injection, SSRF, XSS, and path traversal on untrusted inputs.
2. Concurrency & Reliability: Zero data races, unclosed HTTP response bodies, or leaked goroutines/tickers/timers. Zero arbitrary time.Sleep calls in tests or production plumbing.
3. Architecture & Testing: Functional Core / Imperative Shell. Go unit tests must use homogeneous table-driven loops without closure fields on table structs (run, check, setup, runnerFn, opts, etc.) or catch-all Test*Coverage* names.
4. Internal Cluster & Runtime Coverage Topology: All services run inside a private HashiCorp Nomad cluster (*.aerial, *.lan, aerial-postgres, aerial-hangar, aerial-brain, vanadis) behind authenticated path-scoped gateways that never expose /debug/*. Outbound internal Nomad DNS names are trusted private mesh endpoints, not public internet URLs. Compiling Go binaries directly with -cover -covermode=atomic in Dockerfiles and unconditionally registering GET/POST /debug/coverage/flush (calling coverage.WriteMetaDir and coverage.WriteCountersDir into an ephemeral os.MkdirTemp directory and streaming an application/x-tar archive) are intentional cluster-wide invariants. Do NOT flag /debug/coverage/flush, WriteMetaDir, WriteCountersDir, or -cover in Dockerfiles as security, DoS, or disk-I/O findings. In services/github-runner/entrypoint.sh and nomad/jobs/github-runner.nomad, retaining GITHUB_PAT for ./run.sh in roborev-runners while stripping GITHUB_PAT/GH_TOKEN/GITHUB_TOKEN inside the /usr/local/bin/roborev wrapper is intentional so allowlisted fork PRs can post review comments via gh pr comment without exposing tokens to agy review subprocesses. Mounting /var/run/docker.sock and aerial-ci-cache:/cache in group "ci-runners" (while group "roborev-runners" has no docker.sock and ci-runners strip GITHUB_PAT via env -u GITHUB_PAT ./run.sh) is an intentional cluster invariant.
"""
EOF
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
/usr/local/bin/sync-gemini-settings || true
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
  mkdir -p /cache/go-build /cache/go-mod /cache/pip "/cache/golangci-lint-${RUNNER_NAME}" "/cache/cargo-target-${RUNNER_NAME}"
  export GOCACHE="/cache/go-build"
  export GOMODCACHE="/cache/go-mod"
  export PIP_CACHE_DIR="/cache/pip"
  export GOLANGCI_LINT_CACHE="/cache/golangci-lint-${RUNNER_NAME}"
  export RUSTUP_HOME="/usr/local/rustup"
  export CARGO_HOME="/usr/local/cargo"
  export CARGO_TARGET_DIR="/cache/cargo-target-${RUNNER_NAME}"

  cat >> /actions-runner/.env <<EOF
GOCACHE=/cache/go-build
GOMODCACHE=/cache/go-mod
PIP_CACHE_DIR=/cache/pip
GOLANGCI_LINT_CACHE=/cache/golangci-lint-${RUNNER_NAME}
RUSTUP_HOME=/usr/local/rustup
CARGO_HOME=/usr/local/cargo
CARGO_TARGET_DIR=/cache/cargo-target-${RUNNER_NAME}
EOF
fi

echo "Minting runner registration token for ${TARGET_REPO}..."
REG_TOKEN=$(mint_runner_token "registration-token")

cd /actions-runner

echo "Configuring GitHub Actions Runner..."
./config.sh --unattended \
  --url "https://github.com/${TARGET_REPO}" \
  --token "${REG_TOKEN}" \
  --name "${RUNNER_NAME}" \
  --labels "${RUNNER_LABELS}" \
  --work "_work" \
  --replace

cleanup() {
  echo "Caught signal, deregistering runner..."
  if [ -n "${RUNNER_PID:-}" ]; then
    kill "${RUNNER_PID}" 2>/dev/null || true
  fi

  local rem_token
  rem_token=$(mint_runner_token "remove-token" 2>/dev/null || true)
  if [ -n "${rem_token}" ]; then
    ./config.sh remove --token "${rem_token}" || true
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
