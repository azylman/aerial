#!/usr/bin/env bash
set -euo pipefail

unset OPENAI_API_KEY || true

# Root bootstrap at container start
chmod 0755 /root
mkdir -p /root/.gemini/antigravity-cli /root/.roborev /root/.claude/projects /root/.codex /root/.config/muse /root/.docker/buildx/instances /home/ubuntu /home/ubuntu/.config
for item in roborev claude claude.json codex gemini docker; do
  ln -sfn "/root/.${item}" "/home/ubuntu/.${item}"
done
ln -sfn /root/.config/muse /home/ubuntu/.config/muse

find /root/.claude/projects -type f -mtime +14 -delete 2>/dev/null || true
find /root/.claude/projects -mindepth 1 -type d -empty -delete 2>/dev/null || true
find /root/.codex/sessions -type f -mtime +14 -delete 2>/dev/null || true
find /root/.codex/log -type f -mtime +14 -delete 2>/dev/null || true
if [ -d "/root/.gemini/antigravity-cli/brain" ]; then
  find /root/.gemini/antigravity-cli/brain -mindepth 1 -maxdepth 1 -type d -mtime +14 -exec rm -rf {} + 2>/dev/null || true
fi
if [ -d "/root/.gemini/antigravity-cli/conversations" ]; then
  find /root/.gemini/antigravity-cli/conversations -mindepth 1 -maxdepth 1 -mtime +14 -exec rm -rf {} + 2>/dev/null || true
fi
chmod 0700 /root/.gemini/antigravity-cli /root/.roborev /root/.claude /root/.codex /root/.config/muse

# Pre-populate Claude configuration and deny rules
cat > /root/.claude.json <<'EOF'
{"hasCompletedOnboarding": true}
EOF
chmod 0600 /root/.claude.json

cat > /root/.claude/settings.json <<'EOF'
{
  "permissions": {
    "deny": [
      "Read(/local/**)",
      "Read(//local/**)",
      "Read(/secrets/**)",
      "Read(//secrets/**)",
      "Read(/root/.roborev/**)",
      "Read(//root/.roborev/**)",
      "Read(/root/.claude/**)",
      "Read(//root/.claude/**)",
      "Read(/root/.gemini/**)",
      "Read(//root/.gemini/**)",
      "Read(/root/.codex/**)",
      "Read(//root/.codex/**)",
      "Read(/root/.config/muse/**)",
      "Read(//root/.config/muse/**)",
      "Read(/home/ubuntu/.roborev/**)",
      "Read(//home/ubuntu/.roborev/**)",
      "Read(/home/ubuntu/.claude/**)",
      "Read(//home/ubuntu/.claude/**)",
      "Read(/home/ubuntu/.gemini/**)",
      "Read(//home/ubuntu/.gemini/**)",
      "Read(/home/ubuntu/.codex/**)",
      "Read(//home/ubuntu/.codex/**)",
      "Read(/home/ubuntu/.config/muse/**)",
      "Read(//home/ubuntu/.config/muse/**)"
    ]
  }
}
EOF
chmod 0600 /root/.claude/settings.json

# Seed claude-oauth-token as a writable 0600 file from /local/claude-oauth-token (or fallback /secrets/claude-oauth-token)
if [ -s "/local/claude-oauth-token" ]; then
  cp -f "/local/claude-oauth-token" "/root/.roborev/claude-oauth-token"
  chmod 0600 "/root/.roborev/claude-oauth-token"
elif [ -s "/secrets/claude-oauth-token" ]; then
  cp -f "/secrets/claude-oauth-token" "/root/.roborev/claude-oauth-token"
  chmod 0600 "/root/.roborev/claude-oauth-token"
fi

# Digest-tracked atomic seeding of /root/.codex/auth.json
SEED_CODEX_AUTH=""
if [ -s "/local/codex-auth.json" ]; then
  SEED_CODEX_AUTH="/local/codex-auth.json"
elif [ -s "/secrets/codex-auth.json" ]; then
  SEED_CODEX_AUTH="/secrets/codex-auth.json"
fi

if [ -n "${SEED_CODEX_AUTH}" ] && [ -s "${SEED_CODEX_AUTH}" ]; then
  SEED_SHA=$(sha256sum "${SEED_CODEX_AUTH}" | awk '{print $1}')
  CUR_SEED_SHA=$(cat /root/.codex/.seed-sha256 2>/dev/null || true)
  if [ ! -s "/root/.codex/auth.json" ] || [ "${SEED_SHA}" != "${CUR_SEED_SHA}" ]; then
    TMP_AUTH="/root/.codex/.auth.json.tmp.$$"
    cp -f "${SEED_CODEX_AUTH}" "${TMP_AUTH}"
    chmod 0600 "${TMP_AUTH}"
    mv -f "${TMP_AUTH}" "/root/.codex/auth.json"
    echo "${SEED_SHA}" > /root/.codex/.seed-sha256
    chmod 0600 /root/.codex/.seed-sha256
  fi
fi

# Digest-tracked atomic seeding of /root/.config/muse/auth.json
SEED_MUSE_AUTH=""
if [ -s "/local/muse-auth.json" ]; then
  SEED_MUSE_AUTH="/local/muse-auth.json"
elif [ -s "/secrets/muse-auth.json" ]; then
  SEED_MUSE_AUTH="/secrets/muse-auth.json"
fi

if [ -n "${SEED_MUSE_AUTH}" ] && [ -s "${SEED_MUSE_AUTH}" ]; then
  SEED_SHA=$(sha256sum "${SEED_MUSE_AUTH}" | awk '{print $1}')
  CUR_SEED_SHA=$(cat /root/.config/muse/.seed-sha256 2>/dev/null || true)
  if [ ! -s "/root/.config/muse/auth.json" ] || [ "${SEED_SHA}" != "${CUR_SEED_SHA}" ]; then
    TMP_AUTH="/root/.config/muse/.auth.json.tmp.$$"
    cp -f "${SEED_MUSE_AUTH}" "${TMP_AUTH}"
    chmod 0600 "${TMP_AUTH}"
    mv -f "${TMP_AUTH}" "/root/.config/muse/auth.json"
    echo "${SEED_SHA}" > /root/.config/muse/.seed-sha256
    chmod 0600 /root/.config/muse/.seed-sha256
  fi
fi

if [ ! -s "/root/.config/muse/settings.json" ]; then
  echo '{"schema_version": 1}' > /root/.config/muse/settings.json
  chmod 0600 /root/.config/muse/settings.json
fi

if [ -s "/local/roborev-config.toml" ]; then
  cp -f "/local/roborev-config.toml" "/root/.roborev/config.toml"
elif [ -s "/etc/roborev/config.toml" ]; then
  cp -f "/etc/roborev/config.toml" "/root/.roborev/config.toml"
elif [ -s "$(dirname "$0")/roborev-config.toml" ]; then
  cp -f "$(dirname "$0")/roborev-config.toml" "/root/.roborev/config.toml"
fi

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
      "read_file(/root/.claude/*)",
      "read_file(/root/.gemini/*)",
      "read_file(/root/.codex/*)",
      "read_file(/root/.config/muse/*)",
      "read_file(/home/ubuntu/.roborev/*)",
      "read_file(/home/ubuntu/.claude/*)",
      "read_file(/home/ubuntu/.gemini/*)",
      "read_file(/home/ubuntu/.codex/*)",
      "read_file(/home/ubuntu/.config/muse/*)",
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

BASE_DEFAULTS='{"model": "gemini-4-hydrogen-low", "trustedWorkspaces": ["/app", "/actions-runner"], "useG1Credits": true}'
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

# The agy and roborev wrappers are baked into the image (agy-wrapper.sh,
# roborev-wrapper.sh) over agy.real / roborev.real; never rename them here.

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
  mkdir -p /data
  chmod 0755 /data 2>/dev/null || true
  ln -sfn /cache /data/cache
  chown -h 1000:1000 /data/cache 2>/dev/null || true
  (
    flock 200
    mkdir -p /cache/go-build /cache/go/pkg /cache/pip /cache/golangci-lint /cache/cargo-target /cache/npm /cache/buildx-config
    if [ -d "/cache/go-mod" ] && [ ! -L "/cache/go-mod" ]; then
      mkdir -p /cache/go/pkg/mod
      cp -a /cache/go-mod/. /cache/go/pkg/mod/ 2>/dev/null || true
      rm -rf /cache/go-mod
      ln -sfn go/pkg/mod /cache/go-mod
    elif [ ! -e "/cache/go-mod" ] && [ ! -L "/cache/go-mod" ]; then
      ln -sfn go/pkg/mod /cache/go-mod
    fi
    find /cache \( -not -user 1000 -o -not -group 1000 \) -exec chown -h 1000:1000 {} + 2>/dev/null || true
  ) 200>"/cache/.init.lock"
  export GOCACHE="/data/cache/go-build"
  export GOPATH="/data/cache/go"
  export GOMODCACHE="/data/cache/go/pkg/mod"
  export GOLANGCI_LINT_CACHE="/data/cache/golangci-lint"
  export GOFLAGS="-trimpath"
  export PIP_CACHE_DIR="/cache/pip"
  export RUSTUP_HOME="/usr/local/rustup"
  export CARGO_HOME="/usr/local/cargo"
  export CARGO_TARGET_DIR="/cache/cargo-target"
  export NPM_CONFIG_CACHE="/cache/npm"

  cat >> /actions-runner/.env <<EOF
GOCACHE=/data/cache/go-build
GOPATH=/data/cache/go
GOMODCACHE=/data/cache/go/pkg/mod
GOLANGCI_LINT_CACHE=/data/cache/golangci-lint
GOFLAGS=-trimpath
PIP_CACHE_DIR=/cache/pip
RUSTUP_HOME=/usr/local/rustup
CARGO_HOME=/usr/local/cargo
CARGO_TARGET_DIR=/cache/cargo-target
NPM_CONFIG_CACHE=/cache/npm
EOF
fi

if [ -S "/var/run/docker.sock" ]; then
  DOCKER_GID=$(stat -c '%g' /var/run/docker.sock 2>/dev/null || true)
  if [ -n "${DOCKER_GID}" ] && [ "${DOCKER_GID}" != "0" ]; then
    groupadd -for -g "${DOCKER_GID}" hostdocker 2>/dev/null || true
    usermod -aG hostdocker ubuntu 2>/dev/null || true
  fi
fi

for f in /actions-runner/.path /actions-runner/.env /root/.claude.json; do
  if [ -e "$f" ] || [ -L "$f" ]; then
    chown -h 1000:1000 "$f" 2>/dev/null || true
  fi
done
find /root/.roborev /root/.claude /root/.codex /root/.config /root/.gemini /root/.docker /home/ubuntu /actions-runner \( -not -user 1000 -o -not -group 1000 \) -exec chown -h 1000:1000 {} + 2>/dev/null || true

run_as_ubuntu() {
  setpriv --reuid=ubuntu --regid=ubuntu --init-groups env -u RUNNER_ALLOW_RUNASROOT HOME=/home/ubuntu USER=ubuntu LOGNAME=ubuntu "$@"
}

cd /actions-runner

if [ -n "${ACTIONS_RUNNER_INPUT_JITCONFIG:-}" ]; then
  echo "Starting ephemeral ARC runner with JIT configuration..."
else
  echo "Minting runner registration token for ${TARGET_REPO}..."
  REG_TOKEN=$(mint_runner_token "registration-token")

  echo "Configuring GitHub Actions Runner..."
  run_as_ubuntu ./config.sh --unattended \
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
    run_as_ubuntu env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real daemon stop 2>/dev/null || true
  fi

  if [ -z "${ACTIONS_RUNNER_INPUT_JITCONFIG:-}" ]; then
    local rem_token
    rem_token=$(mint_runner_token "remove-token" 2>/dev/null || true)
    if [ -n "${rem_token}" ]; then
      run_as_ubuntu ./config.sh remove --token "${rem_token}" || true
    fi
  fi
  exit 0
}

trap cleanup SIGTERM SIGINT

echo "Starting runner..."
if [[ ",${RUNNER_LABELS}," == *",ci,"* ]]; then
  run_as_ubuntu env -u GITHUB_PAT ./run.sh &
else
  run_as_ubuntu ./run.sh &
fi
RUNNER_PID=$!
wait "${RUNNER_PID}"
