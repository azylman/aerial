#!/usr/bin/env bash
set -euo pipefail

unset OPENAI_API_KEY || true

# Root bootstrap at container start
chmod 0755 /root
mkdir -p /root/.roborev /root/.claude/projects /root/.config/muse/sessions /root/.local/share /root/.docker/buildx/instances /home/ubuntu /home/ubuntu/.config /home/ubuntu/.local/share
for item in roborev claude claude.json docker; do
  ln -sfn "/root/.${item}" "/home/ubuntu/.${item}"
done
ln -sfn /root/.config/muse /home/ubuntu/.config/muse
ln -sfn /root/.config/muse /root/.local/share/muse
ln -sfn /root/.config/muse /home/ubuntu/.local/share/muse

find /root/.claude/projects -type f -mtime +14 -delete 2>/dev/null || true
find /root/.claude/projects -mindepth 1 -type d -empty -delete 2>/dev/null || true
find /root/.config/muse/sessions -type f -mtime +14 -delete 2>/dev/null || true
find /root/.config/muse/sessions -mindepth 1 -type d -empty -delete 2>/dev/null || true
chmod 0700 /root/.roborev /root/.claude /root/.config/muse

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
      "Read(/root/.config/muse/**)",
      "Read(//root/.config/muse/**)",
      "Read(/root/.local/share/muse/**)",
      "Read(//root/.local/share/muse/**)",
      "Read(/home/ubuntu/.roborev/**)",
      "Read(//home/ubuntu/.roborev/**)",
      "Read(/home/ubuntu/.claude/**)",
      "Read(//home/ubuntu/.claude/**)",
      "Read(/home/ubuntu/.config/muse/**)",
      "Read(//home/ubuntu/.config/muse/**)",
      "Read(/home/ubuntu/.local/share/muse/**)",
      "Read(//home/ubuntu/.local/share/muse/**)",
      "Read(/actions-runner/.env)",
      "Read(//actions-runner/.env)"
    ]
  }
}
EOF
chmod 0600 /root/.claude/settings.json

# Seed claude-oauth-token into /actions-runner/.env and /root/.roborev/claude-oauth-token from /local/claude-oauth-token (or fallback /secrets/claude-oauth-token)
: > /actions-runner/.env
chmod 0600 /actions-runner/.env
CLAUDE_TOKEN_SRC=""
if [ -s "/local/claude-oauth-token" ]; then
  CLAUDE_TOKEN_SRC="/local/claude-oauth-token"
elif [ -s "/secrets/claude-oauth-token" ]; then
  CLAUDE_TOKEN_SRC="/secrets/claude-oauth-token"
fi
if [ -n "${CLAUDE_TOKEN_SRC}" ]; then
  CLAUDE_TOKEN_VAL=$(tr -d '\r\n' < "${CLAUDE_TOKEN_SRC}")
  if [ -n "${CLAUDE_TOKEN_VAL}" ]; then
    export CLAUDE_CODE_OAUTH_TOKEN="${CLAUDE_TOKEN_VAL}"
    echo "CLAUDE_CODE_OAUTH_TOKEN=${CLAUDE_TOKEN_VAL}" >> /actions-runner/.env
    mkdir -p /root/.roborev 2>/dev/null || true
    cp -f "${CLAUDE_TOKEN_SRC}" "/root/.roborev/claude-oauth-token" 2>/dev/null || true
    chmod 0600 "/root/.roborev/claude-oauth-token" 2>/dev/null || true
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
chmod 0600 /root/.roborev/config.toml

if [ -s "/local/postgres-password" ]; then
  cp -f "/local/postgres-password" "/root/.roborev/postgres-password"
  chmod 0600 "/root/.roborev/postgres-password"
elif [ -s "/secrets/postgres-password" ]; then
  cp -f "/secrets/postgres-password" "/root/.roborev/postgres-password"
  chmod 0600 "/root/.roborev/postgres-password"
fi

if [ ! -s "/root/.roborev/postgres-password" ]; then
  sed -i -e '/^\[sync\]/,/^interval = .*/d' /root/.roborev/config.toml
fi
chmod 0600 /root/.roborev/config.toml

RUNNER_LABELS="${RUNNER_LABELS:-self-hosted,roborev}"

echo "${PATH}" > /actions-runner/.path

if [ -d "/cache" ]; then
  mkdir -p /data
  chmod 0755 /data 2>/dev/null || true
  ln -sfn /cache /data/cache
  chown -h 1000:1000 /data/cache 2>/dev/null || true
  (
    flock 200
    mkdir -p /cache/go-build /cache/go/pkg/mod /cache/pip /cache/golangci-lint /cache/cargo-target /cache/npm /cache/buildx-config
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
find /root/.roborev /root/.claude /root/.config /root/.docker /home/ubuntu /actions-runner \( -not -user 1000 -o -not -group 1000 \) -exec chown -h 1000:1000 {} + 2>/dev/null || true

run_as_ubuntu() {
  setpriv --reuid=ubuntu --regid=ubuntu --init-groups env -u RUNNER_ALLOW_RUNASROOT HOME=/home/ubuntu USER=ubuntu LOGNAME=ubuntu "$@"
}

cd /actions-runner

cleanup() {
  echo "Caught signal, stopping runner..."
  if [ -n "${RUNNER_PID:-}" ]; then
    kill "${RUNNER_PID}" 2>/dev/null || true
  fi

  if [[ ",${RUNNER_LABELS}," == *",roborev,"* ]] && [ -x /usr/local/bin/roborev.real ]; then
    run_as_ubuntu env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real daemon stop 2>/dev/null || true
  fi
  exit 0
}

trap cleanup SIGTERM SIGINT

echo "Starting runner..."
if [[ ",${RUNNER_LABELS}," == *",ci,"* || ",${RUNNER_LABELS}," == *",deploy,"* ]]; then
  run_as_ubuntu env -u GITHUB_PAT ./run.sh &
else
  run_as_ubuntu ./run.sh &
fi
RUNNER_PID=$!
wait "${RUNNER_PID}"
