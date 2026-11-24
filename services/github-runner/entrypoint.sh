#!/usr/bin/env bash
set -euo pipefail
unset OPENAI_API_KEY || true
chmod 0755 /root
mkdir -p /root/.roborev /root/.claude/projects /root/.config/muse/sessions && chmod 0700 /root/.roborev /root/.claude /root/.config/muse
find /root/.claude/projects /root/.config/muse/sessions -type f -mtime +14 -delete 2>/dev/null || true
echo '{"hasCompletedOnboarding":true}' > /root/.claude.json && chmod 0600 /root/.claude.json
install -m 0600 /etc/claude-settings.json /root/.claude/settings.json
[ -s /root/.config/muse/settings.json ] || { echo '{"schema_version":1}' > /root/.config/muse/settings.json && chmod 0600 /root/.config/muse/settings.json; }

: > /actions-runner/.env && chmod 0600 /actions-runner/.env
seed_file() { for s in "/local/$1" "/secrets/$1" "${3:-}"; do [ -n "$s" ] && [ -s "$s" ] && install -m 0600 "$s" "$2" && return 0; done; return 1; }
if seed_file claude-oauth-token /root/.roborev/claude-oauth-token; then
  CLAUDE_CODE_OAUTH_TOKEN=$(tr -d '\r\n' < /root/.roborev/claude-oauth-token)
  export CLAUDE_CODE_OAUTH_TOKEN
  echo "CLAUDE_CODE_OAUTH_TOKEN=${CLAUDE_CODE_OAUTH_TOKEN}" >> /actions-runner/.env
fi
seed_file muse-auth.json /root/.config/muse/auth.json || true
seed_file roborev-config.toml /root/.roborev/config.toml /etc/roborev/config.toml || true
seed_file postgres-password /root/.roborev/postgres-password || sed -i '/^\[sync\]/,/^interval = .*/d' /root/.roborev/config.toml

RUNNER_LABELS="${RUNNER_LABELS:-self-hosted,roborev}"
echo "${PATH}" > /actions-runner/.path

if [ -d "/cache" ]; then
  mkdir -p /data /cache/go-build /cache/go/pkg/mod /cache/pip /cache/golangci-lint /cache/cargo-target /cache/npm /cache/buildx-config /cache/bazel-repo-contents /cache/bazel-repo-cache
  ln -sfn /cache /data/cache
  chown -h 1000:1000 /data/cache /cache /cache/go-build /cache/go /cache/go/pkg /cache/go/pkg/mod /cache/pip /cache/golangci-lint /cache/cargo-target /cache/npm /cache/buildx-config /cache/bazel-repo-contents /cache/bazel-repo-cache 2>/dev/null || true
  if [ -z "$(find /cache -maxdepth 1 -name .last-trim -mmin -360 2>/dev/null)" ]; then
    touch /cache/.last-trim 2>/dev/null || true
    find /cache/go-build /cache/golangci-lint -maxdepth 2 -type f -mmin +1440 -delete 2>/dev/null || true
  fi
  printf '%s\n' GOCACHE=/data/cache/go-build GOPATH=/data/cache/go GOMODCACHE=/data/cache/go/pkg/mod GOLANGCI_LINT_CACHE=/data/cache/golangci-lint GOFLAGS=-trimpath PIP_CACHE_DIR=/cache/pip RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo CARGO_TARGET_DIR=/cache/cargo-target NPM_CONFIG_CACHE=/cache/npm BAZELISK_HOME=/opt/bazel-cache >> /actions-runner/.env
fi

if [ -S "/var/run/docker.sock" ]; then
  DOCKER_GID=$(stat -c '%g' /var/run/docker.sock 2>/dev/null || true)
  [ -n "${DOCKER_GID}" ] && [ "${DOCKER_GID}" != "0" ] && groupadd -for -g "${DOCKER_GID}" hostdocker 2>/dev/null && usermod -aG hostdocker ubuntu 2>/dev/null || true
fi

find /root/.roborev /root/.claude /root/.config /root/.docker /home/ubuntu /actions-runner \( -not -user 1000 -o -not -group 1000 \) -exec chown -h 1000:1000 {} + 2>/dev/null || true

run_as_ubuntu() {
  setpriv --reuid=ubuntu --regid=ubuntu --init-groups env -u RUNNER_ALLOW_RUNASROOT HOME=/home/ubuntu USER=ubuntu LOGNAME=ubuntu "$@"
}
cleanup() {
  echo "Caught signal, stopping runner..."
  [ -n "${RUNNER_PID:-}" ] && kill "${RUNNER_PID}" 2>/dev/null || true
  if [[ ",${RUNNER_LABELS}," == *",roborev,"* ]] && [ -x /usr/local/bin/roborev.real ]; then
    run_as_ubuntu env -u GITHUB_PAT -u GH_TOKEN -u GITHUB_TOKEN /usr/local/bin/roborev.real daemon stop 2>/dev/null || true
  fi
  exit 0
}
trap cleanup SIGTERM SIGINT
cd /actions-runner
echo "Starting runner..."
RUN_ENV=()
[[ ",${RUNNER_LABELS}," == *",ci,"* || ",${RUNNER_LABELS}," == *",deploy,"* ]] && RUN_ENV=(-u GITHUB_PAT)
run_as_ubuntu env "${RUN_ENV[@]}" ./run.sh &
RUNNER_PID=$!
wait "${RUNNER_PID}"
