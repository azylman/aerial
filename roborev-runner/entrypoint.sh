#!/usr/bin/env bash
set -euo pipefail

export RUNNER_ALLOW_RUNASROOT=1

# Create isolated directories with 0700 permissions
mkdir -p /root/.gemini/antigravity-cli /root/.roborev
chmod 0700 /root/.gemini/antigravity-cli /root/.roborev

# Symlink ONLY antigravity-oauth-token, installation_id, and jetski_state.pbtxt from /mnt/gemini-auth/antigravity-cli/
AUTH_SRC="/mnt/gemini-auth/antigravity-cli"
if [ -d "${AUTH_SRC}" ]; then
  for item in antigravity-oauth-token installation_id jetski_state.pbtxt; do
    if [ -e "${AUTH_SRC}/${item}" ]; then
      ln -sf "${AUTH_SRC}/${item}" "/root/.gemini/antigravity-cli/${item}"
    fi
  done
fi

# Pre-populate /root/.gemini/antigravity-cli/settings.json with read-only review tool permissions
# so 3 concurrent agy review goroutines never race on updating settings.json.
cat > /root/.gemini/antigravity-cli/settings.json <<'EOF'
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
chmod 0600 /root/.gemini/antigravity-cli/settings.json

# Define a local helper mint_runner_token() that reads /secrets/github_pat
# (falling back to ${GITHUB_PAT:-} only if /secrets/github_pat is absent) into a local pat variable
# and calls POST https://api.github.com/repos/${TARGET_REPO}/actions/runners/$1 without exporting GITHUB_PAT.
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
RUNNER_NAME="${RUNNER_NAME:-roborev-runner-$(hostname)}"
RUNNER_LABELS="${RUNNER_LABELS:-self-hosted,roborev}"

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
env -u GITHUB_PAT ./run.sh &
RUNNER_PID=$!
wait "${RUNNER_PID}"
