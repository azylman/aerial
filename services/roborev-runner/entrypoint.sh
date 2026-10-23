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
  "pnpm-lock.yaml"
]

review_guidelines = """
Global Aerial Stack & Cluster Invariants (applies across all repositories):
1. Security & Secrets: Zero plaintext tokens, API keys, passwords, or GitHub PATs in code, configs, logs, or git configs. Zero imperative SQL DDL (ALTER TABLE, DROP TABLE) in application code (must use declarative schema.sql + Atlas migrations). Prevent command injection, SSRF, XSS, and path traversal on untrusted inputs.
2. Concurrency & Reliability: Zero data races, unclosed HTTP response bodies, or leaked goroutines/tickers/timers. Zero arbitrary time.Sleep calls in tests or production plumbing.
3. Architecture & Testing: Functional Core / Imperative Shell. Go unit tests must use homogeneous table-driven loops without closure fields on table structs (run, check, setup, runnerFn, opts, etc.) or catch-all Test*Coverage* names.
4. Internal Cluster & Runtime Coverage Topology: All services run inside a private HashiCorp Nomad cluster (*.aerial, *.lan, aerial-postgres, aerial-hangar, aerial-brain, vanadis) behind authenticated path-scoped gateways that never expose /debug/*. Outbound internal Nomad DNS names are trusted private mesh endpoints, not public internet URLs. Compiling Go binaries directly with -cover -covermode=atomic in Dockerfiles and unconditionally registering GET/POST /debug/coverage/flush (calling coverage.WriteMetaDir(dir), which is an os.Stat no-op in Go's stdlib when covmeta.<hash> exists and self-heals after 7-day covmeta.* pruning, followed by coverage.WriteCountersDir(dir) when GOCOVERDIR is set) are intentional cluster-wide invariants. Do NOT flag /debug/coverage/flush, WriteMetaDir, or -cover in Dockerfiles as security, DoS, or disk-I/O findings.
"""
EOF
chmod 0600 /root/.roborev/config.toml

# Symlink ONLY antigravity-oauth-token, installation_id, and jetski_state.pbtxt from /mnt/gemini-auth/antigravity-cli/
AUTH_SRC="/mnt/gemini-auth/antigravity-cli"
if [ -d "${AUTH_SRC}" ]; then
  for item in antigravity-oauth-token installation_id jetski_state.pbtxt; do
    if [ -e "${AUTH_SRC}/${item}" ]; then
      ln -sf "${AUTH_SRC}/${item}" "/root/.gemini/antigravity-cli/${item}"
    fi
  done
fi

# Pre-populate /root/.gemini/antigravity-cli/permissions.json with read-only review tool permissions
# and merge with Brain's /mnt/gemini-auth/antigravity-cli/settings.json (inheriting useG1Credits, model, etc.)
# so concurrent agy review goroutines inherit Brain settings without racing on settings.json updates.
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

AUTH_SETTINGS="${AUTH_SRC:-/mnt/gemini-auth/antigravity-cli}/settings.json"
PERMS_FILE="/root/.gemini/antigravity-cli/permissions.json"
TARGET_SETTINGS="/root/.gemini/antigravity-cli/settings.json"
TMP_SETTINGS="/root/.gemini/antigravity-cli/.settings.json.tmp.$$"

trap 'rm -f "${TMP_SETTINGS}"' EXIT

if [ -f "${AUTH_SETTINGS}" ] && jq -e 'type == "object"' "${AUTH_SETTINGS}" >/dev/null 2>&1; then
  jq -s '.[0] * .[1]' "${AUTH_SETTINGS}" "${PERMS_FILE}" > "${TMP_SETTINGS}"
else
  cp "${PERMS_FILE}" "${TMP_SETTINGS}"
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
exec /usr/local/bin/roborev.real "$@"
EOF
  chmod 0755 /usr/local/bin/roborev
fi

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
