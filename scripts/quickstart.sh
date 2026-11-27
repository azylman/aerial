#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat << 'EOF'
Usage: quickstart.sh [options]
Bootstrap an Aerial cluster on a single-node k3s host from scratch.
Idempotently verifies/creates the private aerial-config repository,
installs k3s and Flux CLI if missing, seeds bootstrap secrets,
bootstraps Flux CD, and launches agy authentication in eri-restate.

Required: GITHUB_PAT, DISCORD_BOT_TOKEN
Optional: GITHUB_OWNER, AERIAL_CONFIG_REPO (aerial-config), AERIAL_TEMPLATE_REPO (azylman/aerial-config-example)
          KUBECONFIG (~/.kube/config), GITHUB_API_URL, K3S_INSTALL_URL, FLUX_INSTALL_URL
          AERIAL_WAIT_TIMEOUT (15m), AERIAL_SKIP_TTY_EXEC (0), AERIAL_POLL_SLEEP (1)
Options:  -h, --help
EOF
}

for a in "$@"; do case "$a" in -h|--help) usage; exit 0;; esac; done
[ -z "${GITHUB_PAT:-}" ] && { echo "ERROR: GITHUB_PAT environment variable is required" >&2; exit 1; }
[ -z "${DISCORD_BOT_TOKEN:-}" ] && { echo "ERROR: DISCORD_BOT_TOKEN environment variable is required" >&2; exit 1; }
GITHUB_API_URL="${GITHUB_API_URL:-https://api.github.com}"; AERIAL_CONFIG_REPO="${AERIAL_CONFIG_REPO:-aerial-config}"
AERIAL_TEMPLATE_REPO="${AERIAL_TEMPLATE_REPO:-azylman/aerial-config-example}"; KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
export KUBECONFIG; K3S_INSTALL_URL="${K3S_INSTALL_URL:-https://get.k3s.io}"; FLUX_INSTALL_URL="${FLUX_INSTALL_URL:-https://fluxcd.io/install.sh}"
AERIAL_WAIT_TIMEOUT="${AERIAL_WAIT_TIMEOUT:-15m}"; AERIAL_POLL_SLEEP="${AERIAL_POLL_SLEEP:-1}"
if [ -z "${GITHUB_OWNER:-}" ]; then
  user_json=$(curl -fsS -H "Authorization: token $GITHUB_PAT" -H "Accept: application/vnd.github+json" "$GITHUB_API_URL/user" 2>/dev/null || true)
  [ -z "$user_json" ] && { echo "ERROR: Failed to fetch authenticated user from $GITHUB_API_URL/user" >&2; exit 1; }
  GITHUB_OWNER=$(echo "$user_json" | sed -n 's/.*"login"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
  [ -z "$GITHUB_OWNER" ] && { echo "ERROR: Failed to extract login from $GITHUB_API_URL/user response" >&2; exit 1; }
fi
[[ "$GITHUB_OWNER" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "ERROR: Invalid GITHUB_OWNER '$GITHUB_OWNER'. Must match ^[A-Za-z0-9._-]+$" >&2; exit 1; }
[[ "$AERIAL_CONFIG_REPO" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "ERROR: Invalid AERIAL_CONFIG_REPO '$AERIAL_CONFIG_REPO'. Must match ^[A-Za-z0-9._-]+$" >&2; exit 1; }
[[ "$AERIAL_TEMPLATE_REPO" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]] || { echo "ERROR: Invalid AERIAL_TEMPLATE_REPO '$AERIAL_TEMPLATE_REPO'. Must match ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$" >&2; exit 1; }
repo_status=$(curl -sS -o /dev/null -w "%{http_code}" -H "Authorization: token $GITHUB_PAT" -H "Accept: application/vnd.github+json" "$GITHUB_API_URL/repos/$GITHUB_OWNER/$AERIAL_CONFIG_REPO")
if [ "$repo_status" = "404" ]; then
  echo "Repository $GITHUB_OWNER/$AERIAL_CONFIG_REPO not found. Generating from template $AERIAL_TEMPLATE_REPO..."
  curl -fsS -X POST -H "Authorization: token $GITHUB_PAT" -H "Accept: application/vnd.github+json" "$GITHUB_API_URL/repos/$AERIAL_TEMPLATE_REPO/generate" -d "{\"owner\":\"$GITHUB_OWNER\",\"name\":\"$AERIAL_CONFIG_REPO\",\"private\":true}" >/dev/null
  echo "Waiting for repository initialization..."; ready=0
  for ((i=1; i<=15; i++)); do
    commit_status=$(curl -sS -o /dev/null -w "%{http_code}" -H "Authorization: token $GITHUB_PAT" -H "Accept: application/vnd.github+json" "$GITHUB_API_URL/repos/$GITHUB_OWNER/$AERIAL_CONFIG_REPO/commits/main")
    [ "$commit_status" = "200" ] && { ready=1; break; }
    sleep "$AERIAL_POLL_SLEEP"
  done
  [ "$ready" -eq 1 ] || { echo "ERROR: Timed out waiting for repository $GITHUB_OWNER/$AERIAL_CONFIG_REPO to initialize from template" >&2; exit 1; }
  echo "Repository $GITHUB_OWNER/$AERIAL_CONFIG_REPO initialized successfully."
elif [ "$repo_status" = "200" ]; then echo "Repository $GITHUB_OWNER/$AERIAL_CONFIG_REPO exists."
else echo "ERROR: Unexpected HTTP status $repo_status checking $GITHUB_API_URL/repos/$GITHUB_OWNER/$AERIAL_CONFIG_REPO" >&2; exit 1; fi
if ! command -v k3s >/dev/null 2>&1; then
  echo "k3s not found. Installing single-node k3s..."; mkdir -p "$(dirname "$KUBECONFIG")"
  curl -sfL "$K3S_INSTALL_URL" | sh -s - --disable traefik --write-kubeconfig "$KUBECONFIG" --write-kubeconfig-mode 600
  echo "Waiting for k3s API server readiness..."; k3s_ready=0
  for ((i=1; i<=30; i++)); do kubectl get namespace default >/dev/null 2>&1 && { k3s_ready=1; break; }; sleep "$AERIAL_POLL_SLEEP"; done
  [ "$k3s_ready" -eq 1 ] || { echo "ERROR: Timed out waiting for k3s API server to become ready" >&2; exit 1; }
fi
if ! command -v flux >/dev/null 2>&1; then
  echo "Flux CLI not found. Installing flux..."
  if command -v sudo >/dev/null 2>&1 && [ "$(id -u)" -ne 0 ]; then curl -s "$FLUX_INSTALL_URL" | sudo bash; else curl -s "$FLUX_INSTALL_URL" | bash; fi
fi
echo "Seeding bootstrap secrets..."
kubectl create secret generic aerial-bootstrap-secrets --from-literal=DISCORD_BOT_TOKEN="$DISCORD_BOT_TOKEN" --from-literal=GITHUB_PAT="$GITHUB_PAT" --dry-run=client -o yaml | kubectl apply -f -
echo "Bootstrapping Flux CD on $GITHUB_OWNER/$AERIAL_CONFIG_REPO..."
GITHUB_TOKEN="$GITHUB_PAT" flux bootstrap github --owner="$GITHUB_OWNER" --repository="$AERIAL_CONFIG_REPO" --branch=main --path=k8s --personal
echo "Waiting for eri-restate pod to be created..."; pod_name=""
for ((i=1; i<=60; i++)); do
  pod_name=$(kubectl get pod -l app.kubernetes.io/name=eri-restate -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [ -n "$pod_name" ] && break; sleep "$AERIAL_POLL_SLEEP"
done
[ -n "$pod_name" ] || { echo "ERROR: Timed out waiting for eri-restate pod to be created by Flux" >&2; exit 1; }
echo "Waiting for eri-restate pod ($pod_name) to become ready..."
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=eri-restate --timeout="$AERIAL_WAIT_TIMEOUT"
ERI_POD=$(kubectl get pod -l app.kubernetes.io/name=eri-restate -o jsonpath='{.items[0].metadata.name}')
if [ "${AERIAL_SKIP_TTY_EXEC:-0}" = "1" ] || { [ "${AERIAL_FORCE_TTY_EXEC:-0}" != "1" ] && ! (: </dev/tty) 2>/dev/null; }; then
  echo "To authenticate your execution engine (agy), run:"
  echo "kubectl exec -it $ERI_POD -c eri-restate -- agy"
else
  (: </dev/tty) 2>/dev/null && kubectl exec -it "$ERI_POD" -c eri-restate -- agy </dev/tty || kubectl exec -it "$ERI_POD" -c eri-restate -- agy
fi
