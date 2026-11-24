#!/usr/bin/env bash
# scripts/update-oci-base-digests.sh: Refresh Two-Phase rules_oci base digests and Dockerfile SHA256 markers in MODULE.bazel.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && cd "$REPO_ROOT"
OWNER="${OWNER:-azylman}"; TAG="${TAG:-latest}"
command -v crane >/dev/null 2>&1 || { echo "::error::crane binary is required to query GHCR base image digests" >&2; exit 1; }
declare -A SERVICE_DOCKERFILES=(["eri-restate"]="services/eri-restate/Dockerfile" ["github-runner"]="services/github-runner/Dockerfile" ["discord-mcp"]="mcp/discord-mcp/Dockerfile" ["infisical-mcp"]="mcp/infisical-mcp/Dockerfile")
declare -A SERVICE_BASE_REPOS=(["eri-restate"]="eri_restate_base" ["github-runner"]="github_runner_base" ["discord-mcp"]="discord_mcp_base" ["infisical-mcp"]="infisical_mcp_base")
ALL_SERVICES=("eri-restate" "github-runner" "discord-mcp" "infisical-mcp")
SERVICES_TO_UPDATE=()
if [ $# -gt 0 ]; then
    for arg in "$@"; do
        [ "$arg" = "--help" ] || [ "$arg" = "-h" ] && { echo "Usage: $0 [eri-restate] [github-runner] [discord-mcp] [infisical-mcp]"; exit 0; }
        [ -n "${SERVICE_DOCKERFILES[$arg]+_}" ] || { echo "::error::Unknown Two-Phase service: $arg. Valid options: ${ALL_SERVICES[*]}" >&2; exit 1; }
        SERVICES_TO_UPDATE+=("$arg")
    done
else
    SERVICES_TO_UPDATE=("${ALL_SERVICES[@]}")
fi
for svc in "${SERVICES_TO_UPDATE[@]}"; do
    df_file="${SERVICE_DOCKERFILES[$svc]}"
    base_repo="${SERVICE_BASE_REPOS[$svc]}"
    image_ref="ghcr.io/${OWNER}/aerial-${svc}:${TAG}"
    [ -f "$REPO_ROOT/$df_file" ] || { echo "::error::Base Dockerfile not found at $REPO_ROOT/$df_file" >&2; exit 1; }
    echo "Querying digest for $image_ref..." >&2
    digest=$(crane digest "$image_ref")
    [ -n "$digest" ] || { echo "::error::Failed to resolve digest for $image_ref" >&2; exit 1; }
    df_sha=$(sha256sum "$REPO_ROOT/$df_file" | awk '{print $1}')
    echo "Updating $svc ($base_repo): sha256=$df_sha digest=$digest" >&2
    python3 - "$REPO_ROOT/MODULE.bazel" "$base_repo" "$df_sha" "$digest" << 'EOF'
import sys, re
module_path, repo, sha, digest = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
with open(module_path, "r", encoding="utf-8") as f:
    text = f.read()
pattern = re.compile(r'(?:# base_dockerfile_sha256:[^\n]*\n)?(oci\.pull\([^)]*?name\s*=\s*"' + re.escape(repo) + r'"[^)]*?\))', re.MULTILINE)
def repl(m):
    block = re.sub(r'digest\s*=\s*"[^"]+"', f'digest = "{digest}"', m.group(1))
    return f"# base_dockerfile_sha256: {sha}\n{block}"
new_text, count = pattern.subn(repl, text, count=1)
if count == 0:
    print(f"Error: could not find oci.pull block for {repo} in {module_path}", file=sys.stderr)
    sys.exit(1)
with open(module_path, "w", encoding="utf-8") as f:
    f.write(new_text)
EOF
done
echo "Running ensure_bazel_build_files..." >&2
# shellcheck source=scripts/lib/hermetic-go-env.sh
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"
echo "Successfully updated OCI base digests in MODULE.bazel." >&2
