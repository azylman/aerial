#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && cd "$REPO_ROOT"
OWNER="${OWNER:-azylman}"; FORCE_ALL="${FORCE_ALL:-false}"; GITHUB_OUT="${GITHUB_OUTPUT:-}"; CHANGED_FILES=()
while [ $# -gt 0 ]; do
    case "$1" in
        --force-all|-force-all) FORCE_ALL="true"; shift ;;
        --github-output|-github-output) GITHUB_OUT="$2"; shift 2 ;;
        *) [ -z "${P_OWNER:-}" ] && { P_OWNER="$1"; OWNER="$1"; }; shift ;;
    esac
done
[ ! -t 0 ] && while IFS= read -r l || [ -n "$l" ]; do [ -n "$l" ] && CHANGED_FILES+=("$l"); done
if [ ${#CHANGED_FILES[@]} -eq 0 ] && git rev-parse --git-dir >/dev/null 2>&1; then
    cur_b=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
    [ "$cur_b" != "main" ] && git rev-parse --verify origin/main >/dev/null 2>&1 && mapfile -t CHANGED_FILES < <(git diff --name-only origin/main...HEAD 2>/dev/null || true)
    [ ${#CHANGED_FILES[@]} -eq 0 ] && mapfile -t CHANGED_FILES < <(git diff --name-only HEAD~1 2>/dev/null || git diff --name-only HEAD 2>/dev/null || git status --porcelain | awk '{print $NF}')
fi
for f in "${CHANGED_FILES[@]}"; do
    case "$f" in .github/workflows/docker-publish.yml|.dockerignore|images/BUILD.bazel|build/*|MODULE.bazel*|BUILD.bazel|go.mod|go.sum) FORCE_ALL="true" ;; esac
done
OCI_SVCS=("eri" "haro" "coven" "docket-mcp" "kernel-mcp" "synapse" "transcripts" "vanadis" "vanadis-restate" "vanadis-nats" "prospera" "console" "redact-pattern" "coverage-ingest" "docs" "migrator")
DOCKER_CFG=("eri-restate:.:./services/eri-restate/Dockerfile:services/eri-restate/*|services/eri/*|scripts/*|rules/*|.agents/*|pkg/*|go.mod|go.sum" "github-runner:.:./services/github-runner/Dockerfile:services/github-runner/*" "discord-mcp:.:./mcp/discord-mcp/Dockerfile:mcp/discord-mcp/*" "infisical-mcp:./mcp/infisical-mcp:./mcp/infisical-mcp/Dockerfile:mcp/infisical-mcp/*")
PKG_CHANGED="false"; for f in "${CHANGED_FILES[@]}"; do [[ "$f" == pkg/* ]] && { PKG_CHANGED="true"; break; }; done
declare -A AFF SEL; for s in "${OCI_SVCS[@]}"; do AFF["$s"]="false"; SEL["$s"]="false"; done; for ent in "${DOCKER_CFG[@]}"; do IFS=":" read -r s _ _ _ <<< "$ent"; AFF["$s"]="false"; SEL["$s"]="false"; done
CAND_OCI=()
if [ "$FORCE_ALL" = "true" ]; then CAND_OCI=("${OCI_SVCS[@]}"); else
    filtered=()
    for f in "${CHANGED_FILES[@]}"; do case "$f" in services/*|mcp/*|pkg/*|web/theme/*|images/*) [ -f "$f" ] && filtered+=("$f") ;; esac; done
    if [ ${#filtered[@]} -gt 0 ]; then
        . "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"
        f_set=$(printf '%s ' "${filtered[@]}")
        while IFS= read -r m || [ -n "$m" ]; do c="${m#//images:}"; [ -n "$c" ] && CAND_OCI+=("$c"); done < <(bazel query "kind(oci_image, rdeps(//images:all, same_pkg_direct_rdeps(set($f_set))))" 2>/dev/null || true)
    fi
fi
if [ "$FORCE_ALL" = "true" ]; then
    for s in "${OCI_SVCS[@]}"; do SEL["$s"]="true"; AFF["$s"]="true"; done
elif [ ${#CAND_OCI[@]} -gt 0 ]; then
    b_targets=()
    for s in "${CAND_OCI[@]}"; do b_targets+=("//images:${s}"); AFF["$s"]="true"; done
    bazel build --remote_download_toplevel "${b_targets[@]}"
    for s in "${CAND_OCI[@]}"; do
        ldig=$(jq -r '.manifests[0].digest // empty' "bazel-bin/images/${s}/index.json" 2>/dev/null || true)
        rdig=$(crane digest "ghcr.io/${OWNER}/aerial-${s}:latest" 2>/dev/null || true)
        [ -z "$rdig" ] || [ "$ldig" != "$rdig" ] && { echo "Image $s changed: local=$ldig remote=$rdig" >&2; SEL["$s"]="true"; }
    done
fi
SEL_MAT=()
for s in "${OCI_SVCS[@]}"; do [ "${SEL[$s]}" = "true" ] && SEL_MAT+=("{\"service\":\"$s\",\"image_name\":\"aerial-$s\",\"type\":\"bazel\"}"); done
for ent in "${DOCKER_CFG[@]}"; do
    IFS=":" read -r s ctx df pat <<< "$ent"
    sel="$FORCE_ALL"
    if [ "$sel" = "false" ]; then
        for f in "${CHANGED_FILES[@]}"; do eval "case \"\$f\" in $pat) AFF[\"\$s\"]=\"true\"; [[ \"\$f\" =~ (_test\.go|testdata/) ]] || sel=\"true\" ;; esac"; [ "$sel" = "true" ] && break; done
    else AFF["$s"]="true"; fi
    [ "$sel" = "true" ] && { SEL["$s"]="true"; SEL_MAT+=("{\"service\":\"$s\",\"image_name\":\"aerial-$s\",\"type\":\"docker\",\"context\":\"$ctx\",\"file\":\"$df\"}"); }
done
MAT_JSON="[$(IFS=,; echo "${SEL_MAT[*]}")]"; HAS_B="false"; [ ${#SEL_MAT[@]} -gt 0 ] && HAS_B="true"
if [ -n "$GITHUB_OUT" ]; then
    { echo "pkg=$PKG_CHANGED"; for s in "${OCI_SVCS[@]}"; do echo "$s=${AFF[$s]}"; done; for ent in "${DOCKER_CFG[@]}"; do IFS=":" read -r s _ _ _ <<< "$ent"; echo "$s=${AFF[$s]}"; done; echo "matrix=$MAT_JSON"; echo "has_builds=$HAS_B"; } >> "$GITHUB_OUT"
fi
echo "Build Selection Result: pkg=$PKG_CHANGED has_builds=$HAS_B count=${#SEL_MAT[@]}"
