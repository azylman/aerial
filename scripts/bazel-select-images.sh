#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && cd "$REPO_ROOT"
OWNER="${OWNER:-azylman}"; FORCE_ALL="${FORCE_ALL:-false}"; GITHUB_OUT="${GITHUB_OUTPUT:-}"; CHANGED_FILES=()
while [ $# -gt 0 ]; do case "$1" in
    --force-all|-force-all) FORCE_ALL="true"; shift ;;
    --github-output|-github-output) GITHUB_OUT="$2"; shift 2 ;;
    *) [ -z "${P_OWNER:-}" ] && { P_OWNER="$1"; OWNER="$1"; }; shift ;;
esac; done
[ ! -t 0 ] && while IFS= read -r l || [ -n "$l" ]; do [ -n "$l" ] && CHANGED_FILES+=("$l"); done
if [ ${#CHANGED_FILES[@]} -eq 0 ] && git rev-parse --git-dir >/dev/null 2>&1; then
    cur_b=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
    [ "$cur_b" != "main" ] && git rev-parse --verify origin/main >/dev/null 2>&1 && mapfile -t CHANGED_FILES < <(git diff --name-only origin/main...HEAD 2>/dev/null || true)
    [ ${#CHANGED_FILES[@]} -eq 0 ] && mapfile -t CHANGED_FILES < <(git diff --name-only HEAD~1 2>/dev/null || git diff --name-only HEAD 2>/dev/null || git status --porcelain | awk '{print $NF}')
fi
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"
mapfile -t OCI_SVCS < <(bazel query "kind(oci_image, //images:all)" 2>/dev/null | sed 's#^//images:##' | sort -u)
DOCKER_CFG=("eri-restate:.:./services/eri-restate/Dockerfile:services/eri-restate/Dockerfile:eri_restate_base" "github-runner:.:./services/github-runner/Dockerfile:services/github-runner/Dockerfile:github_runner_base" "discord-mcp:.:./mcp/discord-mcp/Dockerfile:mcp/discord-mcp/Dockerfile:discord_mcp_base" "infisical-mcp:./mcp/infisical-mcp:./mcp/infisical-mcp/Dockerfile:mcp/infisical-mcp/Dockerfile:infisical_mcp_base")
PKG_CHANGED="false"; for f in "${CHANGED_FILES[@]}"; do [[ "$f" == pkg/* ]] && { PKG_CHANGED="true"; break; }; done
is_test_file() { case "$1" in *_test.go|*.test.[tj]s|*.test.[tj]sx|scripts/test-*.sh|build/tests/*) return 0;; *) return 1;; esac; }
declare -A AFF SEL D_TRIG; for s in "${OCI_SVCS[@]}"; do AFF["$s"]="false"; SEL["$s"]="false"; D_TRIG["$s"]="false"; done
for f in "${CHANGED_FILES[@]}"; do
    case "$f" in
        .github/workflows/docker-publish.yml|.dockerignore) FORCE_ALL="true" ;;
        services/docs-service/*) AFF["docs"]="true" ;;
        services/eri/pkg/db/migrations/*) AFF["migrator"]="true"; AFF["eri"]="true" ;;
        web/theme/*) AFF["console"]="true"; AFF["docs"]="true" ;;
        services/*|mcp/*) sd=$(echo "$f" | cut -d/ -f2); [ -n "${AFF[$sd]+_}" ] && AFF["$sd"]="true" ;;
    esac
done
for ent in "${DOCKER_CFG[@]}"; do
    IFS=":" read -r s _ _ df_f brepo <<< "$ent"
    for f in "${CHANGED_FILES[@]}"; do [ "$f" = "$df_f" ] && { D_TRIG["$s"]="true"; AFF["$s"]="true"; SEL["$s"]="true"; break; }; done
    if [ "${D_TRIG[$s]}" != "true" ] && [ -n "${brepo:-}" ] && [ -f "$REPO_ROOT/$df_f" ]; then
        act_sha=$(sha256sum "$REPO_ROOT/$df_f" | awk '{print $1}')
        rec_sha=$(awk -v r="$brepo" '/base_dockerfile_sha256:/ {for(i=1;i<=NF;i++) if($i ~ /^[0-9a-f]{64}$/) sha=$i} $0 ~ "name = \"" r "\"" {print sha; exit}' "$REPO_ROOT/MODULE.bazel")
        if [ -z "$rec_sha" ] || [ "$act_sha" != "$rec_sha" ]; then
            D_TRIG["$s"]="true"; AFF["$s"]="true"; SEL["$s"]="true"
            echo "Base Dockerfile hash for $s differs from MODULE.bazel ($brepo); falling back to type=docker" >&2
        fi
    fi
done
CAND_OCI=()
if [ "$FORCE_ALL" = "true" ]; then CAND_OCI=("${OCI_SVCS[@]}"); else
    b_pkgs=(); chk_all="false"; [ ${#CHANGED_FILES[@]} -eq 0 ] && chk_all="true"
    for f in "${CHANGED_FILES[@]}"; do
        is_test_file "$f" && continue
        case "$f" in
            images/BUILD.bazel|build/BUILD.bazel|build/oci.bzl|MODULE.bazel*|BUILD.bazel|go.mod|go.sum) chk_all="true" ;;
            scripts/*|rules/*|.agents/*) b_pkgs+=("//:all") ;;
            services/docs-service/*) [ -f "$REPO_ROOT/services/docs-service/BUILD.bazel" ] && b_pkgs+=("//services/docs-service:all") ;;
            services/eri/pkg/db/migrations/*) [ -f "$REPO_ROOT/services/eri/pkg/db/BUILD.bazel" ] && b_pkgs+=("//services/eri/pkg/db:all") ;;
            services/*|mcp/*|pkg/*)
                d=$(dirname "$f"); pkg=""
                while [ -n "$d" ] && [ "$d" != "." ] && [ "$d" != "/" ]; do [ -f "$REPO_ROOT/$d/BUILD.bazel" ] && { pkg="$d"; break; }; d=$(dirname "$d"); done
                [ -n "$pkg" ] && b_pkgs+=("//${pkg}:all") || chk_all="true" ;;
            web/theme/*) [ -f "$REPO_ROOT/web/theme/BUILD.bazel" ] && b_pkgs+=("//web/theme:all") ;;
            images/*) [ -f "$REPO_ROOT/images/BUILD.bazel" ] && b_pkgs+=("//images:all") ;;
        esac
    done
    if [ "$chk_all" = "true" ]; then CAND_OCI=("${OCI_SVCS[@]}"); elif [ ${#b_pkgs[@]} -gt 0 ]; then
        p_set=$(printf '%s ' "${b_pkgs[@]}")
        q_out=$(bazel query "kind(oci_image, rdeps(//images:all, set($p_set)))" 2>/dev/null) || CAND_OCI=("${OCI_SVCS[@]}")
        if [ ${#CAND_OCI[@]} -eq 0 ]; then
            while IFS= read -r m || [ -n "$m" ]; do c="${m#//images:}"; [ -n "$c" ] && { CAND_OCI+=("$c"); [ -n "${AFF[$c]+_}" ] && AFF["$c"]="true"; }; done <<< "$q_out"
        fi
    fi
    [ ${#CAND_OCI[@]} -gt 0 ] && IFS=" " read -r -a CAND_OCI <<< "$(printf '%s\n' "${CAND_OCI[@]}" | sort -u | tr '\n' ' ')"
fi
if [ "$FORCE_ALL" = "true" ]; then
    for s in "${OCI_SVCS[@]}"; do [ "${D_TRIG[$s]}" = "true" ] && continue; SEL["$s"]="true"; AFF["$s"]="true"; done
elif [ ${#CAND_OCI[@]} -gt 0 ]; then
    b_targets=(); b_svcs=()
    for s in "${CAND_OCI[@]}"; do [ "${D_TRIG[$s]:-false}" = "true" ] && continue; b_targets+=("//images:${s}"); b_svcs+=("$s"); done
    if [ ${#b_targets[@]} -gt 0 ]; then
        command -v crane >/dev/null 2>&1 || { echo "::error::crane binary is required" >&2; exit 1; }
        bazel build --remote_download_toplevel "${b_targets[@]}"
        for s in "${b_svcs[@]}"; do
            ldig=$(jq -r '.manifests[0].digest // empty' "bazel-bin/images/${s}/index.json" 2>/dev/null || true)
            [ -n "$ldig" ] || { echo "::error::missing local digest for $s" >&2; exit 1; }
            rdig=""
            if rdig_out=$(crane digest "ghcr.io/${OWNER}/aerial-${s}:latest" 2>&1); then rdig="$rdig_out"; elif echo "$rdig_out" | grep -q -i -E '(404|not found|NAME_UNKNOWN|MANIFEST_UNKNOWN|UNAUTHORIZED|DENIED)'; then rdig=""; else echo "::error::crane error for $s: $rdig_out" >&2; exit 1; fi
            if [ -z "$rdig" ] || [ "$ldig" != "$rdig" ]; then
                echo "Image $s changed: local=$ldig remote=$rdig" >&2
                SEL["$s"]="true"; AFF["$s"]="true"
            fi
        done
    fi
fi
SEL_MAT=(); for s in "${OCI_SVCS[@]}"; do [ "${D_TRIG[$s]}" != "true" ] && [ "${SEL[$s]}" = "true" ] && SEL_MAT+=("{\"service\":\"$s\",\"image_name\":\"aerial-$s\",\"type\":\"bazel\"}"); done
for ent in "${DOCKER_CFG[@]}"; do
    IFS=":" read -r s ctx df _ <<< "$ent"
    [ "${D_TRIG[$s]}" = "true" ] && SEL_MAT+=("{\"service\":\"$s\",\"image_name\":\"aerial-$s\",\"type\":\"docker\",\"context\":\"$ctx\",\"file\":\"$df\"}")
done
MAT_JSON="[$(IFS=,; echo "${SEL_MAT[*]}")]"; HAS_B="false"; [ ${#SEL_MAT[@]} -gt 0 ] && HAS_B="true"
[ -n "$GITHUB_OUT" ] && { echo "pkg=$PKG_CHANGED"; for s in "${OCI_SVCS[@]}"; do echo "$s=${AFF[$s]}"; done; echo "matrix=$MAT_JSON"; echo "has_builds=$HAS_B"; } >> "$GITHUB_OUT"
echo "Build Selection Result: pkg=$PKG_CHANGED has_builds=$HAS_B count=${#SEL_MAT[@]}"
