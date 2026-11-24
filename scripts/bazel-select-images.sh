#!/usr/bin/env bash
# scripts/bazel-select-images.sh: Select changed microservices and images for Continuous Delivery.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" && cd "$REPO_ROOT"
FORCE_ALL="${FORCE_ALL:-false}" GITHUB_OUT="${GITHUB_OUTPUT:-}" CHANGED_FILES=()
while [ $# -gt 0 ]; do
    case "$1" in
        --force-all|-force-all) FORCE_ALL="true"; shift ;;
        --github-output|-github-output) GITHUB_OUT="$2"; shift 2 ;;
        *) shift ;;
    esac
done
[ ! -t 0 ] && while IFS= read -r l || [ -n "$l" ]; do [ -n "$l" ] && CHANGED_FILES+=("$l"); done
if [ ${#CHANGED_FILES[@]} -eq 0 ] && git rev-parse --git-dir >/dev/null 2>&1; then
    cur_b=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
    if git rev-parse --verify origin/main >/dev/null 2>&1 && [ "$cur_b" != "main" ]; then
        mapfile -t CHANGED_FILES < <(git diff --name-only origin/main...HEAD 2>/dev/null || true)
    fi
    [ ${#CHANGED_FILES[@]} -eq 0 ] && mapfile -t CHANGED_FILES < <(git diff --name-only HEAD~1 2>/dev/null || git diff --name-only HEAD 2>/dev/null || git status --porcelain | awk '{print $NF}')
fi
for f in "${CHANGED_FILES[@]}"; do
    case "$f" in .github/workflows/docker-publish.yml|.dockerignore) FORCE_ALL="true" ;; esac
done
OCI_SERVICES=("eri" "haro" "coven" "docket-mcp" "kernel-mcp" "synapse" "transcripts" "vanadis" "vanadis-restate" "vanadis-nats" "prospera" "console" "redact-pattern" "coverage-ingest" "docs" "migrator")
DOCKER_SERVICES=("eri-restate" "github-runner" "discord-mcp" "infisical-mcp")
PKG_CHANGED="false"
for f in "${CHANGED_FILES[@]}"; do [[ "$f" == pkg/* ]] && { PKG_CHANGED="true"; break; }; done
# shellcheck source=scripts/lib/hermetic-go-env.sh
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"
declare -A SELECTED_SERVICES
SELECTED_MATRIX=() RULES_OCI_ALL="$FORCE_ALL" QUERY_SELECTED_SERVICES=()
if [ "$RULES_OCI_ALL" != "true" ]; then
    for f in "${CHANGED_FILES[@]}"; do
        case "$f" in images/BUILD.bazel|build/BUILD.bazel|build/oci.bzl|MODULE.bazel*|BUILD.bazel|go.mod|go.sum) RULES_OCI_ALL="true"; break ;; esac
    done
fi
if [ "$RULES_OCI_ALL" != "true" ]; then
    if ! command -v bazel >/dev/null 2>&1; then RULES_OCI_ALL="true"
    elif [ ${#CHANGED_FILES[@]} -gt 0 ]; then
        bazel_targets=()
        for f in "${CHANGED_FILES[@]}"; do
            case "$f" in
                services/*|mcp/*|pkg/*)
                    cur_d=$(dirname "$f") f_pkg=""
                    while [ -n "$cur_d" ] && [ "$cur_d" != "." ] && [ "$cur_d" != "/" ]; do
                        [ -f "$REPO_ROOT/$cur_d/BUILD.bazel" ] && { f_pkg="$cur_d"; break; }
                        cur_d=$(dirname "$cur_d")
                    done
                    [ -n "$f_pkg" ] && bazel_targets+=("//${f_pkg}:all") || { RULES_OCI_ALL="true"; break; } ;;
                web/theme/*) [ -f "$REPO_ROOT/web/theme/BUILD.bazel" ] && bazel_targets+=("//web/theme:all") ;;
                images/*) [ -f "$REPO_ROOT/images/BUILD.bazel" ] && bazel_targets+=("//images:all") ;;
            esac
        done
        if [ "$RULES_OCI_ALL" != "true" ] && [ ${#bazel_targets[@]} -gt 0 ]; then
            t_set=$(printf '%s ' "${bazel_targets[@]}") q_out=$(mktemp /tmp/aerial-bazel-query.XXXXXX)
            if bazel query "kind(oci_image, rdeps(//images:all, set($t_set)))" >"$q_out" 2>/dev/null; then
                while IFS= read -r m || [ -n "$m" ]; do
                    c=$(echo "$m" | sed 's#^//images:##') && [ -n "$c" ] && QUERY_SELECTED_SERVICES+=("$c")
                done < "$q_out"
            else RULES_OCI_ALL="true"; fi
            rm -f "$q_out"
        fi
    fi
fi
for svc in "${OCI_SERVICES[@]}"; do
    SELECTED_SERVICES["$svc"]="false" sel="$RULES_OCI_ALL"
    [ "$sel" = "false" ] && for q in "${QUERY_SELECTED_SERVICES[@]}"; do [ "$q" = "$svc" ] && { sel="true"; break; }; done
    [ "$sel" = "true" ] && { SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"aerial-${svc}\",\"type\":\"bazel\"}"); SELECTED_SERVICES["$svc"]="true"; }
done
DOCKER_CFG=("eri-restate:.:./services/eri-restate/Dockerfile:services/eri-restate/*|services/eri/*|scripts/*|rules/*|.agents/*|pkg/*|go.mod|go.sum" "github-runner:.:./services/github-runner/Dockerfile:services/github-runner/*" "discord-mcp:.:./mcp/discord-mcp/Dockerfile:mcp/discord-mcp/*" "infisical-mcp:./mcp/infisical-mcp:./mcp/infisical-mcp/Dockerfile:mcp/infisical-mcp/*")
for entry in "${DOCKER_CFG[@]}"; do
    IFS=":" read -r svc ctx df pat <<< "$entry"
    SELECTED_SERVICES["$svc"]="false" sel="$FORCE_ALL"
    [ "$sel" = "false" ] && for f in "${CHANGED_FILES[@]}"; do eval "case \"\$f\" in $pat) sel=\"true\" ;; esac"; [ "$sel" = "true" ] && break; done
    [ "$sel" = "true" ] && { SELECTED_SERVICES["$svc"]="true"; SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"aerial-${svc}\",\"type\":\"docker\",\"context\":\"${ctx}\",\"file\":\"${df}\"}"); }
done
MATRIX_JSON="[$(IFS=,; echo "${SELECTED_MATRIX[*]}")]" HAS_BUILDS="false"
[ ${#SELECTED_MATRIX[@]} -gt 0 ] && HAS_BUILDS="true"
[ -n "$GITHUB_OUT" ] && { echo "pkg=${PKG_CHANGED}"; for svc in "${OCI_SERVICES[@]}" "${DOCKER_SERVICES[@]}"; do echo "${svc}=${SELECTED_SERVICES[$svc]}"; done; echo "matrix=${MATRIX_JSON}"; echo "has_builds=${HAS_BUILDS}"; } >> "$GITHUB_OUT"
echo "Build Selection Result:" && echo "  pkg: ${PKG_CHANGED}" && echo "  has_builds: ${HAS_BUILDS}" && echo "  selected images count: ${#SELECTED_MATRIX[@]}"
for svc in "${OCI_SERVICES[@]}" "${DOCKER_SERVICES[@]}"; do [ "${SELECTED_SERVICES[$svc]}" = "true" ] && echo "    - $svc" || true; done
