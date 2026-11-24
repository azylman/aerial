#!/usr/bin/env bash
# scripts/bazel-select-images.sh: Select changed microservices and images for Continuous Delivery.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

OWNER="${OWNER:-azylman}"
FORCE_ALL="${FORCE_ALL:-false}"
GITHUB_OUT="${GITHUB_OUTPUT:-}"
CHANGED_FILES=()

while [ $# -gt 0 ]; do
    case "$1" in
        --force-all|-force-all) FORCE_ALL="true"; shift ;;
        --github-output|-github-output) GITHUB_OUT="$2"; shift 2 ;;
        *)
            if [ -z "${POSITIONAL_OWNER:-}" ]; then
                POSITIONAL_OWNER="$1"
                OWNER="$1"
            fi
            shift
            ;;
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

OCI_SERVICES=("eri" "haro" "coven" "docket-mcp" "kernel-mcp" "synapse" "transcripts" "vanadis" "vanadis-restate" "vanadis-nats" "prospera" "console" "redact-pattern" "coverage-ingest" "docs" "migrator" "eri-restate" "github-runner" "discord-mcp" "infisical-mcp")
DOCKER_SERVICES=("eri-restate" "github-runner" "discord-mcp" "infisical-mcp")

PKG_CHANGED="false"
for f in "${CHANGED_FILES[@]}"; do [[ "$f" == pkg/* ]] && { PKG_CHANGED="true"; break; }; done

is_test_file() {
    case "$1" in
        *_test.go|*.test.*|*/*.test.*|scripts/test-*.sh|testdata/*|*/testdata/*) return 0 ;;
        *) return 1 ;;
    esac
}

# shellcheck source=scripts/lib/hermetic-go-env.sh
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh" && ensure_bazel_build_files "$REPO_ROOT"

declare -A AFFECTED_SERVICES
declare -A SELECTED_SERVICES
for svc in "${OCI_SERVICES[@]}" "${DOCKER_SERVICES[@]}"; do
    AFFECTED_SERVICES["$svc"]="false"
    SELECTED_SERVICES["$svc"]="false"
done

SELECTED_MATRIX=()
CANDIDATE_OCI=()
CHECK_ALL_OCI="$FORCE_ALL"

if [ "$CHECK_ALL_OCI" != "true" ]; then
    if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
        CHECK_ALL_OCI="true"
    else
        for f in "${CHANGED_FILES[@]}"; do
            case "$f" in
                images/BUILD.bazel|build/BUILD.bazel|build/oci.bzl|MODULE.bazel*|BUILD.bazel|go.mod|go.sum)
                    CHECK_ALL_OCI="true"
                    break
                    ;;
            esac
        done
    fi
fi

bazel_targets=()
for f in "${CHANGED_FILES[@]}"; do
    case "$f" in
        scripts/*|rules/*|.agents/*)
            bazel_targets+=("//:all")
            ;;
        services/docs-service/*)
            AFFECTED_SERVICES["docs"]="true"
            [ -f "$REPO_ROOT/services/docs-service/BUILD.bazel" ] && bazel_targets+=("//services/docs-service:all")
            ;;
        services/eri/pkg/db/migrations/*)
            AFFECTED_SERVICES["migrator"]="true"
            AFFECTED_SERVICES["eri"]="true"
            [ -f "$REPO_ROOT/services/eri/pkg/db/BUILD.bazel" ] && bazel_targets+=("//services/eri/pkg/db:all")
            ;;
        services/*|mcp/*|pkg/*)
            if [[ "$f" == services/* || "$f" == mcp/* ]]; then
                svc_dir=$(echo "$f" | cut -d/ -f2)
                [ -n "${AFFECTED_SERVICES[$svc_dir]+_}" ] && AFFECTED_SERVICES["$svc_dir"]="true"
            fi
            cur_d=$(dirname "$f") f_pkg=""
            while [ -n "$cur_d" ] && [ "$cur_d" != "." ] && [ "$cur_d" != "/" ]; do
                [ -f "$REPO_ROOT/$cur_d/BUILD.bazel" ] && { f_pkg="$cur_d"; break; }
                cur_d=$(dirname "$cur_d")
            done
            [ -n "$f_pkg" ] && bazel_targets+=("//${f_pkg}:all") || CHECK_ALL_OCI="true"
            ;;
        web/theme/*)
            AFFECTED_SERVICES["console"]="true"
            AFFECTED_SERVICES["docs"]="true"
            [ -f "$REPO_ROOT/web/theme/BUILD.bazel" ] && bazel_targets+=("//web/theme:all")
            ;;
        images/*)
            [ -f "$REPO_ROOT/images/BUILD.bazel" ] && bazel_targets+=("//images:all")
            ;;
    esac
done

if [ "$CHECK_ALL_OCI" = "true" ]; then
    CANDIDATE_OCI=("${OCI_SERVICES[@]}")
elif [ ${#bazel_targets[@]} -gt 0 ]; then
    t_set=$(printf '%s ' "${bazel_targets[@]}")
    q_out=$(mktemp /tmp/aerial-bazel-query.XXXXXX)
    if bazel query "kind(oci_image, rdeps(//images:all, set($t_set)))" >"$q_out" 2>/dev/null; then
        while IFS= read -r m || [ -n "$m" ]; do
            c=$(echo "$m" | sed 's#^//images:##')
            if [ -n "$c" ]; then
                CANDIDATE_OCI+=("$c")
                [ -n "${AFFECTED_SERVICES[$c]+_}" ] && AFFECTED_SERVICES["$c"]="true"
            fi
        done < "$q_out"
    else
        CANDIDATE_OCI=("${OCI_SERVICES[@]}")
    fi
    rm -f "$q_out"
fi

DOCKER_CFG=(
    "eri-restate:.:./services/eri-restate/Dockerfile:services/eri-restate/Dockerfile:eri_restate_base"
    "github-runner:.:./services/github-runner/Dockerfile:services/github-runner/Dockerfile:github_runner_base"
    "discord-mcp:.:./mcp/discord-mcp/Dockerfile:mcp/discord-mcp/Dockerfile:discord_mcp_base"
    "infisical-mcp:./mcp/infisical-mcp:./mcp/infisical-mcp/Dockerfile:mcp/infisical-mcp/Dockerfile:infisical_mcp_base"
)

declare -A DOCKER_TRIGGERED
for entry in "${DOCKER_CFG[@]}"; do
    IFS=":" read -r svc ctx df df_file base_repo <<< "$entry"
    DOCKER_TRIGGERED["$svc"]="false"
    for f in "${CHANGED_FILES[@]}"; do
        if [ "$f" = "$df_file" ]; then
            DOCKER_TRIGGERED["$svc"]="true"
            AFFECTED_SERVICES["$svc"]="true"
            SELECTED_SERVICES["$svc"]="true"
            break
        fi
    done
    if [ "${DOCKER_TRIGGERED[$svc]}" != "true" ] && [ -n "${base_repo:-}" ] && [ -f "$REPO_ROOT/$df_file" ]; then
        actual_df_sha=$(sha256sum "$REPO_ROOT/$df_file" | awk '{print $1}')
        recorded_df_sha=$(awk -v repo="$base_repo" '
            /base_dockerfile_sha256:/ {
                for (i = 1; i <= NF; i++) {
                    if ($i ~ /^[0-9a-f]{64}$/) {
                        sha = $i
                    }
                }
            }
            $0 ~ "name = \"" repo "\"" {
                print sha
                exit
            }
        ' "$REPO_ROOT/MODULE.bazel")
        if [ -z "$recorded_df_sha" ] || [ "$actual_df_sha" != "$recorded_df_sha" ]; then
            DOCKER_TRIGGERED["$svc"]="true"
            AFFECTED_SERVICES["$svc"]="true"
            SELECTED_SERVICES["$svc"]="true"
            echo "Base Dockerfile hash for $svc differs from MODULE.bazel ($base_repo); falling back to type=docker until scripts/update-oci-base-digests.sh is run" >&2
        fi
    fi
done

if [ "$FORCE_ALL" = "true" ]; then
    for svc in "${OCI_SERVICES[@]}"; do
        [ "${DOCKER_TRIGGERED[$svc]:-false}" = "true" ] && continue
        echo "Forced rebuild for $svc" >&2
        SELECTED_SERVICES["$svc"]="true"
        AFFECTED_SERVICES["$svc"]="true"
    done
elif [ ${#CANDIDATE_OCI[@]} -gt 0 ]; then
    declare -A IS_CANDIDATE
    build_targets=()
    for svc in "${OCI_SERVICES[@]}"; do
        [ "${DOCKER_TRIGGERED[$svc]:-false}" = "true" ] && continue
        for cand in "${CANDIDATE_OCI[@]}"; do
            if [ "$svc" = "$cand" ] && [ -z "${IS_CANDIDATE[$svc]+_}" ]; then
                IS_CANDIDATE["$svc"]="true"
                build_targets+=("//images:${svc}")
            fi
        done
    done

    if [ ${#build_targets[@]} -gt 0 ]; then
        if ! command -v crane >/dev/null 2>&1; then
            echo "::error::crane binary is required for rules_oci remote digest comparison" >&2
            exit 1
        fi
        echo "Building rules_oci image targets (${build_targets[*]})..." >&2
        bazel build --remote_download_toplevel "${build_targets[@]}"

        for svc in "${OCI_SERVICES[@]}"; do
            [ "${IS_CANDIDATE[$svc]:-false}" = "true" ] || continue
            index_file="bazel-bin/images/${svc}/index.json"
            local_digest=""
            [ -f "$index_file" ] && local_digest=$(jq -r '.manifests[0].digest // empty' "$index_file" 2>/dev/null || true)
            if [ -z "$local_digest" ]; then
                echo "::error::Failed to read local manifest digest for $svc from $index_file" >&2
                exit 1
            fi

            remote_digest=""
            crane_out=""
            if crane_out=$(crane digest "ghcr.io/${OWNER}/aerial-${svc}:latest" 2>&1); then
                remote_digest="$crane_out"
            elif echo "$crane_out" | grep -q -i -E '(MANIFEST_UNKNOWN|NAME_UNKNOWN|404|not found|UNAUTHORIZED|DENIED|authentication required)'; then
                remote_digest=""
            else
                echo "::error::crane digest failed for $svc: $crane_out" >&2
                exit 1
            fi

            if [ -z "$remote_digest" ] || [ "$local_digest" != "$remote_digest" ]; then
                echo "Image $svc changed: local=${local_digest} remote=${remote_digest}" >&2
                SELECTED_SERVICES["$svc"]="true"
                AFFECTED_SERVICES["$svc"]="true"
            else
                echo "Image $svc unchanged (${local_digest})" >&2
            fi
        done
    fi
fi

for svc in "${OCI_SERVICES[@]}"; do
    if [ "${DOCKER_TRIGGERED[$svc]:-false}" = "true" ]; then
        continue
    fi
    if [ "${SELECTED_SERVICES[$svc]}" = "true" ]; then
        SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"aerial-${svc}\",\"type\":\"bazel\"}")
    fi
done

for entry in "${DOCKER_CFG[@]}"; do
    IFS=":" read -r svc ctx df df_file base_repo <<< "$entry"
    if [ "${DOCKER_TRIGGERED[$svc]:-false}" = "true" ]; then
        SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"aerial-${svc}\",\"type\":\"docker\",\"context\":\"${ctx}\",\"file\":\"${df}\"}")
    fi
done

MATRIX_JSON="[$(IFS=,; echo "${SELECTED_MATRIX[*]}")]"
HAS_BUILDS="false"
[ ${#SELECTED_MATRIX[@]} -gt 0 ] && HAS_BUILDS="true"

if [ -n "$GITHUB_OUT" ]; then
    {
        echo "pkg=${PKG_CHANGED}"
        for svc in "${OCI_SERVICES[@]}" "${DOCKER_SERVICES[@]}"; do
            echo "${svc}=${AFFECTED_SERVICES[$svc]}"
        done
        echo "matrix=${MATRIX_JSON}"
        echo "has_builds=${HAS_BUILDS}"
    } >> "$GITHUB_OUT"
fi

echo "Build Selection Result:" && echo "  pkg: ${PKG_CHANGED}" && echo "  has_builds: ${HAS_BUILDS}" && echo "  selected images count: ${#SELECTED_MATRIX[@]}"
for svc in "${OCI_SERVICES[@]}" "${DOCKER_SERVICES[@]}"; do [ "${SELECTED_SERVICES[$svc]}" = "true" ] && echo "    - $svc" || true; done
