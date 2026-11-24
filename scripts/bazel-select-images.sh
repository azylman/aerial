#!/usr/bin/env bash
# scripts/bazel-select-images.sh: Select changed microservices and images for Continuous Delivery.
# Uses Bazel DAG reverse-dependency queries (`bazel query "kind(oci_image, rdeps(//images:all, ...))"`)
# paired with explicit asset and Dockerfile copy-context path triggers (e.g., services/eri-restate/Dockerfile
# copies scripts/ and pkg/). Emits per-service outputs and MATRIX_JSON for build-metadata.cjs and docker-publish.yml.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

FORCE_ALL="${FORCE_ALL:-false}"
GITHUB_OUT="${GITHUB_OUTPUT:-}"

while [ $# -gt 0 ]; do
    case "$1" in
        --force-all|-force-all)
            FORCE_ALL="true"
            shift
            ;;
        --github-output|-github-output)
            GITHUB_OUT="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

# Read changed files from stdin if available, otherwise check git diff
CHANGED_FILES=()
if [ ! -t 0 ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        [ -n "$line" ] && CHANGED_FILES+=("$line")
    done
fi

if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
    if git rev-parse --git-dir >/dev/null 2>&1; then
        CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
        if git rev-parse --verify origin/main >/dev/null 2>&1 && [ "$CURRENT_BRANCH" != "main" ]; then
            mapfile -t CHANGED_FILES < <(git diff --name-only origin/main...HEAD 2>/dev/null || true)
        fi
        if [ ${#CHANGED_FILES[@]} -eq 0 ]; then
            mapfile -t CHANGED_FILES < <(git diff --name-only HEAD~1 2>/dev/null || git diff --name-only HEAD 2>/dev/null || git status --porcelain | awk '{print $NF}')
        fi
    fi
fi

# Check global triggers (workflow or base config changes force all)
for file in "${CHANGED_FILES[@]}"; do
    case "$file" in
        .github/workflows/docker-publish.yml|.dockerignore)
            FORCE_ALL="true"
            ;;
    esac
done

# 16 rules_oci services
OCI_SERVICES=(
    "eri"
    "haro"
    "coven"
    "docket-mcp"
    "kernel-mcp"
    "synapse"
    "transcripts"
    "vanadis"
    "vanadis-restate"
    "vanadis-nats"
    "prospera"
    "console"
    "redact-pattern"
    "coverage-ingest"
    "docs"
    "migrator"
)

# 4 retained Dockerfile services
DOCKER_SERVICES=(
    "eri-restate"
    "github-runner"
    "discord-mcp"
    "infisical-mcp"
)

# Check if pkg/ changed
PKG_CHANGED="false"
for file in "${CHANGED_FILES[@]}"; do
    if [[ "$file" == pkg/* ]]; then
        PKG_CHANGED="true"
        break
    fi
done

# Ensure ephemeral BUILD.bazel files exist before invoking Bazel
# shellcheck source=scripts/lib/hermetic-go-env.sh
. "$REPO_ROOT/scripts/lib/hermetic-go-env.sh"
ensure_bazel_build_files "$REPO_ROOT"

declare -A SELECTED_SERVICES
SELECTED_MATRIX=()

# Check global build triggers affecting all rules_oci images
RULES_OCI_ALL="false"
if [ "$FORCE_ALL" = "true" ]; then
    RULES_OCI_ALL="true"
else
    for file in "${CHANGED_FILES[@]}"; do
        case "$file" in
            images/BUILD.bazel|build/BUILD.bazel|build/oci.bzl|MODULE.bazel*|BUILD.bazel|go.mod|go.sum)
                RULES_OCI_ALL="true"
                break
                ;;
        esac
    done
fi

# Query Bazel DAG for reverse dependencies of changed Go packages and assets
QUERY_SELECTED_SERVICES=()
if [ "$RULES_OCI_ALL" != "true" ]; then
    if ! command -v bazel >/dev/null 2>&1; then
        echo "::warning::Bazel is not available for rdeps change detection; selecting all OCI images" >&2
        RULES_OCI_ALL="true"
    else
        bazel_targets=()
        for file in "${CHANGED_FILES[@]}"; do
            case "$file" in
                services/*|mcp/*|pkg/*)
                    pkg_dir=$(dirname "$file")
                    found_pkg=""
                    cur_dir="$pkg_dir"
                    while [ -n "$cur_dir" ] && [ "$cur_dir" != "." ] && [ "$cur_dir" != "/" ]; do
                        if [ -f "$REPO_ROOT/$cur_dir/BUILD.bazel" ]; then
                            found_pkg="$cur_dir"
                            break
                        fi
                        cur_dir=$(dirname "$cur_dir")
                    done
                    if [ -n "$found_pkg" ]; then
                        bazel_targets+=("//${found_pkg}:all")
                    else
                        echo "::warning::No BUILD.bazel package ancestor found for $file; selecting all OCI images" >&2
                        RULES_OCI_ALL="true"
                        break
                    fi
                    ;;
                web/theme/*)
                    if [ -f "$REPO_ROOT/web/theme/BUILD.bazel" ]; then
                        bazel_targets+=("//web/theme:all")
                    fi
                    ;;
                images/*)
                    if [ -f "$REPO_ROOT/images/BUILD.bazel" ]; then
                        bazel_targets+=("//images:all")
                    fi
                    ;;
            esac
        done

        if [ "$RULES_OCI_ALL" != "true" ] && [ ${#bazel_targets[@]} -gt 0 ]; then
            target_set=$(printf '%s ' "${bazel_targets[@]}")
            query_out=$(mktemp /tmp/aerial-bazel-query.XXXXXX)
            if bazel query "kind(oci_image, rdeps(//images:all, set($target_set)))" >"$query_out" 2>/dev/null; then
                while IFS= read -r match || [ -n "$match" ]; do
                    cleaned=$(echo "$match" | sed 's#^//images:##')
                    [ -n "$cleaned" ] && QUERY_SELECTED_SERVICES+=("$cleaned")
                done < "$query_out"
            else
                echo "::warning::Bazel rdeps query failed; falling back to selecting all OCI images" >&2
                RULES_OCI_ALL="true"
            fi
            rm -f "$query_out"
        fi
    fi
fi

# Evaluate 16 rules_oci images based on Bazel DAG queries and direct input triggers
for svc in "${OCI_SERVICES[@]}"; do
    SELECTED_SERVICES["$svc"]="false"
    image_name="aerial-${svc}"

    image_selected="false"
    if [ "$RULES_OCI_ALL" = "true" ]; then
        image_selected="true"
    else
        # Check Bazel DAG match
        for qsvc in "${QUERY_SELECTED_SERVICES[@]}"; do
            if [ "$qsvc" = "$svc" ]; then
                image_selected="true"
                break
            fi
        done

        # Check direct path triggers
        if [ "$image_selected" = "false" ]; then
            for file in "${CHANGED_FILES[@]}"; do
                case "$svc" in
                    console)
                        case "$file" in
                            services/console/*|web/theme/*)
                                image_selected="true"
                                break
                                ;;
                        esac
                        ;;
                    docs)
                        case "$file" in
                            services/docs-service/*|web/theme/*)
                                image_selected="true"
                                break
                                ;;
                        esac
                        ;;
                    migrator)
                        case "$file" in
                            services/eri/pkg/db/migrations/*|services/eri/pkg/db/*)
                                image_selected="true"
                                break
                                ;;
                        esac
                        ;;
                    redact-pattern)
                        case "$file" in
                            services/eri/cmd/redact-pattern/*)
                                image_selected="true"
                                break
                                ;;
                        esac
                        ;;
                    coverage-ingest)
                        case "$file" in
                            services/coverage-ingest/*|images/cmd/go_wrapper/*)
                                image_selected="true"
                                break
                                ;;
                        esac
                        ;;
                    eri)
                        case "$file" in
                            services/eri/*)
                                image_selected="true"
                                break
                                ;;
                        esac
                        ;;
                    *)
                        case "$file" in
                            services/${svc}/*|mcp/${svc}/*)
                                image_selected="true"
                                break
                                ;;
                        esac
                        ;;
                esac
            done
        fi
    fi

    if [ "$image_selected" = "true" ]; then
        SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"${image_name}\",\"type\":\"bazel\"}")
        SELECTED_SERVICES["$svc"]="true"
    fi
done

# Evaluate 4 retained Dockerfile images based on inputs
for svc in "${DOCKER_SERVICES[@]}"; do
    SELECTED_SERVICES["$svc"]="false"
    image_name="aerial-${svc}"

    if [ "$FORCE_ALL" = "true" ]; then
        SELECTED_SERVICES["$svc"]="true"
    else
        for file in "${CHANGED_FILES[@]}"; do
            case "$svc" in
                eri-restate)
                    case "$file" in
                        services/eri-restate/*|services/eri/*|scripts/*|rules/*|.agents/*|pkg/*|go.mod|go.sum)
                            SELECTED_SERVICES["$svc"]="true"
                            break
                            ;;
                    esac
                    ;;
                github-runner)
                    case "$file" in
                        services/github-runner/*)
                            SELECTED_SERVICES["$svc"]="true"
                            break
                            ;;
                    esac
                    ;;
                discord-mcp)
                    case "$file" in
                        mcp/discord-mcp/*)
                            SELECTED_SERVICES["$svc"]="true"
                            break
                            ;;
                    esac
                    ;;
                infisical-mcp)
                    case "$file" in
                        mcp/infisical-mcp/*)
                            SELECTED_SERVICES["$svc"]="true"
                            break
                            ;;
                    esac
                    ;;
            esac
        done
    fi

    if [ "${SELECTED_SERVICES[$svc]}" = "true" ]; then
        case "$svc" in
            eri-restate)
                SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"${image_name}\",\"type\":\"docker\",\"context\":\".\",\"file\":\"./services/eri-restate/Dockerfile\"}")
                ;;
            github-runner)
                SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"${image_name}\",\"type\":\"docker\",\"context\":\".\",\"file\":\"./services/github-runner/Dockerfile\"}")
                ;;
            discord-mcp)
                SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"${image_name}\",\"type\":\"docker\",\"context\":\".\",\"file\":\"./mcp/discord-mcp/Dockerfile\"}")
                ;;
            infisical-mcp)
                SELECTED_MATRIX+=("{\"service\":\"${svc}\",\"image_name\":\"${image_name}\",\"type\":\"docker\",\"context\":\"./mcp/infisical-mcp\",\"file\":\"./mcp/infisical-mcp/Dockerfile\"}")
                ;;
        esac
    fi
done

# Format matrix JSON
MATRIX_JSON="["
for i in "${!SELECTED_MATRIX[@]}"; do
    if [ "$i" -gt 0 ]; then
        MATRIX_JSON+=","
    fi
    MATRIX_JSON+="${SELECTED_MATRIX[$i]}"
done
MATRIX_JSON+="]"

HAS_BUILDS="false"
if [ ${#SELECTED_MATRIX[@]} -gt 0 ]; then
    HAS_BUILDS="true"
fi

# Write to GITHUB_OUTPUT if requested
if [ -n "$GITHUB_OUT" ]; then
    echo "pkg=${PKG_CHANGED}" >> "$GITHUB_OUT"
    for svc in "${OCI_SERVICES[@]}" "${DOCKER_SERVICES[@]}"; do
        echo "${svc}=${SELECTED_SERVICES[$svc]}" >> "$GITHUB_OUT"
    done
    echo "matrix=${MATRIX_JSON}" >> "$GITHUB_OUT"
    echo "has_builds=${HAS_BUILDS}" >> "$GITHUB_OUT"
fi

# Print human summary
echo "Build Selection Result:"
echo "  pkg: ${PKG_CHANGED}"
echo "  has_builds: ${HAS_BUILDS}"
echo "  selected images count: ${#SELECTED_MATRIX[@]}"
for svc in "${OCI_SERVICES[@]}" "${DOCKER_SERVICES[@]}"; do
    if [ "${SELECTED_SERVICES[$svc]}" = "true" ]; then
        echo "    - $svc"
    fi
done
