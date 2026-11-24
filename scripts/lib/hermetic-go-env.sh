#!/bin/sh
# Sourced helper: run a command under a clean-room environment and discover services/builds.

hermetic_go() {
    env -i ${PATH:+PATH="$PATH"} ${HOME:+HOME="$HOME"} ${BAZELISK_HOME:+BAZELISK_HOME="$BAZELISK_HOME"} \
        ${USE_BAZEL_VERSION:+USE_BAZEL_VERSION="$USE_BAZEL_VERSION"} ${GOROOT:+GOROOT="$GOROOT"} \
        ${GOTOOLCHAIN:+GOTOOLCHAIN="$GOTOOLCHAIN"} ${GOPATH:+GOPATH="$GOPATH"} ${GOCACHE:+GOCACHE="$GOCACHE"} \
        ${GOMODCACHE:+GOMODCACHE="$GOMODCACHE"} ${GOLANGCI_LINT_CACHE:+GOLANGCI_LINT_CACHE="$GOLANGCI_LINT_CACHE"} \
        ${GOFLAGS:+GOFLAGS="$GOFLAGS"} ${TMPDIR:+TMPDIR="$TMPDIR"} MSYS_NO_PATHCONV=1 \
        LANG="${LANG:-en_US.UTF-8}" LC_ALL="${LC_ALL:-en_US.UTF-8}" GIT_TERMINAL_PROMPT=0 "$@"
}

discover_go_services() {
    bazel query "kind('go_library|go_binary|go_test', //...)" 2>/dev/null | sed -E 's#^//([^:]+):.*#\1#' | awk -F/ '$1=="pkg"{print "pkg"} ($1=="services"||$1=="mcp"||$1=="scripts")&&NF>=2{print $1"/"$2}' | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

ensure_bazel_build_files() {
    local root="${1:-${REPO_ROOT:-}}"
    [ -z "$root" ] && { command -v git >/dev/null 2>&1 && root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)" || root="$(pwd)"; }

    [ -f "$root/MODULE.bazel" ] && ! command -v bazel >/dev/null 2>&1 && { echo "🚨 [Aerial] Error: bazel is required when MODULE.bazel is present" >&2; return 1; }
    [ ! -f "$root/MODULE.bazel" ] && return 0

    if [ ! -f "${DOCKER_CONFIG:-$HOME/.docker}/config.json" ] && curl -fsS --connect-timeout 1 -m 2 http://127.0.0.1:4028/healthz >/dev/null 2>&1; then
        echo 'rewrite https://ghcr.io/token(.*) http://127.0.0.1:4028/internal/github-ghcr-token$1' > /tmp/aerial-bazel-downloader.cfg
        mkdir -p "$HOME"
        grep -qF 'aerial-bazel-downloader.cfg' "$HOME/.bazelrc" 2>/dev/null || echo 'common --downloader_config=/tmp/aerial-bazel-downloader.cfg' >> "$HOME/.bazelrc"
    fi
    local tidy=0; [ ! -f "$root/MODULE.bazel.lock" ] && tidy=1
    if [ "$tidy" = "0" ]; then
        if git -C "$root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
            if [ -n "$(git -C "$root" status --porcelain -- go.mod go.sum MODULE.bazel MODULE.bazel.lock 2>/dev/null)" ] || \
               git -C "$root" diff --name-only origin/main...HEAD -- go.mod go.sum MODULE.bazel 2>/dev/null | grep -q . || \
               git -C "$root" diff --name-only HEAD~1 HEAD -- go.mod go.sum MODULE.bazel 2>/dev/null | grep -q .; then
                tidy=1
            fi
        elif [ "$root/go.mod" -nt "$root/MODULE.bazel.lock" ]; then
            tidy=1
        fi
    fi
    [ "$tidy" = "1" ] && { (cd "$root" && bazel mod tidy) || return $?; }

    if command -v gazelle >/dev/null 2>&1; then
        (cd "$root" && gazelle) || return $?
    else
        (cd "$root" && bazel run //:gazelle) || return $?
    fi
    grep -q 'schema.sql' "$root/services/eri/pkg/db/BUILD.bazel" 2>/dev/null || printf '\nexports_files(["schema.sql"], visibility = ["//visibility:public"])\n' >> "$root/services/eri/pkg/db/BUILD.bazel"
}
