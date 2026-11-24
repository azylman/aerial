#!/bin/sh
# Sourced helper: run a command under a clean-room environment and discover services/builds.

hermetic_go() {
    env -i \
        ${PATH:+PATH="$PATH"} \
        ${HOME:+HOME="$HOME"} \
        ${BAZELISK_HOME:+BAZELISK_HOME="$BAZELISK_HOME"} \
        ${USE_BAZEL_VERSION:+USE_BAZEL_VERSION="$USE_BAZEL_VERSION"} \
        ${GOROOT:+GOROOT="$GOROOT"} \
        ${GOTOOLCHAIN:+GOTOOLCHAIN="$GOTOOLCHAIN"} \
        ${GOPATH:+GOPATH="$GOPATH"} \
        ${GOCACHE:+GOCACHE="$GOCACHE"} \
        ${GOMODCACHE:+GOMODCACHE="$GOMODCACHE"} \
        ${GOLANGCI_LINT_CACHE:+GOLANGCI_LINT_CACHE="$GOLANGCI_LINT_CACHE"} \
        ${GOFLAGS:+GOFLAGS="$GOFLAGS"} \
        ${TMPDIR:+TMPDIR="$TMPDIR"} \
        MSYS_NO_PATHCONV=1 \
        LANG="${LANG:-en_US.UTF-8}" \
        LC_ALL="${LC_ALL:-en_US.UTF-8}" \
        GIT_TERMINAL_PROMPT=0 \
        "$@"
}

discover_go_services() {
    (
        find . -maxdepth 3 -name "go.mod" ! -path "./go.mod" ! -path "*/.*" ! -path "*/vendor/*" ! -path "*/testdata/*" 2>/dev/null | \
            sed 's|^\./||; s|/go\.mod$||'
        [ -d "pkg" ] && find pkg -maxdepth 3 -name "*.go" ! -path "*/testdata/*" 2>/dev/null | grep -q '\.go$' && echo "pkg"
        for parent in services mcp scripts; do
            [ -d "$parent" ] || continue
            for child in "$parent"/*; do
                [ -d "$child" ] && [ ! -L "$child" ] || continue
                case "$child" in */.*) continue ;; esac
                find "$child" -maxdepth 3 -name "*.go" ! -path "*/testdata/*" 2>/dev/null | grep -q '\.go$' && echo "${child#./}"
            done
        done
    ) | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

ensure_bazel_build_files() {
    root="${1:-${REPO_ROOT:-}}"
    [ -n "$root" ] || root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
    [ -f "$root/MODULE.bazel" ] || return 0
    command -v bazel >/dev/null 2>&1 || { echo "🚨 [Aerial] Error: bazel is required" >&2; return 1; }
    if [ ! -f "$root/MODULE.bazel.lock" ] || [ "$root/go.mod" -nt "$root/MODULE.bazel.lock" ]; then
        (cd "$root" && bazel mod tidy) || return $?
    fi
    if command -v gazelle >/dev/null 2>&1; then
        (cd "$root" && gazelle) || return $?
    else
        (cd "$root" && bazel run //:gazelle) || return $?
    fi
    db_build="$root/services/eri/pkg/db/BUILD.bazel"
    if [ -f "$db_build" ] && ! grep -q 'schema.sql' "$db_build" 2>/dev/null; then
        printf '\nexports_files(["schema.sql"], visibility = ["//visibility:public"])\n' >> "$db_build"
    fi
}
