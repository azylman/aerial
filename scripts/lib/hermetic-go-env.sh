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
    (
        find . -maxdepth 3 -name "go.mod" ! -path "./go.mod" ! -path "*/.*" ! -path "*/vendor/*" ! -path "*/testdata/*" 2>/dev/null | sed 's|^\./||; s|/go\.mod$||'
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

discover_affected_services() {
    local r="${REPO_ROOT:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
    (cd "$r" && bazel query "rdeps(//..., set($*))" 2>/dev/null | grep -E "^//(services|mcp)/" | sed -E "s#^//(services|mcp)/([^/:]+).*#\2#" | sort -u) || echo "ALL"
}

bootstrap_tool() {
    local name="$1" url="$2" sha="$3" tdir="${BAZEL_TOOLS_DIR:-$HOME/.cache/aerial/bin}"
    command -v "$name" >/dev/null 2>&1 && return 0
    [ -x "$tdir/$name" ] && { export PATH="$tdir:$PATH"; return 0; }
    case "$(uname -m)" in x86_64|amd64) ;; *) return 0 ;; esac
    command -v curl >/dev/null 2>&1 && command -v sha256sum >/dev/null 2>&1 || return 0
    mkdir -p "$tdir" && local tmp="$tdir/.$name.$$"
    if curl -fsSL --retry 3 "$url" -o "$tmp" && echo "$sha  $tmp" | sha256sum -c - >/dev/null 2>&1; then
        chmod 0755 "$tmp" && mv -f "$tmp" "$tdir/$name" && export PATH="$tdir:$PATH"
    else
        rm -f "$tmp"; echo "🚨 [Aerial] Error: failed to bootstrap SHA256-verified $name" >&2; return 1
    fi
}

ensure_bazel_build_files() {
    local root="${1:-${REPO_ROOT:-}}"
    [ -z "$root" ] && { command -v git >/dev/null 2>&1 && root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)" || root="$(pwd)"; }
    bootstrap_tool atlas "https://release.ariga.io/atlas/atlas-linux-amd64-v0.31.0" "c69e5d9cffbb7454238e8ec34731db2ff6fa2c5d4efd7e68e27ca6a2b89694c9" || true
    bootstrap_tool kubectl "https://dl.k8s.io/release/v1.32.2/bin/linux/amd64/kubectl" "4f6a959dcc5b702135f8354cc7109b542a2933c46b808b248a214c1f69f817ea" || true

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

    command -v gazelle >/dev/null 2>&1 && (cd "$root" && gazelle) || (cd "$root" && bazel run //:gazelle) || return $?
    grep -q 'schema.sql' "$root/services/eri/pkg/db/BUILD.bazel" 2>/dev/null || printf '\nexports_files(["schema.sql"], visibility = ["//visibility:public"])\n' >> "$root/services/eri/pkg/db/BUILD.bazel"
}

merge_coverage_profiles() {
    local o="$1"; shift; [ $# -gt 0 ] || { rm -f "$o"; return 1; }; local t="${o}.tmp.$$"
    awk '/^mode:/{if(m!=""&&m!=$0)exit 1;m=$0;next}NF>=3{b=$1" "$(NF-1);if(!(b in s)){B[++n]=b;s[b]=$(NF-1)+0}c[b]+=$NF+0}END{print(m?m:"mode: set");for(i=1;i<=n;i++)print B[i]" "c[B[i]]}' "$@" > "$t" || { rm -f "$t" "$o"; return 1; }
    mv "$t" "$o"
}
