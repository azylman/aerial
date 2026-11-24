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

bootstrap_tool() {
    local name="$1" url="$2" sha="$3" tdir="${BAZEL_TOOLS_DIR:-$HOME/.cache/aerial/bin}"
    if command -v "$name" >/dev/null 2>&1; then return 0; fi
    if [ -x "$tdir/$name" ]; then export PATH="$tdir:$PATH"; return 0; fi
    case "$(uname -m)" in x86_64|amd64) ;; *) return 0 ;; esac
    if ! command -v curl >/dev/null 2>&1 || ! command -v sha256sum >/dev/null 2>&1; then return 0; fi
    mkdir -p "$tdir" && local tmp="$tdir/.$name.$$"
    if curl -fsSL --retry 3 "$url" -o "$tmp" && echo "$sha  $tmp" | sha256sum -c - >/dev/null 2>&1; then
        chmod 0755 "$tmp" && mv -f "$tmp" "$tdir/$name" && export PATH="$tdir:$PATH"
    else
        rm -f "$tmp"
        echo "🚨 [Aerial] Error: failed to bootstrap SHA256-verified $name" >&2
        return 1
    fi
}

ensure_bazel_build_files() {
    local root="${1:-${REPO_ROOT:-}}"
    if [ -z "$root" ]; then
        if command -v git >/dev/null 2>&1; then
            root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
        else
            root="$(pwd)"
        fi
    fi

    bootstrap_tool atlas "https://release.ariga.io/atlas/atlas-linux-amd64-v0.31.0" "c69e5d9cffbb7454238e8ec34731db2ff6fa2c5d4efd7e68e27ca6a2b89694c9" || return 1
    bootstrap_tool kubectl "https://dl.k8s.io/release/v1.32.2/bin/linux/amd64/kubectl" "4f6a959dcc5b702135f8354cc7109b542a2933c46b808b248a214c1f69f817ea" || return 1

    if [ -f "$root/MODULE.bazel" ] && ! command -v bazel >/dev/null 2>&1; then
        echo "🚨 [Aerial] Error: bazel is required when MODULE.bazel is present" >&2
        return 1
    fi
    if [ ! -f "$root/MODULE.bazel" ]; then
        return 0
    fi

    if [ ! -f "${DOCKER_CONFIG:-$HOME/.docker}/config.json" ] && curl -fsS --connect-timeout 1 -m 2 http://127.0.0.1:4028/healthz >/dev/null 2>&1; then
        cat > /tmp/aerial-bazel-downloader.cfg <<'EOF'
rewrite https://ghcr.io/token(.*) http://127.0.0.1:4028/internal/github-ghcr-token$1
EOF
        mkdir -p "$HOME"
        grep -qF 'aerial-bazel-downloader.cfg' "$HOME/.bazelrc" 2>/dev/null || echo 'common --downloader_config=/tmp/aerial-bazel-downloader.cfg' >> "$HOME/.bazelrc"
    fi
    if [ ! -f "$root/MODULE.bazel.lock" ] || [ "$root/go.mod" -nt "$root/MODULE.bazel.lock" ]; then
        (cd "$root" && bazel mod tidy) || return $?
    fi
    if command -v gazelle >/dev/null 2>&1; then
        (cd "$root" && gazelle) || return $?
    else
        (cd "$root" && bazel run //:gazelle) || return $?
    fi
    grep -q 'schema.sql' "$root/services/eri/pkg/db/BUILD.bazel" 2>/dev/null || printf '\nexports_files(["schema.sql"], visibility = ["//visibility:public"])\n' >> "$root/services/eri/pkg/db/BUILD.bazel"
}
