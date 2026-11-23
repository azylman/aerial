#!/bin/sh
# Sourced helper: run a command under a clean-room environment.
#
# hermetic_go VAR=val... cmd args...
#   Runs the command via `env -i` with only a toolchain/OS allowlist, so tests
#   never inherit live pod env (secrets, service URLs, config paths).
#   Extra VAR=val arguments before the command are passed through to env.

hermetic_go() {
    env -i \
        ${PATH:+PATH="$PATH"} \
        ${HOME:+HOME="$HOME"} \
        ${GOROOT:+GOROOT="$GOROOT"} \
        ${GOTOOLCHAIN:+GOTOOLCHAIN="$GOTOOLCHAIN"} \
        ${GOPATH:+GOPATH="$GOPATH"} \
        ${GOCACHE:+GOCACHE="$GOCACHE"} \
        ${GOMODCACHE:+GOMODCACHE="$GOMODCACHE"} \
        ${GOLANGCI_LINT_CACHE:+GOLANGCI_LINT_CACHE="$GOLANGCI_LINT_CACHE"} \
        ${GOFLAGS:+GOFLAGS="$GOFLAGS"} \
        ${TMPDIR:+TMPDIR="$TMPDIR"} \
        ${SystemRoot:+SystemRoot="$SystemRoot"} \
        ${SYSTEMROOT:+SYSTEMROOT="$SYSTEMROOT"} \
        ${USERPROFILE:+USERPROFILE="$USERPROFILE"} \
        ${HOMEDRIVE:+HOMEDRIVE="$HOMEDRIVE"} \
        ${HOMEPATH:+HOMEPATH="$HOMEPATH"} \
        ${TMP:+TMP="$TMP"} \
        ${TEMP:+TEMP="$TEMP"} \
        ${LOCALAPPDATA:+LOCALAPPDATA="$LOCALAPPDATA"} \
        ${APPDATA:+APPDATA="$APPDATA"} \
        ${COMSPEC:+COMSPEC="$COMSPEC"} \
        ${PATHEXT:+PATHEXT="$PATHEXT"} \
        MSYS_NO_PATHCONV=1 \
        LANG="${LANG:-en_US.UTF-8}" \
        LC_ALL="${LC_ALL:-en_US.UTF-8}" \
        GIT_TERMINAL_PROMPT=0 \
        "$@"
}

# Discover Go services/modules in the monorepo.
# Discovers sub-module go.mod dirs (for synthetic test fixtures) plus pkg and
# immediate child dirs of services/, mcp/, and scripts/ containing non-testdata .go files.
discover_go_services() {
    (
        find . -maxdepth 3 -name "go.mod" ! -path "./go.mod" ! -path "*/.*" ! -path "*/vendor/*" ! -path "*/testdata/*" 2>/dev/null | \
            sed 's|^\./||; s|/go\.mod$||'

        if [ -d "pkg" ]; then
            if find pkg -maxdepth 3 -name "*.go" ! -path "*/testdata/*" 2>/dev/null | grep -q '\.go$'; then
                echo "pkg"
            fi
        fi

        for parent in services mcp scripts; do
            if [ -d "$parent" ]; then
                for child in "$parent"/*; do
                    if [ -d "$child" ] && [ ! -L "$child" ]; then
                        case "$child" in
                            */.*) continue ;;
                        esac
                        if find "$child" -maxdepth 3 -name "*.go" ! -path "*/testdata/*" 2>/dev/null | grep -q '\.go$'; then
                            echo "${child#./}"
                        fi
                    fi
                done
            fi
        done
    ) | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

