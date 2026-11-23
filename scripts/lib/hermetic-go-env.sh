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

ensure_bazel_build_files() {
    local root="${1:-${REPO_ROOT:-}}"
    if [ -z "$root" ]; then
        if command -v git >/dev/null 2>&1; then
            root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
        else
            root="$(pwd)"
        fi
    fi

    if [ ! -f "$root/MODULE.bazel" ] || ! command -v bazel >/dev/null 2>&1; then
        return 0
    fi

    if [ ! -f "$root/MODULE.bazel.lock" ] || [ "$root/go.mod" -nt "$root/MODULE.bazel.lock" ]; then
        (cd "$root" && bazel mod tidy) || return $?
    fi

    if command -v gazelle >/dev/null 2>&1; then
        (cd "$root" && gazelle) || return $?
    else
        (cd "$root" && bazel run //:gazelle) || return $?
    fi

    # services/eri/pkg/db/BUILD.bazel: schema.sql and migrations/**
    if [ -d "$root/services/eri/pkg/db" ]; then
        local db_build="$root/services/eri/pkg/db/BUILD.bazel"
        if [ ! -f "$db_build" ] || ! grep -q 'name = "schema"' "$db_build" 2>/dev/null; then
            cat >> "$db_build" <<'EOF'

exports_files(
    ["schema.sql"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "schema",
    srcs = ["schema.sql"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "migrations",
    srcs = glob(["migrations/**"]),
    visibility = ["//visibility:public"],
)
EOF
        fi
    fi

    # services/console/BUILD.bazel: static/**
    if [ -d "$root/services/console" ]; then
        local console_build="$root/services/console/BUILD.bazel"
        if [ ! -f "$console_build" ] || ! grep -q 'name = "static"' "$console_build" 2>/dev/null; then
            cat >> "$console_build" <<'EOF'

filegroup(
    name = "static",
    srcs = glob(["static/**"]),
    visibility = ["//visibility:public"],
)
EOF
        fi
    fi

    # services/synapse/BUILD.bazel: testdata/**
    if [ -d "$root/services/synapse" ]; then
        local synapse_build="$root/services/synapse/BUILD.bazel"
        if [ ! -f "$synapse_build" ] || ! grep -q 'name = "testdata"' "$synapse_build" 2>/dev/null; then
            cat >> "$synapse_build" <<'EOF'

filegroup(
    name = "testdata",
    srcs = glob(["testdata/**"]),
    visibility = ["//visibility:public"],
)
EOF
        fi
    fi

    return 0
}

# Merge multiple Go coverage profile files into a single output file.
# Usage: merge_coverage_profiles <output_file> <profile1> [profile2...]
merge_coverage_profiles() {
    local out_file="$1"
    shift
    [ $# -gt 0 ] || { rm -f "$out_file"; return 1; }
    local tmp_out="${out_file}.tmp.$$"
    if ! awk '
        /^mode:/ {
            if (mode_line != "" && mode_line != $0) {
                print "merge_coverage_profiles: conflicting coverage modes: " mode_line " vs " $0 > "/dev/stderr"
                conflict = 1
                exit 1
            }
            mode_line = $0
            next
        }
        NF >= 3 {
            block = $1 " " $(NF-1)
            if (!(block in stmts)) {
                blocks[++n] = block
                stmts[block] = $(NF-1) + 0
            }
            count[block] += $NF + 0
        }
        END {
            if (conflict) {
                exit 1
            }
            print (mode_line ? mode_line : "mode: set")
            for (i = 1; i <= n; i++) {
                b = blocks[i]
                print b " " count[b]
            }
        }
    ' "$@" > "$tmp_out"; then
        rm -f "$tmp_out" "$out_file"
        return 1
    fi
    mv "$tmp_out" "$out_file"
}


