#!/usr/bin/env bash
# Runs the real-Restate integration tests (build tag restateintegration) in
# services/eri against a pinned restate-server binary. Agent pods and CI
# runners have no docker, so the server runs as a plain child process that
# services/eri/pkg/restatetest starts per test.
#
# Usage: scripts/restate-integration-test.sh [extra go test flags...]
#
# The pin matches the live RestateCluster image (k8s/operators-and-storage.yaml,
# restatedev/restate:1.8.1-hotfix.2). Bump RESTATE_VERSION and both sha256 pins
# together with that image.
set -euo pipefail

RESTATE_VERSION="v1.8.1-hotfix.2"
SHA256_X86_64="57d4496453104ce3a02cdd5b520db14de9086cce20f1483795b6b9b3f65cde03"
SHA256_AARCH64="192ccc91466b59b02a5e30130eb277778fc7ee586d64588480c5971b618e358c"

# Packages (relative to services/eri) that carry restateintegration tests.
PACKAGES=(
    ./pkg/restatetest/...
    ./pkg/queue/...
)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

case "$(uname -m)" in
    x86_64 | amd64) ARCH="x86_64"; WANT_SHA="$SHA256_X86_64" ;;
    aarch64 | arm64) ARCH="aarch64"; WANT_SHA="$SHA256_AARCH64" ;;
    *) echo "restate-integration-test: unsupported arch $(uname -m)" >&2; exit 1 ;;
esac

ASSET="restate-server-${ARCH}-unknown-linux-musl"
CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/aerial/restate-server/${RESTATE_VERSION}"
BIN="$CACHE_DIR/$ASSET/restate-server"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

extract_txz() {
    # $1 archive, $2 destination dir. Prefer xz; fall back to Python's lzma when
    # xz is not installed (agent pods).
    if command -v xz >/dev/null 2>&1; then
        tar -xJf "$1" -C "$2"
    else
        python3 -I -c 'import sys, tarfile; tarfile.open(sys.argv[1]).extractall(sys.argv[2], filter="data")' "$1" "$2"
    fi
}

if [ ! -x "$BIN" ]; then
    mkdir -p "$CACHE_DIR"
    TMP_DIR="$(mktemp -d)"
    trap 'rm -rf "$TMP_DIR"' EXIT
    ARCHIVE="$TMP_DIR/$ASSET.tar.xz"
    URL="https://github.com/restatedev/restate/releases/download/${RESTATE_VERSION}/${ASSET}.tar.xz"
    echo "restate-integration-test: fetching $URL"
    curl -fsSL --retry 3 -o "$ARCHIVE" "$URL"
    GOT_SHA="$(sha256_of "$ARCHIVE")"
    if [ "$GOT_SHA" != "$WANT_SHA" ]; then
        echo "restate-integration-test: sha256 mismatch for $ASSET: got $GOT_SHA, want $WANT_SHA" >&2
        exit 1
    fi
    mkdir -p "$TMP_DIR/out"
    extract_txz "$ARCHIVE" "$TMP_DIR/out"
    rm -rf "${CACHE_DIR:?}/$ASSET"
    mv "$TMP_DIR/out/$ASSET" "$CACHE_DIR/$ASSET"
fi

"$BIN" --version

cd "$REPO_ROOT/services/eri"
go test -tags restateintegration -count=1 -race "$@" "${PACKAGES[@]}" -args -restate-server-bin="$BIN"
