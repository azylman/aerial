#!/bin/sh
set -eu

if [ -n "${TEST_SRCDIR:-}" ]; then
    cd "${TEST_SRCDIR}/${TEST_WORKSPACE:-_main}"
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/console-ui-test.XXXXXX")
cleanup() {
    rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

mkdir -p "$WORK/src/services/console/ui" "$WORK/src/web/theme"
cp -RL services/console/ui/. "$WORK/src/services/console/ui/"
cp -RL web/theme/. "$WORK/src/web/theme/"

lock_hash=""
if command -v sha256sum >/dev/null 2>&1; then
    lock_hash=$(sha256sum services/console/ui/package-lock.json | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
    lock_hash=$(shasum -a 256 services/console/ui/package-lock.json | awk '{print $1}')
fi

CACHE_DIR=""
for candidate in /data/cache/console-ui-node-modules /cache/console-ui-node-modules "${TMPDIR:-/tmp}/console-ui-node-modules"; do
    if mkdir -p "$candidate" 2>/dev/null && [ -w "$candidate" ]; then
        CACHE_DIR="$candidate"
        break
    fi
done

tar_cache=""
if [ -n "$CACHE_DIR" ] && [ -n "$lock_hash" ]; then
    tar_cache="$CACHE_DIR/node_modules-${lock_hash}.tar"
fi

cd "$WORK/src/services/console/ui"

if [ -n "$tar_cache" ] && [ -f "$tar_cache" ]; then
    tar -xf "$tar_cache"
else
    npm ci --no-audit --no-fund --silent
    if [ -n "$tar_cache" ]; then
        tar -cf "$tar_cache.tmp.$$" node_modules && mv -f "$tar_cache.tmp.$$" "$tar_cache"
    fi
fi

npm run --silent test:coverage
