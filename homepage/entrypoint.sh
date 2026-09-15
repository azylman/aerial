#!/bin/sh
set -e

mkdir -p /config

# Execute config preparation with explicit raw-copy fallback
if ! NODE_PATH=/app/node_modules node /app/core-homepage/prepare-config.js; then
    echo "🚨 [Homepage Entrypoint] Config preparation failed! Falling back to raw core baseline..." >&2
    cp -rf /app/core-homepage/config/. /config/ 2>/dev/null || true
fi

# Chown config directory for node user if running as root
if [ "$(id -u)" = "0" ]; then
    chown -R node:node /config 2>/dev/null || true
fi

# Pass through arguments or default to server.js
if [ $# -eq 0 ]; then
    set -- node server.js
fi
exec /usr/local/bin/docker-entrypoint.sh "$@"
