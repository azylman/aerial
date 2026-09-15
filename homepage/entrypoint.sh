#!/bin/sh
set -euo pipefail

CONFIG_DIR="/config"
CORE_DIR="/app/core-config"
USER_DIR="/share/aerial-config/homepage"

# 1. Ensure target config directory exists (resolving /app/config -> /config symlink)
mkdir -p "${CONFIG_DIR}"

# 2. Stage base core configurations
if [ -d "${CORE_DIR}" ]; then
    cp -rf "${CORE_DIR}/." "${CONFIG_DIR}/"
fi

# 3. Non-destructively overlay user configurations (strictly whitelisted YAML + CSS only, NO JS)
if [ -d "${USER_DIR}" ]; then
    for cfg in services.yaml widgets.yaml bookmarks.yaml settings.yaml docker.yaml; do
        if [ -f "${USER_DIR}/${cfg}" ]; then
            cp -f "${USER_DIR}/${cfg}" "${CONFIG_DIR}/${cfg}"
        fi
    done

    # Non-destructive CSS overlay
    if [ -f "${USER_DIR}/custom.css" ]; then
        printf "\n\n/* --- User Overrides from aerial-config --- */\n" >> "${CONFIG_DIR}/custom.css"
        cat "${USER_DIR}/custom.css" >> "${CONFIG_DIR}/custom.css"
    fi
fi

# 4. Ensure runtime user permissions
if [ "$(id -u)" = "0" ]; then
    chown -R node:node "${CONFIG_DIR}" 2>/dev/null || true
fi

# 5. Hand over execution to upstream entrypoint for su-exec and IPv6 binding
exec /usr/local/bin/docker-entrypoint.sh node server.js
