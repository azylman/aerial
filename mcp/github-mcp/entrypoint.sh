#!/bin/sh
set -e

CONFIG_FILE="${CONFIG_PATH:-/local/config.yaml}"
if [ ! -s "$CONFIG_FILE" ]; then
  if [ -s "/config/config.yaml" ]; then
    CONFIG_FILE="/config/config.yaml"
  else
    echo "FATAL: config file is missing or empty (checked $CONFIG_FILE and /config/config.yaml)" >&2
    exit 1
  fi
fi

PORT=$(grep -E '^[[:space:]]*port:' "$CONFIG_FILE" | awk -F: '{print $2}' | tr -d ' "' | tr -d "'")

if [ -z "$PORT" ]; then
  echo "FATAL: port is required in $CONFIG_FILE" >&2
  exit 1
fi

# Stateless single process: every request authenticates via its own
# Authorization: Bearer header, so no per-session children or stored PAT.
exec github-mcp-server http \
  --port "$PORT" \
  --exclude-tools create_pull_request,merge_pull_request,push_files,create_or_update_file,delete_file
