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

exec supergateway \
  --host 0.0.0.0 \
  --port "$PORT" \
  --outputTransport streamableHttp \
  --streamableHttpPath /mcp \
  --stateful \
  --sessionTimeout 60000 \
  --cors \
  --stdio "mcp-server-docker"
