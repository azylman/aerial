#!/bin/sh
set -e

CONFIG_FILE="/config/config.yaml"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "FATAL: $CONFIG_FILE is missing" >&2
  exit 1
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
