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
INF_HOST=$(grep -E '^[[:space:]]*infisical_host_url:' "$CONFIG_FILE" | sed -E 's/^[[:space:]]*infisical_host_url:[[:space:]]*//' | tr -d '"' | tr -d "'")
AUTH_METHOD=$(grep -E '^[[:space:]]*auth_method:' "$CONFIG_FILE" | awk -F: '{print $2}' | tr -d ' "' | tr -d "'")
MASK_SECRETS=$(grep -E '^[[:space:]]*mask_secret_values:' "$CONFIG_FILE" | awk -F: '{print $2}' | tr -d ' "' | tr -d "'")

if [ -z "$PORT" ]; then
  echo "FATAL: port is required in $CONFIG_FILE" >&2
  exit 1
fi
if [ -z "$INF_HOST" ]; then
  echo "FATAL: infisical_host_url is required in $CONFIG_FILE" >&2
  exit 1
fi

export INFISICAL_HOST_URL="$INF_HOST"
if [ -n "$AUTH_METHOD" ]; then
  export INFISICAL_AUTH_METHOD="$AUTH_METHOD"
fi
if [ -n "$MASK_SECRETS" ]; then
  export INFISICAL_MASK_SECRET_VALUES="$MASK_SECRETS"
fi

# Check if required Infisical credentials are present.
AUTH_CONFIGURED=false

if [ -n "$INFISICAL_TOKEN" ]; then
  AUTH_CONFIGURED=true
elif [ -n "$INFISICAL_UNIVERSAL_AUTH_CLIENT_ID" ] && [ -n "$INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET" ]; then
  AUTH_CONFIGURED=true
fi

if [ "$AUTH_CONFIGURED" = "true" ]; then
  echo "[infisical-mcp] Credentials detected. Starting supergateway with @infisical/mcp on port $PORT..."
  exec supergateway \
    --host 0.0.0.0 \
    --port "$PORT" \
    --outputTransport streamableHttp \
    --streamableHttpPath /mcp \
    --stateful \
    --sessionTimeout 60000 \
    --cors \
    --stdio "infisical-mcp"
else
  echo "[infisical-mcp] Notice: Credentials not configured (missing INFISICAL_TOKEN or INFISICAL_UNIVERSAL_AUTH_CLIENT_ID/SECRET)."
  echo "[infisical-mcp] Running in standby healthcheck mode on port $PORT to prevent container restart loops."
  exec node -e '
    const http = require("http");
    const port = parseInt(process.env.TARGET_PORT || "'"$PORT"'", 10);
    const server = http.createServer((req, res) => {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "*");
      if (req.method === "OPTIONS") {
        res.writeHead(204);
        res.end();
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        status: "standby",
        service: "infisical-mcp",
        configured: false,
        message: "Infisical credentials not configured in secrets template"
      }));
    });
    server.listen(port, "0.0.0.0", () => {
      console.log(`[infisical-mcp] Standby HTTP server listening on port ${port}`);
    });
  '
fi
