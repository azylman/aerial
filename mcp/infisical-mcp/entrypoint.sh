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

# Unset empty or whitespace-only Infisical env vars so @infisical/mcp's Zod
# min(1).optional() validator never trips on empty strings rendered by templates.
for var_name in \
  INFISICAL_TOKEN \
  INFISICAL_UNIVERSAL_AUTH_CLIENT_ID \
  INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET \
  INFISICAL_PROJECT_ID \
  INFISICAL_ENABLED_TOOLS; do
  eval "raw_val=\${$var_name:-}"
  trimmed_val=$(printf '%s' "$raw_val" | tr -d '[:space:]')
  if [ -z "$trimmed_val" ]; then
    unset "$var_name"
  fi
done

# Check if required Infisical credentials are present; fall back to mesh auto-auth token endpoint if absent.
AUTH_CONFIGURED=false

if [ -n "${INFISICAL_UNIVERSAL_AUTH_CLIENT_ID:-}" ] && [ -n "${INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET:-}" ]; then
  export INFISICAL_AUTH_METHOD="${AUTH_METHOD:-universal-auth}"
  AUTH_CONFIGURED=true
elif [ -n "${INFISICAL_TOKEN:-}" ]; then
  export INFISICAL_AUTH_METHOD="access-token"
  AUTH_CONFIGURED=true
elif command -v curl >/dev/null 2>&1; then
  FALLBACK_JSON=$(curl -fsS --max-time 5 -X POST "${INF_HOST%/}/api/v1/auth/token" 2>/dev/null || true)
  if [ -n "$FALLBACK_JSON" ]; then
    FALLBACK_TOKEN=$(printf '%s' "$FALLBACK_JSON" | node -e '
      let d = "";
      process.stdin.on("data", c => d += c);
      process.stdin.on("end", () => {
        try {
          const j = JSON.parse(d);
          const t = (j.token || j.accessToken || "").trim();
          if (t) process.stdout.write(t);
        } catch (_) {}
      });
    ' 2>/dev/null || true)
    if [ -n "$FALLBACK_TOKEN" ]; then
      export INFISICAL_TOKEN="$FALLBACK_TOKEN"
      export INFISICAL_AUTH_METHOD="access-token"
      AUTH_CONFIGURED=true
    fi
  fi
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
      if (req.method === "POST" && (req.url || "").startsWith("/mcp")) {
        let body = "";
        req.on("data", (chunk) => { body += chunk; });
        req.on("end", () => {
          let msg;
          try {
            msg = JSON.parse(body);
          } catch (_) {
            res.writeHead(400, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } }));
            return;
          }
          if (msg.method === "notifications/initialized") {
            res.writeHead(204);
            res.end();
            return;
          }
          if (msg.method === "initialize") {
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({
              jsonrpc: "2.0",
              id: msg.id,
              result: {
                protocolVersion: "2024-11-05",
                capabilities: { tools: {} },
                serverInfo: { name: "infisical-mcp", version: "1.0.0" }
              }
            }));
            return;
          }
          if (msg.method === "tools/list") {
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({
              jsonrpc: "2.0",
              id: msg.id,
              result: { tools: [] }
            }));
            return;
          }
          if (msg.method === "tools/call") {
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({
              jsonrpc: "2.0",
              id: msg.id,
              result: {
                content: [{
                  type: "text",
                  text: "infisical-mcp is running in standby mode: Infisical credentials not configured in secrets template"
                }],
                isError: true
              }
            }));
            return;
          }
          if (msg.id === undefined || msg.id === null) {
            res.writeHead(204);
            res.end();
            return;
          }
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({
            jsonrpc: "2.0",
            id: msg.id,
            error: { code: -32601, message: "Method not found" }
          }));
        });
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
