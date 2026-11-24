#!/bin/sh
set -e

CONFIG_FILE="${CONFIG_PATH:-/local/config.yaml}"
[ -s "$CONFIG_FILE" ] || CONFIG_FILE="/config/config.yaml"
if [ ! -s "$CONFIG_FILE" ]; then
  echo "FATAL: config file is missing or empty (checked ${CONFIG_PATH:-/local/config.yaml} and /config/config.yaml)" >&2
  exit 1
fi

PORT=$(grep -E '^[[:space:]]*port:' "$CONFIG_FILE" | awk -F: '{print $2}' | tr -d ' "' | tr -d "'")
INF_HOST=$(grep -E '^[[:space:]]*infisical_host_url:' "$CONFIG_FILE" | sed -E 's/^[[:space:]]*infisical_host_url:[[:space:]]*//' | tr -d '"' | tr -d "'")
AUTH_METHOD=$(grep -E '^[[:space:]]*auth_method:' "$CONFIG_FILE" | awk -F: '{print $2}' | tr -d ' "' | tr -d "'")
MASK_SECRETS=$(grep -E '^[[:space:]]*mask_secret_values:' "$CONFIG_FILE" | awk -F: '{print $2}' | tr -d ' "' | tr -d "'")

if [ -z "$PORT" ] || [ -z "$INF_HOST" ]; then
  echo "FATAL: port and infisical_host_url are required in $CONFIG_FILE" >&2
  exit 1
fi

export INFISICAL_HOST_URL="$INF_HOST"
[ -n "$AUTH_METHOD" ] && export INFISICAL_AUTH_METHOD="$AUTH_METHOD"
[ -n "$MASK_SECRETS" ] && export INFISICAL_MASK_SECRET_VALUES="$MASK_SECRETS"

for v in INFISICAL_TOKEN INFISICAL_UNIVERSAL_AUTH_CLIENT_ID INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET INFISICAL_PROJECT_ID INFISICAL_ENABLED_TOOLS; do
  eval "val=\${$v:-}"
  [ -z "$(printf '%s' "$val" | tr -d '[:space:]')" ] && unset "$v"
done

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
      let d="";process.stdin.on("data",c=>d+=c);process.stdin.on("end",()=>{
        try{const j=JSON.parse(d),t=(j.token||j.accessToken||"").trim();if(t)process.stdout.write(t);}catch(_){}
      });' 2>/dev/null || true)
    if [ -n "$FALLBACK_TOKEN" ]; then
      export INFISICAL_TOKEN="$FALLBACK_TOKEN"
      export INFISICAL_AUTH_METHOD="access-token"
      AUTH_CONFIGURED=true
    fi
  fi
fi

if [ "$AUTH_CONFIGURED" = "true" ]; then
  echo "[infisical-mcp] Credentials detected. Starting mcp-proxy with @infisical/mcp on port $PORT..."
  PROXY_CONFIG=$(mktemp)
  cat > "$PROXY_CONFIG" <<JSON
{"mcpProxy":{"baseURL":"http://127.0.0.1:$PORT","addr":":$PORT","name":"infisical-mcp","version":"1.0.0","type":"streamable-http","options":{"panicIfInvalid":true,"logEnabled":false}},"mcpServers":{"mcp":{"command":"infisical-mcp","args":[]}}}
JSON
  exec mcp-proxy --config "$PROXY_CONFIG"
fi

echo "[infisical-mcp] Notice: Credentials not configured (missing INFISICAL_TOKEN or INFISICAL_UNIVERSAL_AUTH_CLIENT_ID/SECRET)."
echo "[infisical-mcp] Running in standby healthcheck mode on port $PORT to prevent container restart loops."
TARGET_PORT="$PORT" exec node /usr/local/bin/standby.js
