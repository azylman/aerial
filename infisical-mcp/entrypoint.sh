#!/bin/sh
set -e

# Check if required Infisical credentials are present.
# Can authenticate via INFISICAL_TOKEN (Token Auth)
# OR INFISICAL_UNIVERSAL_AUTH_CLIENT_ID + INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET (Universal Auth).
AUTH_CONFIGURED=false

if [ -n "$INFISICAL_TOKEN" ]; then
  AUTH_CONFIGURED=true
elif [ -n "$INFISICAL_UNIVERSAL_AUTH_CLIENT_ID" ] && [ -n "$INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET" ]; then
  AUTH_CONFIGURED=true
fi

if [ "$AUTH_CONFIGURED" = "true" ]; then
  echo "[infisical-mcp] Credentials detected. Starting supergateway with @infisical/mcp..."
  exec supergateway \
    --host 0.0.0.0 \
    --port "${PORT:-4007}" \
    --outputTransport streamableHttp \
    --streamableHttpPath /mcp \
    --stateful \
    --sessionTimeout 60000 \
    --cors \
    --stdio "infisical-mcp"
else
  echo "[infisical-mcp] Notice: Credentials not configured (missing INFISICAL_TOKEN or INFISICAL_UNIVERSAL_AUTH_CLIENT_ID/SECRET)."
  echo "[infisical-mcp] Running in standby healthcheck mode on port ${PORT:-4007} to prevent container restart loops."
  # Start a lightweight Node.js HTTP server responding 200 OK to healthchecks and standby messages
  exec node -e '
    const http = require("http");
    const port = parseInt(process.env.PORT || "4007", 10);
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
        message: "Infisical credentials not configured. Set INFISICAL_TOKEN or INFISICAL_UNIVERSAL_AUTH_CLIENT_ID and SECRET in .env"
      }));
    });
    server.listen(port, "0.0.0.0", () => {
      console.log(`[infisical-mcp] Standby HTTP server listening on port ${port}`);
    });
  '
fi
