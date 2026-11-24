const http = require("http");
const port = parseInt(process.env.TARGET_PORT || "4007", 10);
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
