#!/bin/sh
set -e

if [ "${1:-}" = "--stdio-shaper" ]; then
  exec node -e '
    const { spawn } = require("child_process"), readline = require("readline");
    const child = spawn(process.env.GITHUB_MCP_BIN || "github-mcp-server", ["stdio"], { stdio: ["pipe", "pipe", "inherit"] });
    child.on("exit", (code) => process.exit(code ?? 0));
    for (const sig of ["SIGTERM", "SIGINT"]) process.on(sig, () => child.kill(sig));
    process.stdin.on("end", () => child.stdin.end());
    const listIds = new Set(), fileCalls = new Map();
    readline.createInterface({ input: process.stdin, crlfDelay: Infinity }).on("line", (line) => {
      try {
        const msg = JSON.parse(line), idKey = msg.id !== undefined && msg.id !== null ? String(msg.id) : "";
        if (msg.method === "tools/list" && idKey) listIds.add(idKey);
        if (msg.method === "tools/call" && msg.params && msg.params.name === "get_file_contents" && idKey) {
          const a = msg.params.arguments || {};
          fileCalls.set(idKey, { s: Math.max(1, Math.trunc(Number(a.start_line) || 1)), e: Math.trunc(Number(a.end_line) || 0) });
          delete a.start_line; delete a.end_line;
        }
        child.stdin.write(JSON.stringify(msg) + "\n");
      } catch (_) { child.stdin.write(line + "\n"); }
    });
    readline.createInterface({ input: child.stdout, crlfDelay: Infinity }).on("line", (line) => {
      try {
        const msg = JSON.parse(line), idKey = msg.id !== undefined && msg.id !== null ? String(msg.id) : "";
        if (idKey && listIds.delete(idKey) && msg.result && Array.isArray(msg.result.tools)) {
          for (const t of msg.result.tools) {
            if (t.name === "get_file_contents" && t.inputSchema && t.inputSchema.properties) {
              t.inputSchema.properties.start_line = { type: "integer", description: "1-indexed start line to read (default: 1)" };
              t.inputSchema.properties.end_line = { type: "integer", description: "1-indexed inclusive end line to read (max 250 lines per call)" };
            }
          }
        } else if (idKey && fileCalls.has(idKey)) {
          const req = fileCalls.get(idKey); fileCalls.delete(idKey);
          const content = msg.result && Array.isArray(msg.result.content) ? msg.result.content : [];
          const target = content.find((c) => c.type === "resource" && c.resource && typeof c.resource.text === "string");
          if (target) {
            const lines = target.resource.text.split("\n"), total = lines.length;
            let meta = `Showing lines 0-0 of ${total} (start_line ${req.s} exceeds file length ${total})`;
            if (req.s <= total) {
              const maxEnd = req.s + 249, e = Math.min(total, req.e >= req.s ? Math.min(req.e, maxEnd) : maxEnd);
              let sliced = lines.slice(req.s - 1, e).join("\n"), byteTrunc = false;
              if (sliced.length > 12000) { sliced = sliced.slice(0, 12000) + "\n... [char limit 12000 reached]"; byteTrunc = true; }
              target.resource.text = sliced;
              const more = (e < total || byteTrunc) ? ` (truncated; pass start_line=${e + 1}, end_line=${Math.min(total, e + 250)} to continue)` : "";
              meta = `Showing lines ${req.s}-${e} of ${total}${more}`;
            } else { target.resource.text = ""; }
            const hdr = content.find((c) => c.type === "text" && typeof c.text === "string");
            if (hdr) hdr.text += ` — ${meta}`; else target.resource.text = `${meta}\n${target.resource.text}`;
          }
        }
        process.stdout.write(JSON.stringify(msg) + "\n");
      } catch (_) { process.stdout.write(line + "\n"); }
    });
  '
fi

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
  --stdio "/usr/local/bin/entrypoint.sh --stdio-shaper"
