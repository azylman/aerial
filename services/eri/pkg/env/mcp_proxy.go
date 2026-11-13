package env

import (
	"encoding/json"
	"maps"
	"net/url"
	"strings"
	"sync"
)

// MCPProxyPathPrefix is the loopback route prefix of eri's tracing MCP proxy.
const MCPProxyPathPrefix = "/internal/mcp-proxy/"

// DaemonHeader carries the calling daemon's ERI_DAEMON_ID on Claude Code MCP requests.
const DaemonHeader = "X-Eri-Daemon"

// mcpURLKeys are the MCP config fields naming an HTTP server's URL.
var mcpURLKeys = []string{"serverUrl", "server_url", "url"}

// MCPProxyRoutes routes agents' HTTP MCP traffic through eri's loopback tracing proxy. When
// attached to a Provisioner it rewrites each HTTP MCP entry's URL to
// http://127.0.0.1:<port>/internal/mcp-proxy/<target>/<server> and remembers the original
// URL, which the proxy resolves in memory and never derives from the request. It is safe
// for concurrent use; a nil *MCPProxyRoutes (tracing disabled) rewrites nothing.
type MCPProxyRoutes struct {
	port string

	mu        sync.RWMutex
	upstreams map[string]map[string]string
}

// NewMCPProxyRoutes returns routes for an eri listening on port.
func NewMCPProxyRoutes(port string) *MCPProxyRoutes {
	return &MCPProxyRoutes{port: strings.TrimSpace(port), upstreams: make(map[string]map[string]string)}
}

// Upstream returns the original URL of target's MCP server.
func (r *MCPProxyRoutes) Upstream(target, server string) (string, bool) {
	if r == nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.upstreams[target][server]
	return u, ok
}

func (r *MCPProxyRoutes) proxyBase() string {
	return "http://127.0.0.1:" + r.port + MCPProxyPathPrefix
}

func (r *MCPProxyRoutes) proxyURL(target RuleTarget, server string) string {
	return r.proxyBase() + url.PathEscape(string(target)) + "/" + url.PathEscape(server)
}

// rewrite points servers' streamable-HTTP entries at the proxy, keeping headers and every other
// field, and records target's upstream map. stdio entries and legacy SSE entries (whose
// endpoint event carries upstream-relative URLs) are left untouched.
func (r *MCPProxyRoutes) rewrite(target RuleTarget, servers map[string]any) {
	if r == nil {
		return
	}
	upstreams := make(map[string]string)
	for name, v := range servers {
		entry, ok := v.(map[string]any)
		if !ok || strings.EqualFold(stringField(entry, "type"), "sse") {
			continue
		}
		for _, key := range mcpURLKeys {
			upstream := stringField(entry, key)
			if upstream == "" {
				continue
			}
			rewritten := maps.Clone(entry)
			rewritten[key] = r.proxyURL(target, name)
			servers[name] = rewritten
			upstreams[name] = upstream
			break
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upstreams[string(target)] = upstreams
}

// withDaemonHeader adds X-Eri-Daemon: ${ERI_DAEMON_ID:-} to every proxied entry of a Claude
// Code MCP config; Claude Code expands the placeholder from the daemon's env, naming the
// exact daemon (and so the turn) behind each request. The empty default keeps Claude Code from
// rejecting the config in daemons spawned without the variable; the proxy then falls back to
// target-level attribution.
func (r *MCPProxyRoutes) withDaemonHeader(claudeConfig json.RawMessage) json.RawMessage {
	if r == nil {
		return claudeConfig
	}
	var root map[string]any
	if err := json.Unmarshal(claudeConfig, &root); err != nil {
		return claudeConfig
	}
	servers, ok := root["mcpServers"].(map[string]any)
	if !ok {
		return claudeConfig
	}
	for _, v := range servers {
		entry, ok := v.(map[string]any)
		if !ok || !strings.HasPrefix(stringField(entry, "url"), r.proxyBase()) {
			continue
		}
		headers := make(map[string]any, 1)
		if existing, ok := entry["headers"].(map[string]any); ok {
			headers = maps.Clone(existing)
		}
		headers[DaemonHeader] = "${ERI_DAEMON_ID:-}"
		entry["headers"] = headers
	}
	out, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return claudeConfig
	}
	return out
}

func stringField(m map[string]any, key string) string {
	s, ok := m[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}
