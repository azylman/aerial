package env

import (
	"encoding/json"
	"log"
	"maps"
	"net/url"
	"strings"
	"sync"

	"github.com/itchyny/gojq"
)

// MCPProxyPathPrefix is the loopback route prefix of eri's tracing MCP proxy.
const MCPProxyPathPrefix = "/internal/mcp-proxy/"

// DaemonHeader carries the calling daemon's ERI_DAEMON_ID on Claude Code MCP requests.
const DaemonHeader = "X-Eri-Daemon"

// mcpURLKeys are the MCP config fields naming an HTTP server's URL.
var mcpURLKeys = []string{"serverUrl", "server_url", "url"}

// ServerPolicy configures eri proxy behavior for an upstream MCP server.
type ServerPolicy struct {
	UpstreamURL     string
	Instructions    string
	EnabledTools    map[string]bool
	DisabledTools   map[string]bool
	ResponseFilters map[string]*gojq.Code
}

// MCPProxyRoutes routes agents' HTTP MCP traffic through eri's loopback tracing proxy. When
// attached to a Provisioner it rewrites each HTTP MCP entry's URL to
// http://127.0.0.1:<port>/internal/mcp-proxy/<target>/<server> and remembers the original
// URL, which the proxy resolves in memory and never derives from the request. It is safe
// for concurrent use; a nil *MCPProxyRoutes (tracing disabled) rewrites nothing.
type MCPProxyRoutes struct {
	port string

	mu        sync.RWMutex
	upstreams map[string]map[string]string
	policies  map[string]map[string]ServerPolicy
}

// NewMCPProxyRoutes returns routes for an eri listening on port.
func NewMCPProxyRoutes(port string) *MCPProxyRoutes {
	return &MCPProxyRoutes{
		port:      strings.TrimSpace(port),
		upstreams: make(map[string]map[string]string),
		policies:  make(map[string]map[string]ServerPolicy),
	}
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

// Policy returns the ServerPolicy of target's MCP server.
func (r *MCPProxyRoutes) Policy(target, server string) (ServerPolicy, bool) {
	if r == nil {
		return ServerPolicy{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.policies[target][server]
	return p, ok
}

func (r *MCPProxyRoutes) proxyBase() string {
	return "http://127.0.0.1:" + r.port + MCPProxyPathPrefix
}

func (r *MCPProxyRoutes) proxyURL(target RuleTarget, server string) string {
	return r.proxyBase() + url.PathEscape(string(target)) + "/" + url.PathEscape(server)
}

// rewrite points servers' streamable-HTTP entries at the proxy, keeping headers and every other
// field, and records target's upstream map and server policies. stdio entries and legacy SSE
// entries (whose endpoint event carries upstream-relative URLs) are left untouched.
func (r *MCPProxyRoutes) rewrite(target RuleTarget, servers map[string]any) {
	if r == nil {
		return
	}
	upstreams := make(map[string]string)
	policies := make(map[string]ServerPolicy)
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

			var policy ServerPolicy
			policy.UpstreamURL = upstream
			if inst, ok := entry["instructions"].(string); ok {
				policy.Instructions = strings.TrimSpace(inst)
			}
			policy.EnabledTools = extractToolSet(entry, "enabledTools", "enabled_tools")
			policy.DisabledTools = extractToolSet(entry, "disabledTools", "disabled_tools")
			if policy.DisabledTools == nil {
				policy.DisabledTools = make(map[string]bool)
			}
			policy.ResponseFilters = extractResponseFilters(entry, policy.DisabledTools)
			if len(policy.DisabledTools) == 0 {
				policy.DisabledTools = nil
			}
			policies[name] = policy
			break
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upstreams[string(target)] = upstreams
	r.policies[string(target)] = policies
}

func addToolStrings(set map[string]bool, raw any) {
	switch val := raw.(type) {
	case []string:
		for _, s := range val {
			if clean := strings.ToLower(strings.TrimSpace(s)); clean != "" {
				set[clean] = true
			}
		}
	case []any:
		for _, item := range val {
			if s, ok := item.(string); ok {
				if clean := strings.ToLower(strings.TrimSpace(s)); clean != "" {
					set[clean] = true
				}
			}
		}
	}
}

func extractToolSet(entry map[string]any, keys ...string) map[string]bool {
	result := make(map[string]bool)
	for _, key := range keys {
		if raw, ok := entry[key]; ok && raw != nil {
			addToolStrings(result, raw)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func extractResponseFilters(entry map[string]any, disabledTools map[string]bool) map[string]*gojq.Code {
	rawFilters := make(map[string]string)
	for _, key := range []string{"responseFilters", "response_filters"} {
		switch rf := entry[key].(type) {
		case map[string]any:
			for toolName, expr := range rf {
				if exprStr, ok := expr.(string); ok {
					rawFilters[strings.ToLower(strings.TrimSpace(toolName))] = exprStr
				}
			}
		case map[string]string:
			for toolName, exprStr := range rf {
				rawFilters[strings.ToLower(strings.TrimSpace(toolName))] = exprStr
			}
		}
	}
	if len(rawFilters) == 0 {
		return nil
	}
	compiled := make(map[string]*gojq.Code, len(rawFilters))
	for toolName, exprStr := range rawFilters {
		query, err := gojq.Parse(exprStr)
		if err != nil {
			log.Printf("mcp proxy: invalid jq query for tool %q (%s): %v; tool blocked to prevent unshaped leaks", toolName, exprStr, err)
			if disabledTools != nil {
				disabledTools[toolName] = true
			}
			continue
		}
		code, err := gojq.Compile(query, gojq.WithVariables([]string{"$args"}))
		if err != nil {
			log.Printf("mcp proxy: failed to compile jq query for tool %q (%s): %v; tool blocked to prevent unshaped leaks", toolName, exprStr, err)
			if disabledTools != nil {
				disabledTools[toolName] = true
			}
			continue
		}
		compiled[toolName] = code
	}
	if len(compiled) == 0 {
		return nil
	}
	return compiled
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
