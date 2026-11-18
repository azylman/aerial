package env

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func loadFileBasedMCPServers(dataDir, fallbackConfig string, mergedServers map[string]any) {
	configPaths := []string{
		"/share/aerial-config/mcp.config.json",
		"/share/aerial-config/mcp.json",
		"/config/mcp.config.json",
		"/config/mcp.json",
	}
	if dataDir != "" {
		configPaths = append(configPaths, filepath.Join(dataDir, "mcp.config.json"))
	}
	configPaths = append(configPaths, "./mcp.config.json")

	var rawBytes []byte
	for _, cp := range configPaths {
		if data, err := os.ReadFile(cp); err == nil && len(bytes.TrimSpace(data)) > 0 {
			log.Printf("Loaded MCP configuration from %s", cp)
			rawBytes = data
			break
		}
	}

	if len(rawBytes) == 0 && fallbackConfig != "" {
		rawBytes = []byte(fallbackConfig)
	}

	if len(rawBytes) > 0 {
		var parsed map[string]any
		if err := json.Unmarshal(rawBytes, &parsed); err == nil {
			if servers, ok := parsed["mcpServers"].(map[string]any); ok {
				maps.Copy(mergedServers, servers)
			}
		}
	}
}

func (p *Provisioner) loadDiscordBaseMCPServers(cfg *config.Config, mergedServers map[string]any) {
	dataDir := ""
	if p != nil {
		dataDir = p.dataDir
	}
	fallbackConfig := ""
	if cfg != nil {
		fallbackConfig = cfg.Current().MCPConfig
	}
	loadFileBasedMCPServers(dataDir, fallbackConfig, mergedServers)
}

func selectTargetServers(cur *config.ConfigData, target RuleTarget) []map[string]json.RawMessage {
	if target == TargetEphemeral {
		if cur.McpServers.Ephemeral != nil {
			return []map[string]json.RawMessage{cur.McpServers.Ephemeral}
		}
		return nil
	}

	var targetServers []map[string]json.RawMessage
	if cur.McpServers.Common != nil {
		targetServers = append(targetServers, cur.McpServers.Common)
	}
	switch target {
	case TargetDiscord:
		if cur.McpServers.Discord != nil {
			targetServers = append(targetServers, cur.McpServers.Discord)
		}
	case TargetVoice:
		if cur.McpServers.Voice != nil {
			targetServers = append(targetServers, cur.McpServers.Voice)
		}
	case TargetEphemeral:
	}
	return targetServers
}

func overlayConfigMCPServers(cfg *config.Config, target RuleTarget, mergedServers map[string]any) {
	if cfg == nil {
		return
	}
	cur := cfg.Current()
	targetServers := selectTargetServers(cur, target)

	for _, srvMap := range targetServers {
		for k, v := range srvMap {
			var parsedVal any
			if err := json.Unmarshal(v, &parsedVal); err == nil {
				mergedServers[k] = parsedVal
			} else {
				mergedServers[k] = v
			}
		}
	}
}

// StripProxyPolicyFields removes loopback proxy policy configuration fields from servers
// so clients like agy do not receive unknown proxy-only keys.
func StripProxyPolicyFields(servers map[string]any) {
	for name, v := range servers {
		entry, ok := v.(map[string]any)
		if !ok {
			continue
		}
		delete(entry, "instructions")
		delete(entry, "enabledTools")
		delete(entry, "enabled_tools")
		delete(entry, "disabledTools")
		delete(entry, "disabled_tools")
		delete(entry, "responseFilters")
		delete(entry, "response_filters")
		servers[name] = entry
	}
}

// LoadTargetMCPConfig constructs the merged MCP configuration partitioned for target runtime.
// Headers are emitted verbatim with the credentials declared in config. Server URLs are too,
// unless MCP tracing is enabled (WithMCPProxy): then HTTP servers point at eri's loopback
// proxy, which forwards to the original URL.
func (p *Provisioner) LoadTargetMCPConfig(cfg *config.Config, target RuleTarget) json.RawMessage {
	mergedServers := make(map[string]any)

	if target == TargetDiscord {
		p.loadDiscordBaseMCPServers(cfg, mergedServers)
	}

	// Overlay custom MCP servers from config.yaml
	overlayConfigMCPServers(cfg, target, mergedServers)
	if p != nil {
		p.mcpProxy.rewrite(target, mergedServers)
	}
	StripProxyPolicyFields(mergedServers)

	finalConfig := map[string]any{
		mcpKeyMCPServers: mergedServers,
	}

	outBytes, err := json.Marshal(finalConfig)
	if err != nil {
		log.Printf("Error marshaling merged MCP config: %v", err)
		return json.RawMessage(`{"mcpServers":{}}`)
	}

	return json.RawMessage(outBytes)
}

// ConvertMCPConfigForClaude transforms an agy MCP config JSON message into Claude Code mcp_config.json format.
// Specifically, under "mcpServers", entries with "serverUrl" are transformed to {"type": "http", "url": serverUrl}
// while preserving stdio servers (command, args, env) and any existing fields.
func ConvertMCPConfigForClaude(rawConfig json.RawMessage) json.RawMessage {
	if len(rawConfig) == 0 {
		return json.RawMessage(`{"mcpServers":{}}`)
	}
	var root map[string]any
	if err := json.Unmarshal(rawConfig, &root); err != nil {
		return rawConfig
	}
	servers, ok := root["mcpServers"].(map[string]any)
	if !ok {
		return rawConfig
	}
	newServers := make(map[string]any, len(servers))
	for name, srvVal := range servers {
		srvMap, ok := srvVal.(map[string]any)
		if !ok {
			newServers[name] = srvVal
			continue
		}
		newMap := make(map[string]any, len(srvMap))
		maps.Copy(newMap, srvMap)
		delete(newMap, "disabledTools")
		delete(newMap, "disabled_tools")
		delete(newMap, "enabledTools")
		delete(newMap, "enabled_tools")
		delete(newMap, "instructions")
		delete(newMap, "responseFilters")
		delete(newMap, "response_filters")
		var serverURL string
		if u, ok := newMap["serverUrl"].(string); ok && u != "" {
			serverURL = u
		} else if u, ok := newMap["server_url"].(string); ok && u != "" {
			serverURL = u
		}
		if serverURL != "" {
			delete(newMap, "serverUrl")
			delete(newMap, "server_url")
			newMap["type"] = "http"
			newMap["url"] = serverURL
		}
		newServers[name] = newMap
	}
	root["mcpServers"] = newServers
	outBytes, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return rawConfig
	}
	return json.RawMessage(outBytes)
}

const (
	nullLiteral = "null"
)

// EnsureTargetClaudeMcpConfig writes raw JSON configuration into <claudeDir>/mcp_config.json,
// converting HTTP servers to Claude format.
func (p *Provisioner) EnsureTargetClaudeMcpConfig(claudeDir string, rawConfig json.RawMessage) error {
	if p == nil || claudeDir == "" || len(rawConfig) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == nullLiteral {
		return nil
	}
	if err := os.MkdirAll(claudeDir, 0o755); err != nil {
		return fmt.Errorf("failed to create claude directory %s: %w", claudeDir, err)
	}
	claudeConfig := p.mcpProxy.withDaemonHeader(ConvertMCPConfigForClaude(rawConfig))
	targetPath := filepath.Join(claudeDir, "mcp_config.json")
	return p.writeAtomic(targetPath, string(claudeConfig))
}

func formatCodexServerURL(b *strings.Builder, serverURL string, srvMap map[string]any, proxyBase string) {
	fmt.Fprintf(b, "url = %q\n", serverURL)

	var headers map[string]any
	if h, ok := srvMap["http_headers"].(map[string]any); ok {
		headers = h
	} else if h, ok := srvMap["headers"].(map[string]any); ok {
		headers = h
	}

	if len(headers) > 0 {
		hKeys := make([]string, 0, len(headers))
		for k := range headers {
			hKeys = append(hKeys, k)
		}
		sort.Strings(hKeys)
		pairs := make([]string, 0, len(hKeys))
		for _, k := range hKeys {
			pairs = append(pairs, fmt.Sprintf("%q = %q", k, fmt.Sprint(headers[k])))
		}
		fmt.Fprintf(b, "http_headers = { %s }\n", strings.Join(pairs, ", "))
	}

	if proxyBase != "" && strings.HasPrefix(serverURL, proxyBase) {
		b.WriteString("env_http_headers = { \"X-Eri-Daemon\" = \"ERI_DAEMON_ID\" }\n")
	}
}

func isBareTOMLKey(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func formatTOMLKey(key string) string {
	if isBareTOMLKey(key) {
		return key
	}
	return fmt.Sprintf("%q", key)
}

func formatCodexServerCommand(b *strings.Builder, name string, srvMap map[string]any) {
	cmd := fmt.Sprint(srvMap["command"])
	if srvMap["command"] == nil {
		cmd = ""
	}
	fmt.Fprintf(b, "command = %q\n", cmd)

	var argsList []string
	if argsSlice, ok := srvMap["args"].([]any); ok {
		for _, a := range argsSlice {
			argsList = append(argsList, fmt.Sprintf("%q", fmt.Sprint(a)))
		}
	}
	fmt.Fprintf(b, "args = [%s]\n", strings.Join(argsList, ", "))

	envMap, ok := srvMap["env"].(map[string]any)
	if !ok || len(envMap) == 0 {
		return
	}
	eKeys := make([]string, 0, len(envMap))
	for k := range envMap {
		eKeys = append(eKeys, k)
	}
	sort.Strings(eKeys)

	fmt.Fprintf(b, "\n[mcp_servers.%s.env]\n", formatTOMLKey(name))
	for _, k := range eKeys {
		fmt.Fprintf(b, "%s = %q\n", formatTOMLKey(k), fmt.Sprint(envMap[k]))
	}
}

// FormatCodexConfigTOML transforms an agy/claude MCP config JSON message into Codex config.toml format.
func FormatCodexConfigTOML(rawConfig json.RawMessage, proxyBase string, target ...string) string {
	var b strings.Builder
	b.WriteString("[features]\nhooks = true\n")

	if pb := strings.TrimSpace(proxyBase); pb != "" {
		endpoint := deriveCodexOTelEndpoint(rawConfig, pb, target...)
		b.WriteString("\n[otel]\n")
		b.WriteString("log_user_prompt = false\n")
		b.WriteString("log_agent_responses = false\n")
		b.WriteString("log_sse_deltas = false\n")
		b.WriteString("log_guardian_assessments = false\n")
		b.WriteString("tool_result = { max_bytes = 0 }\n")
		fmt.Fprintf(&b, "exporter = { otlp-http = { endpoint = %q, protocol = \"json\" } }\n", endpoint)
	}

	if len(rawConfig) == 0 {
		return b.String()
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == nullLiteral {
		return b.String()
	}

	configBytes := rawConfig
	var strVal string
	if err := json.Unmarshal(rawConfig, &strVal); err == nil && strVal != "" {
		configBytes = []byte(strVal)
	}

	var root map[string]any
	if err := json.Unmarshal(configBytes, &root); err != nil {
		return b.String()
	}

	servers, ok := root["mcpServers"].(map[string]any)
	if !ok || len(servers) == 0 {
		return b.String()
	}

	names := make([]string, 0, len(servers))
	for name := range servers {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		srvMap, ok := servers[name].(map[string]any)
		if !ok {
			continue
		}

		var serverURL string
		for _, k := range mcpURLKeys {
			if u, ok := srvMap[k].(string); ok && strings.TrimSpace(u) != "" {
				serverURL = strings.TrimSpace(u)
				break
			}
		}

		fmt.Fprintf(&b, "\n[mcp_servers.%s]\n", formatTOMLKey(name))
		if serverURL != "" {
			formatCodexServerURL(&b, serverURL, srvMap, proxyBase)
		} else {
			formatCodexServerCommand(&b, name, srvMap)
		}
	}

	return b.String()
}

func deriveCodexOTelEndpoint(rawConfig json.RawMessage, proxyBase string, target ...string) string {
	pb := strings.TrimSpace(proxyBase)
	if pb == "" {
		return ""
	}
	origin := extractProxyOrigin(pb)

	var targetSegment string
	if len(target) > 0 && strings.TrimSpace(target[0]) != "" {
		targetSegment = strings.TrimSpace(target[0])
	}
	if targetSegment == "" {
		trimmedPB := strings.TrimRight(pb, "/")
		for _, known := range []string{"discord", "voice", "ephemeral"} {
			if strings.HasSuffix(trimmedPB, "/"+known) {
				targetSegment = known
				break
			}
		}
	}
	if targetSegment == "" {
		targetSegment = extractTargetFromRawMCPConfig(rawConfig)
	}

	endpoint := origin + OTelLogsPathPrefix
	if targetSegment != "" {
		endpoint += "/" + url.PathEscape(targetSegment)
	}
	return endpoint
}

func extractProxyOrigin(pb string) string {
	if u, err := url.Parse(pb); err == nil && u.Scheme != "" && u.Host != "" {
		return u.Scheme + "://" + u.Host
	}
	return strings.TrimRight(pb, "/")
}

func extractTargetFromRawMCPConfig(rawConfig json.RawMessage) string {
	servers := parseRawMCPServers(rawConfig)
	for _, srv := range servers {
		srvMap, ok := srv.(map[string]any)
		if !ok {
			continue
		}
		for _, k := range mcpURLKeys {
			u, ok := srvMap[k].(string)
			if !ok || u == "" {
				continue
			}
			idx := strings.Index(u, MCPProxyPathPrefix)
			if idx == -1 {
				continue
			}
			rest := strings.TrimPrefix(u[idx:], MCPProxyPathPrefix)
			parts := strings.Split(rest, "/")
			if len(parts) > 0 && parts[0] != "" {
				return parts[0]
			}
		}
	}
	return ""
}

const (
	mcpKeyType       = "type"
	mcpKeyCommand    = "command"
	mcpKeyHooks      = "hooks"
	mcpKeyMCPServers = "mcpServers"
)

func (p *Provisioner) writeTargetConfigFile(targetDir, fileName string, rawConfig json.RawMessage, formatFn func(raw json.RawMessage, proxyBase string) string) error {
	if p == nil || targetDir == "" || len(rawConfig) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == nullLiteral {
		return nil
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", targetDir, err)
	}
	var proxyBase string
	if p.mcpProxy != nil {
		proxyBase = p.mcpProxy.proxyBase()
	}
	content := formatFn(rawConfig, proxyBase)
	return p.writeAtomic(filepath.Join(targetDir, fileName), content)
}

// EnsureTargetCodexConfigTOML writes TOML configuration into <codexDir>/config.toml.
func (p *Provisioner) EnsureTargetCodexConfigTOML(codexDir string, rawConfig json.RawMessage) error {
	if p == nil || codexDir == "" {
		return nil
	}
	var proxyBase string
	if p.mcpProxy != nil {
		proxyBase = p.mcpProxy.proxyBase()
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if (len(rawConfig) == 0 || trimmed == "" || trimmed == `""` || trimmed == nullLiteral) && proxyBase == "" {
		return nil
	}
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		return fmt.Errorf("failed to create codex directory %s: %w", codexDir, err)
	}
	var target string
	base := filepath.Base(filepath.Dir(codexDir))
	if base == string(TargetDiscord) || base == string(TargetVoice) || base == string(TargetEphemeral) {
		target = base
	}
	content := FormatCodexConfigTOML(rawConfig, proxyBase, target)
	return p.writeAtomic(filepath.Join(codexDir, "config.toml"), content)
}

func extractMCPHeaders(srvMap map[string]any, serverURL, proxyBase string) map[string]string {
	headers := make(map[string]string)
	if h, ok := srvMap["http_headers"].(map[string]any); ok {
		for k, v := range h {
			headers[k] = fmt.Sprint(v)
		}
	} else if h, ok := srvMap["headers"].(map[string]any); ok {
		for k, v := range h {
			headers[k] = fmt.Sprint(v)
		}
	}
	if proxyBase != "" && strings.HasPrefix(serverURL, proxyBase) {
		headers["X-Eri-Daemon"] = "${ERI_DAEMON_ID:-}"
	}
	return headers
}

func extractMCPStdio(srvMap map[string]any) (string, []string, map[string]string) {
	var cmd string
	if srvMap[mcpKeyCommand] != nil {
		cmd = fmt.Sprint(srvMap[mcpKeyCommand])
	}
	args := make([]string, 0)
	if argsSlice, ok := srvMap["args"].([]any); ok {
		for _, a := range argsSlice {
			args = append(args, fmt.Sprint(a))
		}
	}
	env := make(map[string]string)
	if envMap, ok := srvMap["env"].(map[string]any); ok {
		for k, v := range envMap {
			env[k] = fmt.Sprint(v)
		}
	}
	return cmd, args, env
}

func formatMuseServerEntry(srvMap map[string]any, proxyBase string) map[string]any {
	var serverURL string
	for _, k := range mcpURLKeys {
		if u, ok := srvMap[k].(string); ok && strings.TrimSpace(u) != "" {
			serverURL = strings.TrimSpace(u)
			break
		}
	}
	if serverURL != "" {
		return map[string]any{
			mcpKeyType: "streamable-http",
			"url":      serverURL,
			"headers":  extractMCPHeaders(srvMap, serverURL, proxyBase),
		}
	}
	cmd, args, env := extractMCPStdio(srvMap)
	return map[string]any{
		mcpKeyType:    "stdio",
		mcpKeyCommand: cmd,
		"args":        args,
		"env":         env,
	}
}

func parseRawMCPServers(rawConfig json.RawMessage) map[string]any {
	if len(rawConfig) == 0 {
		return nil
	}
	configBytes := rawConfig
	var strVal string
	if err := json.Unmarshal(rawConfig, &strVal); err == nil && strVal != "" {
		configBytes = []byte(strVal)
	}
	var root map[string]any
	if err := json.Unmarshal(configBytes, &root); err != nil || root == nil {
		return nil
	}
	rawServers, ok := root[mcpKeyMCPServers].(map[string]any)
	if !ok {
		return nil
	}
	return rawServers
}

// FormatMuseSettingsJSON transforms an agy/claude MCP config JSON message into Muse settings.json format.
func FormatMuseSettingsJSON(rawConfig json.RawMessage, proxyBase string) string {
	rawServers := parseRawMCPServers(rawConfig)
	serversMap := make(map[string]any)

	names := make([]string, 0, len(rawServers))
	for name := range rawServers {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		srvMap, ok := rawServers[name].(map[string]any)
		if !ok {
			continue
		}
		serversMap[name] = formatMuseServerEntry(srvMap, proxyBase)
	}

	settings := map[string]any{
		"schema_version": 1,
		mcpKeyMCPServers: serversMap,
		mcpKeyHooks:      defaultMuseHooks(),
	}

	outBytes, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(outBytes)
}

// EnsureTargetMuseSettingsJSON writes JSON configuration into <museConfigDir>/settings.json.
func (p *Provisioner) EnsureTargetMuseSettingsJSON(museConfigDir string, rawConfig json.RawMessage) error {
	return p.writeTargetConfigFile(museConfigDir, "settings.json", rawConfig, FormatMuseSettingsJSON)
}

// SyncMCP writes target-partitioned agy and Claude Code mcp_config.json files, carrying each
// server's upstream URL and headers as declared, into:
// 1. Primary ~/.gemini/config/mcp_config.json
// 2. Discord runtime <dataDir>/runtimes/discord/.gemini/config/mcp_config.json
// 3. Voice runtime <dataDir>/runtimes/voice/.gemini/config/mcp_config.json
// 4. Ephemeral runtime <dataDir>/runtimes/ephemeral/.gemini/config/mcp_config.json
func (p *Provisioner) SyncMCP(ctx context.Context, cfg *config.Config) error {
	if p == nil {
		return nil
	}

	discordConfig := p.LoadTargetMCPConfig(cfg, TargetDiscord)
	voiceConfig := p.LoadTargetMCPConfig(cfg, TargetVoice)
	ephemeralConfig := p.LoadTargetMCPConfig(cfg, TargetEphemeral)

	if p.homeDir != "" {
		if err := p.EnsureTargetMcpConfig(filepath.Join(p.homeDir, ".gemini"), discordConfig); err != nil {
			return fmt.Errorf("failed to sync primary mcp config: %w", err)
		}
		if err := p.EnsureTargetClaudeMcpConfig(filepath.Join(p.homeDir, ".claude"), discordConfig); err != nil {
			return fmt.Errorf("failed to sync primary claude mcp config: %w", err)
		}
		if err := p.EnsureTargetCodexConfigTOML(filepath.Join(p.homeDir, ".codex"), discordConfig); err != nil {
			return fmt.Errorf("failed to sync primary codex config: %w", err)
		}
		if err := p.EnsureTargetMuseSettingsJSON(filepath.Join(p.homeDir, ".config", "muse"), discordConfig); err != nil {
			return fmt.Errorf("failed to sync primary muse config: %w", err)
		}
	}

	if p.dataDir != "" {
		configs := map[RuleTarget]json.RawMessage{
			TargetDiscord:   discordConfig,
			TargetVoice:     voiceConfig,
			TargetEphemeral: ephemeralConfig,
		}
		for _, target := range allRuntimeTargets {
			geminiDir := filepath.Join(p.runtimeHomeDir(target), ".gemini")
			if err := p.EnsureTargetMcpConfig(geminiDir, configs[target]); err != nil {
				return fmt.Errorf("failed to sync %s runtime mcp config: %w", target, err)
			}
			claudeDir := filepath.Join(p.runtimeHomeDir(target), ".claude")
			if err := p.EnsureTargetClaudeMcpConfig(claudeDir, configs[target]); err != nil {
				return fmt.Errorf("failed to sync %s runtime claude mcp config: %w", target, err)
			}
			codexDir := filepath.Join(p.runtimeHomeDir(target), ".codex")
			if err := p.EnsureTargetCodexConfigTOML(codexDir, configs[target]); err != nil {
				return fmt.Errorf("failed to sync %s runtime codex config: %w", target, err)
			}
			museDir := filepath.Join(p.runtimeHomeDir(target), ".config", "muse")
			if err := p.EnsureTargetMuseSettingsJSON(museDir, configs[target]); err != nil {
				return fmt.Errorf("failed to sync %s runtime muse config: %w", target, err)
			}
		}
	}

	return nil
}

// EnsureTargetMcpConfig writes raw JSON configuration into <targetGeminiDir>/config/mcp_config.json.
func (p *Provisioner) EnsureTargetMcpConfig(targetGeminiDir string, rawConfig json.RawMessage) error {
	if p == nil || targetGeminiDir == "" {
		return nil
	}
	if len(rawConfig) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == "null" {
		return nil
	}

	configDir := filepath.Join(targetGeminiDir, "config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return fmt.Errorf("failed to create config directory %s: %w", configDir, err)
	}
	targetPath := filepath.Join(configDir, "mcp_config.json")

	var configContent []byte
	var strVal string
	if err := json.Unmarshal(rawConfig, &strVal); err == nil && strVal != "" {
		configContent = []byte(strVal)
	} else {
		configContent = rawConfig
	}

	var js map[string]any
	var serverList []string
	if err := json.Unmarshal(configContent, &js); err == nil {
		if servers, ok := js["mcpServers"].(map[string]any); ok {
			for name := range servers {
				serverList = append(serverList, name)
			}
		}
		if formatted, err := json.MarshalIndent(js, "", "  "); err == nil {
			configContent = formatted
		}
	}

	if err := p.writeAtomic(targetPath, string(configContent)); err != nil {
		log.Printf("Failed to write %s: %v", targetPath, err)
		return fmt.Errorf("failed to write mcp config: %w", err)
	}
	log.Printf("Configured %d MCP server(s) in %s: %v", len(serverList), targetPath, serverList)
	return nil
}
