package env

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/config"
)

func loadOpenObserveMCPServer(cur *config.ConfigData, mergedServers map[string]any) {
	if cur == nil || cur.OpenObserveUser == "" || cur.OpenObservePassword == "" || cur.OpenObserveURL == "" {
		return
	}
	ooURL := cur.OpenObserveURL
	ooOrg := cur.OpenObserveOrg
	if ooOrg == "" {
		ooOrg = "default"
	}
	mcpEndpoint := fmt.Sprintf("%s/api/%s/mcp", strings.TrimSuffix(ooURL, "/"), ooOrg)
	authVal := base64.StdEncoding.EncodeToString([]byte(cur.OpenObserveUser + ":" + cur.OpenObservePassword))
	mergedServers["openobserve"] = map[string]any{
		"serverUrl": mcpEndpoint,
		"headers": map[string]string{
			"Authorization": "Basic " + authVal,
		},
	}
}

func hasAuthorizationHeader(headers any) bool {
	switch h := headers.(type) {
	case map[string]string:
		for k := range h {
			if strings.EqualFold(k, "Authorization") {
				return true
			}
		}
	case map[string]any:
		for k := range h {
			if strings.EqualFold(k, "Authorization") {
				return true
			}
		}
	}
	return false
}

func injectGitHubAuthHeader(cur *config.ConfigData, mergedServers map[string]any) {
	if cur == nil || cur.GitHubPAT == "" {
		return
	}
	val, ok := mergedServers["github"]
	if !ok {
		return
	}
	srvMap, ok := val.(map[string]any)
	if !ok {
		return
	}
	headers, ok := srvMap["headers"]
	if !ok || headers == nil {
		srvMap["headers"] = map[string]string{
			"Authorization": "Bearer " + cur.GitHubPAT,
		}
		mergedServers["github"] = srvMap
		return
	}
	if hasAuthorizationHeader(headers) {
		return
	}
	switch h := headers.(type) {
	case map[string]string:
		h["Authorization"] = "Bearer " + cur.GitHubPAT
		srvMap["headers"] = h
	case map[string]any:
		h["Authorization"] = "Bearer " + cur.GitHubPAT
		srvMap["headers"] = h
	}
	mergedServers["github"] = srvMap
}

// SanitizeMCPServersForDisk scrubs credentials from MCP servers for writing to disk,
// rewriting servers with headers or secret-bearing URLs through the eri loopback proxy.
func SanitizeMCPServersForDisk(servers map[string]any, loopbackPort string) {
	port := strings.TrimPrefix(strings.TrimSpace(loopbackPort), ":")
	if port == "" || port == "0" {
		port = "8088"
	}
	for name, val := range servers {
		srvMap, ok := val.(map[string]any)
		if !ok {
			continue
		}
		needsProxy := false
		if h, ok := srvMap["headers"].(map[string]string); ok && len(h) > 0 {
			needsProxy = true
		} else if h, ok := srvMap["headers"].(map[string]any); ok && len(h) > 0 {
			needsProxy = true
		}
		if !needsProxy {
			if rawURL, ok := srvMap["serverUrl"].(string); ok && rawURL != "" && sanitizer.SanitizeString(rawURL) != rawURL {
				needsProxy = true
			} else if rawURL, ok := srvMap["server_url"].(string); ok && rawURL != "" && sanitizer.SanitizeString(rawURL) != rawURL {
				needsProxy = true
			}
		}
		if needsProxy {
			delete(srvMap, "headers")
			delete(srvMap, "server_url")
			srvMap["serverUrl"] = fmt.Sprintf("http://127.0.0.1:%s/internal/mcp/%s", port, name)
			servers[name] = srvMap
		}
	}
}

// ResolveUpstreamMCPServer builds the unredacted server map from cfg and returns the upstream URL and headers for serverName.
func ResolveUpstreamMCPServer(cfg *config.Config, serverName string) (serverURL string, headers map[string]string, ok bool) {
	if cfg == nil || serverName == "" {
		return "", nil, false
	}
	cur := cfg.Current()

	mergedServers := make(map[string]any)
	loadOpenObserveMCPServer(cur, mergedServers)

	loadFileBasedMCPServers(cur.DataDir, cur.MCPConfig, mergedServers)

	targetMaps := []map[string]json.RawMessage{
		cur.McpServers.Common,
		cur.McpServers.Discord,
		cur.McpServers.Voice,
		cur.McpServers.Ephemeral,
	}
	for _, srvMap := range targetMaps {
		for k, v := range srvMap {
			var parsedVal any
			if err := json.Unmarshal(v, &parsedVal); err == nil {
				mergedServers[k] = parsedVal
			} else {
				mergedServers[k] = v
			}
		}
	}

	injectGitHubAuthHeader(cur, mergedServers)

	val, found := mergedServers[serverName]
	if !found {
		return "", nil, false
	}

	srvMap, ok := val.(map[string]any)
	if !ok {
		return "", nil, false
	}

	var rawURL string
	if u, ok := srvMap["serverUrl"].(string); ok {
		rawURL = u
	} else if u, ok := srvMap["server_url"].(string); ok {
		rawURL = u
	}
	if rawURL == "" {
		return "", nil, false
	}

	hMap := make(map[string]string)
	if h, ok := srvMap["headers"].(map[string]string); ok {
		maps.Copy(hMap, h)
	} else if h, ok := srvMap["headers"].(map[string]any); ok {
		for k, v := range h {
			if sv, ok := v.(string); ok {
				hMap[k] = sv
			}
		}
	}

	return rawURL, hMap, true
}

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
	if cfg != nil {
		cur := cfg.Current()
		loadOpenObserveMCPServer(cur, mergedServers)
	}

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

// LoadTargetMCPConfig constructs the merged MCP configuration partitioned for target runtime.
func (p *Provisioner) LoadTargetMCPConfig(cfg *config.Config, target RuleTarget) json.RawMessage {
	mergedServers := make(map[string]any)

	if target == TargetDiscord {
		p.loadDiscordBaseMCPServers(cfg, mergedServers)
	}

	// Overlay custom MCP servers from config.yaml
	overlayConfigMCPServers(cfg, target, mergedServers)

	if cfg != nil {
		injectGitHubAuthHeader(cfg.Current(), mergedServers)
	}

	var loopbackPort string
	if cfg != nil {
		loopbackPort = cfg.Current().Port
	}
	SanitizeMCPServersForDisk(mergedServers, loopbackPort)

	finalConfig := map[string]any{
		"mcpServers": mergedServers,
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

// EnsureTargetClaudeMcpConfig writes raw JSON configuration into <claudeDir>/mcp_config.json,
// converting HTTP servers to Claude format.
func (p *Provisioner) EnsureTargetClaudeMcpConfig(claudeDir string, rawConfig json.RawMessage) error {
	if p == nil || claudeDir == "" || len(rawConfig) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == "null" {
		return nil
	}
	if err := os.MkdirAll(claudeDir, 0o755); err != nil {
		return fmt.Errorf("failed to create claude directory %s: %w", claudeDir, err)
	}
	claudeConfig := ConvertMCPConfigForClaude(rawConfig)
	targetPath := filepath.Join(claudeDir, "mcp_config.json")
	return p.writeAtomic(targetPath, string(claudeConfig))
}

// SyncMCP loads and synchronizes target-partitioned mcp_config.json into:
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
