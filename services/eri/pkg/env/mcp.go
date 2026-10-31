package env

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func loadOpenObserveMCPServer(cur *config.ConfigData, mergedServers map[string]interface{}) {
	if cur.OpenObserveUser == "" || cur.OpenObservePassword == "" {
		return
	}
	ooURL := cur.OpenObserveURL
	if ooURL == "" {
		ooURL = "http://openobserve:5080/openobserve"
	}
	ooOrg := cur.OpenObserveOrg
	if ooOrg == "" {
		ooOrg = "default"
	}
	mcpEndpoint := fmt.Sprintf("%s/api/%s/mcp", strings.TrimSuffix(ooURL, "/"), ooOrg)
	authVal := base64.StdEncoding.EncodeToString([]byte(cur.OpenObserveUser + ":" + cur.OpenObservePassword))
	mergedServers["openobserve"] = map[string]interface{}{
		"serverUrl": mcpEndpoint,
		"headers": map[string]string{
			"Authorization": "Basic " + authVal,
		},
	}
}

func loadFileBasedMCPServers(dataDir, fallbackConfig string, mergedServers map[string]interface{}) {
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
		var parsed map[string]interface{}
		if err := json.Unmarshal(rawBytes, &parsed); err == nil {
			if servers, ok := parsed["mcpServers"].(map[string]interface{}); ok {
				for k, v := range servers {
					mergedServers[k] = v
				}
			}
		}
	}
}

func (p *Provisioner) loadDiscordBaseMCPServers(cfg *config.Config, mergedServers map[string]interface{}) {
	mergedServers["discord"] = map[string]interface{}{
		"serverUrl": "http://discord-mcp:4001/mcp",
	}
	mergedServers["vanadis"] = map[string]interface{}{
		"serverUrl": "http://vanadis.aerial/mcp",
	}

	if cfg != nil {
		cur := cfg.Current()
		if cur.GitHubPAT != "" {
			mergedServers["github"] = map[string]interface{}{
				"serverUrl": "http://github-mcp:4003/mcp",
			}
		}
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

func overlayConfigMCPServers(cfg *config.Config, target RuleTarget, mergedServers map[string]interface{}) {
	if cfg == nil {
		return
	}
	cur := cfg.Current()
	targetServers := selectTargetServers(cur, target)

	for _, srvMap := range targetServers {
		for k, v := range srvMap {
			var parsedVal interface{}
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
	mergedServers := make(map[string]interface{})

	// 1. Start with built-in default MCP microservices
	// Scheduler and memory are common across all runtimes except ephemeral
	if target != TargetEphemeral {
		mergedServers["scheduler"] = map[string]interface{}{
			"serverUrl": "http://haro.aerial/mcp",
		}
		mergedServers["memory"] = map[string]interface{}{
			"serverUrl": "http://coven.aerial/mcp",
		}
	}

	if target == TargetDiscord {
		p.loadDiscordBaseMCPServers(cfg, mergedServers)
	}

	// 3. Overlay custom MCP servers from config.yaml
	overlayConfigMCPServers(cfg, target, mergedServers)

	finalConfig := map[string]interface{}{
		"mcpServers": mergedServers,
	}

	outBytes, err := json.Marshal(finalConfig)
	if err != nil {
		log.Printf("Error marshaling merged MCP config: %v", err)
		return json.RawMessage(`{"mcpServers":{}}`)
	}

	return json.RawMessage(outBytes)
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

	var js map[string]interface{}
	var serverList []string
	if err := json.Unmarshal(configContent, &js); err == nil {
		if servers, ok := js["mcpServers"].(map[string]interface{}); ok {
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
