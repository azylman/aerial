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

	"github.com/azylman/aerial/brain/pkg/config"
)

// LoadTargetMCPConfig constructs the merged MCP configuration partitioned for target runtime.
func (p *Provisioner) LoadTargetMCPConfig(cfg *config.Config, target RuleTarget) json.RawMessage {
	mergedServers := make(map[string]interface{})

	// 1. Start with built-in default MCP microservices
	// Scheduler is common across all runtimes except ephemeral
	if target != TargetEphemeral {
		mergedServers["scheduler"] = map[string]interface{}{
			"serverUrl": "http://scheduler-mcp:8080/mcp",
		}
	}

	if target == TargetDiscord {
		mergedServers["discord"] = map[string]interface{}{
			"serverUrl": "http://discord-mcp:4001/mcp",
		}
		mergedServers["docker"] = map[string]interface{}{
			"serverUrl": "http://docker-mcp:4002/mcp",
		}
		mergedServers["victoriametrics"] = map[string]interface{}{
			"serverUrl": "http://victoriametrics-mcp:4004/mcp",
		}

		var gitHubPAT string
		if cfg != nil && cfg.Current().GitHubPAT != "" {
			gitHubPAT = cfg.Current().GitHubPAT
		}
		if gitHubPAT != "" {
			mergedServers["github"] = map[string]interface{}{
				"serverUrl": "http://github-mcp:4003/mcp",
			}
		}

		if cfg != nil {
			cur := cfg.Current()
			if cur.OpenObserveUser != "" && cur.OpenObservePassword != "" {
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
		}

		// 2. Check for file-based overrides (applied to Discord target only)
		configPaths := []string{
			"/share/aerial-config/mcp.config.json",
			"/share/aerial-config/mcp.json",
			"/config/mcp.config.json",
			"/config/mcp.json",
		}
		if p != nil && p.dataDir != "" {
			configPaths = append(configPaths, filepath.Join(p.dataDir, "mcp.config.json"))
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

		if len(rawBytes) == 0 && cfg != nil && cfg.Current().MCPConfig != "" {
			rawBytes = []byte(cfg.Current().MCPConfig)
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

	// 3. Overlay custom MCP servers from config.yaml
	if cfg != nil {
		cur := cfg.Current()
		var targetServers []map[string]json.RawMessage
		if target == TargetEphemeral {
			if cur.McpServers.Ephemeral != nil {
				targetServers = append(targetServers, cur.McpServers.Ephemeral)
			}
		} else {
			if cur.McpServers.Common != nil {
				targetServers = append(targetServers, cur.McpServers.Common)
			}
			if target == TargetDiscord {
				if cur.McpServers.Discord != nil {
					targetServers = append(targetServers, cur.McpServers.Discord)
				}
			} else if target == TargetVoice {
				if cur.McpServers.Voice != nil {
					targetServers = append(targetServers, cur.McpServers.Voice)
				}
			}
		}

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

// LoadMCPConfig constructs the merged MCP configuration (defaulting to Discord target).
func (p *Provisioner) LoadMCPConfig(cfg *config.Config) json.RawMessage {
	return p.LoadTargetMCPConfig(cfg, TargetDiscord)
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
		discordGemini := filepath.Join(p.dataDir, "runtimes", "discord", ".gemini")
		if err := p.EnsureTargetMcpConfig(discordGemini, discordConfig); err != nil {
			return fmt.Errorf("failed to sync discord runtime mcp config: %w", err)
		}

		voiceGemini := filepath.Join(p.dataDir, "runtimes", "voice", ".gemini")
		if err := p.EnsureTargetMcpConfig(voiceGemini, voiceConfig); err != nil {
			return fmt.Errorf("failed to sync voice runtime mcp config: %w", err)
		}

		ephemeralGemini := filepath.Join(p.dataDir, "runtimes", "ephemeral", ".gemini")
		if err := p.EnsureTargetMcpConfig(ephemeralGemini, ephemeralConfig); err != nil {
			return fmt.Errorf("failed to sync ephemeral runtime mcp config: %w", err)
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
	if err := os.MkdirAll(configDir, 0755); err != nil {
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

// EnsureMcpConfig writes raw JSON configuration into ~/.gemini/config/mcp_config.json.
func (p *Provisioner) EnsureMcpConfig(rawConfig json.RawMessage) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	return p.EnsureTargetMcpConfig(filepath.Join(p.homeDir, ".gemini"), rawConfig)
}
