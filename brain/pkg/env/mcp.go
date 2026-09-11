package env

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/brain/pkg/config"
)

// LoadMCPConfig constructs the merged MCP configuration for microservices and user overrides.
func (p *Provisioner) LoadMCPConfig(cfg *config.Config) json.RawMessage {
	// 1. Start with built-in default MCP microservices
	mergedServers := map[string]interface{}{
		"scheduler": map[string]interface{}{
			"serverUrl": "http://scheduler-mcp:8080/mcp",
		},
		"discord": map[string]interface{}{
			"serverUrl": "http://discord-mcp:4001/mcp",
		},
		"docker": map[string]interface{}{
			"serverUrl": "http://docker-mcp:4002/mcp",
		},
		"victoriametrics": map[string]interface{}{
			"serverUrl": "http://victoriametrics-mcp:4004/mcp",
		},
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

	// 2. Check for file-based overrides (e.g. /share/aerial-config/mcp.config.json)
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

	if len(rawBytes) == 0 && p != nil && p.dataDir != "" {
		optionsPath := filepath.Join(p.dataDir, "options.json")
		if data, err := os.ReadFile(optionsPath); err == nil {
			var opts struct {
				McpConfig json.RawMessage `json:"mcp_config"`
			}
			if err := json.Unmarshal(data, &opts); err == nil && len(opts.McpConfig) > 0 {
				var strVal string
				if err := json.Unmarshal(opts.McpConfig, &strVal); err == nil && strVal != "" {
					rawBytes = []byte(strVal)
				} else {
					rawBytes = opts.McpConfig
				}
			}
		}
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

	// 3. Overlay custom MCP servers from config.yaml
	if cfg != nil {
		cur := cfg.Current()
		if len(cur.McpServers) > 0 {
			for k, v := range cur.McpServers {
				var parsedVal interface{}
				if err := json.Unmarshal(v, &parsedVal); err == nil {
					mergedServers[k] = parsedVal
				} else {
					mergedServers[k] = v
				}
			}
		}
	}

	// 4. Normalize legacy SSE endpoints to Streamable HTTP
	for _, svc := range []string{"docker", "github", "victoriametrics"} {
		if rawSvc, ok := mergedServers[svc].(map[string]interface{}); ok {
			if url, ok := rawSvc["serverUrl"].(string); ok {
				if strings.HasSuffix(url, "/sse") {
					rawSvc["serverUrl"] = strings.TrimSuffix(url, "/sse") + "/mcp"
					log.Printf("[LoadMCPConfig] Transparently normalized %s serverUrl from /sse to /mcp", svc)
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

// SyncMCP loads and synchronizes mcp_config.json into ~/.gemini/config/mcp_config.json.
func (p *Provisioner) SyncMCP(ctx context.Context, cfg *config.Config) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	rawConfig := p.LoadMCPConfig(cfg)
	return p.EnsureMcpConfig(rawConfig)
}

// EnsureMcpConfig writes raw JSON configuration into ~/.gemini/config/mcp_config.json.
func (p *Provisioner) EnsureMcpConfig(rawConfig json.RawMessage) error {
	if p == nil || p.homeDir == "" {
		return nil
	}
	if len(rawConfig) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == "null" {
		return nil
	}

	configDir := filepath.Join(p.homeDir, ".gemini", "config")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
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
