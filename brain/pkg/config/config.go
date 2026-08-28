package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Options struct {
	Port           int             `json:"port"`
	AgyBin         string          `json:"agy_bin"`
	ApiKey         string          `json:"api_key"`
	Model          string          `json:"model"`
	SystemPrompt   string          `json:"system_prompt"`
	TimeoutMinutes int             `json:"timeout_minutes"`
	McpConfig      json.RawMessage `json:"mcp_config"`
}

func GetEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func EnsureAgySettings(apiKey, model string) error {
	if apiKey == "" {
		return nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	configDir := filepath.Join(homeDir, ".gemini", "antigravity-cli")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	settingsPath := filepath.Join(configDir, "settings.json")
	settings := map[string]interface{}{
		"modelProvider": "gemini",
		"model":         model,
	}
	if data, err := os.ReadFile(settingsPath); err == nil {
		if err := json.Unmarshal(data, &settings); err != nil {
			log.Printf("Warning: failed to unmarshal existing settings from %s: %v", settingsPath, err)
		}
		settings["modelProvider"] = "gemini"
		if model != "" {
			settings["model"] = model
		}
	}
	out, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}
	if err := os.WriteFile(settingsPath, out, 0644); err != nil {
		return fmt.Errorf("failed to write settings: %w", err)
	}
	return nil
}

func EnsureSystemRules(customPrompt string) error {
	var sb strings.Builder
	sb.WriteString("---\ndescription: User custom instructions, persona, and system guidelines\ntrigger: always_on\n---\n\n")

	foundInstructions := false

	// Check instruction files on disk in priority order
	searchPaths := []string{
		"/share/aerial/AGENTS.local.md",
		"/share/aerial/AGENTS.md",
		"/app/AGENTS.local.md",
		"/app/AGENTS.md",
		"/data/AGENTS.md",
		"./AGENTS.local.md",
		"./AGENTS.md",
	}

	for _, p := range searchPaths {
		if data, err := os.ReadFile(p); err == nil && len(bytes.TrimSpace(data)) > 0 {
			sb.WriteString(fmt.Sprintf("# Instructions from %s\n\n%s\n\n", filepath.Base(p), string(data)))
			foundInstructions = true
			log.Printf("Loaded agent instructions from %s", p)
			break
		}
	}

	// System guidelines (SYSTEM.md)
	systemPaths := []string{
		"/share/aerial/SYSTEM.md",
		"/app/SYSTEM.md",
		"./SYSTEM.md",
	}
	for _, p := range systemPaths {
		if data, err := os.ReadFile(p); err == nil && len(bytes.TrimSpace(data)) > 0 {
			sb.WriteString(fmt.Sprintf("# Base System Guidelines (%s)\n\n%s\n\n", filepath.Base(p), string(data)))
			foundInstructions = true
			log.Printf("Loaded base system guidelines from %s", p)
			break
		}
	}

	if strings.TrimSpace(customPrompt) != "" {
		sb.WriteString(fmt.Sprintf("# Environment Prompt Override\n\n%s\n\n", strings.TrimSpace(customPrompt)))
		foundInstructions = true
	}

	if !foundInstructions {
		return nil
	}

	content := sb.String()
	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		homeDir = "/root"
	}

	primaryRulesDir := filepath.Join(homeDir, ".gemini", "rules")
	if err := os.MkdirAll(primaryRulesDir, 0755); err != nil {
		return fmt.Errorf("failed to create primary rules directory: %w", err)
	}

	// Clean up any legacy or conflicting rule files across rules directories
	staleRuleFiles := []string{
		filepath.Join(primaryRulesDir, "agents.md"),
		filepath.Join(primaryRulesDir, "custom_instructions.md"),
		filepath.Join(primaryRulesDir, "system.md"),
		filepath.Join(homeDir, ".gemini", "config", "rules", "custom_instructions.md"),
		filepath.Join(homeDir, ".gemini", "config", "rules", "agents.md"),
		filepath.Join(homeDir, ".gemini", "config", "rules", "system.md"),
	}
	for _, stale := range staleRuleFiles {
		_ = os.Remove(stale)
	}

	primaryRuleFile := filepath.Join(primaryRulesDir, "system_instructions.md")
	if err := os.WriteFile(primaryRuleFile, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write primary system rules: %w", err)
	}
	log.Printf("Configured always_on system instructions in %s", primaryRuleFile)

	additionalDirs := []string{
		filepath.Join(homeDir, ".gemini", "config", "rules"),
		"/share/aerial/.agents/rules",
		"/app/.agents/rules",
	}

	for _, dir := range additionalDirs {
		if err := os.MkdirAll(dir, 0755); err == nil {
			_ = os.WriteFile(filepath.Join(dir, "system_instructions.md"), []byte(content), 0644)
		}
	}

	return nil
}

func LoadMCPConfig() json.RawMessage {
	configPaths := []string{
		"/config/mcp.config.json",
		"/config/mcp.json",
		"/data/mcp.config.json",
		"./mcp.config.json",
	}

	var rawBytes []byte
	for _, p := range configPaths {
		if data, err := os.ReadFile(p); err == nil && len(bytes.TrimSpace(data)) > 0 {
			log.Printf("Loaded MCP configuration from %s", p)
			rawBytes = data
			break
		}
	}

	if len(rawBytes) == 0 {
		if envVal := os.Getenv("MCP_CONFIG"); envVal != "" {
			rawBytes = []byte(envVal)
		}
	}

	if len(rawBytes) == 0 {
		if data, err := os.ReadFile("/data/options.json"); err == nil {
			var opts Options
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

	if len(rawBytes) == 0 {
		defaultConfig := map[string]interface{}{
			"mcpServers": map[string]interface{}{
				"discord": map[string]interface{}{
					"serverUrl": "http://discord-mcp:4001/mcp",
				},
				"docker": map[string]interface{}{
					"serverUrl": "http://docker-mcp:4002/sse",
				},
			},
		}
		if mcpServers, ok := defaultConfig["mcpServers"].(map[string]interface{}); ok {
			if pat := os.Getenv("GITHUB_PAT"); pat != "" {
				mcpServers["github"] = map[string]interface{}{
					"serverUrl": "http://github-mcp:4003/sse",
				}
			}
			if haToken := os.Getenv("HA_TOKEN"); haToken != "" {
				mcpServers["ha-mcp"] = map[string]interface{}{
					"serverUrl": haToken,
				}
			}
		}
		if b, err := json.Marshal(defaultConfig); err == nil {
			rawBytes = b
		}
	}

	expanded := os.ExpandEnv(string(rawBytes))
	return json.RawMessage(expanded)
}

func EnsureMcpConfig(rawConfig json.RawMessage) error {
	if len(rawConfig) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(string(rawConfig))
	if trimmed == "" || trimmed == `""` || trimmed == "null" {
		return nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	configDir := filepath.Join(homeDir, ".gemini", "config")
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

	if err := os.WriteFile(targetPath, configContent, 0644); err != nil {
		log.Printf("Failed to write %s: %v", targetPath, err)
		return fmt.Errorf("failed to write mcp config: %w", err)
	}
	log.Printf("Configured %d MCP server(s) in %s: %v", len(serverList), targetPath, serverList)
	return nil
}

func LoadConfig() (string, string, string, string, string, int, json.RawMessage) {
	port := GetEnv("PORT", "8080")
	agyBin := GetEnv("AGY_BIN", "agy")
	apiKey := GetEnv("GEMINI_API_KEY", GetEnv("ANTIGRAVITY_API_KEY", ""))
	model := GetEnv("AGY_MODEL", "Gemini 3.6 Flash (Low)")
	systemPrompt := GetEnv("SYSTEM_PROMPT", "")
	timeoutMinutes := 15
	if tm := os.Getenv("TIMEOUT_MINUTES"); tm != "" {
		if val, err := strconv.Atoi(tm); err == nil && val > 0 {
			timeoutMinutes = val
		}
	}
	var mcpConfig json.RawMessage

	if data, err := os.ReadFile("/data/options.json"); err == nil {
		var opts Options
		if err := json.Unmarshal(data, &opts); err == nil {
			if opts.Port != 0 {
				port = fmt.Sprintf("%d", opts.Port)
			}
			if strings.TrimSpace(opts.AgyBin) != "" {
				agyBin = opts.AgyBin
			}
			if strings.TrimSpace(opts.ApiKey) != "" {
				apiKey = opts.ApiKey
			}
			if strings.TrimSpace(opts.Model) != "" {
				model = opts.Model
			}
			if strings.TrimSpace(opts.SystemPrompt) != "" {
				systemPrompt = opts.SystemPrompt
			}
			if opts.TimeoutMinutes > 0 {
				timeoutMinutes = opts.TimeoutMinutes
			}
			if len(opts.McpConfig) > 0 {
				mcpConfig = opts.McpConfig
			}
		}
	}

	if len(mcpConfig) == 0 {
		mcpConfig = LoadMCPConfig()
	}

	return port, agyBin, apiKey, model, systemPrompt, timeoutMinutes, mcpConfig
}
