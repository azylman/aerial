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
	"sync"

	"gopkg.in/yaml.v3"
)

var (
	lkgcMutex          sync.RWMutex
	lastKnownGoodRules string

	runtimeConfigMu      sync.RWMutex
	currentRuntimeConfig Config
)

var ConfigSearchPaths = []string{
	"/share/aerial-config/config.yaml",
	"/share/aerial-config/config.yml",
	"/app/config.yaml",
	"/share/aerial/config.yaml",
}

type GitSyncConfig struct {
	Enabled       bool     `yaml:"enabled" json:"enabled"`
	Interval      string   `yaml:"interval" json:"interval"`
	ConfigRepoUrl string   `yaml:"config_repo_url" json:"config_repo_url"`
	Repositories  []string `yaml:"repositories" json:"repositories"`
}

type Config struct {
	Model          string                     `yaml:"model" json:"model"`
	TimeoutMinutes int                        `yaml:"timeout_minutes" json:"timeout_minutes"`
	Timezone       string                     `yaml:"timezone" json:"timezone"`
	SystemChannel  string                     `yaml:"system_channel" json:"system_channel"`
	GitSync        GitSyncConfig              `yaml:"git_sync" json:"git_sync"`
	McpServers     map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type rawConfigHelper struct {
		Model          string                 `yaml:"model"`
		TimeoutMinutes int                    `yaml:"timeout_minutes"`
		Timezone       string                 `yaml:"timezone"`
		SystemChannel  string                 `yaml:"system_channel"`
		GitSync        GitSyncConfig          `yaml:"git_sync"`
		McpServers     map[string]interface{} `yaml:"mcp_servers"`
	}

	var raw rawConfigHelper
	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Model = raw.Model
	c.TimeoutMinutes = raw.TimeoutMinutes
	c.Timezone = raw.Timezone
	c.SystemChannel = raw.SystemChannel
	c.GitSync = raw.GitSync

	if raw.McpServers != nil {
		c.McpServers = make(map[string]json.RawMessage)
		for k, v := range raw.McpServers {
			b, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("failed to marshal mcp_server %q to JSON: %w", k, err)
			}
			c.McpServers[k] = json.RawMessage(b)
		}
	}
	return nil
}

type Options struct {
	Port           int             `json:"port"`
	AgyBin         string          `json:"agy_bin"`
	ApiKey         string          `json:"api_key"`
	Model          string          `json:"model"`
	SystemPrompt   string          `json:"system_prompt"`
	TimeoutMinutes int             `json:"timeout_minutes"`
	McpConfig      json.RawMessage `json:"mcp_config"`
}

func DefaultConfig() Config {
	return Config{
		Model:          "Gemini 3.6 Flash (Low)",
		TimeoutMinutes: 15,
		Timezone:       "America/Los_Angeles",
		SystemChannel:  "aerial-dev",
		GitSync: GitSyncConfig{
			Enabled:       true,
			Interval:      "60s",
			ConfigRepoUrl: "https://github.com/azylman/aerial-config.git",
			Repositories:  []string{"/share/aerial-config", "/share/aerial"},
		},
		McpServers: make(map[string]json.RawMessage),
	}
}

func getFallbackDefaults() Config {
	cfg := DefaultConfig()

	// 1. Check /data/options.json
	if data, err := os.ReadFile("/data/options.json"); err == nil {
		var opts Options
		if err := json.Unmarshal(data, &opts); err == nil {
			if strings.TrimSpace(opts.Model) != "" {
				cfg.Model = opts.Model
			}
			if opts.TimeoutMinutes > 0 {
				cfg.TimeoutMinutes = opts.TimeoutMinutes
			}
		}
	}

	// 2. Check environment variables
	if m := strings.TrimSpace(os.Getenv("AGY_MODEL")); m != "" {
		cfg.Model = m
	}
	if tm := strings.TrimSpace(os.Getenv("TIMEOUT_MINUTES")); tm != "" {
		if val, err := strconv.Atoi(tm); err == nil && val > 0 {
			cfg.TimeoutMinutes = val
		}
	}
	if tz := strings.TrimSpace(os.Getenv("DEFAULT_TIMEZONE")); tz != "" {
		cfg.Timezone = tz
	} else if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
		cfg.Timezone = tz
	}
	if ch := strings.TrimSpace(os.Getenv("SYSTEM_CHANNEL")); ch != "" {
		cfg.SystemChannel = ch
	}

	return cfg
}

func GetRuntimeConfig() Config {
	runtimeConfigMu.RLock()
	defer runtimeConfigMu.RUnlock()
	if currentRuntimeConfig.Model == "" {
		return getFallbackDefaults()
	}
	return currentRuntimeConfig
}

func GetTimezone() string {
	cfg := GetRuntimeConfig()
	if strings.TrimSpace(cfg.Timezone) != "" {
		return cfg.Timezone
	}
	if tz := strings.TrimSpace(os.Getenv("DEFAULT_TIMEZONE")); tz != "" {
		return tz
	}
	if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
		return tz
	}
	return "America/Los_Angeles"
}

func GetSystemChannel() string {
	cfg := GetRuntimeConfig()
	if strings.TrimSpace(cfg.SystemChannel) != "" {
		return cfg.SystemChannel
	}
	if ch := strings.TrimSpace(os.Getenv("SYSTEM_CHANNEL")); ch != "" {
		return ch
	}
	return "aerial-dev"
}

func LoadConfig() (Config, error) {
	return LoadConfigFromPaths(ConfigSearchPaths...)
}

func LoadConfigFromPaths(paths ...string) (Config, error) {
	fallback := getFallbackDefaults()

	runtimeConfigMu.Lock()
	if currentRuntimeConfig.Model == "" {
		currentRuntimeConfig = fallback
	}
	runtimeConfigMu.Unlock()

	var targetPath string
	var rawData []byte
	for _, p := range paths {
		if data, err := os.ReadFile(p); err == nil && len(bytes.TrimSpace(data)) > 0 {
			targetPath = p
			rawData = data
			break
		}
	}

	if targetPath == "" {
		return GetRuntimeConfig(), nil
	}

	expanded := os.ExpandEnv(string(rawData))
	var parsed Config
	if err := yaml.Unmarshal([]byte(expanded), &parsed); err != nil {
		log.Printf("[Config] Warning: Failed to parse %s: %v. Retaining Last Known Good Configuration (LKGC).", targetPath, err)
		return GetRuntimeConfig(), fmt.Errorf("failed to parse %s: %w", targetPath, err)
	}

	if strings.TrimSpace(parsed.Model) == "" {
		parsed.Model = fallback.Model
	}
	if parsed.TimeoutMinutes <= 0 {
		parsed.TimeoutMinutes = fallback.TimeoutMinutes
	}
	if strings.TrimSpace(parsed.Timezone) == "" {
		parsed.Timezone = fallback.Timezone
	}
	if strings.TrimSpace(parsed.SystemChannel) == "" {
		parsed.SystemChannel = fallback.SystemChannel
	}
	if strings.TrimSpace(parsed.GitSync.Interval) == "" {
		parsed.GitSync.Interval = fallback.GitSync.Interval
	}
	if strings.TrimSpace(parsed.GitSync.ConfigRepoUrl) == "" {
		parsed.GitSync.ConfigRepoUrl = fallback.GitSync.ConfigRepoUrl
	}
	if len(parsed.GitSync.Repositories) == 0 {
		parsed.GitSync.Repositories = fallback.GitSync.Repositories
	}

	runtimeConfigMu.Lock()
	currentRuntimeConfig = parsed
	runtimeConfigMu.Unlock()

	log.Printf("[Config] Successfully loaded configuration from %s (model=%s, timeout=%dm, timezone=%s, channel=%s)",
		targetPath, parsed.Model, parsed.TimeoutMinutes, parsed.Timezone, parsed.SystemChannel)

	return parsed, nil
}

func GetEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func EnsureAgySettings(apiKey, model string) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	configDir := filepath.Join(homeDir, ".gemini", "antigravity-cli")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	settingsPath := filepath.Join(configDir, "settings.json")
	settings := map[string]interface{}{}
	if data, err := os.ReadFile(settingsPath); err == nil {
		if err := json.Unmarshal(data, &settings); err != nil {
			log.Printf("Warning: failed to unmarshal existing settings from %s: %v", settingsPath, err)
		}
	}
	if model != "" {
		settings["model"] = model
	}
	if apiKey != "" {
		settings["modelProvider"] = "gemini"
	} else {
		delete(settings, "modelProvider")
	}

	out, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal settings: %w", err)
	}
	if err := writeAtomic(settingsPath, string(out)); err != nil {
		return fmt.Errorf("failed to write settings: %w", err)
	}
	return nil
}

func writeAtomic(targetPath, content string) error {
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	pattern := fmt.Sprintf(".%s.tmp.*", filepath.Base(targetPath))
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return err
	}
	tmpName := f.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, targetPath); err != nil {
		return err
	}
	return nil
}

func EnsureSystemRules(customPrompt string) error {
	var sb strings.Builder
	sb.WriteString("---\ndescription: User custom instructions, persona, and system guidelines\ntrigger: always_on\n---\n\n")

	foundInstructions := false

	// 1. Base system guidelines (SYSTEM.md) placed FIRST
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

	// 2. User persona overrides (AGENTS.md) in priority order
	searchPaths := []string{
		"/share/aerial-config/AGENTS.local.md",
		"/share/aerial-config/AGENTS.md",
		"/share/aerial/AGENTS.md",
		"/app/AGENTS.md",
		"/data/AGENTS.md",
		"./AGENTS.local.md",
		"./AGENTS.md",
	}

	for _, p := range searchPaths {
		if data, err := os.ReadFile(p); err == nil && len(bytes.TrimSpace(data)) > 0 {
			sb.WriteString(fmt.Sprintf("# User Persona Overrides (%s)\n\n%s\n\n", filepath.Base(p), string(data)))
			foundInstructions = true
			log.Printf("Loaded agent instructions from %s", p)
			break
		}
	}

	// 3. Environment Prompt Override
	if strings.TrimSpace(customPrompt) != "" {
		sb.WriteString(fmt.Sprintf("# Environment Prompt Override\n\n%s\n\n", strings.TrimSpace(customPrompt)))
		foundInstructions = true
	}

	var content string
	if foundInstructions {
		content = sb.String()
		lkgcMutex.Lock()
		lastKnownGoodRules = content
		lkgcMutex.Unlock()
	} else {
		lkgcMutex.RLock()
		content = lastKnownGoodRules
		lkgcMutex.RUnlock()
		if content == "" {
			return nil
		}
		log.Printf("Using Last Known Good Configuration (LKGC) for system rules")
	}

	homeDir, err := os.UserHomeDir()
	if err != nil || homeDir == "" {
		homeDir = "/root"
	}

	primaryRulesDir := filepath.Join(homeDir, ".gemini", "rules")
	if err := os.MkdirAll(primaryRulesDir, 0755); err != nil {
		return fmt.Errorf("failed to create primary rules directory: %w", err)
	}

	// Clean up any legacy, conflicting, or repository-level generated rule files
	staleRuleFiles := []string{
		filepath.Join(primaryRulesDir, "agents.md"),
		filepath.Join(primaryRulesDir, "custom_instructions.md"),
		filepath.Join(primaryRulesDir, "system.md"),
		filepath.Join(homeDir, ".gemini", "config", "rules", "custom_instructions.md"),
		filepath.Join(homeDir, ".gemini", "config", "rules", "agents.md"),
		filepath.Join(homeDir, ".gemini", "config", "rules", "system.md"),
		"/share/aerial/.agents/rules/system_instructions.md",
		"/share/aerial/.agents/rules/custom_instructions.md",
		"/share/aerial/.agents/rules/agents.md",
		"/share/aerial/.agents/rules/system.md",
		"/app/.agents/rules/system_instructions.md",
		"/app/.agents/rules/custom_instructions.md",
		"/app/.agents/rules/agents.md",
		"/app/.agents/rules/system.md",
	}
	for _, stale := range staleRuleFiles {
		_ = os.Remove(stale)
	}

	primaryRuleFile := filepath.Join(primaryRulesDir, "system_instructions.md")
	if err := writeAtomic(primaryRuleFile, content); err != nil {
		return fmt.Errorf("failed to write primary system rules: %w", err)
	}
	log.Printf("Configured always_on system instructions in %s", primaryRuleFile)

	// Also sync to ~/.gemini/config/rules for compatibility
	configRulesDir := filepath.Join(homeDir, ".gemini", "config", "rules")
	_ = writeAtomic(filepath.Join(configRulesDir, "system_instructions.md"), content)

	return nil
}

func LoadMCPConfig() json.RawMessage {
	// 1. Start with built-in default MCP microservices
	mergedServers := map[string]interface{}{
		"scheduler": map[string]interface{}{
			"serverUrl": "http://scheduler-mcp:8080/mcp",
		},
		"discord": map[string]interface{}{
			"serverUrl": "http://discord-mcp:4001/mcp",
		},
		"docker": map[string]interface{}{
			"serverUrl": "http://docker-mcp:4002/sse",
		},
	}
	if pat := os.Getenv("GITHUB_PAT"); pat != "" {
		mergedServers["github"] = map[string]interface{}{
			"serverUrl": "http://github-mcp:4003/sse",
		}
	}

	// 2. Check for file-based overrides (e.g. /share/aerial-config/mcp.config.json)
	configPaths := []string{
		"/share/aerial-config/mcp.config.json",
		"/share/aerial-config/mcp.json",
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
	cfg := GetRuntimeConfig()
	if len(cfg.McpServers) > 0 {
		for k, v := range cfg.McpServers {
			var parsedVal interface{}
			if err := json.Unmarshal(v, &parsedVal); err == nil {
				mergedServers[k] = parsedVal
			} else {
				mergedServers[k] = v
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

	expanded := os.ExpandEnv(string(outBytes))
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

	if err := writeAtomic(targetPath, string(configContent)); err != nil {
		log.Printf("Failed to write %s: %v", targetPath, err)
		return fmt.Errorf("failed to write mcp config: %w", err)
	}
	log.Printf("Configured %d MCP server(s) in %s: %v", len(serverList), targetPath, serverList)
	return nil
}

