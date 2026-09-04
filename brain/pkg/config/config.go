package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

var (
	lkgcMutex                   sync.RWMutex
	lastKnownGoodRules          string
	lastKnownGoodPersona        string
	lastKnownGoodPersonaSource  string

	systemRulesMu sync.Mutex

	runtimeConfigMu      sync.RWMutex
	currentRuntimeConfig Config

	instructionsCacheMu sync.RWMutex
	instructionsCache   = make(map[string]string)

	reChannelInstructionsTag = regexp.MustCompile(`(?i)</channel_instructions\s*>`)
)

var ConfigSearchPaths = []string{
	"/share/aerial-config/config.yaml",
	"/share/aerial-config/config.yml",
	"/app/config.yaml",
	"/share/aerial/config.yaml",
	"/data/.config.yaml.lkgc",
}

// Deprecated: SystemGuidelinesSearchPaths is no longer used by EnsureSystemRules
// because Antigravity CLI discovers GEMINI.md natively in cmd.Dir.
var SystemGuidelinesSearchPaths = []string{
	"/share/aerial-config/SYSTEM.local.md",
	"/share/aerial-config/SYSTEM.md",
	"/share/aerial/SYSTEM.md",
	"/app/SYSTEM.md",
	"./SYSTEM.md",
}

var AgentInstructionsSearchPaths = []string{
	"/share/aerial-config/AGENTS.local.md",
	"/share/aerial-config/AGENTS.md",
	"/share/aerial/AGENTS.md",
	"/app/AGENTS.md",
	"/data/AGENTS.md",
	"/data/.AGENTS.md.lkgc",
	"./AGENTS.local.md",
	"./AGENTS.md",
}

var ChannelInstructionsDirs = []string{
	"/share/aerial-config/channels",
	"/share/aerial/channels",
	"/app/channels",
	"./channels",
}

type GitSyncConfig struct {
	Enabled       bool     `yaml:"enabled" json:"enabled"`
	Interval      string   `yaml:"interval" json:"interval"`
	ConfigRepoUrl string   `yaml:"config_repo_url" json:"config_repo_url"`
	Repositories  []string `yaml:"repositories" json:"repositories"`
}

type ChannelPolicy struct {
	Mode                 string   `yaml:"mode" json:"mode"`
	WakeMode             string   `yaml:"wake_mode,omitempty" json:"wake_mode,omitempty"`
	TypingIndicator      string   `yaml:"typing_indicator" json:"typing_indicator"`
	IgnoreBots           *bool    `yaml:"ignore_bots,omitempty" json:"ignore_bots,omitempty"`
	AllowSystemOps       bool     `yaml:"allow_system_ops" json:"allow_system_ops"`
	MaxSessionTurns      int      `yaml:"max_session_turns" json:"max_session_turns"`
	AmbientWakeThreshold *float64 `yaml:"ambient_wake_threshold,omitempty" json:"ambient_wake_threshold,omitempty"`
	AmbientWakePrompt    string   `yaml:"ambient_wake_prompt,omitempty" json:"ambient_wake_prompt,omitempty"`
}

// IsIgnored reports whether the channel policy specifies an ignored/disabled channel.
func (p ChannelPolicy) IsIgnored() bool {
	m := strings.ToLower(strings.TrimSpace(p.Mode))
	return m == "ignore" || m == "disabled"
}

// GetWakeMode returns the effective wake mode for this channel policy.
// Supported canonical values: "mention", "classifier", "all".
// Accepted aliases: "mentions"/"direct" -> "mention", "ambient" -> "classifier", "always" -> "all".
// If unspecified, defaults to "classifier" for channel mode and "all" for threads mode.
func (p ChannelPolicy) GetWakeMode() string {
	m := strings.ToLower(strings.TrimSpace(p.WakeMode))
	switch m {
	case "mention", "mentions", "direct":
		return "mention"
	case "classifier", "ambient":
		return "classifier"
	case "all", "always":
		return "all"
	}
	if strings.ToLower(strings.TrimSpace(p.Mode)) == "channel" {
		return "classifier"
	}
	return "all"
}

// GetAmbientWakeThreshold returns the ambient wake threshold, defaulting to 0.80 for channel mode and 0.0 otherwise.
func (p ChannelPolicy) GetAmbientWakeThreshold() float64 {
	if p.AmbientWakeThreshold != nil {
		return *p.AmbientWakeThreshold
	}
	if strings.ToLower(strings.TrimSpace(p.Mode)) == "channel" {
		return 0.80
	}
	return 0.0
}

// GetAmbientWakePrompt returns the trimmed ambient wake prompt for this channel policy.
func (p ChannelPolicy) GetAmbientWakePrompt() string {
	return strings.TrimSpace(p.AmbientWakePrompt)
}

// IsBotIgnored reports whether bot messages should be ignored for this channel policy.
func (p ChannelPolicy) IsBotIgnored() bool {
	if p.IgnoreBots != nil {
		return *p.IgnoreBots
	}
	return false
}

type Config struct {
	Model          string                     `yaml:"model" json:"model"`
	TimeoutMinutes int                        `yaml:"timeout_minutes" json:"timeout_minutes"`
	Timezone       string                     `yaml:"timezone" json:"timezone"`
	SystemChannel  string                     `yaml:"system_channel" json:"system_channel"`
	AdminUsers     []string                   `yaml:"admin_users" json:"admin_users"`
	Channels       map[string]ChannelPolicy   `yaml:"channels" json:"channels"`
	GitSync        GitSyncConfig              `yaml:"git_sync" json:"git_sync"`
	McpServers     map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type rawConfigHelper struct {
		Model                string                   `yaml:"model"`
		TimeoutMinutes       int                      `yaml:"timeout_minutes"`
		Timezone             string                   `yaml:"timezone"`
		SystemChannel        string                   `yaml:"system_channel"`
		AdminUsers           []string                 `yaml:"admin_users"`
		Channels             map[string]ChannelPolicy `yaml:"channels"`
		GitSync              GitSyncConfig            `yaml:"git_sync"`
		McpServers           map[string]interface{}   `yaml:"mcp_servers"`
	}

	var raw rawConfigHelper
	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Model = raw.Model
	c.TimeoutMinutes = raw.TimeoutMinutes
	c.Timezone = raw.Timezone
	c.SystemChannel = raw.SystemChannel
	c.AdminUsers = raw.AdminUsers
	c.Channels = raw.Channels
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
	defaultIgnoreBots := true
	return Config{
		Model:          "Gemini 3.6 Flash (Low)",
		TimeoutMinutes: 15,
		Timezone:       "America/Los_Angeles",
		SystemChannel:  "aerial-dev",
		AdminUsers:     []string{},
		Channels: map[string]ChannelPolicy{
			"default": {
				Mode:            "threads",
				TypingIndicator: "always",
				IgnoreBots:      &defaultIgnoreBots,
				AllowSystemOps:  false,
				MaxSessionTurns: 0,
			},
		},
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

	// Validate channels.default existence
	if parsed.Channels == nil {
		log.Printf("[Config] Validation error: channels.default is required in %s. Retaining Last Known Good Configuration (LKGC).", targetPath)
		return GetRuntimeConfig(), fmt.Errorf("channels.default is required")
	}

	defPolicy, hasDefault := parsed.Channels["default"]
	if !hasDefault {
		for k, v := range parsed.Channels {
			if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(k)), "#") == "default" {
				defPolicy = v
				hasDefault = true
				break
			}
		}
	}

	if !hasDefault {
		log.Printf("[Config] Validation error: channels.default is required in %s. Retaining Last Known Good Configuration (LKGC).", targetPath)
		return GetRuntimeConfig(), fmt.Errorf("channels.default is required")
	}

	defMode := strings.ToLower(strings.TrimSpace(defPolicy.Mode))
	if defMode != "threads" && defMode != "channel" && defMode != "ignore" && defMode != "disabled" {
		log.Printf("[Config] Validation error: channels.default mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q in %s. Retaining Last Known Good Configuration (LKGC).", defPolicy.Mode, targetPath)
		return GetRuntimeConfig(), fmt.Errorf("channels.default mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q", defPolicy.Mode)
	}

	if defPolicy.AmbientWakeThreshold != nil {
		if *defPolicy.AmbientWakeThreshold < 0.0 || *defPolicy.AmbientWakeThreshold > 1.0 {
			log.Printf("[Config] Validation error: channels.default ambient_wake_threshold must be between 0.0 and 1.0, got %f in %s. Retaining Last Known Good Configuration (LKGC).", *defPolicy.AmbientWakeThreshold, targetPath)
			return GetRuntimeConfig(), fmt.Errorf("channels.default ambient_wake_threshold must be between 0.0 and 1.0, got %f", *defPolicy.AmbientWakeThreshold)
		}
	}

	if defPolicy.WakeMode != "" {
		wLower := strings.ToLower(strings.TrimSpace(defPolicy.WakeMode))
		if wLower != "mention" && wLower != "mentions" && wLower != "direct" &&
			wLower != "classifier" && wLower != "ambient" &&
			wLower != "all" && wLower != "always" {
			log.Printf("[Config] Validation error: channels.default wake_mode must be 'mention', 'classifier', or 'all', got %q in %s. Retaining Last Known Good Configuration (LKGC).", defPolicy.WakeMode, targetPath)
			return GetRuntimeConfig(), fmt.Errorf("channels.default wake_mode must be 'mention', 'classifier', or 'all', got %q", defPolicy.WakeMode)
		}
		defPolicy.WakeMode = defPolicy.GetWakeMode()
	}

	// Normalize channels.default
	if defPolicy.TypingIndicator == "" {
		if defPolicy.Mode == "channel" {
			defPolicy.TypingIndicator = "on_mention"
		} else if defPolicy.Mode == "threads" {
			defPolicy.TypingIndicator = "always"
		}
	}
	if defPolicy.Mode == "channel" && defPolicy.MaxSessionTurns <= 0 {
		defPolicy.MaxSessionTurns = 50
	}
	parsed.Channels["default"] = defPolicy

	// Normalize and validate other channels
	for k, policy := range parsed.Channels {
		if k == "default" {
			continue
		}
		if policy.Mode != "" {
			modeLower := strings.ToLower(strings.TrimSpace(policy.Mode))
			if modeLower != "threads" && modeLower != "channel" && modeLower != "ignore" && modeLower != "disabled" {
				log.Printf("[Config] Validation error: channel %q mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q in %s. Retaining Last Known Good Configuration (LKGC).", k, policy.Mode, targetPath)
				return GetRuntimeConfig(), fmt.Errorf("channel %q mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q", k, policy.Mode)
			}
			policy.Mode = modeLower
		}
		if policy.AmbientWakeThreshold != nil {
			if *policy.AmbientWakeThreshold < 0.0 || *policy.AmbientWakeThreshold > 1.0 {
				log.Printf("[Config] Validation error: channel %q ambient_wake_threshold must be between 0.0 and 1.0, got %f in %s. Retaining Last Known Good Configuration (LKGC).", k, *policy.AmbientWakeThreshold, targetPath)
				return GetRuntimeConfig(), fmt.Errorf("channel %q ambient_wake_threshold must be between 0.0 and 1.0, got %f", k, *policy.AmbientWakeThreshold)
			}
		}
		if policy.WakeMode != "" {
			wLower := strings.ToLower(strings.TrimSpace(policy.WakeMode))
			if wLower != "mention" && wLower != "mentions" && wLower != "direct" &&
				wLower != "classifier" && wLower != "ambient" &&
				wLower != "all" && wLower != "always" {
				log.Printf("[Config] Validation error: channel %q wake_mode must be 'mention', 'classifier', or 'all', got %q in %s. Retaining Last Known Good Configuration (LKGC).", k, policy.WakeMode, targetPath)
				return GetRuntimeConfig(), fmt.Errorf("channel %q wake_mode must be 'mention', 'classifier', or 'all', got %q", k, policy.WakeMode)
			}
			policy.WakeMode = policy.GetWakeMode()
		}
		if policy.TypingIndicator == "" {
			if policy.Mode == "channel" {
				policy.TypingIndicator = "on_mention"
			} else if policy.Mode == "threads" {
				policy.TypingIndicator = "always"
			}
		}
		if policy.Mode == "channel" && policy.MaxSessionTurns <= 0 {
			policy.MaxSessionTurns = 50
		}
		parsed.Channels[k] = policy
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
	if parsed.AdminUsers == nil {
		parsed.AdminUsers = []string{}
	}

	runtimeConfigMu.Lock()
	currentRuntimeConfig = parsed
	runtimeConfigMu.Unlock()

	// Persist durable on-disk LKGC to survive cold starts and container recycles
	if targetPath != "/data/.config.yaml.lkgc" {
		_ = writeAtomic("/data/.config.yaml.lkgc", string(rawData))
	}

	log.Printf("[Config] Successfully loaded configuration from %s (model=%s, timeout=%dm, timezone=%s, channel=%s)",
		targetPath, parsed.Model, parsed.TimeoutMinutes, parsed.Timezone, parsed.SystemChannel)

	return parsed, nil
}

// IsAdmin checks if any of the given identifiers (userID, username, globalName) match cfg.AdminUsers.
func (c Config) IsAdmin(identifiers ...string) bool {
	if len(c.AdminUsers) == 0 {
		return false
	}
	for _, id := range identifiers {
		trimmedID := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(id)), "@")
		if trimmedID == "" {
			continue
		}
		for _, admin := range c.AdminUsers {
			trimmedAdmin := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(admin)), "@")
			if trimmedAdmin == trimmedID {
				return true
			}
		}
	}
	return false
}

// ResolveChannelPolicy resolves the effective ChannelPolicy for a given channel ID or name.
// Lookup hierarchy: Channel Snowflake ID -> Channel Name (case-insensitive, '#' stripped) -> channels["default"].
// Any unspecified field inherits from channels["default"].
func (c Config) ResolveChannelPolicy(channelID, channelName string) ChannelPolicy {
	def, hasDef := c.getChannelPolicyRaw("default")
	if !hasDef {
		defaultIgnoreBots := true
		def = ChannelPolicy{
			Mode:            "threads",
			TypingIndicator: "always",
			IgnoreBots:      &defaultIgnoreBots,
			AllowSystemOps:  false,
			MaxSessionTurns: 0,
		}
	} else {
		if def.Mode == "" {
			def.Mode = "threads"
		}
		if def.TypingIndicator == "" {
			if def.Mode == "channel" {
				def.TypingIndicator = "on_mention"
			} else if def.Mode == "threads" {
				def.TypingIndicator = "always"
			}
		}
		if def.Mode == "channel" && def.MaxSessionTurns <= 0 {
			def.MaxSessionTurns = 50
		}
	}

	var matched ChannelPolicy
	var found bool

	// 1. Match by Snowflake ID
	if normID := strings.TrimSpace(channelID); normID != "" {
		matched, found = c.getChannelPolicyRaw(normID)
	}

	// 2. Match by Channel Name (in-memory map key normalization)
	if !found {
		if normName := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(channelName)), "#"); normName != "" {
			matched, found = c.getChannelPolicyRaw(normName)
		}
	}

	// 3. Fallback to default
	if !found {
		return def
	}

	// Apply inheritance from def to matched
	res := matched
	if res.Mode == "" {
		res.Mode = def.Mode
	}
	if res.IsIgnored() {
		return res
	}
	if res.TypingIndicator == "" {
		if res.Mode == "channel" {
			if def.Mode == "channel" && def.TypingIndicator != "" {
				res.TypingIndicator = def.TypingIndicator
			} else {
				res.TypingIndicator = "on_mention"
			}
		} else {
			if def.TypingIndicator != "" {
				res.TypingIndicator = def.TypingIndicator
			} else {
				res.TypingIndicator = "always"
			}
		}
	}
	if res.WakeMode == "" && def.WakeMode != "" {
		res.WakeMode = def.WakeMode
	}
	if res.IgnoreBots == nil && def.IgnoreBots != nil {
		val := *def.IgnoreBots
		res.IgnoreBots = &val
	}
	if res.AmbientWakeThreshold == nil && def.AmbientWakeThreshold != nil {
		val := *def.AmbientWakeThreshold
		res.AmbientWakeThreshold = &val
	}
	if res.AmbientWakePrompt == "" && def.AmbientWakePrompt != "" {
		res.AmbientWakePrompt = def.AmbientWakePrompt
	}
	if !res.AllowSystemOps && def.AllowSystemOps {
		res.AllowSystemOps = true
	}
	if res.MaxSessionTurns <= 0 {
		if def.MaxSessionTurns > 0 {
			res.MaxSessionTurns = def.MaxSessionTurns
		} else if res.Mode == "channel" {
			res.MaxSessionTurns = 50
		}
	}
	return res
}

func (c Config) getChannelPolicyRaw(key string) (ChannelPolicy, bool) {
	if len(c.Channels) == 0 || key == "" {
		return ChannelPolicy{}, false
	}
	if p, ok := c.Channels[key]; ok {
		return p, true
	}
	normKey := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(key)), "#")
	for k, v := range c.Channels {
		if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(k)), "#") == normKey {
			return v, true
		}
	}
	return ChannelPolicy{}, false
}

// NormalizeChannelName standardizes channel names for config key and file lookups.
// It trims whitespace, strips any leading '#', lowercases, and strictly blocks directory traversal.
func NormalizeChannelName(channelName string) string {
	s := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(channelName)), "#")
	if strings.Contains(s, "..") || strings.Contains(s, "/") || strings.Contains(s, "\\") {
		return ""
	}
	s = filepath.Base(s)
	if s == "." || s == "/" || s == "\\" || s == "" {
		return ""
	}
	return s
}

// LoadChannelInstructions loads per-channel instructions from convention-based markdown files.
// It normalizes the channel name, searches ChannelInstructionsDirs with path confinement,
// enforces regular file checks, caps reading at 64KB, and sanitizes instructions closing tags.
func LoadChannelInstructions(channelName string) string {
	normName := NormalizeChannelName(channelName)
	if normName == "" {
		return ""
	}

	candidateNames := []string{normName}
	if strings.Contains(normName, " ") {
		candidateNames = append(candidateNames, strings.ReplaceAll(normName, " ", "-"))
	}
	if strings.Contains(normName, "-") {
		candidateNames = append(candidateNames, strings.ReplaceAll(normName, "-", " "))
	}

	for _, dir := range ChannelInstructionsDirs {
		cleanDir := filepath.Clean(dir)
		for _, cand := range candidateNames {
			targetPath := filepath.Join(cleanDir, cand+".md")
			cleanTarget := filepath.Clean(targetPath)
			if !strings.HasPrefix(cleanTarget, cleanDir+string(filepath.Separator)) {
				continue
			}
			info, err := os.Stat(cleanTarget)
			if err != nil || !info.Mode().IsRegular() {
				continue
			}
			f, err := os.Open(cleanTarget)
			if err != nil {
				continue
			}
			data, err := io.ReadAll(io.LimitReader(f, 64*1024))
			_ = f.Close()
			if err != nil {
				continue
			}
			trimmed := strings.TrimSpace(string(data))
			if len(trimmed) > 0 {
				// Defensively escape any closing delimiter tag to prevent breakout
				sanitized := reChannelInstructionsTag.ReplaceAllString(trimmed, "<\\/CHANNEL_INSTRUCTIONS>")
				instructionsCacheMu.Lock()
				instructionsCache[normName] = sanitized
				instructionsCacheMu.Unlock()
				return sanitized
			}

			instructionsCacheMu.RLock()
			cached, hasCached := instructionsCache[normName]
			instructionsCacheMu.RUnlock()
			if hasCached && cached != "" {
				log.Printf("[Config] Active file is empty (possible GitSync torn read), using cached instructions for %q", normName)
				return cached
			}
		}
	}
	return ""
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
	systemRulesMu.Lock()
	defer systemRulesMu.Unlock()

	var sb strings.Builder
	sb.WriteString("---\ndescription: User persona, tone, and identity overrides\ntrigger: always_on\n---\n\n")

	foundPersona := false
	tornRead := false
	var personaContent string
	var personaSource string

	// User persona overrides (AGENTS.md) in priority order
	for _, p := range AgentInstructionsSearchPaths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		if len(bytes.TrimSpace(data)) > 0 {
			personaContent = string(data)
			personaSource = filepath.Base(p)
			foundPersona = true
			log.Printf("Loaded agent instructions from %s", p)
			break
		}
		// File exists but is 0-bytes or whitespace-only (torn read during git sync)
		log.Printf("[Config] Active agent instructions file %s is empty (possible GitSync torn read), engaging Last Known Good Persona (LKGC)", p)
		tornRead = true
		break
	}

	if tornRead {
		lkgcMutex.RLock()
		personaContent = lastKnownGoodPersona
		personaSource = lastKnownGoodPersonaSource
		lkgcMutex.RUnlock()
		if personaSource == "" {
			personaSource = "AGENTS.md"
		}
		if personaContent != "" {
			foundPersona = true
		}
	} else if foundPersona {
		lkgcMutex.Lock()
		lastKnownGoodPersona = personaContent
		lastKnownGoodPersonaSource = personaSource
		lkgcMutex.Unlock()

		if personaSource != ".AGENTS.md.lkgc" {
			_ = writeAtomic("/data/.AGENTS.md.lkgc", personaContent)
		}
	}

	foundInstructions := false
	if foundPersona && personaContent != "" {
		sb.WriteString(fmt.Sprintf("# User Persona Overrides (%s)\n\n%s\n\n", personaSource, personaContent))
		foundInstructions = true
	}

	// Environment Prompt Override
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

	configRulesDir := filepath.Join(homeDir, ".gemini", "config", "rules")
	if err := os.MkdirAll(configRulesDir, 0755); err != nil {
		return fmt.Errorf("failed to create config rules directory: %w", err)
	}

	// Clean up any legacy, conflicting, or repository-level generated rule files
	staleRuleFiles := []string{
		filepath.Join(primaryRulesDir, "system_instructions.md"),
		filepath.Join(primaryRulesDir, "SYSTEM_INSTRUCTIONS.md"),
		filepath.Join(primaryRulesDir, "system.md"),
		filepath.Join(primaryRulesDir, "SYSTEM.md"),
		filepath.Join(primaryRulesDir, "gemini.md"),
		filepath.Join(primaryRulesDir, "GEMINI.md"),
		filepath.Join(primaryRulesDir, "agents.md"),
		filepath.Join(primaryRulesDir, "custom_instructions.md"),

		filepath.Join(configRulesDir, "system_instructions.md"),
		filepath.Join(configRulesDir, "SYSTEM_INSTRUCTIONS.md"),
		filepath.Join(configRulesDir, "system.md"),
		filepath.Join(configRulesDir, "SYSTEM.md"),
		filepath.Join(configRulesDir, "gemini.md"),
		filepath.Join(configRulesDir, "GEMINI.md"),
		filepath.Join(configRulesDir, "agents.md"),
		filepath.Join(configRulesDir, "custom_instructions.md"),

		"/app/.agents/rules/system_instructions.md",
		"/app/.agents/rules/custom_instructions.md",
		"/app/.agents/rules/agents.md",
		"/app/.agents/rules/system.md",
		"/app/.agents/rules/gemini.md",
	}
	for _, stale := range staleRuleFiles {
		_ = os.Remove(stale)
	}

	primaryRuleFile := filepath.Join(primaryRulesDir, "user_persona.md")
	if err := writeAtomic(primaryRuleFile, content); err != nil {
		return fmt.Errorf("failed to write primary system rules: %w", err)
	}
	log.Printf("Configured always_on user persona in %s", primaryRuleFile)

	// Also sync to ~/.gemini/config/rules for compatibility
	configRuleFile := filepath.Join(configRulesDir, "user_persona.md")
	_ = writeAtomic(configRuleFile, content)

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

