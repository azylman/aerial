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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

var (
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
	IgnoreBots           *bool    `yaml:"ignore_bots,omitempty" json:"ignore_bots,omitempty"`
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

type OllamaConfig struct {
	BaseURL     string `yaml:"base_url" json:"base_url"`
	Model       string `yaml:"model" json:"model"`
	QueryPrefix string `yaml:"query_prefix" json:"query_prefix"`
}

type ConfigData struct {
	Model           string                     `yaml:"model" json:"model"`
	Timezone        string                     `yaml:"timezone" json:"timezone"`
	SystemChannel   string                     `yaml:"system_channel" json:"system_channel"`
	AdminUsers      []string                   `yaml:"admin_users" json:"admin_users"`
	Channels        map[string]ChannelPolicy   `yaml:"channels" json:"channels"`
	GitSync         GitSyncConfig              `yaml:"git_sync" json:"git_sync"`
	McpServers      map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`
	DatabaseURL     string                     `yaml:"database_url" json:"database_url"`
	Port            string                     `yaml:"port" json:"port"`
	AgyBin          string                     `yaml:"agy_bin" json:"agy_bin"`
	APIKey          string                     `yaml:"api_key" json:"api_key"`
	SystemPrompt    string                     `yaml:"system_prompt" json:"system_prompt"`
	DiscordToken    string                     `yaml:"discord_token" json:"discord_token"`
	GitHubPAT       string                     `yaml:"github_pat" json:"github_pat"`
	Ollama          OllamaConfig               `yaml:"ollama" json:"ollama"`
	LowEffortModel  string                     `yaml:"low_effort_model" json:"low_effort_model"`
	ClassifierModel string                     `yaml:"classifier_model,omitempty" json:"classifier_model,omitempty"` // Deprecated alias
	GeminiHomeDir   string                     `yaml:"gemini_home_dir,omitempty" json:"gemini_home_dir,omitempty"`
	DataDir         string                     `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
}

func (c *ConfigData) UnmarshalYAML(value *yaml.Node) error {
	type rawConfigHelper struct {
		GeminiHomeDir   string                     `yaml:"gemini_home_dir"`
		DataDir         string                     `yaml:"data_dir"`
		Model           string                     `yaml:"model"`
		Timezone        string                     `yaml:"timezone"`
		SystemChannel   string                     `yaml:"system_channel"`
		AdminUsers      []string                   `yaml:"admin_users"`
		Channels        map[string]ChannelPolicy   `yaml:"channels"`
		GitSync         GitSyncConfig              `yaml:"git_sync"`
		McpServers      map[string]interface{}     `yaml:"mcp_servers"`
		DatabaseURL     string                     `yaml:"database_url"`
		DBPath          string                     `yaml:"db_path"`
		Port            string                     `yaml:"port"`
		AgyBin          string                     `yaml:"agy_bin"`
		APIKey          string                     `yaml:"api_key"`
		SystemPrompt    string                     `yaml:"system_prompt"`
		DiscordToken    string                     `yaml:"discord_token"`
		GitHubPAT       string                     `yaml:"github_pat"`
		Ollama          OllamaConfig               `yaml:"ollama"`
		LowEffortModel  string                     `yaml:"low_effort_model"`
		ClassifierModel string                     `yaml:"classifier_model"`
	}

	var raw rawConfigHelper
	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Model = raw.Model
	c.Timezone = raw.Timezone
	c.SystemChannel = raw.SystemChannel
	c.AdminUsers = raw.AdminUsers
	c.Channels = raw.Channels
	c.GitSync = raw.GitSync
	c.DatabaseURL = raw.DatabaseURL
	if c.DatabaseURL == "" {
		c.DatabaseURL = raw.DBPath
	}
	c.Port = raw.Port
	c.AgyBin = raw.AgyBin
	c.APIKey = raw.APIKey
	c.SystemPrompt = raw.SystemPrompt
	c.DiscordToken = raw.DiscordToken
	c.GitHubPAT = raw.GitHubPAT
	c.Ollama = raw.Ollama
	c.LowEffortModel = strings.TrimSpace(raw.LowEffortModel)
	if c.LowEffortModel == "" && strings.TrimSpace(raw.ClassifierModel) != "" {
		c.LowEffortModel = strings.TrimSpace(raw.ClassifierModel)
		log.Printf("[Config] Warning: 'classifier_model' key in config is deprecated; please rename to 'low_effort_model'")
	}
	c.ClassifierModel = raw.ClassifierModel
	c.GeminiHomeDir = raw.GeminiHomeDir
	c.DataDir = raw.DataDir

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

type Config struct {
	current atomic.Pointer[ConfigData]
}

func cloneChannelPolicy(p ChannelPolicy) ChannelPolicy {
	cp := p
	if p.IgnoreBots != nil {
		b := *p.IgnoreBots
		cp.IgnoreBots = &b
	}
	if p.AmbientWakeThreshold != nil {
		f := *p.AmbientWakeThreshold
		cp.AmbientWakeThreshold = &f
	}
	return cp
}

func cloneConfigData(src *ConfigData) *ConfigData {
	if src == nil {
		return nil
	}
	dst := *src

	if src.AdminUsers != nil {
		dst.AdminUsers = make([]string, len(src.AdminUsers))
		copy(dst.AdminUsers, src.AdminUsers)
	}
	if src.Channels != nil {
		dst.Channels = make(map[string]ChannelPolicy, len(src.Channels))
		for k, v := range src.Channels {
			dst.Channels[k] = cloneChannelPolicy(v)
		}
	}
	if src.GitSync.Repositories != nil {
		dst.GitSync.Repositories = make([]string, len(src.GitSync.Repositories))
		copy(dst.GitSync.Repositories, src.GitSync.Repositories)
	}
	if src.McpServers != nil {
		dst.McpServers = make(map[string]json.RawMessage, len(src.McpServers))
		for k, v := range src.McpServers {
			dst.McpServers[k] = append(json.RawMessage(nil), v...)
		}
	}
	return &dst
}

func getGeminiHomeDirDefault() string {
	homeDir := os.Getenv("HOME")
	if homeDir == "" {
		var err error
		homeDir, err = os.UserHomeDir()
		if err != nil || homeDir == "" {
			homeDir = "/root"
		}
	}
	return homeDir
}

func DefaultConfigData() *ConfigData {
	defaultIgnoreBots := true
	return &ConfigData{
		Model:         "Gemini 3.6 Flash (Low)",
		Timezone:      "America/Los_Angeles",
		SystemChannel: "aerial-dev",
		AdminUsers:    []string{},
		Channels: map[string]ChannelPolicy{
			"default": {
				Mode:       "threads",
				IgnoreBots: &defaultIgnoreBots,
			},
		},
		GitSync: GitSyncConfig{
			Enabled:       true,
			Interval:      "60s",
			ConfigRepoUrl: "https://github.com/azylman/aerial-config.git",
			Repositories:  []string{"/share/aerial-config", "/share/aerial"},
		},
		McpServers:      make(map[string]json.RawMessage),
		Port:            "8080",
		AgyBin:          "agy",
		LowEffortModel:  "Gemini 3.8 Flash (Low)",
		ClassifierModel: "Gemini 3.8 Flash (Low)",
		DataDir:         "/data",
		GeminiHomeDir:   getGeminiHomeDirDefault(),
		Ollama: OllamaConfig{
			BaseURL:     "http://ollama:11434",
			Model:       "all-minilm",
			QueryPrefix: "Represent this sentence for searching relevant passages: ",
		},
	}
}

func (c *Config) Current() *ConfigData {
	if c == nil {
		return DefaultConfigData()
	}
	cur := c.current.Load()
	if cur == nil {
		return DefaultConfigData()
	}
	return cur
}

// GeminiHomeDir returns the resolved base directory for .gemini files.
func (c *Config) GeminiHomeDir() string {
	cur := c.Current()
	if cur.GeminiHomeDir != "" {
		return cur.GeminiHomeDir
	}
	return getGeminiHomeDirDefault()
}

// DataDir returns the resolved base directory for persistent data.
func (c *Config) DataDir() string {
	cur := c.Current()
	if cur.DataDir != "" {
		return cur.DataDir
	}
	return "/data"
}

// Update atomically swaps the underlying ConfigData snapshot.
func (c *Config) Update(fresh *ConfigData) {
	c.update(fresh)
}

func (c *Config) update(fresh *ConfigData) {
	if c == nil || fresh == nil {
		return
	}
	c.current.Store(cloneConfigData(fresh))
}

// NewTestConfig returns a hermetic in-memory *Config initialized with DefaultConfigData(),
// with DatabaseURL set to ":memory:", Port set to "0", sensitive tokens cleared,
// and any caller-provided mutators applied.
func NewTestConfig(mutators ...func(*ConfigData)) *Config {
	data := DefaultConfigData()
	data.DatabaseURL = ":memory:"
	data.Port = "0"
	data.DiscordToken = ""
	data.APIKey = ""
	data.GitHubPAT = ""
	for _, fn := range mutators {
		if fn != nil {
			fn(data)
		}
	}
	return NewFromData(data)
}

func NewFromData(data *ConfigData) *Config {
	if data == nil {
		data = DefaultConfigData()
	}
	cfg := &Config{}
	cfg.current.Store(cloneConfigData(data))
	return cfg
}

func New() (*Config, error) {
	return LoadConfigFromPaths(ConfigSearchPaths...)
}

func LoadConfig() (*Config, error) {
	return New()
}

func Reload(activeCfg *Config) error {
	if activeCfg == nil {
		return fmt.Errorf("config: activeCfg cannot be nil")
	}
	fresh, err := New()
	if err != nil {
		return err
	}
	activeCfg.update(fresh.Current())
	return nil
}

func buildPostgresDSNFromEnv() string {
	if envDSN := strings.TrimSpace(os.Getenv("DATABASE_URL")); envDSN != "" {
		return envDSN
	}
	if envDBPath := strings.TrimSpace(os.Getenv("DB_PATH")); envDBPath != "" {
		return envDBPath
	}
	dbHost := strings.TrimSpace(os.Getenv("POSTGRES_HOST"))
	if dbHost == "" {
		return ""
	}
	dbPort := strings.TrimSpace(os.Getenv("POSTGRES_PORT"))
	if dbPort == "" {
		dbPort = "5432"
	}
	dbUser := strings.TrimSpace(os.Getenv("POSTGRES_USER"))
	if dbUser == "" {
		dbUser = "aerial"
	}
	dbPass := os.Getenv("POSTGRES_PASSWORD")
	if dbPass == "" {
		dbPass = "aerial_secure_pass"
	}
	dbName := strings.TrimSpace(os.Getenv("POSTGRES_DB"))
	if dbName == "" {
		dbName = "aerial"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
}

func applyEnvironmentOverrides(data *ConfigData) {
	if data == nil {
		return
	}
	if p := getEnv("PORT", ""); p != "" {
		data.Port = p
	}
	if b := getEnv("AGY_BIN", ""); b != "" {
		data.AgyBin = b
	}
	if k := getEnv("GEMINI_API_KEY", getEnv("ANTIGRAVITY_API_KEY", "")); k != "" {
		data.APIKey = k
	}
	if sp := getEnv("SYSTEM_PROMPT", ""); sp != "" {
		data.SystemPrompt = sp
	}
	if dt := getEnv("DISCORD_TOKEN", getEnv("DISCORD_BOT_TOKEN", "")); dt != "" {
		data.DiscordToken = dt
	}
	if pat := getEnv("GITHUB_PAT", ""); pat != "" {
		data.GitHubPAT = pat
	}
	if dsn := buildPostgresDSNFromEnv(); dsn != "" {
		data.DatabaseURL = dsn
	}
	if m := getEnv("AGY_MODEL", ""); m != "" {
		data.Model = m
	}
	if tz := getEnv("DEFAULT_TIMEZONE", getEnv("TZ", "")); tz != "" {
		data.Timezone = tz
	}
	if sc := getEnv("SYSTEM_CHANNEL", ""); sc != "" {
		data.SystemChannel = sc
	}
	if lem := getEnv("LOW_EFFORT_MODEL", getEnv("AMBIENT_CLASSIFIER_MODEL", getEnv("CLASSIFIER_MODEL", ""))); lem != "" {
		data.LowEffortModel = lem
		data.ClassifierModel = lem
	}
	if ou := getEnv("OLLAMA_URL", ""); ou != "" {
		data.Ollama.BaseURL = ou
	}
	if om := getEnv("EMBEDDING_MODEL", getEnv("OLLAMA_EMBEDDING_MODEL", "")); om != "" {
		data.Ollama.Model = om
	}
	if qp := getEnv("EMBEDDING_QUERY_PREFIX", ""); qp != "" {
		data.Ollama.QueryPrefix = qp
	}
	if gh := getEnv("GEMINI_HOME", ""); gh != "" {
		data.GeminiHomeDir = gh
	}
	if dd := getEnv("DATA_DIR", ""); dd != "" {
		data.DataDir = dd
	}
}

var activeGlobalConfig = NewFromData(DefaultConfigData())

func getFallbackDefaults() ConfigData {
	data := DefaultConfigData()

	// 1. Check /data/options.json
	if fData, err := os.ReadFile("/data/options.json"); err == nil {
		var opts Options
		if err := json.Unmarshal(fData, &opts); err == nil {
			if strings.TrimSpace(opts.Model) != "" {
				data.Model = opts.Model
			}
		}
	}

	// 2. Check environment variables
	if m := strings.TrimSpace(os.Getenv("AGY_MODEL")); m != "" {
		data.Model = m
	}
	if tz := strings.TrimSpace(os.Getenv("DEFAULT_TIMEZONE")); tz != "" {
		data.Timezone = tz
	} else if tz := strings.TrimSpace(os.Getenv("TZ")); tz != "" {
		data.Timezone = tz
	}
	if ch := strings.TrimSpace(os.Getenv("SYSTEM_CHANNEL")); ch != "" {
		data.SystemChannel = ch
	}

	return *data
}

// ActiveConfig returns the global active *Config instance.
func ActiveConfig() *Config {
	return activeGlobalConfig
}

func GetRuntimeConfig() ConfigData {
	return *activeGlobalConfig.Current()
}

func GetTimezone() string {
	cfg := activeGlobalConfig.Current()
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
	cfg := activeGlobalConfig.Current()
	if strings.TrimSpace(cfg.SystemChannel) != "" {
		return cfg.SystemChannel
	}
	if sc := strings.TrimSpace(os.Getenv("SYSTEM_CHANNEL")); sc != "" {
		return sc
	}
	return "aerial-dev"
}

func isContainerColdBoot(paths ...string) bool {
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
			return false
		}
	}
	if fi, err := os.Stat("/share/aerial-config"); err == nil && fi.IsDir() {
		return true
	}
	return false
}

func waitForColdBootConfig(paths []string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range paths {
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
				log.Printf("[Config] Cold-boot configuration detected at %s", p)
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	log.Printf("[Config] Cold-boot wait timed out after %v, proceeding with search", timeout)
}

func LoadConfigFromPaths(paths ...string) (*Config, error) {
	if isContainerColdBoot(paths...) {
		waitForColdBootConfig(paths, 5*time.Second)
	}

	data := DefaultConfigData()
	var loadedPath string
	var lastErr error

	for _, p := range paths {
		rawData, err := os.ReadFile(p)
		if err != nil || len(bytes.TrimSpace(rawData)) == 0 {
			continue
		}

		expanded := os.ExpandEnv(string(rawData))
		var parsed ConfigData
		if err := yaml.Unmarshal([]byte(expanded), &parsed); err != nil {
			log.Printf("[Config] Warning: Failed to parse %s: %v. Checking fallback search paths...", p, err)
			lastErr = fmt.Errorf("failed to parse %s: %w", p, err)
			continue
		}

		if err := validateChannels(&parsed, p); err != nil {
			log.Printf("[Config] Warning: Validation error for %s: %v. Checking fallback search paths...", p, err)
			lastErr = err
			continue
		}

		loadedPath = p
		data = &parsed
		target := filepath.Join(data.DataDir, ".config.yaml.lkgc")
		if data.DataDir == "" {
			target = "/data/.config.yaml.lkgc"
		}
		if p != target {
			_ = writeAtomicFile(target, string(rawData))
		}
		break
	}

	if loadedPath == "" {
		if lastErr != nil {
			log.Printf("[Config] Retaining Last Known Good Configuration (LKGC) due to load error: %v", lastErr)
			fallback := activeGlobalConfig.Current()
			cloned := cloneConfigData(fallback)
			applyEnvironmentOverrides(cloned)
			activeGlobalConfig.update(cloned)
			return activeGlobalConfig, lastErr
		}
		// No files found and no parse errors: apply defaults + env overrides
		applyEnvironmentOverrides(data)
		activeGlobalConfig.update(data)
		return activeGlobalConfig, nil
	}

	applyEnvironmentOverrides(data)
	activeGlobalConfig.update(data)
	log.Printf("[Config] Successfully loaded configuration from %s (model=%s, timezone=%s, channel=%s)",
		loadedPath, data.Model, data.Timezone, data.SystemChannel)
	return activeGlobalConfig, nil
}

func validateChannels(parsed *ConfigData, targetPath string) error {
	if strings.TrimSpace(parsed.Model) == "" {
		parsed.Model = DefaultConfigData().Model
	}
	if strings.TrimSpace(parsed.LowEffortModel) == "" {
		parsed.LowEffortModel = DefaultConfigData().LowEffortModel
		parsed.ClassifierModel = DefaultConfigData().LowEffortModel
	}

	if parsed.Channels == nil {
		log.Printf("[Config] Validation error: channels.default is required in %s.", targetPath)
		return fmt.Errorf("channels.default is required")
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
		log.Printf("[Config] Validation error: channels.default is required in %s.", targetPath)
		return fmt.Errorf("channels.default is required")
	}

	defMode := strings.ToLower(strings.TrimSpace(defPolicy.Mode))
	if defMode != "threads" && defMode != "channel" && defMode != "ignore" && defMode != "disabled" {
		log.Printf("[Config] Validation error: channels.default mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q in %s.", defPolicy.Mode, targetPath)
		return fmt.Errorf("channels.default mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q", defPolicy.Mode)
	}

	if defPolicy.AmbientWakeThreshold != nil {
		if *defPolicy.AmbientWakeThreshold < 0.0 || *defPolicy.AmbientWakeThreshold > 1.0 {
			log.Printf("[Config] Validation error: channels.default ambient_wake_threshold must be between 0.0 and 1.0, got %f in %s.", *defPolicy.AmbientWakeThreshold, targetPath)
			return fmt.Errorf("channels.default ambient_wake_threshold must be between 0.0 and 1.0, got %f", *defPolicy.AmbientWakeThreshold)
		}
	}

	if defPolicy.WakeMode != "" {
		wLower := strings.ToLower(strings.TrimSpace(defPolicy.WakeMode))
		if wLower != "mention" && wLower != "mentions" && wLower != "direct" &&
			wLower != "classifier" && wLower != "ambient" &&
			wLower != "all" && wLower != "always" {
			log.Printf("[Config] Validation error: channels.default wake_mode must be 'mention', 'classifier', or 'all', got %q in %s.", defPolicy.WakeMode, targetPath)
			return fmt.Errorf("channels.default wake_mode must be 'mention', 'classifier', or 'all', got %q", defPolicy.WakeMode)
		}
		defPolicy.WakeMode = defPolicy.GetWakeMode()
	}

	parsed.Channels["default"] = defPolicy

	for k, policy := range parsed.Channels {
		if k == "default" {
			continue
		}
		if policy.Mode != "" {
			modeLower := strings.ToLower(strings.TrimSpace(policy.Mode))
			if modeLower != "threads" && modeLower != "channel" && modeLower != "ignore" && modeLower != "disabled" {
				log.Printf("[Config] Validation error: channel %q mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q in %s.", k, policy.Mode, targetPath)
				return fmt.Errorf("channel %q mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q", k, policy.Mode)
			}
			policy.Mode = modeLower
		}
		if policy.AmbientWakeThreshold != nil {
			if *policy.AmbientWakeThreshold < 0.0 || *policy.AmbientWakeThreshold > 1.0 {
				log.Printf("[Config] Validation error: channel %q ambient_wake_threshold must be between 0.0 and 1.0, got %f in %s.", k, *policy.AmbientWakeThreshold, targetPath)
				return fmt.Errorf("channel %q ambient_wake_threshold must be between 0.0 and 1.0, got %f", k, *policy.AmbientWakeThreshold)
			}
		}
		if policy.WakeMode != "" {
			wLower := strings.ToLower(strings.TrimSpace(policy.WakeMode))
			if wLower != "mention" && wLower != "mentions" && wLower != "direct" &&
				wLower != "classifier" && wLower != "ambient" &&
				wLower != "all" && wLower != "always" {
				log.Printf("[Config] Validation error: channel %q wake_mode must be 'mention', 'classifier', or 'all', got %q in %s.", k, policy.WakeMode, targetPath)
				return fmt.Errorf("channel %q wake_mode must be 'mention', 'classifier', or 'all', got %q", k, policy.WakeMode)
			}
			policy.WakeMode = policy.GetWakeMode()
		}
		parsed.Channels[k] = policy
	}
	return nil
}

type Options struct {
	Port         int             `json:"port"`
	AgyBin       string          `json:"agy_bin"`
	ApiKey       string          `json:"api_key"`
	Model        string          `json:"model"`
	SystemPrompt string          `json:"system_prompt"`
	McpConfig    json.RawMessage `json:"mcp_config"`
}

func IsAdmin(adminUsers []string, identifiers ...string) bool {
	if len(adminUsers) == 0 {
		return false
	}
	for _, id := range identifiers {
		trimmedID := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(id)), "@")
		if trimmedID == "" {
			continue
		}
		for _, admin := range adminUsers {
			trimmedAdmin := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(admin)), "@")
			if trimmedAdmin == trimmedID {
				return true
			}
		}
	}
	return false
}

func ResolveChannelPolicy(channels map[string]ChannelPolicy, channelID, channelName string) ChannelPolicy {
	def, hasDef := getChannelPolicyRaw(channels, "default")
	if !hasDef {
		defaultIgnoreBots := true
		def = ChannelPolicy{
			Mode:       "threads",
			IgnoreBots: &defaultIgnoreBots,
		}
	} else {
		if def.Mode == "" {
			def.Mode = "threads"
		}
	}

	var matched ChannelPolicy
	var found bool

	if normID := strings.TrimSpace(channelID); normID != "" {
		matched, found = getChannelPolicyRaw(channels, normID)
	}

	if !found {
		if normName := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(channelName)), "#"); normName != "" {
			matched, found = getChannelPolicyRaw(channels, normName)
		}
	}

	if !found {
		return def
	}

	res := matched
	if res.Mode == "" {
		res.Mode = def.Mode
	}
	if res.IsIgnored() {
		return res
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
	return res
}

func getChannelPolicyRaw(channels map[string]ChannelPolicy, key string) (ChannelPolicy, bool) {
	if len(channels) == 0 || key == "" {
		return ChannelPolicy{}, false
	}
	if p, ok := channels[key]; ok {
		return p, true
	}
	normKey := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(key)), "#")
	for k, v := range channels {
		if strings.TrimPrefix(strings.ToLower(strings.TrimSpace(k)), "#") == normKey {
			return v, true
		}
	}
	return ChannelPolicy{}, false
}

func (c *Config) ResolveChannelPolicy(channelID, channelName string) ChannelPolicy {
	return ResolveChannelPolicy(c.Current().Channels, channelID, channelName)
}

func (c *Config) IsAdmin(identifiers ...string) bool {
	return IsAdmin(c.Current().AdminUsers, identifiers...)
}

func (c *Config) getChannelPolicyRaw(key string) (ChannelPolicy, bool) {
	return getChannelPolicyRaw(c.Current().Channels, key)
}

// Deprecated: Shim for backward compatibility during migration.
func (c ConfigData) ResolveChannelPolicy(channelID, channelName string) ChannelPolicy {
	return ResolveChannelPolicy(c.Channels, channelID, channelName)
}

// Deprecated: Shim for backward compatibility during migration.
func (c ConfigData) IsAdmin(identifiers ...string) bool {
	return IsAdmin(c.AdminUsers, identifiers...)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// Deprecated: GetEnv is deprecated. Subpackages should read from cfg.Current().
func GetEnv(key, defaultVal string) string {
	return getEnv(key, defaultVal)
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

func writeAtomicFile(targetPath, content string) error {
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
		_ = os.Remove(targetPath)
		return os.Rename(tmpName, targetPath)
	}
	return nil
}

