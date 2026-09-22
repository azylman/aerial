package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

type WebhookEndpoint struct {
	URL             string `yaml:"url" json:"url"`
	TimeoutMs       int    `yaml:"timeout_ms,omitempty" json:"timeout_ms,omitempty"`
	OnTimeoutAction string `yaml:"on_timeout,omitempty" json:"on_timeout,omitempty"` // "proceed", "drop", "retry", "classify", "wake"
}

type ChannelHooksConfig struct {
	OnWake   *WebhookEndpoint `yaml:"on_wake,omitempty" json:"on_wake,omitempty"`
	PreTurn  *WebhookEndpoint `yaml:"pre_turn,omitempty" json:"pre_turn,omitempty"`
	PostTurn *WebhookEndpoint `yaml:"post_turn,omitempty" json:"post_turn,omitempty"`
}

type ChannelPolicy struct {
	Mode                 string             `yaml:"mode" json:"mode"`
	WakeMode             string             `yaml:"wake_mode,omitempty" json:"wake_mode,omitempty"`
	IgnoreBots           *bool              `yaml:"ignore_bots,omitempty" json:"ignore_bots,omitempty"`
	AmbientWakeThreshold *float64           `yaml:"ambient_wake_threshold,omitempty" json:"ambient_wake_threshold,omitempty"`
	AmbientWakePrompt    string             `yaml:"ambient_wake_prompt,omitempty" json:"ambient_wake_prompt,omitempty"`
	Hooks                ChannelHooksConfig `yaml:"hooks,omitempty" json:"hooks,omitempty"`
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

// GetHooks returns the channel lifecycle webhook configuration.
func (p ChannelPolicy) GetHooks() ChannelHooksConfig {
	return p.Hooks
}

// GetTimeout returns the configured timeout duration, defaulting to 3000ms if unspecified or non-positive.
func (e *WebhookEndpoint) GetTimeout() time.Duration {
	if e == nil || e.TimeoutMs <= 0 {
		return 3000 * time.Millisecond
	}
	return time.Duration(e.TimeoutMs) * time.Millisecond
}

// GetOnTimeout returns the normalized lowercase on_timeout action, or defaultAction if unspecified.
func (e *WebhookEndpoint) GetOnTimeout(defaultAction string) string {
	if e == nil || strings.TrimSpace(e.OnTimeoutAction) == "" {
		return defaultAction
	}
	return strings.ToLower(strings.TrimSpace(e.OnTimeoutAction))
}

type OllamaConfig struct {
	BaseURL     string `yaml:"base_url" json:"base_url"`
	Model       string `yaml:"model" json:"model"`
	QueryPrefix string `yaml:"query_prefix" json:"query_prefix"`
	NumCtx      int    `yaml:"num_ctx" json:"num_ctx"`
}

type ConfigData struct {
	Model           string                     `yaml:"model" json:"model"`
	Timezone        string                     `yaml:"timezone" json:"timezone"`
	SystemChannel   string                     `yaml:"system_channel" json:"system_channel"`
	AdminUsers      []string                   `yaml:"admin_users" json:"admin_users"`
	Channels        map[string]ChannelPolicy   `yaml:"channels" json:"channels"`
	McpServers      map[string]json.RawMessage `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`
	DatabaseURL     string                     `yaml:"database_url" json:"database_url"`
	Port            string                     `yaml:"port" json:"port"`
	AgyBin          string                     `yaml:"agy_bin" json:"agy_bin"`
	APIKey          string                     `yaml:"api_key" json:"api_key"`
	SystemPrompt    string                     `yaml:"system_prompt" json:"system_prompt"`
	DiscordToken    string                     `yaml:"discord_token" json:"discord_token"`
	GitHubPAT       string                     `yaml:"github_pat" json:"github_pat"`
	OpenObserveURL  string                     `yaml:"openobserve_url,omitempty" json:"openobserve_url,omitempty"`
	OpenObserveOrg  string                     `yaml:"openobserve_org,omitempty" json:"openobserve_org,omitempty"`
	OpenObserveUser string                     `yaml:"openobserve_user,omitempty" json:"openobserve_user,omitempty"`
	OpenObservePassword string                     `yaml:"openobserve_password,omitempty" json:"openobserve_password,omitempty"`
	Ollama          OllamaConfig               `yaml:"ollama" json:"ollama"`
	LowEffortModel  string                     `yaml:"low_effort_model" json:"low_effort_model"`
	GeminiHomeDir       string                     `yaml:"gemini_home_dir,omitempty" json:"gemini_home_dir,omitempty"`
	DataDir             string                     `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
	MCPConfig           string                     `yaml:"mcp_config,omitempty" json:"mcp_config,omitempty"`
	DaemonIdleTimeout   time.Duration              `yaml:"daemon_idle_timeout,omitempty" json:"daemon_idle_timeout,omitempty"`
	MaxConcurrentDaemons int                       `yaml:"max_concurrent_daemons,omitempty" json:"max_concurrent_daemons,omitempty"`
	MaxBackgroundTaskDuration time.Duration        `yaml:"max_background_task_duration,omitempty" json:"max_background_task_duration,omitempty"`
}

func (c *ConfigData) UnmarshalYAML(value *yaml.Node) error {
	type rawConfigHelper struct {
		GeminiHomeDir             string                   `yaml:"gemini_home_dir"`
		DataDir                   string                   `yaml:"data_dir"`
		Model                     string                   `yaml:"model"`
		Timezone                  string                   `yaml:"timezone"`
		SystemChannel             string                   `yaml:"system_channel"`
		AdminUsers                []string                 `yaml:"admin_users"`
		Channels                  map[string]ChannelPolicy `yaml:"channels"`
		McpServers                map[string]interface{}   `yaml:"mcp_servers"`
		DatabaseURL               string                   `yaml:"database_url"`
		Port                      string                   `yaml:"port"`
		AgyBin                    string                   `yaml:"agy_bin"`
		APIKey                    string                   `yaml:"api_key"`
		SystemPrompt              string                   `yaml:"system_prompt"`
		DiscordToken              string                   `yaml:"discord_token"`
		GitHubPAT                 string                   `yaml:"github_pat"`
		OpenObserveURL            string                   `yaml:"openobserve_url"`
		OpenObserveOrg            string                   `yaml:"openobserve_org"`
		OpenObserveUser           string                   `yaml:"openobserve_user"`
		OpenObservePassword       string                   `yaml:"openobserve_password"`
		Ollama                    OllamaConfig             `yaml:"ollama"`
		LowEffortModel            string                   `yaml:"low_effort_model"`
		MCPConfig                 interface{}              `yaml:"mcp_config"`
		DaemonIdleTimeout         time.Duration            `yaml:"daemon_idle_timeout"`
		MaxConcurrentDaemons      int                      `yaml:"max_concurrent_daemons"`
		MaxBackgroundTaskDuration time.Duration            `yaml:"max_background_task_duration"`
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
	c.DatabaseURL = raw.DatabaseURL
	c.Port = raw.Port
	c.AgyBin = raw.AgyBin
	c.APIKey = raw.APIKey
	c.SystemPrompt = raw.SystemPrompt
	c.DiscordToken = raw.DiscordToken
	c.GitHubPAT = raw.GitHubPAT
	c.OpenObserveURL = raw.OpenObserveURL
	c.OpenObserveOrg = raw.OpenObserveOrg
	c.OpenObserveUser = raw.OpenObserveUser
	c.OpenObservePassword = raw.OpenObservePassword
	c.Ollama = raw.Ollama
	c.LowEffortModel = strings.TrimSpace(raw.LowEffortModel)
	c.GeminiHomeDir = raw.GeminiHomeDir
	c.DataDir = raw.DataDir

	if raw.DaemonIdleTimeout > 0 {
		c.DaemonIdleTimeout = raw.DaemonIdleTimeout
	} else {
		c.DaemonIdleTimeout = 24 * time.Hour
	}
	if raw.MaxConcurrentDaemons > 0 {
		c.MaxConcurrentDaemons = raw.MaxConcurrentDaemons
	} else {
		c.MaxConcurrentDaemons = 40
	}
	if raw.MaxBackgroundTaskDuration > 0 {
		c.MaxBackgroundTaskDuration = raw.MaxBackgroundTaskDuration
	} else {
		c.MaxBackgroundTaskDuration = 2 * time.Hour
	}

	if raw.MCPConfig != nil {
		switch v := raw.MCPConfig.(type) {
		case string:
			c.MCPConfig = v
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("failed to marshal mcp_config to JSON: %w", err)
			}
			c.MCPConfig = string(b)
		}
	}

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
	if p.Hooks.OnWake != nil {
		w := *p.Hooks.OnWake
		cp.Hooks.OnWake = &w
	}
	if p.Hooks.PreTurn != nil {
		w := *p.Hooks.PreTurn
		cp.Hooks.PreTurn = &w
	}
	if p.Hooks.PostTurn != nil {
		w := *p.Hooks.PostTurn
		cp.Hooks.PostTurn = &w
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
	if src.McpServers != nil {
		dst.McpServers = make(map[string]json.RawMessage, len(src.McpServers))
		for k, v := range src.McpServers {
			dst.McpServers[k] = append(json.RawMessage(nil), v...)
		}
	}
	return &dst
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
		McpServers:      make(map[string]json.RawMessage),
		Port:            "8080",
		AgyBin:          "agy",
		LowEffortModel:  "Gemini 3.8 Flash (Low)",
		DataDir:         "",
		GeminiHomeDir:   "",
		Ollama: OllamaConfig{
			BaseURL:     "http://ollama:11434",
			Model:       "all-minilm",
			QueryPrefix: "Represent this sentence for searching relevant passages: ",
			NumCtx:      512,
		},
		DaemonIdleTimeout:         24 * time.Hour,
		MaxConcurrentDaemons:      40,
		MaxBackgroundTaskDuration: 2 * time.Hour,
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

// Get returns the current ConfigData pointer (alias for Current).
func (c *Config) Get() *ConfigData {
	return c.Current()
}

// LoadConfigFromBytes parses YAML bytes into a Config instance.
func LoadConfigFromBytes(data []byte) (*Config, error) {
	var cfgData ConfigData
	if err := yaml.Unmarshal(data, &cfgData); err != nil {
		return nil, err
	}
	cfg := &Config{}
	cfg.current.Store(&cfgData)
	return cfg, nil
}

// GeminiHomeDir returns the configured base directory for .gemini files.
func (c *Config) GeminiHomeDir() string {
	return c.Current().GeminiHomeDir
}

// DataDir returns the configured base directory for persistent data.
func (c *Config) DataDir() string {
	return c.Current().DataDir
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
// hermetic temporary directories set for GeminiHomeDir and DataDir,
// and any caller-provided mutators applied.
func NewTestConfig(mutators ...func(*ConfigData)) *Config {
	data := DefaultConfigData()
	data.DatabaseURL = ":memory:"
	data.Port = "0"
	data.DiscordToken = ""
	data.APIKey = ""
	data.GitHubPAT = ""
	data.GeminiHomeDir = filepath.Join(os.TempDir(), "aerial-test-gemini")
	data.DataDir = filepath.Join(os.TempDir(), "aerial-test-data")
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

func buildPostgresDSN(lookup func(string) string) string {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	if envDSN := strings.TrimSpace(lookup("DATABASE_URL")); envDSN != "" {
		return envDSN
	}
	dbHost := strings.TrimSpace(lookup("POSTGRES_HOST"))
	if dbHost == "" {
		return ""
	}
	dbPort := strings.TrimSpace(lookup("POSTGRES_PORT"))
	if dbPort == "" {
		dbPort = "5432"
	}
	dbUser := strings.TrimSpace(lookup("POSTGRES_USER"))
	if dbUser == "" {
		dbUser = "aerial"
	}
	dbPass := lookup("POSTGRES_PASSWORD")
	if dbPass == "" {
		dbPass = "aerial_secure_pass"
	}
	dbName := strings.TrimSpace(lookup("POSTGRES_DB"))
	if dbName == "" {
		dbName = "aerial"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbPort, dbName)
}

func buildPostgresDSNFromEnv() string {
	return buildPostgresDSN(os.Getenv)
}

func applyEnvironmentOverrides(data *ConfigData, lookup func(string) string) {
	if data == nil {
		return
	}
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	if p := getEnvFromLookup(lookup, "PORT", ""); p != "" {
		data.Port = p
	}
	if b := getEnvFromLookup(lookup, "AGY_BIN", ""); b != "" {
		data.AgyBin = b
	}
	if k := getEnvFromLookup(lookup, "GEMINI_API_KEY", getEnvFromLookup(lookup, "ANTIGRAVITY_API_KEY", "")); k != "" {
		data.APIKey = k
	}
	if sp := getEnvFromLookup(lookup, "SYSTEM_PROMPT", ""); sp != "" {
		data.SystemPrompt = sp
	}
	if dt := getEnvFromLookup(lookup, "DISCORD_TOKEN", getEnvFromLookup(lookup, "DISCORD_BOT_TOKEN", "")); dt != "" {
		data.DiscordToken = dt
	}
	if pat := getEnvFromLookup(lookup, "GITHUB_PAT", ""); pat != "" {
		data.GitHubPAT = pat
	}
	if dsn := buildPostgresDSN(lookup); dsn != "" {
		data.DatabaseURL = dsn
	}
	if m := getEnvFromLookup(lookup, "AGY_MODEL", ""); m != "" {
		data.Model = m
	}
	if tz := getEnvFromLookup(lookup, "DEFAULT_TIMEZONE", getEnvFromLookup(lookup, "TZ", "")); tz != "" {
		data.Timezone = tz
	}
	if sc := getEnvFromLookup(lookup, "SYSTEM_CHANNEL", ""); sc != "" {
		data.SystemChannel = sc
	}
	if lem := getEnvFromLookup(lookup, "LOW_EFFORT_MODEL", getEnvFromLookup(lookup, "AMBIENT_CLASSIFIER_MODEL", getEnvFromLookup(lookup, "CLASSIFIER_MODEL", ""))); lem != "" {
		data.LowEffortModel = lem
	}
	if ou := getEnvFromLookup(lookup, "OLLAMA_URL", ""); ou != "" {
		data.Ollama.BaseURL = ou
	}
	if om := getEnvFromLookup(lookup, "EMBEDDING_MODEL", getEnvFromLookup(lookup, "OLLAMA_EMBEDDING_MODEL", "")); om != "" {
		data.Ollama.Model = om
	}
	if qp := getEnvFromLookup(lookup, "EMBEDDING_QUERY_PREFIX", ""); qp != "" {
		data.Ollama.QueryPrefix = qp
	}
	if ncStr := getEnvFromLookup(lookup, "OLLAMA_NUM_CTX", getEnvFromLookup(lookup, "EMBEDDING_NUM_CTX", "")); ncStr != "" {
		if nc, err := strconv.Atoi(strings.TrimSpace(ncStr)); err == nil && nc > 0 {
			data.Ollama.NumCtx = nc
		}
	}
	if gh := getEnvFromLookup(lookup, "GEMINI_HOME", ""); gh != "" {
		data.GeminiHomeDir = gh
	}
	if dd := getEnvFromLookup(lookup, "DATA_DIR", ""); dd != "" {
		data.DataDir = dd
	}
	if mcp := getEnvFromLookup(lookup, "MCP_CONFIG", ""); mcp != "" {
		data.MCPConfig = mcp
	}
	if oUrl := getEnvFromLookup(lookup, "OPENOBSERVE_URL", ""); oUrl != "" {
		data.OpenObserveURL = oUrl
	}
	if oOrg := getEnvFromLookup(lookup, "OPENOBSERVE_ORG", ""); oOrg != "" {
		data.OpenObserveOrg = oOrg
	}
	if oUser := getEnvFromLookup(lookup, "OPENOBSERVE_ROOT_USER_EMAIL", getEnvFromLookup(lookup, "OPENOBSERVE_USER", "")); oUser != "" {
		data.OpenObserveUser = oUser
	}
	if oPass := getEnvFromLookup(lookup, "OPENOBSERVE_ROOT_USER_PASSWORD", getEnvFromLookup(lookup, "OPENOBSERVE_PASSWORD", "")); oPass != "" {
		data.OpenObservePassword = oPass
	}
}

var activeGlobalConfig = NewFromData(DefaultConfigData())

func getFallbackDefaults() ConfigData {
	data := DefaultConfigData()

	// Read from active config snapshot (zero ambient reads)
	cfg := activeGlobalConfig.Current()
	if cfg != nil {
		if strings.TrimSpace(cfg.Model) != "" {
			data.Model = cfg.Model
		}
		if strings.TrimSpace(cfg.Timezone) != "" {
			data.Timezone = cfg.Timezone
		}
		if strings.TrimSpace(cfg.SystemChannel) != "" {
			data.SystemChannel = cfg.SystemChannel
		}
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
	if cfg != nil && strings.TrimSpace(cfg.Timezone) != "" {
		return cfg.Timezone
	}
	return "America/Los_Angeles"
}

func GetSystemChannel() string {
	cfg := activeGlobalConfig.Current()
	if cfg != nil && strings.TrimSpace(cfg.SystemChannel) != "" {
		return cfg.SystemChannel
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
	if len(paths) == 0 {
		return
	}
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
			log.Printf("[Config] Cold-boot configuration detected at %s", p)
			return
		}
	}
	if timeout <= 0 {
		log.Printf("[Config] Cold-boot wait timed out after %v, proceeding with search", timeout)
		return
	}

	deadline := time.Now().Add(timeout)
	for {
		for _, p := range paths {
			if fi, err := os.Stat(p); err == nil && !fi.IsDir() && fi.Size() > 0 {
				log.Printf("[Config] Cold-boot configuration detected at %s", p)
				return
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		sleepDur := 10 * time.Millisecond
		if remaining < sleepDur {
			sleepDur = remaining
		}
		time.Sleep(sleepDur)
	}
	log.Printf("[Config] Cold-boot wait timed out after %v, proceeding with search", timeout)
}

// LoadConfigFromLookup loads configuration searching the given paths and applying
// environment variable interpolation and overrides using the provided lookup function.
// It returns a newly allocated, isolated *Config instance without mutating the global active config.
func LoadConfigFromLookup(lookup func(string) string, paths ...string) (*Config, error) {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	data := DefaultConfigData()
	var loadedPath string
	var lastErr error

	for _, p := range paths {
		rawData, err := os.ReadFile(p)
		if err != nil || len(bytes.TrimSpace(rawData)) == 0 {
			continue
		}

		expanded := os.Expand(string(rawData), lookup)
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
			if writeErr := writeAtomicFile(target, string(rawData)); writeErr != nil {
				log.Printf("[Config] Warning writing fallback configuration to %s: %v", target, writeErr)
			}
		}
		break
	}

	if loadedPath == "" {
		if lastErr != nil {
			log.Printf("[Config] Retaining Last Known Good Configuration (LKGC) due to load error: %v", lastErr)
			fallback := activeGlobalConfig.Current()
			cloned := cloneConfigData(fallback)
			applyEnvironmentOverrides(cloned, lookup)
			return NewFromData(cloned), lastErr
		}
		// No files found and no parse errors: apply defaults + env overrides
		applyEnvironmentOverrides(data, lookup)
		return NewFromData(data), nil
	}

	applyEnvironmentOverrides(data, lookup)
	log.Printf("[Config] Successfully loaded configuration from %s (model=%s, timezone=%s, channel=%s)",
		loadedPath, data.Model, data.Timezone, data.SystemChannel)
	return NewFromData(data), nil
}

// NewFromLookup constructs a Config instance by searching standard paths using the provided lookup.
func NewFromLookup(lookup func(string) string) (*Config, error) {
	return LoadConfigFromLookup(lookup, ConfigSearchPaths...)
}

func LoadConfigFromPaths(paths ...string) (*Config, error) {
	if isContainerColdBoot(paths...) {
		waitForColdBootConfig(paths, 5*time.Second)
	}

	fresh, err := LoadConfigFromLookup(os.Getenv, paths...)
	if fresh != nil {
		activeGlobalConfig.update(fresh.Current())
	}
	return activeGlobalConfig, err
}

func validateChannels(parsed *ConfigData, targetPath string) error {
	if strings.TrimSpace(parsed.Model) == "" {
		parsed.Model = DefaultConfigData().Model
	}
	if strings.TrimSpace(parsed.LowEffortModel) == "" {
		parsed.LowEffortModel = DefaultConfigData().LowEffortModel
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

	if err := validateHooks("default", defPolicy.Hooks, targetPath); err != nil {
		return err
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
		if err := validateHooks(k, policy.Hooks, targetPath); err != nil {
			return err
		}
		parsed.Channels[k] = policy
	}
	return nil
}

func validateWebhookEndpoint(channelName, hookName string, ep *WebhookEndpoint, targetPath string) error {
	if ep == nil {
		return nil
	}
	rawURL := strings.TrimSpace(ep.URL)
	if rawURL == "" {
		log.Printf("[Config] Validation error: channel %q %s webhook url is required in %s.", channelName, hookName, targetPath)
		return fmt.Errorf("channel %q %s webhook url is required", channelName, hookName)
	}
	parsedURL, err := url.ParseRequestURI(rawURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		log.Printf("[Config] Validation error: channel %q %s webhook url must be a valid http or https URL, got %q in %s.", channelName, hookName, ep.URL, targetPath)
		return fmt.Errorf("channel %q %s webhook url must be a valid http or https URL, got %q", channelName, hookName, ep.URL)
	}
	if ep.TimeoutMs < 0 {
		log.Printf("[Config] Validation error: channel %q %s webhook timeout_ms cannot be negative, got %d in %s.", channelName, hookName, ep.TimeoutMs, targetPath)
		return fmt.Errorf("channel %q %s webhook timeout_ms cannot be negative, got %d", channelName, hookName, ep.TimeoutMs)
	}
	return nil
}

func validateHooks(channelName string, hooks ChannelHooksConfig, targetPath string) error {
	if hooks.OnWake != nil {
		if err := validateWebhookEndpoint(channelName, "on_wake", hooks.OnWake, targetPath); err != nil {
			return err
		}
	}
	if hooks.PreTurn != nil {
		if err := validateWebhookEndpoint(channelName, "pre_turn", hooks.PreTurn, targetPath); err != nil {
			return err
		}
	}
	if hooks.PostTurn != nil {
		if err := validateWebhookEndpoint(channelName, "post_turn", hooks.PostTurn, targetPath); err != nil {
			return err
		}
	}
	return nil
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
	if res.Hooks.OnWake != nil {
		w := *res.Hooks.OnWake
		res.Hooks.OnWake = &w
	} else if def.Hooks.OnWake != nil {
		w := *def.Hooks.OnWake
		res.Hooks.OnWake = &w
	}
	if res.Hooks.PreTurn != nil {
		w := *res.Hooks.PreTurn
		res.Hooks.PreTurn = &w
	} else if def.Hooks.PreTurn != nil {
		w := *def.Hooks.PreTurn
		res.Hooks.PreTurn = &w
	}
	if res.Hooks.PostTurn != nil {
		w := *res.Hooks.PostTurn
		res.Hooks.PostTurn = &w
	} else if def.Hooks.PostTurn != nil {
		w := *def.Hooks.PostTurn
		res.Hooks.PostTurn = &w
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

func getEnvFromLookup(lookup func(string) string, key, defaultVal string) string {
	if lookup == nil {
		return defaultVal
	}
	if val := lookup(key); val != "" {
		return val
	}
	return defaultVal
}

// GetEnvFromLookup returns the value of the environment variable resolved via lookup or defaultVal.
func GetEnvFromLookup(lookup func(string) string, key, defaultVal string) string {
	return getEnvFromLookup(lookup, key, defaultVal)
}

func getEnv(key, defaultVal string) string {
	return getEnvFromLookup(os.Getenv, key, defaultVal)
}

// Deprecated: GetEnv is deprecated. Subpackages should read from cfg.Current().
func GetEnv(key, defaultVal string) string {
	return getEnvFromLookup(os.Getenv, key, defaultVal)
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
			if closeErr := f.Close(); closeErr != nil {
				log.Printf("[Config] Warning closing instructions file %s: %v", cleanTarget, closeErr)
			}
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
		if rmErr := os.Remove(tmpName); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			log.Printf("[Config] Warning removing temporary atomic file %s: %v", tmpName, rmErr)
		}
	}()

	if _, err := f.WriteString(content); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[Config] Warning closing temporary atomic file %s: %v", tmpName, closeErr)
		}
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, targetPath); err != nil {
		if rmErr := os.Remove(targetPath); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			log.Printf("[Config] Warning removing target file %s prior to fallback rename: %v", targetPath, rmErr)
		}
		return os.Rename(tmpName, targetPath)
	}
	return nil
}

