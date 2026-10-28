package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/sanitizer"
)

var (
	instructionsCacheMu      sync.RWMutex
	instructionsCache        = make(map[string]string)
	reChannelInstructionsTag = regexp.MustCompile(`(?i)</channel_instructions\s*>`)
	reScopeInstructionsTag   = regexp.MustCompile(`(?i)</scope_instructions\s*>`)
)

var ConfigSearchPaths = []string{
	"/local/config.yaml",
	"/share/aerial-config/config.yaml",
	"/share/aerial-config/config.yml",
	"/app/config.yaml",
	"/share/aerial/config.yaml",
}

var ScopeInstructionsDirs = []string{
	"/share/aerial-config/channels",
	"/share/aerial/channels",
	"/app/channels",
	"./channels",
	"/share/aerial-config/scopes",
	"/share/aerial/scopes",
	"/app/scopes",
	"./scopes",
	"/share/aerial-config/nodes",
	"/share/aerial/nodes",
	"/app/nodes",
	"./nodes",
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

type AmbientToolConfig struct {
	Name      string                 `yaml:"name" json:"name"`
	Server    string                 `yaml:"server,omitempty" json:"server,omitempty"`
	Tool      string                 `yaml:"tool" json:"tool"`
	Arguments map[string]interface{} `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

type AmbientContextConfig struct {
	CacheTTL     string              `yaml:"cache_ttl,omitempty" json:"cache_ttl,omitempty"`
	TimeoutMs    int                 `yaml:"timeout_ms,omitempty" json:"timeout_ms,omitempty"`
	TemplatePath string              `yaml:"template_path,omitempty" json:"template_path,omitempty"`
	Template     string              `yaml:"template,omitempty" json:"template,omitempty"`
	Tools        []AmbientToolConfig `yaml:"tools,omitempty" json:"tools,omitempty"`
}

type ChannelPolicy struct {
	Mode                 string                `yaml:"mode" json:"mode"`
	Effort               string                `yaml:"effort,omitempty" json:"effort,omitempty"`
	WakeMode             string                `yaml:"wake_mode,omitempty" json:"wake_mode,omitempty"`
	IgnoreBots           *bool                 `yaml:"ignore_bots,omitempty" json:"ignore_bots,omitempty"`
	AmbientWakeThreshold *float64              `yaml:"ambient_wake_threshold,omitempty" json:"ambient_wake_threshold,omitempty"`
	Hooks                ChannelHooksConfig    `yaml:"hooks,omitempty" json:"hooks,omitempty"`
	AmbientContext       *AmbientContextConfig `yaml:"ambient_context,omitempty" json:"ambient_context,omitempty"`
}

// GetEffort returns the normalized effort tier for this channel policy ("low", "medium", or "high"), defaulting to "high".
func (p ChannelPolicy) GetEffort() string {
	switch strings.ToLower(strings.TrimSpace(p.Effort)) {
	case "low":
		return "low"
	case "medium":
		return "medium"
	default:
		return "high"
	}
}

// IsThread reports whether the channel policy specifies thread-based interaction.
func (p ChannelPolicy) IsThread() bool {
	m := strings.ToLower(strings.TrimSpace(p.Mode))
	return m == "thread" || m == "threads"
}

// IsIgnored reports whether the channel policy specifies an ignored/disabled channel.
func (p ChannelPolicy) IsIgnored() bool {
	m := strings.ToLower(strings.TrimSpace(p.Mode))
	return m == "ignore" || m == "disabled"
}

// GetWakeMode returns the effective wake mode for this channel policy.
// Supported canonical values: "mention", "classifier", "all".
func (p ChannelPolicy) GetWakeMode() string {
	if p.WakeMode != "" {
		wm := strings.ToLower(strings.TrimSpace(p.WakeMode))
		switch wm {
		case "mention", "mentions", "direct":
			return "mention"
		case "classifier", "ambient":
			return "classifier"
		case "all", "always":
			return "all"
		}
	}
	m := strings.ToLower(strings.TrimSpace(p.Mode))
	switch m {
	case "mention":
		return "mention"
	case "classifier", "channel":
		return "classifier"
	case "thread", "threads":
		return "all"
	case "ignore", "disabled":
		return "ignore"
	}
	return "all"
}

// GetAmbientWakeThreshold returns the ambient wake threshold, defaulting to 0.77 for classifier mode and 0.0 otherwise.
func (p ChannelPolicy) GetAmbientWakeThreshold() float64 {
	if p.AmbientWakeThreshold != nil {
		return *p.AmbientWakeThreshold
	}
	m := strings.ToLower(strings.TrimSpace(p.Mode))
	if m == "classifier" || m == "channel" {
		return 0.77
	}
	return 0.0
}

// IsBotIgnored reports whether bot messages should be ignored for this channel policy.
func (p ChannelPolicy) IsBotIgnored() bool {
	if p.IgnoreBots != nil {
		return *p.IgnoreBots
	}
	return false
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

type TargetMcpConfig struct {
	Common    map[string]json.RawMessage `yaml:"common,omitempty" json:"common,omitempty"`
	Discord   map[string]json.RawMessage `yaml:"discord,omitempty" json:"discord,omitempty"`
	Voice     map[string]json.RawMessage `yaml:"voice,omitempty" json:"voice,omitempty"`
	Ephemeral map[string]json.RawMessage `yaml:"ephemeral,omitempty" json:"ephemeral,omitempty"`
}

// SecretsConfig encapsulates credential configurations for redaction registration.
type SecretsConfig struct {
	DiscordToken string `yaml:"discord_token,omitempty" json:"discord_token,omitempty"`
}

// MCPServerConfig defines an MCP server configuration.
type MCPServerConfig struct {
	Command   string            `json:"command,omitempty" yaml:"command,omitempty"`
	Args      []string          `json:"args,omitempty" yaml:"args,omitempty"`
	Env       map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	ServerURL string            `json:"serverUrl,omitempty" yaml:"server_url,omitempty"`
	Headers   map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
}

// MCPServersConfig groups MCP server configurations by target category.
type MCPServersConfig struct {
	Common    map[string]MCPServerConfig `yaml:"common,omitempty" json:"common,omitempty"`
	Eri       map[string]MCPServerConfig `yaml:"eri,omitempty" json:"eri,omitempty"`
	Voice     map[string]MCPServerConfig `yaml:"voice,omitempty" json:"voice,omitempty"`
	Ephemeral map[string]MCPServerConfig `yaml:"ephemeral,omitempty" json:"ephemeral,omitempty"`
}

type VoiceConfig struct {
	Engine           string                `yaml:"engine,omitempty" json:"engine,omitempty"`
	Model            string                `yaml:"model,omitempty" json:"model,omitempty"`
	PrewarmedTargets []string              `yaml:"prewarmed_targets,omitempty" json:"prewarmed_targets,omitempty"`
	AllowedTools     []string              `yaml:"allowed_tools,omitempty" json:"allowed_tools,omitempty"`
	AmbientContext   *AmbientContextConfig `yaml:"ambient_context,omitempty" json:"ambient_context,omitempty"`
}

type ConfigData struct {
	Model                     string                   `yaml:"model" json:"model"`
	Timezone                  string                   `yaml:"timezone" json:"timezone"`
	SystemChannel             string                   `yaml:"system_channel" json:"system_channel"`
	AdminUsers                []string                 `yaml:"admin_users" json:"admin_users"`
	Channels                  map[string]ChannelPolicy `yaml:"channels" json:"channels"`
	Secrets                   SecretsConfig            `yaml:"secrets,omitempty" json:"secrets,omitempty"`
	McpServers                TargetMcpConfig          `yaml:"mcp_servers,omitempty" json:"mcp_servers,omitempty"`
	MCPServers                MCPServersConfig         `yaml:"-" json:"-"`
	Voice                     VoiceConfig              `yaml:"voice,omitempty" json:"voice,omitempty"`
	DatabaseURL               string                   `yaml:"database_url" json:"database_url"`
	Port                      string                   `yaml:"port" json:"port"`
	AgyBin                    string                   `yaml:"agy_bin" json:"agy_bin"`
	APIKey                    string                   `yaml:"api_key" json:"api_key"`
	HarnessAPIKey             string                   `yaml:"harness_api_key,omitempty" json:"harness_api_key,omitempty"`
	SystemPrompt              string                   `yaml:"system_prompt" json:"system_prompt"`
	DiscordToken              string                   `yaml:"discord_token" json:"discord_token"`
	GitHubPAT                 string                   `yaml:"github_pat" json:"github_pat"`
	OpenObserveURL            string                   `yaml:"openobserve_url,omitempty" json:"openobserve_url,omitempty"`
	OpenObserveOrg            string                   `yaml:"openobserve_org,omitempty" json:"openobserve_org,omitempty"`
	OpenObserveUser           string                   `yaml:"openobserve_user,omitempty" json:"openobserve_user,omitempty"`
	OpenObservePassword       string                   `yaml:"openobserve_password,omitempty" json:"openobserve_password,omitempty"`
	Ollama                    OllamaConfig             `yaml:"ollama" json:"ollama"`
	LowEffortModel            string                   `yaml:"low_effort_model" json:"low_effort_model"`
	MediumEffortModel         string                   `yaml:"medium_effort_model" json:"medium_effort_model"`
	ClassifierURL             string                   `yaml:"classifier_url,omitempty" json:"classifier_url,omitempty"`
	ClassifierModel           string                   `yaml:"classifier_model,omitempty" json:"classifier_model,omitempty"`
	ClassifierProtocol        string                   `yaml:"classifier_protocol,omitempty" json:"classifier_protocol,omitempty"`
	ThreadTitleURL            string                   `yaml:"thread_title_url,omitempty" json:"thread_title_url,omitempty"`
	ThreadTitleModel          string                   `yaml:"thread_title_model,omitempty" json:"thread_title_model,omitempty"`
	GeminiHomeDir             string                   `yaml:"gemini_home_dir,omitempty" json:"gemini_home_dir,omitempty"`
	DataDir                   string                   `yaml:"data_dir,omitempty" json:"data_dir,omitempty"`
	MCPConfig                 string                   `yaml:"mcp_config,omitempty" json:"mcp_config,omitempty"`
	DaemonIdleTimeout         time.Duration            `yaml:"daemon_idle_timeout,omitempty" json:"daemon_idle_timeout,omitempty"`
	MaxConcurrentDaemons      int                      `yaml:"max_concurrent_daemons,omitempty" json:"max_concurrent_daemons,omitempty"`
	MaxBackgroundTaskDuration time.Duration            `yaml:"max_background_task_duration,omitempty" json:"max_background_task_duration,omitempty"`
}

func normalizePrewarmedTargets(targets []string) []string {
	if targets == nil {
		return []string{"kiosk"}
	}
	cleanTargets := make([]string, 0, len(targets))
	for _, t := range targets {
		trimmed := strings.TrimSpace(t)
		if trimmed != "" {
			cleanTargets = append(cleanTargets, trimmed)
		}
	}
	return cleanTargets
}

func applyDaemonDefaults(c *ConfigData, idleTimeout time.Duration, maxDaemons int, maxBgDuration time.Duration) {
	if idleTimeout > 0 {
		c.DaemonIdleTimeout = idleTimeout
	} else {
		c.DaemonIdleTimeout = 24 * time.Hour
	}
	if maxDaemons > 0 {
		c.MaxConcurrentDaemons = maxDaemons
	} else {
		c.MaxConcurrentDaemons = 40
	}
	if maxBgDuration > 0 {
		c.MaxBackgroundTaskDuration = maxBgDuration
	} else {
		c.MaxBackgroundTaskDuration = 2 * time.Hour
	}
}

func unmarshalRawMCPConfig(rawMCP interface{}) (string, error) {
	if rawMCP == nil {
		return "", nil
	}
	switch v := rawMCP.(type) {
	case string:
		return v, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("failed to marshal mcp_config to JSON: %w", err)
		}
		return string(b), nil
	}
}

func populateTargetMcpServers(rawServers map[string]interface{}) (TargetMcpConfig, error) {
	cfg := TargetMcpConfig{
		Common:    make(map[string]json.RawMessage),
		Discord:   make(map[string]json.RawMessage),
		Voice:     make(map[string]json.RawMessage),
		Ephemeral: make(map[string]json.RawMessage),
	}
	if rawServers == nil {
		return cfg, nil
	}
	for targetKey, targetVal := range rawServers {
		normalizedKey := strings.ToLower(strings.TrimSpace(targetKey))
		var targetMap map[string]json.RawMessage
		switch normalizedKey {
		case "common":
			targetMap = cfg.Common
		case "discord":
			targetMap = cfg.Discord
		case "voice":
			targetMap = cfg.Voice
		case "ephemeral":
			targetMap = cfg.Ephemeral
		default:
			return cfg, fmt.Errorf("mcp_servers contains unrecognized target category %q: only 'common', 'discord', 'voice', and 'ephemeral' are allowed", targetKey)
		}

		if targetVal == nil {
			continue
		}
		subMap, ok := targetVal.(map[string]interface{})
		if !ok {
			return cfg, fmt.Errorf("mcp_servers category %q must be a mapping of server names to configurations", targetKey)
		}
		for srvName, srvConf := range subMap {
			b, err := json.Marshal(srvConf)
			if err != nil {
				return cfg, fmt.Errorf("failed to marshal mcp_server %s/%s to JSON: %w", targetKey, srvName, err)
			}
			targetMap[srvName] = json.RawMessage(b)
		}
	}
	return cfg, nil
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
		Voice                     VoiceConfig              `yaml:"voice"`
		DatabaseURL               string                   `yaml:"database_url"`
		Port                      string                   `yaml:"port"`
		AgyBin                    string                   `yaml:"agy_bin"`
		APIKey                    string                   `yaml:"api_key"`
		HarnessAPIKey             string                   `yaml:"harness_api_key"`
		SystemPrompt              string                   `yaml:"system_prompt"`
		DiscordToken              string                   `yaml:"discord_token"`
		GitHubPAT                 string                   `yaml:"github_pat"`
		OpenObserveURL            string                   `yaml:"openobserve_url"`
		OpenObserveOrg            string                   `yaml:"openobserve_org"`
		OpenObserveUser           string                   `yaml:"openobserve_user"`
		OpenObservePassword       string                   `yaml:"openobserve_password"`
		Ollama                    OllamaConfig             `yaml:"ollama"`
		LowEffortModel            string                   `yaml:"low_effort_model"`
		MediumEffortModel         string                   `yaml:"medium_effort_model"`
		ClassifierURL             string                   `yaml:"classifier_url"`
		ClassifierModel           string                   `yaml:"classifier_model"`
		ClassifierProtocol        string                   `yaml:"classifier_protocol"`
		ThreadTitleURL            string                   `yaml:"thread_title_url"`
		ThreadTitleModel          string                   `yaml:"thread_title_model"`
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
	c.Voice = raw.Voice
	c.Voice.Engine = strings.TrimSpace(raw.Voice.Engine)
	c.Voice.PrewarmedTargets = normalizePrewarmedTargets(raw.Voice.PrewarmedTargets)
	c.DatabaseURL = raw.DatabaseURL
	c.Port = raw.Port
	c.AgyBin = raw.AgyBin
	c.APIKey = raw.APIKey
	c.HarnessAPIKey = raw.HarnessAPIKey
	c.SystemPrompt = raw.SystemPrompt
	c.DiscordToken = raw.DiscordToken
	c.GitHubPAT = raw.GitHubPAT
	c.OpenObserveURL = raw.OpenObserveURL
	c.OpenObserveOrg = raw.OpenObserveOrg
	c.OpenObserveUser = raw.OpenObserveUser
	c.OpenObservePassword = raw.OpenObservePassword
	c.Ollama = raw.Ollama
	c.LowEffortModel = strings.TrimSpace(raw.LowEffortModel)
	c.MediumEffortModel = strings.TrimSpace(raw.MediumEffortModel)
	c.ClassifierURL = strings.TrimSpace(raw.ClassifierURL)
	c.ClassifierModel = strings.TrimSpace(raw.ClassifierModel)
	c.ClassifierProtocol = strings.ToLower(strings.TrimSpace(raw.ClassifierProtocol))
	c.ThreadTitleURL = strings.TrimSpace(raw.ThreadTitleURL)
	c.ThreadTitleModel = strings.TrimSpace(raw.ThreadTitleModel)
	c.GeminiHomeDir = raw.GeminiHomeDir
	c.DataDir = raw.DataDir

	applyDaemonDefaults(c, raw.DaemonIdleTimeout, raw.MaxConcurrentDaemons, raw.MaxBackgroundTaskDuration)

	mcpCfg, err := unmarshalRawMCPConfig(raw.MCPConfig)
	if err != nil {
		return err
	}
	c.MCPConfig = mcpCfg

	mcpServers, err := populateTargetMcpServers(raw.McpServers)
	if err != nil {
		return err
	}
	c.McpServers = mcpServers

	return nil
}

type Config struct {
	current atomic.Pointer[ConfigData]
}

func cloneAmbientContextConfig(src *AmbientContextConfig) *AmbientContextConfig {
	if src == nil {
		return nil
	}
	dst := *src
	if src.Tools != nil {
		dst.Tools = make([]AmbientToolConfig, len(src.Tools))
		for i, t := range src.Tools {
			tool := t
			if t.Arguments != nil {
				tool.Arguments = cloneInterfaceMap(t.Arguments)
			}
			dst.Tools[i] = tool
		}
	}
	return &dst
}

func cloneValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		return cloneInterfaceMap(val)
	case []interface{}:
		res := make([]interface{}, len(val))
		for i, item := range val {
			res[i] = cloneValue(item)
		}
		return res
	default:
		return val
	}
}

func cloneInterfaceMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	res := make(map[string]interface{}, len(m))
	for k, v := range m {
		res[k] = cloneValue(v)
	}
	return res
}

func clonePtr[T any](src *T) *T {
	if src == nil {
		return nil
	}
	v := *src
	return &v
}

func cloneStringSlice(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func cloneRawMessageMap(src map[string]json.RawMessage) map[string]json.RawMessage {
	if src == nil {
		return nil
	}
	dst := make(map[string]json.RawMessage, len(src))
	for k, v := range src {
		dst[k] = append(json.RawMessage(nil), v...)
	}
	return dst
}

func cloneChannelPolicy(p ChannelPolicy) ChannelPolicy {
	cp := p
	cp.IgnoreBots = clonePtr(p.IgnoreBots)
	cp.AmbientWakeThreshold = clonePtr(p.AmbientWakeThreshold)
	cp.Hooks.OnWake = clonePtr(p.Hooks.OnWake)
	cp.Hooks.PreTurn = clonePtr(p.Hooks.PreTurn)
	cp.Hooks.PostTurn = clonePtr(p.Hooks.PostTurn)
	if p.AmbientContext != nil {
		cp.AmbientContext = cloneAmbientContextConfig(p.AmbientContext)
	}
	return cp
}

func cloneConfigData(src *ConfigData) *ConfigData {
	if src == nil {
		return nil
	}
	dst := *src
	dst.AdminUsers = cloneStringSlice(src.AdminUsers)
	if src.Channels != nil {
		dst.Channels = make(map[string]ChannelPolicy, len(src.Channels))
		for k, v := range src.Channels {
			dst.Channels[k] = cloneChannelPolicy(v)
		}
	}
	dst.McpServers = TargetMcpConfig{
		Common:    cloneRawMessageMap(src.McpServers.Common),
		Discord:   cloneRawMessageMap(src.McpServers.Discord),
		Voice:     cloneRawMessageMap(src.McpServers.Voice),
		Ephemeral: cloneRawMessageMap(src.McpServers.Ephemeral),
	}
	dst.MCPServers = cloneMCPServers(src.MCPServers)
	dst.Voice = VoiceConfig{
		Engine:           src.Voice.Engine,
		Model:            src.Voice.Model,
		PrewarmedTargets: cloneStringSlice(src.Voice.PrewarmedTargets),
		AllowedTools:     cloneStringSlice(src.Voice.AllowedTools),
	}
	if dst.Voice.PrewarmedTargets == nil {
		dst.Voice.PrewarmedTargets = []string{}
	}
	if src.Voice.AmbientContext != nil {
		dst.Voice.AmbientContext = cloneAmbientContextConfig(src.Voice.AmbientContext)
	}
	return &dst
}

func DefaultConfigData() *ConfigData {
	defaultIgnoreBots := true
	return &ConfigData{
		Model:         "gemini-3.6-flash-low",
		Timezone:      "America/Los_Angeles",
		SystemChannel: "aerial-dev",
		AdminUsers:    []string{},
		Channels: map[string]ChannelPolicy{
			"default": {
				Mode:       "threads",
				IgnoreBots: &defaultIgnoreBots,
			},
		},
		McpServers: TargetMcpConfig{
			Common:    make(map[string]json.RawMessage),
			Discord:   make(map[string]json.RawMessage),
			Voice:     make(map[string]json.RawMessage),
			Ephemeral: make(map[string]json.RawMessage),
		},
		MCPServers: MCPServersConfig{
			Common:    make(map[string]MCPServerConfig),
			Eri:       make(map[string]MCPServerConfig),
			Voice:     make(map[string]MCPServerConfig),
			Ephemeral: make(map[string]MCPServerConfig),
		},
		Voice: VoiceConfig{
			Engine:           "agy",
			PrewarmedTargets: []string{"kiosk"},
			AllowedTools:     nil,
		},
		Port:               "8080",
		AgyBin:             "agy",
		LowEffortModel:     "gemini-3.8-flash-low",
		MediumEffortModel:  "gemini-3.8-flash-medium",
		ClassifierURL:      "",
		ClassifierModel:    "",
		ClassifierProtocol: "",
		ThreadTitleURL:     "",
		ThreadTitleModel:   "",
		DataDir:            "",
		GeminiHomeDir:      "",
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

// VoicePrewarmedTargets returns a defensive copy of configured voice daemon targets to eagerly pre-warm.
func (c *Config) VoicePrewarmedTargets() []string {
	if c == nil {
		return []string{}
	}
	cur := c.Current()
	if cur == nil || len(cur.Voice.PrewarmedTargets) == 0 {
		return []string{}
	}
	targets := make([]string, len(cur.Voice.PrewarmedTargets))
	copy(targets, cur.Voice.PrewarmedTargets)
	return targets
}

// VoiceAllowedTools returns a defensive copy of configured voice allowed tools on ConfigData.
// A nil return indicates unconstrained (all tools allowed); an empty non-nil slice indicates zero tools allowed.
func (cd *ConfigData) VoiceAllowedTools() []string {
	if cd == nil || cd.Voice.AllowedTools == nil {
		return nil
	}
	tools := make([]string, len(cd.Voice.AllowedTools))
	copy(tools, cd.Voice.AllowedTools)
	return tools
}

// VoiceEngine returns the configured voice engine ("agy" or "gemini_api"), defaulting to "agy".
func (c *Config) VoiceEngine() string {
	if c == nil {
		return "agy"
	}
	return c.Current().VoiceEngine()
}

// VoiceEngine returns the configured voice engine ("agy" or "gemini_api"), defaulting to "agy".
func (c *ConfigData) VoiceEngine() string {
	if c == nil {
		return "agy"
	}
	engine := strings.ToLower(strings.TrimSpace(c.Voice.Engine))
	switch engine {
	case "gemini_api", "gemini", "direct_api":
		return "gemini_api"
	default:
		return "agy"
	}
}

// VoiceAmbientContext returns a deep copy of the configured voice ambient context, or nil if unset.
func (c *ConfigData) VoiceAmbientContext() *AmbientContextConfig {
	if c == nil || c.Voice.AmbientContext == nil {
		return nil
	}
	return cloneAmbientContextConfig(c.Voice.AmbientContext)
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

func ReloadWithSupplier(activeCfg *Config, supplier func() (*Config, error)) error {
	if activeCfg == nil {
		return fmt.Errorf("config: activeCfg cannot be nil")
	}
	if supplier == nil {
		supplier = New
	}
	fresh, err := supplier()
	if err != nil {
		return err
	}
	activeCfg.update(fresh.Current())
	return nil
}

func Reload(activeCfg *Config) error {
	return ReloadWithSupplier(activeCfg, New)
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

func overrideEnvStr(dst *string, lookup func(string) string, keys ...string) {
	for _, k := range keys {
		if v := getEnvFromLookup(lookup, k, ""); v != "" {
			*dst = v
			return
		}
	}
}

func overrideEnvDuration(dst *time.Duration, lookup func(string) string, key string, allowZero bool) {
	if v := getEnvFromLookup(lookup, key, ""); v != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(v)); err == nil {
			if allowZero || d > 0 {
				*dst = d
			}
		}
	}
}

func applyEnvironmentOverrides(data *ConfigData, lookup func(string) string) {
	if data == nil {
		return
	}
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	overrideEnvStr(&data.Port, lookup, "PORT")
	overrideEnvStr(&data.AgyBin, lookup, "AGY_BIN")
	overrideEnvStr(&data.APIKey, lookup, "AGY_GEMINI_API_KEY", "AGY_API_KEY")
	overrideEnvStr(&data.HarnessAPIKey, lookup, "GEMINI_HARNESS_API_KEY", "AERIAL_GEMINI_API_KEY")
	overrideEnvStr(&data.SystemPrompt, lookup, "SYSTEM_PROMPT")
	overrideEnvStr(&data.DiscordToken, lookup, "DISCORD_TOKEN", "DISCORD_BOT_TOKEN")
	overrideEnvStr(&data.GitHubPAT, lookup, "GITHUB_PAT")
	if dsn := buildPostgresDSN(lookup); dsn != "" {
		data.DatabaseURL = dsn
	}
	overrideEnvStr(&data.Model, lookup, "AGY_MODEL")
	overrideEnvStr(&data.Timezone, lookup, "DEFAULT_TIMEZONE", "TZ")
	overrideEnvStr(&data.SystemChannel, lookup, "SYSTEM_CHANNEL")
	overrideEnvStr(&data.LowEffortModel, lookup, "LOW_EFFORT_MODEL", "AMBIENT_CLASSIFIER_MODEL")
	overrideEnvStr(&data.MediumEffortModel, lookup, "MEDIUM_EFFORT_MODEL")
	overrideEnvStr(&data.ClassifierURL, lookup, "CLASSIFIER_URL")
	overrideEnvStr(&data.ClassifierModel, lookup, "CLASSIFIER_MODEL")
	if cp := getEnvFromLookup(lookup, "CLASSIFIER_PROTOCOL", ""); cp != "" {
		data.ClassifierProtocol = strings.ToLower(strings.TrimSpace(cp))
	}
	overrideEnvStr(&data.ThreadTitleURL, lookup, "THREAD_TITLE_URL")
	overrideEnvStr(&data.ThreadTitleModel, lookup, "THREAD_TITLE_MODEL")
	overrideEnvStr(&data.Ollama.BaseURL, lookup, "OLLAMA_URL")
	overrideEnvStr(&data.Ollama.Model, lookup, "EMBEDDING_MODEL", "OLLAMA_EMBEDDING_MODEL")
	overrideEnvStr(&data.Ollama.QueryPrefix, lookup, "EMBEDDING_QUERY_PREFIX")
	if ncStr := getEnvFromLookup(lookup, "OLLAMA_NUM_CTX", getEnvFromLookup(lookup, "EMBEDDING_NUM_CTX", "")); ncStr != "" {
		if nc, err := strconv.Atoi(strings.TrimSpace(ncStr)); err == nil && nc > 0 {
			data.Ollama.NumCtx = nc
		}
	}
	overrideEnvStr(&data.GeminiHomeDir, lookup, "GEMINI_HOME")
	overrideEnvStr(&data.DataDir, lookup, "DATA_DIR")
	overrideEnvStr(&data.MCPConfig, lookup, "MCP_CONFIG")
	overrideEnvStr(&data.OpenObserveURL, lookup, "OPENOBSERVE_URL")
	overrideEnvStr(&data.OpenObserveOrg, lookup, "OPENOBSERVE_ORG")
	overrideEnvStr(&data.OpenObserveUser, lookup, "OPENOBSERVE_ROOT_USER_EMAIL", "OPENOBSERVE_USER")
	overrideEnvStr(&data.OpenObservePassword, lookup, "OPENOBSERVE_ROOT_USER_PASSWORD", "OPENOBSERVE_PASSWORD")
	overrideEnvDuration(&data.DaemonIdleTimeout, lookup, "DAEMON_IDLE_TIMEOUT", false)
	overrideEnvDuration(&data.MaxBackgroundTaskDuration, lookup, "MAX_BACKGROUND_TASK_DURATION", false)
}

var activeGlobalConfig = NewFromData(DefaultConfigData())

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
	var loadedPath string
	var lastErr error
	var data *ConfigData

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
		break
	}

	if loadedPath == "" {
		if lastErr != nil {
			return nil, fmt.Errorf("failed to load configuration from any path in %v: %w", paths, lastErr)
		}
		return nil, fmt.Errorf("no configuration file found in search paths: %v", paths)
	}

	applyEnvironmentOverrides(data, lookup)
	log.Printf("[Config] Successfully loaded configuration from %s (model=%s, timezone=%s, channel=%s)",
		loadedPath, data.Model, data.Timezone, data.SystemChannel)
	return NewFromData(data), nil
}

func LoadConfigFromPaths(paths ...string) (*Config, error) {
	if isContainerColdBoot(paths...) {
		waitForColdBootConfig(paths, 5*time.Second)
	}

	fresh, err := LoadConfigFromLookup(os.Getenv, paths...)
	if err != nil {
		return nil, err
	}
	activeGlobalConfig.update(fresh.Current())
	return activeGlobalConfig, nil
}

func validateChannelPolicy(name string, p *ChannelPolicy, isDefault bool, targetPath string) error {
	prefix := fmt.Sprintf("channel %q", name)
	if isDefault {
		prefix = "channels.default"
	}
	if isDefault || p.Mode != "" {
		m := strings.ToLower(strings.TrimSpace(p.Mode))
		if m != "thread" && m != "threads" && m != "classifier" && m != "mention" && m != "ignore" && m != "channel" && m != "disabled" {
			log.Printf("[Config] Validation error: %s mode must be 'threads', 'channel', 'ignore', or 'disabled' (or 'thread', 'classifier', 'mention'), got %q in %s.", prefix, p.Mode, targetPath)
			return fmt.Errorf("%s mode must be 'threads', 'channel', 'ignore', or 'disabled', got %q", prefix, p.Mode)
		}
		p.Mode = m
	}
	if p.AmbientWakeThreshold != nil {
		if *p.AmbientWakeThreshold < 0.0 || *p.AmbientWakeThreshold > 1.0 {
			log.Printf("[Config] Validation error: %s ambient_wake_threshold must be between 0.0 and 1.0, got %f in %s.", prefix, *p.AmbientWakeThreshold, targetPath)
			return fmt.Errorf("%s ambient_wake_threshold must be between 0.0 and 1.0, got %f", prefix, *p.AmbientWakeThreshold)
		}
	}
	if p.WakeMode != "" {
		switch strings.ToLower(strings.TrimSpace(p.WakeMode)) {
		case "mention", "mentions", "direct", "classifier", "ambient", "all", "always":
			p.WakeMode = p.GetWakeMode()
		default:
			log.Printf("[Config] Validation error: %s wake_mode must be 'mention', 'classifier', or 'all', got %q in %s.", prefix, p.WakeMode, targetPath)
			return fmt.Errorf("%s wake_mode must be 'mention', 'classifier', or 'all', got %q", prefix, p.WakeMode)
		}
	}
	if p.Effort != "" {
		switch eLower := strings.ToLower(strings.TrimSpace(p.Effort)); eLower {
		case "low", "medium", "high":
			p.Effort = eLower
		default:
			log.Printf("[Config] Validation error: %s effort must be 'low', 'medium', or 'high', got %q in %s.", prefix, p.Effort, targetPath)
			return fmt.Errorf("%s effort must be 'low', 'medium', or 'high', got %q", prefix, p.Effort)
		}
	}
	return validateHooks(name, p.Hooks, targetPath)
}

func validateChannels(parsed *ConfigData, targetPath string) error {
	if strings.TrimSpace(parsed.Model) == "" {
		parsed.Model = DefaultConfigData().Model
	}
	if strings.TrimSpace(parsed.LowEffortModel) == "" {
		parsed.LowEffortModel = DefaultConfigData().LowEffortModel
	}
	if strings.TrimSpace(parsed.MediumEffortModel) == "" {
		parsed.MediumEffortModel = DefaultConfigData().MediumEffortModel
	}

	if parsed.ClassifierProtocol != "" {
		pLower := strings.ToLower(strings.TrimSpace(parsed.ClassifierProtocol))
		if pLower != "systemone" && pLower != "ollama" {
			log.Printf("[Config] Validation error: classifier_protocol must be 'systemone' or 'ollama', got %q in %s.", parsed.ClassifierProtocol, targetPath)
			return fmt.Errorf("classifier_protocol must be 'systemone' or 'ollama', got %q", parsed.ClassifierProtocol)
		}
		if pLower == "systemone" && strings.TrimSpace(parsed.ClassifierURL) == "" {
			log.Printf("[Config] Validation error: classifier_url is required when classifier_protocol is 'systemone' in %s.", targetPath)
			return fmt.Errorf("classifier_url is required when classifier_protocol is 'systemone'")
		}
		parsed.ClassifierProtocol = pLower
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

	if err := validateChannelPolicy("default", &defPolicy, true, targetPath); err != nil {
		return err
	}
	parsed.Channels["default"] = defPolicy

	for k, policy := range parsed.Channels {
		if k == "default" {
			continue
		}
		if err := validateChannelPolicy(k, &policy, false, targetPath); err != nil {
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
	for name, ep := range map[string]*WebhookEndpoint{"on_wake": hooks.OnWake, "pre_turn": hooks.PreTurn, "post_turn": hooks.PostTurn} {
		if ep != nil {
			if err := validateWebhookEndpoint(channelName, name, ep, targetPath); err != nil {
				return err
			}
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
		return cloneChannelPolicy(def)
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
	if res.Effort == "" && def.Effort != "" {
		res.Effort = def.Effort
	}
	if res.IgnoreBots == nil {
		res.IgnoreBots = clonePtr(def.IgnoreBots)
	} else {
		res.IgnoreBots = clonePtr(res.IgnoreBots)
	}
	if res.AmbientWakeThreshold == nil {
		res.AmbientWakeThreshold = clonePtr(def.AmbientWakeThreshold)
	} else {
		res.AmbientWakeThreshold = clonePtr(res.AmbientWakeThreshold)
	}
	if res.Hooks.OnWake == nil {
		res.Hooks.OnWake = def.Hooks.OnWake
	}
	if res.Hooks.PreTurn == nil {
		res.Hooks.PreTurn = def.Hooks.PreTurn
	}
	if res.Hooks.PostTurn == nil {
		res.Hooks.PostTurn = def.Hooks.PostTurn
	}
	res.Hooks.OnWake = clonePtr(res.Hooks.OnWake)
	res.Hooks.PreTurn = clonePtr(res.Hooks.PreTurn)
	res.Hooks.PostTurn = clonePtr(res.Hooks.PostTurn)
	if res.AmbientContext != nil {
		res.AmbientContext = cloneAmbientContextConfig(res.AmbientContext)
	} else if def.AmbientContext != nil {
		res.AmbientContext = cloneAmbientContextConfig(def.AmbientContext)
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

func getEnvFromLookup(lookup func(string) string, key, defaultVal string) string {
	if lookup == nil {
		return defaultVal
	}
	if val := lookup(key); val != "" {
		return val
	}
	return defaultVal
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

// NormalizeScopeName standardizes scope names (channels, nodes, devices) for config key and file lookups.
// It trims whitespace, strips any leading '#', lowercases, and strictly blocks directory traversal.
func NormalizeScopeName(scopeName string) string {
	return NormalizeChannelName(scopeName)
}

// LoadScopeInstructions loads per-scope (channel, node, kiosk) instructions from convention-based markdown files.
// It normalizes the scope name, searches ScopeInstructionsDirs with path confinement,
// enforces regular file checks, caps reading at 64KB, and sanitizes instructions closing tags.
func LoadScopeInstructions(scopeName string) string {
	normName := NormalizeScopeName(scopeName)
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

	searchDirs := make([]string, 0, len(ScopeInstructionsDirs)+len(ChannelInstructionsDirs))
	seenDirs := make(map[string]bool)
	for _, d := range append(append([]string{}, ChannelInstructionsDirs...), ScopeInstructionsDirs...) {
		cd := filepath.Clean(d)
		if !seenDirs[cd] {
			seenDirs[cd] = true
			searchDirs = append(searchDirs, cd)
		}
	}

	for _, cleanDir := range searchDirs {
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
				// Defensively escape any closing delimiter tags to prevent breakout
				sanitized := reChannelInstructionsTag.ReplaceAllString(trimmed, "<\\/CHANNEL_INSTRUCTIONS>")
				sanitized = reScopeInstructionsTag.ReplaceAllString(sanitized, "<\\/SCOPE_INSTRUCTIONS>")
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

func cloneMCPServers(src MCPServersConfig) MCPServersConfig {
	return MCPServersConfig{
		Common:    cloneMCPServerConfigMap(src.Common),
		Eri:       cloneMCPServerConfigMap(src.Eri),
		Voice:     cloneMCPServerConfigMap(src.Voice),
		Ephemeral: cloneMCPServerConfigMap(src.Ephemeral),
	}
}

func cloneMCPServerConfigMap(src map[string]MCPServerConfig) map[string]MCPServerConfig {
	if src == nil {
		return nil
	}
	dst := make(map[string]MCPServerConfig, len(src))
	for k, v := range src {
		srv := MCPServerConfig{
			Command:   v.Command,
			ServerURL: v.ServerURL,
		}
		if v.Args != nil {
			srv.Args = append([]string(nil), v.Args...)
		}
		if v.Env != nil {
			srv.Env = make(map[string]string, len(v.Env))
			for ek, ev := range v.Env {
				srv.Env[ek] = ev
			}
		}
		if v.Headers != nil {
			srv.Headers = make(map[string]string, len(v.Headers))
			for hk, hv := range v.Headers {
				srv.Headers[hk] = hv
			}
		}
		dst[k] = srv
	}
	return dst
}

func collectCoreTokens(cur *ConfigData) []string {
	var tokens []string
	if cur.DiscordToken != "" {
		tokens = append(tokens, cur.DiscordToken)
	}
	if cur.Secrets.DiscordToken != "" && cur.Secrets.DiscordToken != cur.DiscordToken {
		tokens = append(tokens, cur.Secrets.DiscordToken)
	}
	if cur.APIKey != "" {
		tokens = append(tokens, cur.APIKey)
	}
	if cur.HarnessAPIKey != "" {
		tokens = append(tokens, cur.HarnessAPIKey)
	}
	if cur.GitHubPAT != "" {
		tokens = append(tokens, cur.GitHubPAT)
	}
	if cur.DatabaseURL != "" {
		if u, err := url.Parse(cur.DatabaseURL); err == nil && u.User != nil {
			if pass, ok := u.User.Password(); ok && pass != "" {
				tokens = append(tokens, pass)
			}
		}
	}
	return tokens
}

func extractMapTokens(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var tokens []string
	for _, k := range keys {
		if v := m[k]; v != "" {
			tokens = append(tokens, v)
		}
	}
	return tokens
}

func extractMCPServerTokens(srv MCPServerConfig) []string {
	var tokens []string
	tokens = append(tokens, extractMapTokens(srv.Env)...)
	tokens = append(tokens, extractMapTokens(srv.Headers)...)
	return tokens
}

func collectMCPServerTokens(serverMaps []map[string]MCPServerConfig) []string {
	var tokens []string
	for _, m := range serverMaps {
		if len(m) == 0 {
			continue
		}
		names := make([]string, 0, len(m))
		for name := range m {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			tokens = append(tokens, extractMCPServerTokens(m[name])...)
		}
	}
	return tokens
}

func collectRawMCPServerTokens(rawMaps []map[string]json.RawMessage) []string {
	var tokens []string
	for _, m := range rawMaps {
		if len(m) == 0 {
			continue
		}
		names := make([]string, 0, len(m))
		for name := range m {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			raw := m[name]
			if len(raw) == 0 {
				continue
			}
			var srv MCPServerConfig
			if err := json.Unmarshal(raw, &srv); err != nil {
				continue
			}
			tokens = append(tokens, extractMCPServerTokens(srv)...)
		}
	}
	return tokens
}

// RegisterConfigTokens extracts secrets from the injected *Config and registers them
// into the global redaction pool.
func RegisterConfigTokens(cfg *Config) {
	if cfg == nil {
		return
	}
	cur := cfg.Current()

	var tokens []string
	tokens = append(tokens, collectCoreTokens(cur)...)
	tokens = append(tokens, collectMCPServerTokens([]map[string]MCPServerConfig{
		cur.MCPServers.Common,
		cur.MCPServers.Eri,
		cur.MCPServers.Voice,
		cur.MCPServers.Ephemeral,
	})...)
	tokens = append(tokens, collectRawMCPServerTokens([]map[string]json.RawMessage{
		cur.McpServers.Common,
		cur.McpServers.Discord,
		cur.McpServers.Voice,
		cur.McpServers.Ephemeral,
	})...)

	if len(tokens) > 0 {
		sanitizer.RegisterSensitiveTokens(tokens...)
	}
}
