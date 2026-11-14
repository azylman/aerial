package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// NewTestConfig returns a hermetic in-memory *Config initialized with DefaultConfigData().
func NewTestConfig(mutators ...func(*ConfigData)) *Config {
	data := DefaultConfigData()
	data.DatabaseURL = ":memory:"
	data.Port = "0"
	data.DiscordToken = ""
	data.APIKey = ""
	data.HarnessAPIKey = ""
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

func (c *Config) getChannelPolicyRaw(key string) (ChannelPolicy, bool) {
	return getChannelPolicyRaw(c.Current().Channels, key)
}

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("writeTempYAML: %v", err)
	}
	return p
}

func mapLookup(kv map[string]string) func(string) string {
	return func(k string) string {
		if kv == nil {
			return ""
		}
		return kv[k]
	}
}

type loadConfigIngestionCase struct {
	name           string
	content        string
	envMap         map[string]string
	wantModel      string
	wantTZ         string
	wantSysChan    string
	wantBotID      string
	wantSynapseURL string
	wantVanadisURL string
	wantHaroURL    string
	wantCovenURL   string
	wantTx         TranscriptsConfig
	wantErr        bool
	errContains    string
}

func assertLoadConfigIngestionValues(t *testing.T, cur *ConfigData, tc loadConfigIngestionCase) {
	t.Helper()
	if tc.wantModel != "" && cur.Model != tc.wantModel {
		t.Errorf("Model = %q, want %q", cur.Model, tc.wantModel)
	}
	if tc.wantTZ != "" && cur.Timezone != tc.wantTZ {
		t.Errorf("Timezone = %q, want %q", cur.Timezone, tc.wantTZ)
	}
	if tc.wantSysChan != "" && cur.SystemChannel != tc.wantSysChan {
		t.Errorf("SystemChannel = %q, want %q", cur.SystemChannel, tc.wantSysChan)
	}
	if tc.wantBotID != "" && cur.BotID != tc.wantBotID {
		t.Errorf("BotID = %q, want %q", cur.BotID, tc.wantBotID)
	}
	if tc.wantSynapseURL != "" && cur.SynapseURL != tc.wantSynapseURL {
		t.Errorf("SynapseURL = %q, want %q", cur.SynapseURL, tc.wantSynapseURL)
	}
	if tc.wantVanadisURL != "" && cur.VanadisURL != tc.wantVanadisURL {
		t.Errorf("VanadisURL = %q, want %q", cur.VanadisURL, tc.wantVanadisURL)
	}
	if tc.wantHaroURL != "" && cur.HaroURL != tc.wantHaroURL {
		t.Errorf("HaroURL = %q, want %q", cur.HaroURL, tc.wantHaroURL)
	}
	if tc.wantCovenURL != "" && cur.CovenURL != tc.wantCovenURL {
		t.Errorf("CovenURL = %q, want %q", cur.CovenURL, tc.wantCovenURL)
	}
	if cur.Transcripts != tc.wantTx {
		t.Errorf("Transcripts = %+v, want %+v", cur.Transcripts, tc.wantTx)
	}
}

func assertLoadConfigIngestionCase(t *testing.T, tc loadConfigIngestionCase) {
	t.Helper()
	lookup := mapLookup(tc.envMap)
	p := writeTempYAML(t, tc.content)
	cfg, err := LoadConfigFromLookup(lookup, p)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", tc.errContains)
		}
		if !strings.Contains(err.Error(), tc.errContains) {
			t.Fatalf("expected error containing %q, got: %v", tc.errContains, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertLoadConfigIngestionValues(t, cfg.Current(), tc)
}

func loadConfigIngestionCases() []loadConfigIngestionCase {
	return []loadConfigIngestionCase{
		{
			name: "valid modern yaml",
			content: `
model: "gemini-1.5-pro"
timezone: "America/New_York"
system_channel: "my-alerts"
channels:
  default:
    mode: "threads"
mcp_servers:
  discord:
    weather:
      serverUrl: "http://weather:8080/mcp"
`,
			wantModel:   "gemini-1.5-pro",
			wantTZ:      "America/New_York",
			wantSysChan: "my-alerts",
		},
		{
			name: "env interpolation",
			content: `
model: "${TEST_EXPAND_MODEL}"
system_channel: "${TEST_EXPAND_CHAN}"
port: "${TEST_EXPAND_PORT}"
channels:
  default:
    mode: "threads"
`,
			envMap: map[string]string{
				"TEST_EXPAND_MODEL": "interpolated-gemini-model",
				"TEST_EXPAND_CHAN":  "interpolated-channel",
				"TEST_EXPAND_PORT":  "9090",
			},
			wantModel:   "interpolated-gemini-model",
			wantSysChan: "interpolated-channel",
		},
		{
			name: "legacy and unknown keys unmarshal safely",
			content: `
model: "gemini-3.7-flash"
timezone: "America/Los_Angeles"
system_channel: "aerial-dev"
git_sync:
  enabled: true
  interval: "30s"
classifier_model: "gemini-test-deprecated"
low_effort_model: "gemini-test-lem"
channels:
  default:
    mode: "threads"
`,
			wantModel:   "gemini-3.7-flash",
			wantTZ:      "America/Los_Angeles",
			wantSysChan: "aerial-dev",
		},
		{
			name: "hashed default channel normalization",
			content: `
channels:
  "#default":
    mode: "channel"
`,
			wantModel: DefaultConfigData().Model,
		},
		{
			name:        "invalid yaml syntax",
			content:     `model: [invalid {::`,
			wantErr:     true,
			errContains: "failed to parse",
		},
		{
			name:        "missing channels section",
			content:     `model: "gemini-flash"`,
			wantErr:     true,
			errContains: "channels.default is required",
		},
		{
			name: "minimal valid channels config",
			content: `
channels:
  default:
    mode: "threads"
`,
			wantModel: DefaultConfigData().Model,
		},
		{
			name: "isolated config with prompt",
			content: `
model: "isolated-model-123"
system_channel: "isolated-channel-456"
channels:
  default:
    mode: "threads"
`,
			envMap:    map[string]string{"SYSTEM_PROMPT": "prompt from lookup"},
			wantModel: "isolated-model-123",
		},
		{
			name: "polymorphic mcp_config string",
			content: `
channels:
  default:
    mode: "threads"
mcp_config: '{"mcpServers":{"s1":{"serverUrl":"http://s1:1000"}}}'
`,
			wantModel: DefaultConfigData().Model,
		},
		{
			name: "polymorphic mcp_config mapping",
			content: `
channels:
  default:
    mode: "threads"
mcp_config:
  mcpServers:
    s2:
      serverUrl: "http://s2:2000"
`,
			wantModel: DefaultConfigData().Model,
		},
		{
			name: "service urls trimming and bot id",
			content: `
model: "gemini-1.5-pro"
bot_id: "  aerial-prime  "
synapse_url: "  http://synapse.internal:4020///  "
vanadis_url: "http://vanadis.internal:4025/  "
haro_url: "  http://haro.internal:4026  "
coven_url: "http://coven.internal:4027/"
transcripts:
  database: "  shared_db  "
  stack: " stack-b "
channels:
  default:
    mode: "threads"
`,
			wantModel:      "gemini-1.5-pro",
			wantBotID:      "aerial-prime",
			wantSynapseURL: "http://synapse.internal:4020",
			wantVanadisURL: "http://vanadis.internal:4025",
			wantHaroURL:    "http://haro.internal:4026",
			wantCovenURL:   "http://coven.internal:4027",
			wantTx:         TranscriptsConfig{Database: "shared_db", Stack: "stack-b"},
		},
	}
}

func TestLoadConfig_Ingestion(t *testing.T) {
	origGlobal := activeGlobalConfig.Current()
	t.Cleanup(func() {
		activeGlobalConfig.update(origGlobal)
	})

	tests := loadConfigIngestionCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertLoadConfigIngestionCase(t, tc)
		})
	}

	t.Setenv("AGY_MODEL", "")
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	_, _ = New()
}

type loadConfigLKGCBootCase struct {
	name          string
	files         []string
	wantModel     string
	wantErr       bool
	errSubstr     string
	testReload    bool
	badReloadYAML string
}

func assertLoadConfigLKGCBootCase(t *testing.T, tc loadConfigLKGCBootCase) {
	t.Helper()
	t.Setenv("AGY_MODEL", "")
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	tmpDir := t.TempDir()
	paths := make([]string, 0, len(tc.files))
	for _, f := range tc.files {
		parts := strings.SplitN(f, ":", 2)
		p := filepath.Join(tmpDir, parts[0])
		if len(parts) == 2 {
			if err := os.WriteFile(p, []byte(parts[1]), 0o600); err != nil {
				t.Fatalf("failed to write %s: %v", p, err)
			}
		}
		paths = append(paths, p)
	}

	cfg, err := LoadConfigFromPaths(paths...)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
		}
		if !strings.Contains(err.Error(), tc.errSubstr) {
			t.Fatalf("expected error containing %q, got: %v", tc.errSubstr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantModel != "" && cfg.Current().Model != tc.wantModel {
		t.Fatalf("got model %q, want %q", cfg.Current().Model, tc.wantModel)
	}

	if tc.testReload {
		primaryPath := paths[0]
		_ = os.WriteFile(primaryPath, []byte(tc.badReloadYAML), 0o600)
		errReload := ReloadWithSupplier(cfg, func() (*Config, error) {
			return LoadConfigFromPaths(primaryPath)
		})
		if errReload == nil {
			t.Fatal("expected ReloadWithSupplier to fail on corrupted file")
		}
		if cfg.Current().Model != tc.wantModel {
			t.Errorf("active config failed to retain LKGC: got %q, want %q", cfg.Current().Model, tc.wantModel)
		}
	}
}

func TestLoadConfig_LKGCAndColdBoot(t *testing.T) {
	origGlobal := activeGlobalConfig.Current()
	t.Cleanup(func() {
		activeGlobalConfig.update(origGlobal)
	})

	tests := []loadConfigLKGCBootCase{
		{
			name:      "missing search paths fails",
			files:     []string{"nonexistent.yaml"},
			wantErr:   true,
			errSubstr: "no configuration file found in search paths",
		},
		{
			name:      "corrupted yaml fails fast",
			files:     []string{"broken.yaml:model: [broken {::"},
			wantErr:   true,
			errSubstr: "failed to load configuration from any path",
		},
		{
			name: "corrupted primary corrupted fallback fails",
			files: []string{
				"p1.yaml:model: [broken1",
				"p2.yaml:model: [broken2",
			},
			wantErr:   true,
			errSubstr: "failed to load configuration from any path",
		},
		{
			name: "stale openobserve keys are ignored",
			files: []string{
				"stale.yaml:model: gemini-stale-keys\nopenobserve_url: http://openobserve:5080/openobserve\nopenobserve_org: default\nopenobserve_user: root@example.com\nchannels:\n  default:\n    mode: threads\n",
			},
			wantModel: "gemini-stale-keys",
		},
		{
			name: "corrupted primary on-disk lkgc fallback succeeds",
			files: []string{
				"primary.yaml:model: [broken",
				"lkgc.yaml:model: gemini-disk-lkgc\nchannels:\n  default:\n    mode: threads\n",
			},
			wantModel: "gemini-disk-lkgc",
		},
		{
			name: "reload corrupt retains in-memory lkgc",
			files: []string{
				"active.yaml:model: gemini-retained\nsystem_channel: retained-alerts\nchannels:\n  default:\n    mode: threads\n",
			},
			wantModel:     "gemini-retained",
			testReload:    true,
			badReloadYAML: "corrupt: [broken:::",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertLoadConfigLKGCBootCase(t, tc)
		})
	}

	tmpDir := t.TempDir()
	validBoot := filepath.Join(tmpDir, "boot.yaml")
	_ = os.WriteFile(validBoot, []byte("model: test\nchannels:\n  default:\n    mode: threads\n"), 0o600)
	waitForColdBootConfig([]string{validBoot}, 500*time.Millisecond)
	waitForColdBootConfig([]string{filepath.Join(tmpDir, "absent")}, 1*time.Millisecond)
	waitForColdBootConfig(nil, 1*time.Millisecond)
	waitForColdBootConfig([]string{filepath.Join(tmpDir, "absent")}, 0)

	_ = isContainerColdBoot(filepath.Join(tmpDir, "absent"))
	_ = isContainerColdBoot(validBoot)

	if err := Reload(nil); err == nil {
		t.Errorf("expected error for Reload(nil)")
	}
	if err := ReloadWithSupplier(nil, nil); err == nil {
		t.Errorf("expected error for ReloadWithSupplier(nil)")
	}
	if err := ReloadWithSupplier(ActiveConfig(), func() (*Config, error) {
		return nil, fmt.Errorf("supplier failed")
	}); err == nil {
		t.Errorf("expected error for ReloadWithSupplier failure")
	}
	if err := ReloadWithSupplier(ActiveConfig(), func() (*Config, error) {
		return NewTestConfig(func(d *ConfigData) { d.Model = "reloaded-hermetic" }), nil
	}); err != nil || ActiveConfig().Current().Model != "reloaded-hermetic" {
		t.Errorf("expected ReloadWithSupplier success, got err=%v model=%q", err, ActiveConfig().Current().Model)
	}
	_ = ReloadWithSupplier(ActiveConfig(), nil)
	_ = Reload(ActiveConfig())

	defaultPaths := []string{"/default/a.yaml", "/default/b.yaml"}
	if got := ResolveConfigSearchPaths(nil, defaultPaths...); len(got) != len(defaultPaths) || got[0] != defaultPaths[0] {
		t.Errorf("ResolveConfigSearchPaths(nil) = %v, want %v", got, defaultPaths)
	}
	if got := ResolveConfigSearchPaths(func(k string) string { return "" }, defaultPaths...); len(got) != len(defaultPaths) || got[0] != defaultPaths[0] {
		t.Errorf("ResolveConfigSearchPaths(empty) = %v, want %v", got, defaultPaths)
	}
	if got := ResolveConfigSearchPaths(func(k string) string {
		if k == "CONFIG_PATH" {
			return "/custom/config.yaml"
		}
		return ""
	}, defaultPaths...); len(got) != len(defaultPaths)+1 || got[0] != "/custom/config.yaml" || got[1] != defaultPaths[0] {
		t.Errorf("ResolveConfigSearchPaths(custom) = %v, want prepended /custom/config.yaml", got)
	}

	t.Setenv("AGY_MODEL", "")
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	t.Setenv("CONFIG_PATH", validBoot)
	newCfg, errNew := New()
	if errNew != nil {
		t.Fatalf("New() with CONFIG_PATH=%s failed: %v", validBoot, errNew)
	}
	if newCfg.Current().Model != "test" {
		t.Errorf("New() got model %q, want %q", newCfg.Current().Model, "test")
	}
}

type channelPolicyResolutionCase struct {
	channels           map[string]ChannelPolicy
	channelID          string
	channelName        string
	wantMode           string
	wantEffort         string
	wantWake           string
	wantIgnore         bool
	wantBotIgn         bool
	wantNoBuildUpdates bool
	wantThreshold      float64
	wantHasHook        bool
}

func assertChannelPolicyResolutionCase(t *testing.T, tc channelPolicyResolutionCase) {
	t.Helper()
	res := ResolveChannelPolicy(tc.channels, tc.channelID, tc.channelName)
	if tc.wantMode != "" && res.Mode != tc.wantMode {
		t.Errorf("Mode = %q, want %q", res.Mode, tc.wantMode)
	}
	if tc.wantEffort != "" && res.GetEffort() != tc.wantEffort {
		t.Errorf("Effort = %q, want %q", res.GetEffort(), tc.wantEffort)
	}
	if tc.wantWake != "" && res.GetWakeMode() != tc.wantWake {
		t.Errorf("WakeMode = %q, want %q", res.GetWakeMode(), tc.wantWake)
	}
	if res.IsIgnored() != tc.wantIgnore {
		t.Errorf("IsIgnored = %v, want %v", res.IsIgnored(), tc.wantIgnore)
	}
	if res.IsBotIgnored() != tc.wantBotIgn {
		t.Errorf("IsBotIgnored = %v, want %v", res.IsBotIgnored(), tc.wantBotIgn)
	}
	if res.NoBuildUpdates != tc.wantNoBuildUpdates {
		t.Errorf("NoBuildUpdates = %v, want %v", res.NoBuildUpdates, tc.wantNoBuildUpdates)
	}
	if tc.wantThreshold > 0 && res.GetAmbientWakeThreshold() != tc.wantThreshold {
		t.Errorf("AmbientWakeThreshold = %v, want %v", res.GetAmbientWakeThreshold(), tc.wantThreshold)
	}
	if tc.wantHasHook && (res.Hooks.OnWake == nil || res.Hooks.OnWake.URL == "") {
		t.Errorf("expected inherited OnWake hook, got nil")
	}
}

type channelPolicyValidationCase struct {
	name        string
	yaml        string
	expectedErr string
}

func channelPolicyValidationCases() []channelPolicyValidationCase {
	return []channelPolicyValidationCase{
		{
			name:        "missing channels",
			yaml:        `model: "gemini-flash"`,
			expectedErr: "channels.default is required",
		},
		{
			name: "missing default channel",
			yaml: `
channels:
  general:
    mode: "channel"
`,
			expectedErr: "channels.default is required",
		},
		{
			name: "invalid default mode",
			yaml: `
channels:
  default:
    mode: "unsupported_mode"
`,
			expectedErr: "mode must be 'threads', 'channel', 'ignore', or 'disabled'",
		},
		{
			name: "invalid non-default mode",
			yaml: `
channels:
  default:
    mode: "threads"
  lounge:
    mode: "invalid_mode"
`,
			expectedErr: "mode must be 'threads', 'channel', 'ignore', or 'disabled'",
		},
		{
			name: "negative default ambient threshold",
			yaml: `
channels:
  default:
    mode: "threads"
    ambient_wake_threshold: -0.1
`,
			expectedErr: "channels.default ambient_wake_threshold must be between 0.0 and 1.0",
		},
		{
			name: "excessive non-default ambient threshold",
			yaml: `
channels:
  default:
    mode: "threads"
  lounge:
    mode: "channel"
    ambient_wake_threshold: 1.5
`,
			expectedErr: "channel \"lounge\" ambient_wake_threshold must be between 0.0 and 1.0",
		},
		{
			name: "invalid default wake_mode",
			yaml: `
channels:
  default:
    mode: "channel"
    wake_mode: "unsupported_wake"
`,
			expectedErr: "wake_mode must be 'mention', 'classifier', or 'all'",
		},
		{
			name: "invalid non-default wake_mode",
			yaml: `
channels:
  default:
    mode: "channel"
  lounge:
    wake_mode: "unsupported_wake"
`,
			expectedErr: "wake_mode must be 'mention', 'classifier', or 'all'",
		},
		{
			name: "invalid classifier_protocol",
			yaml: `
classifier_protocol: "unsupported"
channels:
  default:
    mode: "threads"
`,
			expectedErr: "classifier_protocol must be 'systemone' or 'ollama'",
		},
		{
			name: "systemone protocol missing url",
			yaml: `
classifier_protocol: "systemone"
channels:
  default:
    mode: "threads"
`,
			expectedErr: "classifier_url is required when classifier_protocol is 'systemone'",
		},
		{
			name: "empty webhook url",
			yaml: `
channels:
  default:
    mode: "threads"
    hooks:
      on_wake:
        url: ""
`,
			expectedErr: "webhook url is required",
		},
		{
			name: "invalid webhook url scheme",
			yaml: `
channels:
  default:
    mode: "threads"
    hooks:
      on_wake:
        url: "ftp://example.com"
`,
			expectedErr: "must be a valid http or https URL",
		},
		{
			name: "negative webhook timeout",
			yaml: `
channels:
  default:
    mode: "threads"
    hooks:
      on_wake:
        url: "https://example.com"
        timeout_ms: -1
`,
			expectedErr: "webhook timeout_ms cannot be negative",
		},
		{
			name:        "invalid channel effort",
			yaml:        "channels:\n  default:\n    mode: threads\n    effort: ultra\n",
			expectedErr: "effort must be 'low', 'medium', or 'high'",
		},
	}
}

func runChannelPolicyValidationTests(t *testing.T) {
	t.Helper()
	validationTests := channelPolicyValidationCases()
	for _, tc := range validationTests {
		t.Run("validate_"+tc.name, func(t *testing.T) {
			p := writeTempYAML(t, tc.yaml)
			_, err := LoadConfigFromLookup(nil, p)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.expectedErr)
			}
			if !strings.Contains(err.Error(), tc.expectedErr) {
				t.Errorf("expected error containing %q, got: %v", tc.expectedErr, err)
			}
		})
	}
}

func channelPolicyResolutionCases(channels map[string]ChannelPolicy) []channelPolicyResolutionCase {
	return []channelPolicyResolutionCase{
		{
			channels:           channels,
			channelID:          "12345",
			channelName:        "lounge",
			wantMode:           "mention",
			wantEffort:         "medium",
			wantWake:           "mention",
			wantBotIgn:         true,
			wantNoBuildUpdates: true,
			wantThreshold:      0.75,
			wantHasHook:        true,
		},
		{
			channels:      channels,
			channelID:     "1543668253363150928",
			channelName:   "dev",
			wantMode:      "channel",
			wantEffort:    "medium",
			wantWake:      "mention",
			wantBotIgn:    false,
			wantThreshold: 0.75,
			wantHasHook:   true,
		},
		{
			channels:      channels,
			channelID:     "99999",
			channelName:   "#General",
			wantMode:      "channel",
			wantEffort:    "low",
			wantWake:      "classifier",
			wantBotIgn:    true,
			wantThreshold: 0.85,
			wantHasHook:   true,
		},
		{
			channels:      channels,
			channelID:     "88888",
			channelName:   "random-topic",
			wantMode:      "threads",
			wantEffort:    "medium",
			wantWake:      "mention",
			wantBotIgn:    true,
			wantThreshold: 0.75,
			wantHasHook:   true,
		},
		{
			channels:    channels,
			channelID:   "77777",
			channelName: "ignored-channel",
			wantMode:    "ignore",
			wantIgnore:  true,
		},
		{
			channels:    channels,
			channelID:   "66666",
			channelName: "disabled-channel",
			wantMode:    "disabled",
			wantIgnore:  true,
		},
		{
			channels:      nil,
			channelID:     "",
			channelName:   "any-chan",
			wantMode:      "threads",
			wantEffort:    "high",
			wantBotIgn:    true,
			wantThreshold: 0.0,
		},
	}
}

func runChannelPolicyResolutionTests(t *testing.T) {
	t.Helper()
	thresh := 0.75
	channels := map[string]ChannelPolicy{
		"default": {
			Mode:                 "threads",
			Effort:               "medium",
			IgnoreBots:           new(true),
			WakeMode:             "mention",
			AmbientWakeThreshold: &thresh,
			Hooks: ChannelHooksConfig{
				OnWake:   &WebhookEndpoint{URL: "https://example.com/wake", TimeoutMs: 200, OnTimeoutAction: "ignore"},
				PreTurn:  &WebhookEndpoint{URL: "https://example.com/pre", TimeoutMs: 300},
				PostTurn: &WebhookEndpoint{URL: "https://example.com/post", TimeoutMs: 400},
			},
		},
		"general": {
			Mode:                 "channel",
			Effort:               "low",
			WakeMode:             "classifier",
			AmbientWakeThreshold: new(0.85),
		},
		"aerial-dev": {
			IgnoreBots: new(false),
		},
		"lounge": {
			Mode:           "mention",
			NoBuildUpdates: true,
		},
		"1543668253363150928": {
			Mode:       "channel",
			IgnoreBots: new(false),
		},
		"ignored-channel": {
			Mode: "ignore",
		},
		"disabled-channel": {
			Mode: "disabled",
		},
	}

	resolutionTests := channelPolicyResolutionCases(channels)
	for _, tc := range resolutionTests {
		t.Run("resolve_"+tc.channelName, func(t *testing.T) {
			assertChannelPolicyResolutionCase(t, tc)
		})
	}

	if _, ok := getChannelPolicyRaw(nil, "default"); ok {
		t.Errorf("expected getChannelPolicyRaw(nil) to return false")
	}
	if _, ok := getChannelPolicyRaw(channels, ""); ok {
		t.Errorf("expected getChannelPolicyRaw('', empty) to return false")
	}

	cfgWrap := NewFromData(&ConfigData{Channels: channels})
	if _, ok := cfgWrap.getChannelPolicyRaw("general"); !ok {
		t.Errorf("expected cfgWrap.getChannelPolicyRaw to find general")
	}
	resWrap := cfgWrap.ResolveChannelPolicy("1543668253363150928", "dev")
	if resWrap.Mode != "channel" {
		t.Errorf("cfgWrap.ResolveChannelPolicy mode = %q, want 'channel'", resWrap.Mode)
	}

	defaultBlockedChannels := map[string]ChannelPolicy{
		"default": {
			Mode:           "threads",
			NoBuildUpdates: true,
		},
		"open": {
			Mode: "channel",
		},
	}
	resInherited := ResolveChannelPolicy(defaultBlockedChannels, "any-id", "open")
	if !resInherited.NoBuildUpdates {
		t.Errorf("expected NoBuildUpdates to be inherited from default")
	}

	hooks := channels["default"].Hooks
	if hooks.OnWake == nil || hooks.OnWake.GetTimeout() != 200*time.Millisecond {
		t.Errorf("GetTimeout() unexpected value")
	}
	if hooks.OnWake.GetOnTimeout("drop") != "ignore" {
		t.Errorf("GetOnTimeout() unexpected value: %q", hooks.OnWake.GetOnTimeout("drop"))
	}
	var nilEndpoint *WebhookEndpoint
	if nilEndpoint.GetTimeout() != 3000*time.Millisecond || nilEndpoint.GetOnTimeout("fallback") != "fallback" {
		t.Errorf("nil WebhookEndpoint methods unexpected")
	}
}

func TestChannelPolicy_ValidationAndResolution(t *testing.T) {
	runChannelPolicyValidationTests(t)
	runChannelPolicyResolutionTests(t)
}

type envOverrideCase struct {
	name             string
	envMap           map[string]string
	wantPort         string
	wantBin          string
	wantEngine       string
	wantClaude       string
	wantKey          string
	wantModel        string
	wantTZ           string
	wantChan         string
	wantDiscordToken string
	wantIdle         time.Duration
	wantMaxDur       time.Duration
	wantCtx          int
}

func assertEnvOverrideBasic(t *testing.T, data *ConfigData, tc envOverrideCase) {
	t.Helper()
	if tc.wantPort != "" && data.Port != tc.wantPort {
		t.Errorf("Port = %q, want %q", data.Port, tc.wantPort)
	}
	if tc.wantBin != "" && data.AgyBin != tc.wantBin {
		t.Errorf("AgyBin = %q, want %q", data.AgyBin, tc.wantBin)
	}
	if tc.wantEngine != "" && data.Engine != tc.wantEngine {
		t.Errorf("Engine = %q, want %q", data.Engine, tc.wantEngine)
	}
	if tc.wantClaude != "" && data.ClaudeBin != tc.wantClaude {
		t.Errorf("ClaudeBin = %q, want %q", data.ClaudeBin, tc.wantClaude)
	}
	if tc.wantKey != "" && data.APIKey != tc.wantKey {
		t.Errorf("APIKey = %q, want %q", data.APIKey, tc.wantKey)
	}
	if tc.wantModel != "" && data.Model != tc.wantModel {
		t.Errorf("Model = %q, want %q", data.Model, tc.wantModel)
	}
}

func assertEnvOverrideExtended(t *testing.T, data *ConfigData, tc envOverrideCase) {
	t.Helper()
	if tc.wantTZ != "" && data.Timezone != tc.wantTZ {
		t.Errorf("Timezone = %q, want %q", data.Timezone, tc.wantTZ)
	}
	if tc.wantChan != "" && data.SystemChannel != tc.wantChan {
		t.Errorf("SystemChannel = %q, want %q", data.SystemChannel, tc.wantChan)
	}
	if tc.wantDiscordToken != "" && data.DiscordToken != tc.wantDiscordToken {
		t.Errorf("DiscordToken = %q, want %q", data.DiscordToken, tc.wantDiscordToken)
	}
	if tc.wantIdle > 0 && data.DaemonIdleTimeout != tc.wantIdle {
		t.Errorf("DaemonIdleTimeout = %v, want %v", data.DaemonIdleTimeout, tc.wantIdle)
	}
	if tc.wantMaxDur > 0 && data.MaxBackgroundTaskDuration != tc.wantMaxDur {
		t.Errorf("MaxBackgroundTaskDuration = %v, want %v", data.MaxBackgroundTaskDuration, tc.wantMaxDur)
	}
	if tc.wantCtx > 0 && data.Ollama.NumCtx != tc.wantCtx {
		t.Errorf("Ollama.NumCtx = %v, want %v", data.Ollama.NumCtx, tc.wantCtx)
	}
}

func assertEnvOverrideCase(t *testing.T, tc envOverrideCase) {
	t.Helper()
	data := DefaultConfigData()
	applyEnvironmentOverrides(data, mapLookup(tc.envMap))
	assertEnvOverrideBasic(t, data, tc)
	assertEnvOverrideExtended(t, data, tc)
}

func TestConfig_EnvironmentAndDSN(t *testing.T) {
	lookupTests := []struct {
		name     string
		lookup   map[string]string
		key      string
		defVal   string
		expected string
		isNil    bool
	}{
		{"existing key", map[string]string{"K": "v1"}, "K", "def", "v1", false},
		{"missing key", map[string]string{"K": "v1"}, "MISSING", "def", "def", false},
		{"nil lookup", nil, "K", "def", "def", true},
	}
	for _, tc := range lookupTests {
		t.Run("lookup_"+tc.name, func(t *testing.T) {
			var fn func(string) string
			if !tc.isNil {
				fn = mapLookup(tc.lookup)
			}
			if got := getEnvFromLookup(fn, tc.key, tc.defVal); got != tc.expected {
				t.Errorf("getEnvFromLookup() = %q, want %q", got, tc.expected)
			}
		})
	}

	dsnTests := []struct {
		name     string
		envMap   map[string]string
		expected string
	}{
		{
			name:     "unset host returns empty",
			envMap:   map[string]string{},
			expected: "",
		},
		{
			name:     "host only uses defaults",
			envMap:   map[string]string{"POSTGRES_HOST": "db.internal"},
			expected: "postgres://aerial:aerial_secure_pass@db.internal:5432/aerial?sslmode=disable",
		},
		{
			name: "full custom postgres overrides",
			envMap: map[string]string{
				"POSTGRES_HOST":     "custom-pg",
				"POSTGRES_PORT":     "5433",
				"POSTGRES_USER":     "myuser",
				"POSTGRES_PASSWORD": "mypassword",
				"POSTGRES_DB":       "mydb",
			},
			expected: "postgres://myuser:mypassword@custom-pg:5433/mydb?sslmode=disable",
		},
		{
			name: "database url direct precedence",
			envMap: map[string]string{
				"DATABASE_URL":  "postgres://direct:conn@db:5432/app",
				"POSTGRES_HOST": "custom-pg",
			},
			expected: "postgres://direct:conn@db:5432/app",
		},
	}
	for _, tc := range dsnTests {
		t.Run("dsn_"+tc.name, func(t *testing.T) {
			got := buildPostgresDSN(mapLookup(tc.envMap))
			if got != tc.expected {
				t.Errorf("buildPostgresDSN() = %q, want %q", got, tc.expected)
			}
		})
	}

	overrideTests := []envOverrideCase{
		{
			name: "primary env vars",
			envMap: map[string]string{
				"PORT":                         "8081",
				"AGY_BIN":                      "/bin/agy",
				"AGY_GEMINI_API_KEY":           "primary-key",
				"AGY_MODEL":                    "gemini-custom",
				"DEFAULT_TIMEZONE":             "UTC",
				"SYSTEM_CHANNEL_ID":            "alerts",
				"ENGINE":                       "claude",
				"CLAUDE_BIN":                   "/usr/local/bin/claude",
				"DAEMON_IDLE_TIMEOUT":          "15m",
				"MAX_BACKGROUND_TASK_DURATION": "45m",
				"OLLAMA_NUM_CTX":               "8192",
			},
			wantPort:   "8081",
			wantBin:    "/bin/agy",
			wantEngine: "claude",
			wantClaude: "/usr/local/bin/claude",
			wantKey:    "primary-key",
			wantModel:  "gemini-custom",
			wantTZ:     "UTC",
			wantChan:   "alerts",
			wantIdle:   15 * time.Minute,
			wantMaxDur: 45 * time.Minute,
			wantCtx:    8192,
		},
		{
			name: "tz fallback env var",
			envMap: map[string]string{
				"TZ": "America/Chicago",
			},
			wantTZ: "America/Chicago",
		},
		{
			name: "discord bot token fallback",
			envMap: map[string]string{
				"DISCORD_BOT_TOKEN": "fallback-bot-token-123",
			},
			wantDiscordToken: "fallback-bot-token-123",
		},
	}
	for _, tc := range overrideTests {
		t.Run("override_"+tc.name, func(t *testing.T) {
			assertEnvOverrideCase(t, tc)
		})
	}

	applyEnvironmentOverrides(nil, mapLookup(nil))
	applyEnvironmentOverrides(DefaultConfigData(), nil)
}

func assertConfigBasics(t *testing.T, nilCfg *Config) {
	t.Helper()
	if nilCfg.Current() == nil || (&Config{}).Current() == nil || NewFromData(nil).Current() == nil {
		t.Errorf("expected DefaultConfigData on nil/empty Config, got nil")
	}
	if _, ok := nilCfg.getChannelPolicyRaw("nonexistent"); ok {
		t.Errorf("expected false from nilCfg.getChannelPolicyRaw")
	}
	if nilCfg.Get() == nil {
		t.Errorf("expected DefaultConfigData from nilCfg.Get()")
	}
	if nilCfg.VoiceEngine() != "agy" {
		t.Errorf("expected agy voice engine on nil Config, got %q", nilCfg.VoiceEngine())
	}
	if nilCfg.VoicePrewarmedTargets() == nil {
		t.Errorf("expected non-nil prewarmed targets on nil Config")
	}
}

func runConfigAccessors(t *testing.T) {
	t.Helper()
	var nilCfg *Config
	assertConfigBasics(t, nilCfg)
	origGlobal := ActiveConfig().Current()
	ActiveConfig().Update(&ConfigData{Timezone: "", SystemChannel: ""})
	if ActiveConfig().Current().SystemChannel != "" || ActiveConfig().Current().Timezone != "" {
		t.Errorf("expected empty on updated ActiveConfig")
	}
	ActiveConfig().Update(&ConfigData{Timezone: "UTC", SystemChannel: "custom-sys"})
	if ActiveConfig().Current().SystemChannel != "custom-sys" || ActiveConfig().Current().Timezone != "UTC" {
		t.Errorf("expected custom values on ActiveConfig")
	}
	ActiveConfig().Update(origGlobal)
	if nilCfg.GeminiHomeDir() != "" || nilCfg.DataDir() != "" {
		t.Errorf("expected empty dirs on nil Config")
	}
	var nilData *ConfigData
	if nilData.VoiceEngine() != "agy" || nilData.VoiceAmbientContext() != nil {
		t.Errorf("unexpected nil ConfigData responses")
	}
	if nilData.VoiceAllowedTools() != nil {
		t.Errorf("expected nil tools on nil ConfigData")
	}
	if cloneConfigData(nil) != nil {
		t.Errorf("expected nil from cloneConfigData(nil)")
	}

	testCfg := NewTestConfig(func(d *ConfigData) {
		d.Model = "test-model-override"
	})
	testCfg.Update(nil)
	nilCfg.Update(DefaultConfigData())
	cur := testCfg.Current()
	if cur.Model != "test-model-override" || cur.DatabaseURL != ":memory:" || cur.Port != "0" {
		t.Errorf("NewTestConfig unexpected values: %+v", cur)
	}
	if testCfg.GeminiHomeDir() != cur.GeminiHomeDir || testCfg.DataDir() != cur.DataDir {
		t.Errorf("dirs mismatch: %q vs %q", testCfg.DataDir(), cur.DataDir)
	}
	testCfg.Update(&ConfigData{Model: "updated-model"})
	if testCfg.Current().Model != "updated-model" {
		t.Errorf("Update failed: got %q", testCfg.Current().Model)
	}
}

func runConfigConcurrency(t *testing.T) {
	t.Helper()
	origCD := &ConfigData{
		AdminUsers: []string{"u1"},
		Channels:   map[string]ChannelPolicy{"default": {Mode: "threads"}},
		McpServers: TargetMcpConfig{
			Discord: map[string]json.RawMessage{"s": json.RawMessage(`{}`)},
		},
	}
	clonedCD := cloneConfigData(origCD)
	origCD.AdminUsers[0] = "mutated"
	if clonedCD.AdminUsers[0] == "mutated" {
		t.Errorf("slice backing array shared after clone")
	}
	origCD.Channels["mutated"] = ChannelPolicy{Mode: "channel"}
	if _, exists := clonedCD.Channels["mutated"]; exists {
		t.Errorf("channels map shared after clone")
	}

	raceCfg := NewFromData(&ConfigData{
		Model:    "init",
		Channels: map[string]ChannelPolicy{"default": {Mode: "threads"}},
	})
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 50 {
				cur := raceCfg.Current()
				_ = cur.Model
				_ = cur.Channels["default"].Mode
			}
		})
	}
	for i := range 4 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := range 25 {
				raceCfg.update(&ConfigData{
					Model:    fmt.Sprintf("m-%d-%d", idx, j),
					Channels: map[string]ChannelPolicy{"default": {Mode: "threads"}},
				})
			}
		}(i)
	}
	wg.Wait()

	_ = GetRuntimeConfig()
	origG := activeGlobalConfig.Current()
	activeGlobalConfig.update(&ConfigData{
		Timezone:      "America/New_York",
		SystemChannel: "ops-channel",
	})
	if GetTimezone() != "America/New_York" || activeGlobalConfig.Current().SystemChannel != "ops-channel" {
		t.Errorf("expected updated timezone and channel from globals")
	}
	activeGlobalConfig.update(origG)

	cfgWithAmbient := NewFromData(&ConfigData{
		Voice: VoiceConfig{
			AmbientContext: &AmbientContextConfig{CacheTTL: "5m"},
		},
	})
	if cfgWithAmbient.Current().VoiceAmbientContext() == nil {
		t.Errorf("expected non-nil voice ambient context")
	}

	_ = LoadScopeInstructions("general")
}

func TestConfig_AccessorsAndConcurrency(t *testing.T) {
	adminTests := []struct {
		name       string
		adminUsers []string
		ids        []string
		wantAdmin  bool
	}{
		{"exact snowflake", []string{"1234567890"}, []string{"1234567890"}, true},
		{"exact username", []string{"testadmin"}, []string{"testadmin"}, true},
		{"case and @ prefix", []string{"@AliceAdmin"}, []string{"aliceadmin"}, true},
		{"variadic match", []string{"admin1"}, []string{"user", "admin1", "other"}, true},
		{"no match", []string{"admin1"}, []string{"regular-user"}, false},
		{"empty identifiers", []string{"admin1"}, []string{"", "  ", "@"}, false},
		{"nil admin list", nil, []string{"admin1"}, false},
	}
	for _, tc := range adminTests {
		t.Run("admin_"+tc.name, func(t *testing.T) {
			if got := IsAdmin(tc.adminUsers, tc.ids...); got != tc.wantAdmin {
				t.Errorf("IsAdmin(%v, %v) = %v, want %v", tc.adminUsers, tc.ids, got, tc.wantAdmin)
			}
			cfg := NewFromData(&ConfigData{AdminUsers: tc.adminUsers})
			if got := cfg.IsAdmin(tc.ids...); got != tc.wantAdmin {
				t.Errorf("cfg.IsAdmin() = %v, want %v", got, tc.wantAdmin)
			}
		})
	}

	normTests := []struct {
		input    string
		expected string
	}{
		{"general", "general"},
		{"#general", "general"},
		{"#Dev Chat", "dev chat"},
		{"../../etc/passwd", ""},
		{"/secret", ""},
		{"..\\windows", ""},
		{".", ""},
		{"", ""},
	}
	for _, tc := range normTests {
		t.Run("norm_"+tc.input, func(t *testing.T) {
			if got := NormalizeChannelName(tc.input); got != tc.expected {
				t.Errorf("NormalizeChannelName(%q) = %q, want %q", tc.input, got, tc.expected)
			}
			if got := NormalizeScopeName(tc.input); got != tc.expected {
				t.Errorf("NormalizeScopeName(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}

	policyTests := []struct {
		mode       string
		wantIgnore bool
		wantThread bool
	}{
		{"ignore", true, false},
		{"disabled", true, false},
		{"threads", false, true},
		{"thread", false, true},
		{"channel", false, false},
	}
	for _, tc := range policyTests {
		t.Run("policy_"+tc.mode, func(t *testing.T) {
			p := ChannelPolicy{Mode: tc.mode}
			if p.IsIgnored() != tc.wantIgnore {
				t.Errorf("p.IsIgnored() = %v, want %v", p.IsIgnored(), tc.wantIgnore)
			}
			if p.IsThread() != tc.wantThread {
				t.Errorf("p.IsThread() = %v, want %v", p.IsThread(), tc.wantThread)
			}
		})
	}

	runConfigAccessors(t)
	runConfigConcurrency(t)
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
