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

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/sanitizer"
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

func TestLoadConfig_Ingestion(t *testing.T) {
	origGlobal := activeGlobalConfig.Current()
	t.Cleanup(func() {
		activeGlobalConfig.update(origGlobal)
	})

	tests := []struct {
		name        string
		content     string
		envMap      map[string]string
		wantModel   string
		wantTZ      string
		wantSysChan string
		wantErr     bool
		errContains string
	}{
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
			cur := cfg.Current()
			if tc.wantModel != "" && cur.Model != tc.wantModel {
				t.Errorf("Model = %q, want %q", cur.Model, tc.wantModel)
			}
			if tc.wantTZ != "" && cur.Timezone != tc.wantTZ {
				t.Errorf("Timezone = %q, want %q", cur.Timezone, tc.wantTZ)
			}
			if tc.wantSysChan != "" && cur.SystemChannel != tc.wantSysChan {
				t.Errorf("SystemChannel = %q, want %q", cur.SystemChannel, tc.wantSysChan)
			}
		})
	}

	t.Setenv("AGY_MODEL", "")
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	_, _ = New()
}

func TestLoadConfig_LKGCAndColdBoot(t *testing.T) {
	origGlobal := activeGlobalConfig.Current()
	t.Cleanup(func() {
		activeGlobalConfig.update(origGlobal)
	})

	tests := []struct {
		name          string
		files         []string
		wantModel     string
		wantErr       bool
		errSubstr     string
		testReload    bool
		badReloadYAML string
	}{
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
			t.Setenv("AGY_MODEL", "")
			t.Setenv("DEFAULT_TIMEZONE", "")
			t.Setenv("TZ", "")
			tmpDir := t.TempDir()
			var paths []string
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
		})
	}

	tmpDir := t.TempDir()
	validBoot := filepath.Join(tmpDir, "boot.yaml")
	_ = os.WriteFile(validBoot, []byte("model: test\n"), 0o600)
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

	t.Setenv("AGY_MODEL", "")
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	_, _ = New()
}

func TestChannelPolicy_ValidationAndResolution(t *testing.T) {
	validationTests := []struct {
		name        string
		yaml        string
		expectedErr string
	}{
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
			Mode:            "mention",
			BlockBuildCards: true,
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

	resolutionTests := []struct {
		name          string
		channels      map[string]ChannelPolicy
		channelID     string
		channelName   string
		wantMode      string
		wantEffort    string
		wantWake      string
		wantIgnore    bool
		wantBotIgn    bool
		wantBlockCard bool
		wantThreshold float64
		wantHasHook   bool
	}{
		{
			name:          "match lounge with block build cards",
			channels:      channels,
			channelID:     "12345",
			channelName:   "lounge",
			wantMode:      "mention",
			wantEffort:    "medium",
			wantWake:      "mention",
			wantBotIgn:    true,
			wantBlockCard: true,
			wantThreshold: 0.75,
			wantHasHook:   true,
		},
		{
			name:          "match snowflake id",
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
			name:          "match channel name case insensitive with hash",
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
			name:          "unmatched falls back to default",
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
			name:        "ignored mode stops resolution",
			channels:    channels,
			channelID:   "77777",
			channelName: "ignored-channel",
			wantMode:    "ignore",
			wantIgnore:  true,
		},
		{
			name:        "disabled mode stops resolution",
			channels:    channels,
			channelID:   "66666",
			channelName: "disabled-channel",
			wantMode:    "disabled",
			wantIgnore:  true,
		},
		{
			name:          "nil map fallback",
			channels:      nil,
			channelID:     "",
			channelName:   "any-chan",
			wantMode:      "threads",
			wantEffort:    "high",
			wantBotIgn:    true,
			wantThreshold: 0.0,
		},
	}

	for _, tc := range resolutionTests {
		t.Run("resolve_"+tc.name, func(t *testing.T) {
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
			if res.BlockBuildCards != tc.wantBlockCard {
				t.Errorf("BlockBuildCards = %v, want %v", res.BlockBuildCards, tc.wantBlockCard)
			}
			if tc.wantThreshold > 0 && res.GetAmbientWakeThreshold() != tc.wantThreshold {
				t.Errorf("AmbientWakeThreshold = %v, want %v", res.GetAmbientWakeThreshold(), tc.wantThreshold)
			}
			if tc.wantHasHook && (res.Hooks.OnWake == nil || res.Hooks.OnWake.URL == "") {
				t.Errorf("expected inherited OnWake hook, got nil")
			}
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
			Mode:            "threads",
			BlockBuildCards: true,
		},
		"open": {
			Mode: "channel",
		},
	}
	resInherited := ResolveChannelPolicy(defaultBlockedChannels, "any-id", "open")
	if !resInherited.BlockBuildCards {
		t.Errorf("expected BlockBuildCards to be inherited from default")
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

	overrideTests := []struct {
		name       string
		envMap     map[string]string
		wantPort   string
		wantBin    string
		wantKey    string
		wantModel  string
		wantTZ     string
		wantChan   string
		wantOrg    string
		wantUser   string
		wantIdle   time.Duration
		wantMaxDur time.Duration
		wantCtx    int
	}{
		{
			name: "primary env vars",
			envMap: map[string]string{
				"PORT":                         "8081",
				"AGY_BIN":                      "/bin/agy",
				"AGY_GEMINI_API_KEY":           "primary-key",
				"AGY_MODEL":                    "gemini-custom",
				"DEFAULT_TIMEZONE":             "UTC",
				"SYSTEM_CHANNEL_ID":            "alerts",
				"OPENOBSERVE_ORG":              "custom-org",
				"OPENOBSERVE_USER":             "root@domain.com",
				"DAEMON_IDLE_TIMEOUT":          "15m",
				"MAX_BACKGROUND_TASK_DURATION": "45m",
				"OLLAMA_NUM_CTX":               "8192",
			},
			wantPort:   "8081",
			wantBin:    "/bin/agy",
			wantKey:    "primary-key",
			wantModel:  "gemini-custom",
			wantTZ:     "UTC",
			wantChan:   "alerts",
			wantOrg:    "custom-org",
			wantUser:   "root@domain.com",
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
	}
	for _, tc := range overrideTests {
		t.Run("override_"+tc.name, func(t *testing.T) {
			data := DefaultConfigData()
			applyEnvironmentOverrides(data, mapLookup(tc.envMap))
			if tc.wantPort != "" && data.Port != tc.wantPort {
				t.Errorf("Port = %q, want %q", data.Port, tc.wantPort)
			}
			if tc.wantBin != "" && data.AgyBin != tc.wantBin {
				t.Errorf("AgyBin = %q, want %q", data.AgyBin, tc.wantBin)
			}
			if tc.wantKey != "" && data.APIKey != tc.wantKey {
				t.Errorf("APIKey = %q, want %q", data.APIKey, tc.wantKey)
			}
			if tc.wantModel != "" && data.Model != tc.wantModel {
				t.Errorf("Model = %q, want %q", data.Model, tc.wantModel)
			}
			if tc.wantTZ != "" && data.Timezone != tc.wantTZ {
				t.Errorf("Timezone = %q, want %q", data.Timezone, tc.wantTZ)
			}
			if tc.wantChan != "" && data.SystemChannel != tc.wantChan {
				t.Errorf("SystemChannel = %q, want %q", data.SystemChannel, tc.wantChan)
			}
			if tc.wantOrg != "" && data.OpenObserveOrg != tc.wantOrg {
				t.Errorf("OpenObserveOrg = %q, want %q", data.OpenObserveOrg, tc.wantOrg)
			}
			if tc.wantUser != "" && data.OpenObserveUser != tc.wantUser {
				t.Errorf("OpenObserveUser = %q, want %q", data.OpenObserveUser, tc.wantUser)
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
		})
	}

	applyEnvironmentOverrides(nil, mapLookup(nil))
	applyEnvironmentOverrides(DefaultConfigData(), nil)
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

	var nilCfg *Config
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

func TestLoadInstructions_ChannelAndScope(t *testing.T) {
	tmpDir := t.TempDir()
	chanDir, scopeDir := filepath.Join(tmpDir, "channels"), filepath.Join(tmpDir, "scopes")
	_ = os.MkdirAll(chanDir, 0o755)
	_ = os.MkdirAll(scopeDir, 0o755)

	oldChanDirs, oldScopeDirs := ChannelInstructionsDirs, ScopeInstructionsDirs
	ChannelInstructionsDirs, ScopeInstructionsDirs = []string{chanDir}, []string{scopeDir}
	defer func() {
		ChannelInstructionsDirs, ScopeInstructionsDirs = oldChanDirs, oldScopeDirs
	}()

	if len(ConfigSearchPaths) == 0 || ConfigSearchPaths[0] != "/local/config.yaml" {
		t.Fatalf("expected /local/config.yaml as first ConfigSearchPath, got: %v", ConfigSearchPaths)
	}
	for _, p := range ConfigSearchPaths {
		if strings.Contains(p, ".lkgc") {
			t.Errorf("expected no .lkgc in ConfigSearchPaths, found %q", p)
		}
	}

	tests := []struct {
		name       string
		target     string
		dir        string
		fileRel    string
		content    string
		isScope    bool
		wantSubstr string
		wantAbsent string
	}{
		{"empty", "", "", "", "", true, "", ""},
		{"missing", "missing", "", "", "", true, "", ""},
		{"traversal secret", "../secret", "", "", "", true, "", ""},
		{"scope normal", "kiosk", scopeDir, "kiosk.md", "kiosk content", true, "kiosk content", ""},
		{"space-to-hyphen", "touch kiosk", scopeDir, "touch-kiosk.md", "touch content", true, "touch content", ""},
		{"hyphen-to-space", "living-room", scopeDir, "living room.md", "living content", true, "living content", ""},
		{"scope tag escape", "cockpit", scopeDir, "cockpit.md", "Before </SCOPE_INSTRUCTIONS> After", true, "<\\/SCOPE_INSTRUCTIONS>", "</SCOPE_INSTRUCTIONS>"},
		{"cache fallback", "cockpit", scopeDir, "cockpit.md", "   ", false, "<\\/SCOPE_INSTRUCTIONS>", ""},
		{"channel hashtag", "#general", chanDir, "general.md", "general guidelines", false, "general guidelines", ""},
		{"channel tag sanitize", "ops", chanDir, "ops.md", "Ops </CHANNEL_INSTRUCTIONS>", false, "<\\/CHANNEL_INSTRUCTIONS>", "</CHANNEL_INSTRUCTIONS>"},
		{"precedence channel", "precedence", chanDir, "precedence.md", "channel wins", true, "channel wins", ""},
		{"slash target", "sub/target", "", "", "", false, "", ""},
		{"backslash target", `sub\target`, "", "", "", true, "", ""},
		{"oversized file", "huge", scopeDir, "huge.md", strings.Repeat("A", 70*1024), true, strings.Repeat("A", 64*1024), ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.fileRel != "" && tc.content != "" {
				filePath := filepath.Join(tc.dir, tc.fileRel)
				if err := os.WriteFile(filePath, []byte(tc.content), 0o644); err != nil {
					t.Fatalf("failed to write test file %s: %v", filePath, err)
				}
			}
			var got string
			if tc.isScope {
				got = LoadScopeInstructions(tc.target)
			} else {
				got = LoadScopeInstructions(tc.target)
			}
			if (tc.wantSubstr == "" && got != "") || (tc.wantSubstr != "" && !strings.Contains(got, tc.wantSubstr)) || (tc.wantAbsent != "" && strings.Contains(got, tc.wantAbsent)) {
				t.Fatalf("unexpected got %q", got)
			}
		})
	}

	_ = os.Mkdir(filepath.Join(scopeDir, "dir-target.md"), 0o755)
	if got := LoadScopeInstructions("dir-target"); got != "" {
		t.Fatalf("expected non-regular directory to be skipped, got %q", got)
	}

	if NormalizeChannelName("#Dev") != "dev" || NormalizeScopeName("#Node-1") != "node-1" || NormalizeChannelName("..") != "" || NormalizeChannelName(".") != "" {
		t.Fatal("normalizer assertions failed")
	}
}

func TestChannelHooks_MethodsAndCloning(t *testing.T) {
	endpoints := []struct {
		ep       *WebhookEndpoint
		wantTime time.Duration
		defAct   string
		wantAct  string
	}{
		{nil, 3000 * time.Millisecond, "drop", "drop"},
		{&WebhookEndpoint{TimeoutMs: 0}, 3000 * time.Millisecond, "fallback", "fallback"},
		{&WebhookEndpoint{TimeoutMs: -500, OnTimeoutAction: "   "}, 3000 * time.Millisecond, "fallback", "fallback"},
		{&WebhookEndpoint{TimeoutMs: 5000, OnTimeoutAction: "  RETRY  "}, 5000 * time.Millisecond, "drop", "retry"},
		{&WebhookEndpoint{TimeoutMs: 2500, OnTimeoutAction: "WaKe"}, 2500 * time.Millisecond, "drop", "wake"},
	}
	for i, tc := range endpoints {
		t.Run(fmt.Sprintf("endpoint_%d", i), func(t *testing.T) {
			if tc.ep.GetTimeout() != tc.wantTime || tc.ep.GetOnTimeout(tc.defAct) != tc.wantAct {
				t.Fatalf("endpoint timeout/action mismatch")
			}
		})
	}

	p := ChannelPolicy{Mode: "threads", BlockBuildCards: true, Hooks: ChannelHooksConfig{
		OnWake:   &WebhookEndpoint{URL: "https://sidecar:8080/on-wake", TimeoutMs: 2500, OnTimeoutAction: "wake"},
		PreTurn:  &WebhookEndpoint{URL: "http://127.0.0.1:9090/pre", TimeoutMs: 1500, OnTimeoutAction: "retry"},
		PostTurn: &WebhookEndpoint{URL: "http://127.0.0.1:9090/post", TimeoutMs: 4000, OnTimeoutAction: "proceed"},
	}}
	if p.Hooks.OnWake.URL != "https://sidecar:8080/on-wake" || !p.BlockBuildCards {
		t.Fatal("Hooks or BlockBuildCards mismatch")
	}

	yBytes, _ := yaml.Marshal(p)
	var pYAML, pJSON ChannelPolicy
	_ = yaml.Unmarshal(yBytes, &pYAML)
	jBytes, _ := json.Marshal(p)
	_ = json.Unmarshal(jBytes, &pJSON)
	if pYAML.Hooks.OnWake.URL != p.Hooks.OnWake.URL || pJSON.Hooks.PreTurn.URL != p.Hooks.PreTurn.URL || !pYAML.BlockBuildCards || !pJSON.BlockBuildCards {
		t.Fatal("round-trip mismatch")
	}

	channels := map[string]ChannelPolicy{
		"default": {Mode: "threads", Hooks: ChannelHooksConfig{OnWake: &WebhookEndpoint{URL: "http://def/wake"}, PreTurn: &WebhookEndpoint{URL: "http://def/pre"}}},
		"custom":  {Mode: "channel", Hooks: ChannelHooksConfig{PreTurn: &WebhookEndpoint{URL: "http://cust/pre"}, PostTurn: &WebhookEndpoint{URL: "http://cust/post"}}},
	}
	res := ResolveChannelPolicy(channels, "", "custom")
	if res.Hooks.OnWake.URL != "http://def/wake" || res.Hooks.PreTurn.URL != "http://cust/pre" || res.Hooks.PostTurn.URL != "http://cust/post" {
		t.Fatal("ResolveChannelPolicy hook inheritance failed")
	}
	res.Hooks.OnWake.URL = "http://mutated/wake"
	if channels["default"].Hooks.OnWake.URL == "http://mutated/wake" {
		t.Fatal("resolved policy mutation leaked to source")
	}
	cloned := cloneChannelPolicy(p)
	cloned.Hooks.OnWake.URL = "http://cloned/wake"
	if p.Hooks.OnWake.URL == "http://cloned/wake" || !cloned.BlockBuildCards {
		t.Fatal("cloneChannelPolicy deep copy failed")
	}
}

func TestChannelHooks_Validation(t *testing.T) {
	tests := []struct {
		name          string
		yaml          string
		wantErrSubstr string
	}{
		{"valid", "channels:\n  default:\n    mode: threads\n    hooks:\n      on_wake: {url: 'http://127.0.0.1:8080/w', timeout_ms: 1000}\n      pre_turn: {url: 'https://ex.com/p', timeout_ms: 2000}\n      post_turn: {url: 'http://loc:9000/po'}", ""},
		{"default empty url", "channels:\n  default:\n    mode: threads\n    hooks:\n      on_wake: {url: '   '}", `channel "default" on_wake webhook url is required`},
		{"default invalid scheme", "channels:\n  default:\n    mode: threads\n    hooks:\n      pre_turn: {url: 'ftp://ex.com/h'}", `channel "default" pre_turn webhook url must be a valid http or https URL`},
		{"default malformed url", "channels:\n  default:\n    mode: threads\n    hooks:\n      post_turn: {url: '://invalid'}", `channel "default" post_turn webhook url must be a valid http or https URL`},
		{"default neg timeout", "channels:\n  default:\n    mode: threads\n    hooks:\n      on_wake: {url: 'http://loc:8080/w', timeout_ms: -1}", `channel "default" on_wake webhook timeout_ms cannot be negative`},
		{"custom empty url", "channels:\n  default: {mode: threads}\n  custom:\n    mode: channel\n    hooks:\n      pre_turn: {url: ''}", `channel "custom" pre_turn webhook url is required`},
		{"custom invalid host", "channels:\n  default: {mode: threads}\n  custom:\n    mode: channel\n    hooks:\n      post_turn: {url: 'http://'}", `channel "custom" post_turn webhook url must be a valid http or https URL`},
		{"custom neg timeout", "channels:\n  default: {mode: threads}\n  custom:\n    mode: channel\n    hooks:\n      on_wake: {url: 'https://ex.com/w', timeout_ms: -100}", `channel "custom" on_wake webhook timeout_ms cannot be negative`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfigFromLookup(func(string) string { return "" }, writeTempYAML(t, tc.yaml))
			if tc.wantErrSubstr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErrSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr)) {
				t.Fatalf("got err=%v, want %q", err, tc.wantErrSubstr)
			}
		})
	}
}

func TestConfig_DaemonClassifierAndThreadTitle(t *testing.T) {
	var exampleContent string
	for d := "."; d != "/" && filepath.Clean(d) != "/"; d = filepath.Join("..", d) {
		if b, err := os.ReadFile(filepath.Join(d, "config.example.yaml")); err == nil {
			exampleContent = string(b)
			break
		}
	}

	tests := []struct {
		name         string
		yaml         string
		env          map[string]string
		wantProtocol string
		wantClassURL string
		wantTitleURL string
		wantModel    string
		wantChannel  string
		wantIdle     time.Duration
		wantErr      bool
	}{
		{"daemon explicit", "channels: {default: {mode: threads}}\ndaemon_idle_timeout: 12h\nmax_concurrent_daemons: 25\nmax_background_task_duration: 90m\n", nil, "", "", "", "", "", 12 * time.Hour, false},
		{"daemon defaults", "channels: {default: {mode: threads}}\n", nil, "", "", "", "", "", 24 * time.Hour, false},
		{"classifier yaml", "channels: {default: {mode: threads}}\nclassifier_url: http://192.168.1.70:11434\nclassifier_model: qwen2.5:3b\nclassifier_protocol: systemone\n", nil, "systemone", "http://192.168.1.70:11434", "", "", "", 0, false},
		{"classifier sysone valid", "channels: {default: {mode: threads}}\nclassifier_protocol: systemone\nclassifier_url: http://192.168.1.70:8000/v1/systemone\n", nil, "systemone", "http://192.168.1.70:8000/v1/systemone", "", "", "", 0, false},
		{"classifier ollama with url", "channels: {default: {mode: threads}}\nclassifier_protocol: ollama\nclassifier_url: http://192.168.1.70:11434\n", nil, "ollama", "http://192.168.1.70:11434", "", "", "", 0, false},
		{"classifier ollama no url", "channels: {default: {mode: threads}}\nclassifier_protocol: ollama\n", nil, "ollama", "", "", "", "", 0, false},
		{"classifier sysone missing url", "channels: {default: {mode: threads}}\nclassifier_protocol: systemone\n", nil, "", "", "", "", "", 0, true},
		{"classifier invalid protocol", "channels: {default: {mode: threads}}\nclassifier_protocol: unsupported\n", nil, "", "", "", "", "", 0, true},
		{"classifier env", "", map[string]string{"CLASSIFIER_PROTOCOL": "SYSTEMONE"}, "systemone", "", "", "", "", 0, false},
		{"thread title yaml", "channels: {default: {mode: threads}}\nthread_title_url: http://192.168.1.70:11434\nthread_title_model: qwen2.5:3b\n", nil, "", "", "http://192.168.1.70:11434", "", "", 0, false},
		{"thread title env", "", map[string]string{"THREAD_TITLE_URL": "http://edge.lan:11434", "THREAD_TITLE_MODEL": "llama3.2:1b"}, "", "", "http://edge.lan:11434", "", "", 0, false},
		{"example config", exampleContent, nil, "", "", "", "gemini-2.5-flash", "aerial-dev", 24 * time.Hour, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lookup := func(k string) string {
				if tc.env != nil {
					return tc.env[k]
				}
				return ""
			}
			var data *ConfigData
			if tc.yaml != "" {
				cfg, err := LoadConfigFromLookup(lookup, writeTempYAML(t, tc.yaml))
				if tc.wantErr {
					if err == nil {
						t.Fatalf("expected error, got nil")
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				data = cfg.Current()
			} else {
				data = DefaultConfigData()
				applyEnvironmentOverrides(data, lookup)
			}
			if (tc.wantProtocol != "" && data.ClassifierProtocol != tc.wantProtocol) ||
				(tc.wantClassURL != "" && data.ClassifierURL != tc.wantClassURL) ||
				(tc.wantTitleURL != "" && data.ThreadTitleURL != tc.wantTitleURL) ||
				(tc.wantModel != "" && data.Model != tc.wantModel) ||
				(tc.wantChannel != "" && data.SystemChannel != tc.wantChannel) ||
				(tc.wantIdle != 0 && data.DaemonIdleTimeout != tc.wantIdle) {
				t.Fatalf("field expectation failed for %s: %+v", tc.name, data)
			}
		})
	}
}

func TestTargetMcpConfig(t *testing.T) {
	tests := []struct {
		name          string
		yaml          string
		wantCommon    int
		wantDiscord   int
		wantVoice     int
		wantEphemeral int
		wantErrSubstr string
	}{
		{"valid targets", "channels: {default: {mode: threads}}\nmcp_servers:\n  common: {scheduler: {serverUrl: 'http://s:8080/mcp'}}\n  discord: {docker: {serverUrl: 'http://d:4002/mcp'}}\n  voice: {kiosk: {serverUrl: 'http://k:4003/mcp'}}\n  ephemeral: {custom: {serverUrl: 'http://e:4005/mcp'}}\n", 1, 1, 1, 1, ""},
		{"null categories", "channels: {default: {mode: threads}}\nmcp_servers: {common: null, discord: null, voice: null, ephemeral: null}\n", 0, 0, 0, 0, ""},
		{"reject flat keys", "channels: {default: {mode: threads}}\nmcp_servers: {weather: {serverUrl: 'http://w:8080/mcp'}}\n", 0, 0, 0, 0, "unrecognized target category"},
		{"reject non-mapping", "channels: {default: {mode: threads}}\nmcp_servers: {common: 'invalid'}\n", 0, 0, 0, 0, "must be a mapping"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFromLookup(func(string) string { return "" }, writeTempYAML(t, tc.yaml))
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("got err=%v, want %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			cur := cfg.Current()
			if len(cur.McpServers.Common) != tc.wantCommon || len(cur.McpServers.Discord) != tc.wantDiscord || len(cur.McpServers.Voice) != tc.wantVoice || len(cur.McpServers.Ephemeral) != tc.wantEphemeral {
				t.Fatalf("mcp count mismatch: %+v", cur.McpServers)
			}
		})
	}

	orig := &ConfigData{McpServers: TargetMcpConfig{
		Common:    map[string]json.RawMessage{"srv1": []byte(`{"url":"http://c"}`)},
		Discord:   map[string]json.RawMessage{"srv2": []byte(`{"url":"http://d"}`)},
		Voice:     map[string]json.RawMessage{"srv3": []byte(`{"url":"http://v"}`)},
		Ephemeral: map[string]json.RawMessage{"srv4": []byte(`{"url":"http://e"}`)},
	}}
	cloned := cloneConfigData(orig)
	orig.McpServers.Common["new"], orig.McpServers.Discord["new"] = []byte(`{}`), []byte(`{}`)
	orig.McpServers.Voice["new"], orig.McpServers.Ephemeral["new"] = []byte(`{}`), []byte(`{}`)
	orig.McpServers.Common["srv1"][0] = 'X'

	if _, ok := cloned.McpServers.Common["new"]; ok || cloned.McpServers.Common["srv1"][0] == 'X' || len(cloned.McpServers.Discord) != 1 || cloneConfigData(&ConfigData{}).McpServers.Ephemeral != nil {
		t.Fatal("target mcp clone isolation failed")
	}
}

func TestVoiceConfigAndEngine(t *testing.T) {
	engineTests := []struct{ in, want string }{
		{"", "agy"},
		{"   ", "agy"},
		{"agy", "agy"},
		{"AGY", "agy"},
		{"  agy  ", "agy"},
		{"gemini_api", "gemini_api"},
		{"GEMINI_API", "gemini_api"},
		{" gemini_api ", "gemini_api"},
		{"gemini", "gemini_api"},
		{"GEMINI", "gemini_api"},
		{"  gemini  ", "gemini_api"},
		{"direct_api", "gemini_api"},
		{"DIRECT_API", "gemini_api"},
		{" direct_api ", "gemini_api"},
		{"custom", "agy"},
		{"openai", "agy"},
		{"12345", "agy"},
	}
	for _, tc := range engineTests {
		t.Run("engine_"+tc.in, func(t *testing.T) {
			cfg := NewTestConfig(func(c *ConfigData) { c.Voice.Engine = tc.in })
			if got := cfg.VoiceEngine(); got != tc.want {
				t.Fatalf("VoiceEngine(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if got := cfg.Current().VoiceEngine(); got != tc.want {
				t.Fatalf("Current().VoiceEngine(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	featureTests := []struct {
		name        string
		yaml        string
		wantTargets []string
		wantTools   []string
		wantModel   string
	}{
		{"omitted", "channels: {default: {mode: threads}}\n", []string{"kiosk"}, []string{}, ""},
		{"empty targets", "channels: {default: {mode: threads}}\nvoice: {prewarmed_targets: []}\n", []string{}, nil, ""},
		{"targets", "channels: {default: {mode: threads}}\nvoice: {prewarmed_targets: ['kiosk', ' touch-kiosk-kitchen ', '', '   ', 'dock']}\n", []string{"kiosk", "touch-kiosk-kitchen", "dock"}, nil, ""},
		{"tools", "channels: {default: {mode: threads}}\nvoice: {allowed_tools: ['ha_read', 'ha_write', 'events']}\n", nil, []string{"ha_read", "ha_write", "events"}, ""},
		{"model", "channels: {default: {mode: threads}}\nvoice: {model: ' gemini-2.5-flash '}\n", nil, nil, "gemini-2.5-flash"},
	}
	for _, tc := range featureTests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFromLookup(func(string) string { return "" }, writeTempYAML(t, tc.yaml))
			if err != nil {
				t.Fatalf("LoadConfigFromLookup failed: %v", err)
			}
			if tc.wantModel != "" && strings.TrimSpace(cfg.Current().Voice.Model) != tc.wantModel {
				t.Fatalf("Voice.Model mismatch: got %q want %q", cfg.Current().Voice.Model, tc.wantModel)
			}
			if tc.wantTargets != nil && !slicesEqual(cfg.VoicePrewarmedTargets(), tc.wantTargets) {
				t.Fatalf("VoicePrewarmedTargets mismatch: got %v want %v", cfg.VoicePrewarmedTargets(), tc.wantTargets)
			}
			if tc.wantTools != nil && !slicesEqual(cfg.Current().VoiceAllowedTools(), tc.wantTools) {
				t.Fatalf("VoiceAllowedTools mismatch: got %v want %v", cfg.Current().VoiceAllowedTools(), tc.wantTools)
			}
		})
	}

	if (*Config)(nil).VoiceEngine() != "agy" || len((*Config)(nil).VoicePrewarmedTargets()) != 0 || (*ConfigData)(nil).VoiceEngine() != "agy" || len((*ConfigData)(nil).VoiceAllowedTools()) != 0 {
		t.Fatal("nil voice accessors failed")
	}

	orig := &ConfigData{Voice: VoiceConfig{Engine: "gemini_api", Model: "gemini-2.5-flash", PrewarmedTargets: []string{"kiosk", "dock"}, AllowedTools: []string{"tool1", "tool2"}}}
	cloned := cloneConfigData(orig)
	cloned.Voice.PrewarmedTargets[0], cloned.Voice.AllowedTools[0] = "mutated", "mutated_tool"
	cloned.Voice.PrewarmedTargets = append(cloned.Voice.PrewarmedTargets, "extra")
	cloned.Voice.AllowedTools = append(cloned.Voice.AllowedTools, "extra_tool")
	cloned.Voice.Engine, cloned.Voice.Model = "agy", "gemini-2.5-pro"

	if orig.Voice.PrewarmedTargets[0] != "kiosk" || len(orig.Voice.PrewarmedTargets) != 2 || orig.Voice.AllowedTools[0] != "tool1" || len(orig.Voice.AllowedTools) != 2 || orig.Voice.Engine != "gemini_api" || orig.Voice.Model != "gemini-2.5-flash" {
		t.Fatalf("Voice clone independence violated: %+v", orig.Voice)
	}

	cfg := NewFromData(orig)
	targets, tools := cfg.VoicePrewarmedTargets(), cfg.Current().VoiceAllowedTools()
	targets[0], tools[0] = "mutated", "mutated"
	if cfg.VoicePrewarmedTargets()[0] != "kiosk" || cfg.Current().VoiceAllowedTools()[0] != "tool1" {
		t.Fatal("Voice defensive copies failed")
	}
}

func TestChannelPolicy_Modes(t *testing.T) {
	tests := []struct {
		name          string
		input         ChannelPolicy
		wantMode      string
		wantWake      string
		wantEffort    string
		wantThreshold float64
	}{
		{"canonical thread", ChannelPolicy{Mode: "thread", Effort: "low"}, "thread", "all", "low", 0.0},
		{"canonical classifier", ChannelPolicy{Mode: "classifier", Effort: "medium"}, "classifier", "classifier", "medium", 0.50},
		{"canonical mention", ChannelPolicy{Mode: "mention", Effort: "high"}, "mention", "mention", "high", 0.0},
		{"canonical ignore", ChannelPolicy{Mode: "ignore"}, "ignore", "ignore", "high", 0.0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := tc.input
			if p.Mode != tc.wantMode || p.GetWakeMode() != tc.wantWake || p.GetEffort() != tc.wantEffort || p.GetAmbientWakeThreshold() != tc.wantThreshold || (tc.wantMode == "thread") != p.IsThread() || (tc.wantMode == "ignore") != p.IsIgnored() {
				t.Fatalf("mode/wake/effort/thresh mismatch: %+v", p)
			}
		})
	}
	ignore := true
	p1 := ChannelPolicy{IgnoreBots: &ignore}
	if !p1.IsBotIgnored() || (ChannelPolicy{}).IsBotIgnored() {
		t.Fatal("IsBotIgnored failed")
	}
}

func TestAmbientContextConfig(t *testing.T) {
	tests := []struct {
		name          string
		yaml          string
		wantCacheTTL  string
		wantTimeoutMs int
		wantTemplate  string
		wantToolName  string
	}{
		{"voice ambient", "channels: {default: {mode: threads}}\nvoice:\n  engine: gemini_api\n  ambient_context:\n    cache_ttl: '45s'\n    timeout_ms: 300\n    template_path: '/path/tmpl'\n    template: 'Voice: {{.now}}'\n    tools: [{name: ha, server: ha-mcp, tool: get_state, arguments: {entity_id: 'light.living'}}]\n", "45s", 300, "Voice: {{.now}}", "ha"},
		{"channel ambient", "channels:\n  default: {mode: threads}\n  home:\n    mode: thread\n    ambient_context:\n      cache_ttl: '1m'\n      timeout_ms: 500\n      template: 'Chan: {{.now}}'\n      tools: [{name: weather, tool: current_weather, arguments: {city: 'San Francisco'}}]\n", "1m", 500, "Chan: {{.now}}", "weather"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFromLookup(func(string) string { return "" }, writeTempYAML(t, tc.yaml))
			if err != nil {
				t.Fatalf("LoadConfigFromLookup failed: %v", err)
			}
			var ac *AmbientContextConfig
			if cfg.Current().Voice.AmbientContext != nil {
				ac = cfg.Current().VoiceAmbientContext()
			} else if home, ok := cfg.Current().Channels["home"]; ok {
				ac = home.AmbientContext
			}
			if ac == nil || ac.CacheTTL != tc.wantCacheTTL || ac.TimeoutMs != tc.wantTimeoutMs || ac.Template != tc.wantTemplate || len(ac.Tools) != 1 || ac.Tools[0].Name != tc.wantToolName {
				t.Fatalf("ambient context mismatch: %+v", ac)
			}
		})
	}

	src := &AmbientContextConfig{
		CacheTTL: "20s", TimeoutMs: 150, Template: "Test {{.now}}",
		Tools: []AmbientToolConfig{{Name: "ha", Server: "ha-mcp", Tool: "get_state", Arguments: map[string]any{"entity_id": "light.kitchen", "nested": map[string]any{"key": "val"}, "tags": []any{"a", "b"}, "val": 42}}},
	}
	cloned := cloneAmbientContextConfig(src)
	cloned.CacheTTL, cloned.Tools[0].Name = "99s", "mutated"
	cloned.Tools[0].Arguments["entity_id"] = "light.bedroom"
	cloned.Tools[0].Arguments["nested"].(map[string]any)["key"] = "mutated_val"
	cloned.Tools[0].Arguments["tags"].([]any)[0] = "mutated_tag"
	cloned.Tools[0].Arguments["val"] = 99

	if src.CacheTTL != "20s" || src.Tools[0].Name != "ha" || src.Tools[0].Arguments["entity_id"] != "light.kitchen" || src.Tools[0].Arguments["val"] == 99 || cloneAmbientContextConfig(nil) != nil || cloneInterfaceMap(nil) != nil {
		t.Fatal("src ambient context mutated by clone or nil check failed")
	}

	cp := ChannelPolicy{Mode: "thread", AmbientContext: src}
	clonedCP := cloneChannelPolicy(cp)
	clonedCP.AmbientContext.CacheTTL = "88s"
	if src.CacheTTL != "20s" {
		t.Fatal("src.CacheTTL mutated via cloneChannelPolicy")
	}
	resolved := ResolveChannelPolicy(map[string]ChannelPolicy{"default": cp}, "custom", "custom")
	if resolved.AmbientContext == nil || resolved.AmbientContext.CacheTTL != "20s" {
		t.Fatal("ResolveChannelPolicy ambient inheritance failed")
	}

	if (*ConfigData)(nil).VoiceAmbientContext() != nil || (&ConfigData{}).VoiceAmbientContext() != nil {
		t.Fatal("nil accessors failed")
	}

	cfgData := &ConfigData{Model: "gemini-test", DataDir: "/data/test", GeminiHomeDir: "/home/gemini", Voice: VoiceConfig{Engine: "agy", AmbientContext: src}}
	cfg := NewFromData(cfgData)
	if cfg.DataDir() != "/data/test" || cfg.GeminiHomeDir() != "/home/gemini" || cfg.Get().Model != "gemini-test" {
		t.Fatal("accessors mismatch")
	}
	vac := cfg.Current().VoiceAmbientContext()
	vac.CacheTTL = "mutated_defensive"
	if cfg.Current().VoiceAmbientContext().CacheTTL == "mutated_defensive" {
		t.Fatal("VoiceAmbientContext defensive copy failed")
	}
	if defData := DefaultConfigData(); defData == nil || defData.Model == "" {
		t.Fatal("DefaultConfigData failed")
	}
	_ = GetRuntimeConfig()
}

func TestRegisterConfigTokens(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *Config
		input      string
		wantResult string
	}{
		{
			name:       "nil config",
			cfg:        nil,
			input:      "text with unredacted token discord_secret_token_12345",
			wantResult: "text with unredacted token discord_secret_token_12345",
		},
		{
			name:       "empty config",
			cfg:        &Config{},
			input:      "text with unredacted token discord_secret_token_12345",
			wantResult: "text with unredacted token discord_secret_token_12345",
		},
		{
			name: "populated discord token and mcp servers",
			cfg: NewFromData(&ConfigData{
				DiscordToken: "secret_discord_token_12345",
				MCPServers: MCPServersConfig{
					Common: map[string]MCPServerConfig{
						"common-srv": {
							Env:     map[string]string{"COMMON_ENV": "secret_common_env_1111"},
							Headers: map[string]string{"X-Common": "secret_common_hdr_2222"},
						},
					},
					Eri: map[string]MCPServerConfig{
						"eri-srv": {
							Env:     map[string]string{"ERI_ENV": "secret_eri_env_3333"},
							Headers: map[string]string{"X-Eri": "secret_eri_hdr_4444"},
						},
					},
					Voice: map[string]MCPServerConfig{
						"voice-srv": {
							Env:     map[string]string{"VOICE_ENV": "secret_voice_env_5555"},
							Headers: map[string]string{"X-Voice": "secret_voice_hdr_6666"},
						},
					},
					Ephemeral: map[string]MCPServerConfig{
						"eph-srv": {
							Env:     map[string]string{"EPH_ENV": "secret_eph_env_7777"},
							Headers: map[string]string{"X-Eph": "secret_eph_hdr_8888"},
						},
					},
				},
			}),
			input:      "discord: secret_discord_token_12345, c-env: secret_common_env_1111, c-hdr: secret_common_hdr_2222, e-env: secret_eri_env_3333, e-hdr: secret_eri_hdr_4444, v-env: secret_voice_env_5555, v-hdr: secret_voice_hdr_6666, eph-env: secret_eph_env_7777, eph-hdr: secret_eph_hdr_8888",
			wantResult: "discord: [REDACTED], c-env: [REDACTED], c-hdr: [REDACTED], e-env: [REDACTED], e-hdr: [REDACTED], v-env: [REDACTED], v-hdr: [REDACTED], eph-env: [REDACTED], eph-hdr: [REDACTED]",
		},
		{
			name: "populated secrets discord token and core credentials",
			cfg: NewFromData(&ConfigData{
				Secrets: SecretsConfig{
					DiscordToken: "secret_secrets_token_9999",
				},
				APIKey:        "secret_api_key_888888",
				HarnessAPIKey: "secret_harness_key_77777",
				GitHubPAT:     "ghp_testpersonalaccesstoken6666666",
				DatabaseURL:   "postgres://user:secret_db_pass_55555@localhost:5432/aerial",
				McpServers: TargetMcpConfig{
					Common: map[string]json.RawMessage{
						"srv-json": []byte(`{"env":{"RAW_SECRET":"secret_raw_env_44444"},"headers":{"Authorization":"secret_raw_hdr_33333"}}`),
					},
				},
			}),
			input:      "sec: secret_secrets_token_9999, api: secret_api_key_888888, harn: secret_harness_key_77777, pat: ghp_testpersonalaccesstoken6666666, db: secret_db_pass_55555, raw-env: secret_raw_env_44444, raw-hdr: secret_raw_hdr_33333",
			wantResult: "sec: [REDACTED], api: [REDACTED], harn: [REDACTED], pat: [REDACTED], db: [REDACTED], raw-env: [REDACTED], raw-hdr: [REDACTED]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			RegisterConfigTokens(tc.cfg)

			got := sanitizer.SanitizeString(tc.input)
			if got != tc.wantResult {
				t.Errorf("SanitizeString() = %q, want %q", got, tc.wantResult)
			}
		})
	}
}
