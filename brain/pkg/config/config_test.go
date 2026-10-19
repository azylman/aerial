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
)

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

func f64Ptr(v float64) *float64 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
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

			if !tc.wantErr && tc.envMap == nil {
				bCfg, bErr := LoadConfigFromBytes([]byte(tc.content))
				if bErr != nil {
					t.Errorf("LoadConfigFromBytes failed: %v", bErr)
				}
				_ = bCfg
			}
		})
	}

	bCfg, bErr := LoadConfigFromBytes([]byte("{}"))
	if bErr != nil || bCfg == nil {
		t.Errorf("LoadConfigFromBytes on empty failed: %v", bErr)
	}

	t.Setenv("AGY_MODEL", "")
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	_, _ = LoadConfig()
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
				content := ""
				if len(parts) == 2 {
					content = parts[1]
					if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
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
	_, _ = NewFromLookup(func(string) string { return "" })
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
			IgnoreBots:           boolPtr(true),
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
			WakeMode:             "classifier",
			AmbientWakeThreshold: f64Ptr(0.85),
		},
		"aerial-dev": {
			IgnoreBots: boolPtr(false),
		},
		"1543668253363150928": {
			Mode:       "channel",
			IgnoreBots: boolPtr(false),
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
		wantWake      string
		wantIgnore    bool
		wantBotIgn    bool
		wantThreshold float64
		wantHasHook   bool
	}{
		{
			name:          "match snowflake id",
			channels:      channels,
			channelID:     "1543668253363150928",
			channelName:   "dev",
			wantMode:      "channel",
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
			if tc.wantWake != "" && res.GetWakeMode() != tc.wantWake {
				t.Errorf("WakeMode = %q, want %q", res.GetWakeMode(), tc.wantWake)
			}
			if res.IsIgnored() != tc.wantIgnore {
				t.Errorf("IsIgnored = %v, want %v", res.IsIgnored(), tc.wantIgnore)
			}
			if res.IsBotIgnored() != tc.wantBotIgn {
				t.Errorf("IsBotIgnored = %v, want %v", res.IsBotIgnored(), tc.wantBotIgn)
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

	normPolicy := ChannelPolicy{Mode: "threads", WakeMode: "ambient"}
	normPolicy.Normalize()
	if normPolicy.Mode != "thread" || normPolicy.GetWakeMode() != "classifier" {
		t.Errorf("Normalize failed: mode=%q wake=%q", normPolicy.Mode, normPolicy.GetWakeMode())
	}
	pThresh := ChannelPolicy{Mode: "channel", AmbientWakeThreshold: f64Ptr(0.9)}
	pThresh.Normalize()
	pClass := ChannelPolicy{Mode: "classifier"}
	pClass.Normalize()
	hooks := channels["default"].GetHooks()
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
			if got := GetEnvFromLookup(fn, tc.key, tc.defVal); got != tc.expected {
				t.Errorf("GetEnvFromLookup() = %q, want %q", got, tc.expected)
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
				"SYSTEM_CHANNEL":               "alerts",
				"OPENOBSERVE_ORG":              "custom-org",
				"OPENOBSERVE_ROOT_USER_EMAIL":  "root@domain.com",
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
			name: "fallback env vars",
			envMap: map[string]string{
				"AGY_API_KEY":       "fallback-key",
				"TZ":                "America/Chicago",
				"OPENOBSERVE_USER":  "user@domain.com",
				"EMBEDDING_NUM_CTX": "4096",
			},
			wantKey:  "fallback-key",
			wantTZ:   "America/Chicago",
			wantUser: "user@domain.com",
			wantCtx:  4096,
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
	if _, err := LoadConfigFromBytes([]byte("broken: [")); err == nil {
		t.Errorf("expected error from LoadConfigFromBytes on invalid YAML")
	}
	if nilCfg.Get() == nil {
		t.Errorf("expected DefaultConfigData from nilCfg.Get()")
	}
	if nilCfg.VoiceEngine() != "agy" {
		t.Errorf("expected agy voice engine on nil Config, got %q", nilCfg.VoiceEngine())
	}
	if nilCfg.VoiceModel() != "" {
		t.Errorf("expected empty voice model on nil Config, got %q", nilCfg.VoiceModel())
	}
	if nilCfg.VoiceAllowedTools() != nil {
		t.Errorf("expected nil tools on nil Config")
	}
	if nilCfg.VoicePrewarmedTargets() == nil {
		t.Errorf("expected non-nil prewarmed targets on nil Config")
	}
	if nilCfg.VoiceAmbientContext() != nil {
		t.Errorf("expected nil ambient context on nil Config")
	}
	origGlobal := ActiveConfig().Current()
	ActiveConfig().Update(&ConfigData{Timezone: "", SystemChannel: ""})
	if GetSystemChannel() != "aerial-dev" || GetTimezone() != "America/Los_Angeles" {
		t.Errorf("expected fallback defaults on empty ActiveConfig")
	}
	ActiveConfig().Update(&ConfigData{Timezone: "UTC", SystemChannel: "custom-sys"})
	if GetSystemChannel() != "custom-sys" || GetTimezone() != "UTC" {
		t.Errorf("expected custom values on ActiveConfig")
	}
	ActiveConfig().Update(origGlobal)
	if nilCfg.GeminiHomeDir() != "" || nilCfg.DataDir() != "" {
		t.Errorf("expected empty dirs on nil Config")
	}
	var nilData *ConfigData
	if nilData.VoiceEngine() != "agy" || nilData.VoiceModel() != "" || nilData.VoiceAmbientContext() != nil {
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
	clonedCfg := testCfg.Clone()
	if clonedCfg == nil || clonedCfg.Current().Model != "updated-model" {
		t.Errorf("Clone failed on Config")
	}
	clonedData := testCfg.Current().Clone()
	if clonedData == nil || clonedData.Model != "updated-model" {
		t.Errorf("Clone failed on ConfigData")
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
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				cur := raceCfg.Current()
				_ = cur.Model
				_ = cur.Channels["default"].Mode
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
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
	if GetTimezone() != "America/New_York" || GetSystemChannel() != "ops-channel" {
		t.Errorf("expected updated timezone and channel from globals")
	}
	activeGlobalConfig.update(origG)

	cfgWithAmbient := NewFromData(&ConfigData{
		Voice: VoiceConfig{
			AmbientContext: &AmbientContextConfig{CacheTTL: "5m"},
		},
	})
	if cfgWithAmbient.VoiceAmbientContext() == nil {
		t.Errorf("expected non-nil voice ambient context")
	}

	_ = LoadChannelInstructions("general")
}

func TestLoadConfigFromLookup_Isolation(t *testing.T) {
	origCurrent := activeGlobalConfig.Current()
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "isolated.yaml")
	content := `
model: "isolated-model-123"
system_channel: "isolated-channel-456"
channels:
  default:
    mode: "threads"
`
	if err := os.WriteFile(yamlPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test yaml: %v", err)
	}

	lookup := func(k string) string {
		if k == "SYSTEM_PROMPT" {
			return "prompt from lookup"
		}
		return ""
	}

	cfg, err := LoadConfigFromLookup(lookup, yamlPath)
	if err != nil {
		t.Fatalf("LoadConfigFromLookup failed: %v", err)
	}

	if cfg.Current().Model != "isolated-model-123" {
		t.Errorf("expected isolated model, got %q", cfg.Current().Model)
	}
	if cfg.Current().SystemPrompt != "prompt from lookup" {
		t.Errorf("expected prompt from lookup, got %q", cfg.Current().SystemPrompt)
	}

	// Verify activeGlobalConfig was NOT mutated
	afterCurrent := activeGlobalConfig.Current()
	if afterCurrent.Model != origCurrent.Model || afterCurrent.SystemChannel != origCurrent.SystemChannel {
		t.Errorf("activeGlobalConfig was unexpectedly mutated: before=%+v, after=%+v", origCurrent, afterCurrent)
	}
}

func TestLoadConfigFromLookup_PolymorphicMCPConfig(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. mcp_config as string
	p1 := filepath.Join(tmpDir, "cfg_str.yaml")
	c1 := `
channels:
  default:
    mode: "threads"
mcp_config: '{"mcpServers":{"s1":{"serverUrl":"http://s1:1000"}}}'
`
	if err := os.WriteFile(p1, []byte(c1), 0644); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}
	cfg1, err := LoadConfigFromLookup(nil, p1)
	if err != nil {
		t.Fatalf("LoadConfigFromLookup string mcp_config failed: %v", err)
	}
	if !strings.Contains(cfg1.Current().MCPConfig, "http://s1:1000") {
		t.Errorf("expected string mcp_config to contain s1 url, got %q", cfg1.Current().MCPConfig)
	}

	// 2. mcp_config as mapping
	p2 := filepath.Join(tmpDir, "cfg_map.yaml")
	c2 := `
channels:
  default:
    mode: "threads"
mcp_config:
  mcpServers:
    s2:
      serverUrl: "http://s2:2000"
`
	if err := os.WriteFile(p2, []byte(c2), 0644); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}
	cfg2, err := LoadConfigFromLookup(nil, p2)
	if err != nil {
		t.Fatalf("LoadConfigFromLookup map mcp_config failed: %v", err)
	}
	if !strings.Contains(cfg2.Current().MCPConfig, "http://s2:2000") {
		t.Errorf("expected map mcp_config serialized to JSON containing s2 url, got %q", cfg2.Current().MCPConfig)
	}
}

func TestBuildPostgresDSN_Lookup(t *testing.T) {
	// 1. Direct DATABASE_URL
	lookup1 := func(k string) string {
		if k == "DATABASE_URL" {
			return "postgres://user:pass@host1:5432/db1"
		}
		return ""
	}
	if dsn := buildPostgresDSN(lookup1); dsn != "postgres://user:pass@host1:5432/db1" {
		t.Errorf("expected direct DATABASE_URL, got %q", dsn)
	}

	// 2. DB_PATH is ignored (returns empty when no Postgres vars set)
	lookup2 := func(k string) string {
		if k == "DB_PATH" {
			return "/var/data/app.db"
		}
		return ""
	}
	if dsn := buildPostgresDSN(lookup2); dsn != "" {
		t.Errorf("expected DB_PATH to be ignored, got %q", dsn)
	}

	// 3. Components
	lookup3 := func(k string) string {
		switch k {
		case "POSTGRES_HOST":
			return "pg.internal"
		case "POSTGRES_PORT":
			return "5433"
		case "POSTGRES_USER":
			return "myuser"
		case "POSTGRES_PASSWORD":
			return "secret123"
		case "POSTGRES_DB":
			return "aerial_db"
		default:
			return ""
		}
	}
	expected := "postgres://myuser:secret123@pg.internal:5433/aerial_db?sslmode=disable"
	if dsn := buildPostgresDSN(lookup3); dsn != expected {
		t.Errorf("expected constructed DSN %q, got %q", expected, dsn)
	}

	// 4. Nil lookup safe
	if dsn := buildPostgresDSN(nil); dsn != "" {
		t.Errorf("expected empty DSN for nil lookup, got %q", dsn)
	}
}

func TestConfig_PackageLevelHelpers(t *testing.T) {
	// 1. ActiveConfig()
	ac := ActiveConfig()
	if ac == nil {
		t.Errorf("expected non-nil ActiveConfig")
	}

	// 4. buildPostgresDSN defaults with POSTGRES_HOST
	lookupHostOnly := func(k string) string {
		if k == "POSTGRES_HOST" {
			return "localhost"
		}
		return ""
	}
	expectedDefault := "postgres://aerial:aerial_secure_pass@localhost:5432/aerial?sslmode=disable"
	if dsn := buildPostgresDSN(lookupHostOnly); dsn != expectedDefault {
		t.Errorf("expected default postgres DSN %q, got %q", expectedDefault, dsn)
	}

	// 5. Reload nil check
	if err := Reload(nil); err == nil {
		t.Errorf("expected error for Reload(nil)")
	}

	tmpDir := t.TempDir()

	// 7. isContainerColdBoot
	_ = isContainerColdBoot(filepath.Join(tmpDir, "nonexistent"))

	// 8. NewFromLookup
	mockLookup := func(string) string { return "" }
	cfg, err := NewFromLookup(mockLookup)
	if cfg == nil && err == nil {
		t.Errorf("unexpected nil cfg and nil err")
	}

	// 9. New() and LoadConfig()
	t.Setenv("AGY_MODEL", "")
	t.Setenv("DEFAULT_TIMEZONE", "")
	t.Setenv("TZ", "")
	cfgNew, _ := New()
	_ = cfgNew
	cfgLoad, _ := LoadConfig()
	_ = cfgLoad

	// 10. waitForColdBootConfig
	validBootFile := filepath.Join(tmpDir, "boot.yaml")
	_ = os.WriteFile(validBootFile, []byte("model: test\n"), 0644)
	waitForColdBootConfig([]string{validBootFile}, 500*time.Millisecond)
	waitForColdBootConfig([]string{filepath.Join(tmpDir, "does_not_exist")}, 1*time.Millisecond)
	waitForColdBootConfig(nil, 1*time.Millisecond)
	waitForColdBootConfig([]string{filepath.Join(tmpDir, "does_not_exist")}, 0)

	// 11. cloneConfigData nil and update nil
	if res := cloneConfigData(nil); res != nil {
		t.Errorf("expected nil from cloneConfigData(nil)")
	}
	cfgDummy := NewTestConfig()
	cfgDummy.update(nil)

	// 12. NewFromData nil
	cfgFromNil := NewFromData(nil)
	if cfgFromNil == nil || cfgFromNil.Current() == nil {
		t.Errorf("expected non-nil default from NewFromData(nil)")
	}

	// 13. applyEnvironmentOverrides nil checks
	applyEnvironmentOverrides(nil, mockLookup)
	dummyData := DefaultConfigData()
	applyEnvironmentOverrides(dummyData, nil)

	// 14. Reload with valid active config
	_ = Reload(ActiveConfig())
}

func TestChannelHooksConfig(t *testing.T) {
	t.Run("YAML_And_JSON_RoundTrip", func(t *testing.T) {
		yamlData := `
mode: "threads"
hooks:
  on_wake:
    url: "https://agent-sidecar:8080/on-wake"
    timeout_ms: 2500
    on_timeout: "wake"
  pre_turn:
    url: "http://127.0.0.1:9090/pre-turn"
    timeout_ms: 1500
    on_timeout: "retry"
  post_turn:
    url: "http://127.0.0.1:9090/post-turn"
    timeout_ms: 4000
    on_timeout: "proceed"
`
		var policy ChannelPolicy
		if err := yaml.Unmarshal([]byte(yamlData), &policy); err != nil {
			t.Fatalf("Failed to unmarshal YAML into ChannelPolicy: %v", err)
		}

		if policy.Hooks.OnWake == nil || policy.Hooks.OnWake.URL != "https://agent-sidecar:8080/on-wake" {
			t.Errorf("Unexpected OnWake: %+v", policy.Hooks.OnWake)
		}
		if policy.Hooks.OnWake.TimeoutMs != 2500 || policy.Hooks.OnWake.OnTimeoutAction != "wake" {
			t.Errorf("Unexpected OnWake fields: %+v", policy.Hooks.OnWake)
		}
		if policy.Hooks.PreTurn == nil || policy.Hooks.PreTurn.URL != "http://127.0.0.1:9090/pre-turn" {
			t.Errorf("Unexpected PreTurn: %+v", policy.Hooks.PreTurn)
		}
		if policy.Hooks.PreTurn.TimeoutMs != 1500 || policy.Hooks.PreTurn.OnTimeoutAction != "retry" {
			t.Errorf("Unexpected PreTurn fields: %+v", policy.Hooks.PreTurn)
		}
		if policy.Hooks.PostTurn == nil || policy.Hooks.PostTurn.URL != "http://127.0.0.1:9090/post-turn" {
			t.Errorf("Unexpected PostTurn: %+v", policy.Hooks.PostTurn)
		}
		if policy.Hooks.PostTurn.TimeoutMs != 4000 || policy.Hooks.PostTurn.OnTimeoutAction != "proceed" {
			t.Errorf("Unexpected PostTurn fields: %+v", policy.Hooks.PostTurn)
		}

		// Re-marshal to YAML and verify round-trip
		marshaledYAML, err := yaml.Marshal(policy)
		if err != nil {
			t.Fatalf("Failed to marshal ChannelPolicy to YAML: %v", err)
		}
		var policyFromYAML ChannelPolicy
		if err := yaml.Unmarshal(marshaledYAML, &policyFromYAML); err != nil {
			t.Fatalf("Failed to unmarshal round-tripped YAML: %v", err)
		}
		if policyFromYAML.Hooks.OnWake == nil || policyFromYAML.Hooks.OnWake.URL != policy.Hooks.OnWake.URL {
			t.Errorf("YAML round-trip mismatch: %+v", policyFromYAML.Hooks)
		}

		// Re-marshal to JSON and verify round-trip
		marshaledJSON, err := json.Marshal(policy)
		if err != nil {
			t.Fatalf("Failed to marshal ChannelPolicy to JSON: %v", err)
		}
		var policyFromJSON ChannelPolicy
		if err := json.Unmarshal(marshaledJSON, &policyFromJSON); err != nil {
			t.Fatalf("Failed to unmarshal round-tripped JSON: %v", err)
		}
		if policyFromJSON.Hooks.PreTurn == nil || policyFromJSON.Hooks.PreTurn.URL != policy.Hooks.PreTurn.URL {
			t.Errorf("JSON round-trip mismatch: %+v", policyFromJSON.Hooks)
		}
	})

	t.Run("GetHooks_Helper", func(t *testing.T) {
		p := ChannelPolicy{
			Mode: "channel",
			Hooks: ChannelHooksConfig{
				OnWake: &WebhookEndpoint{URL: "http://localhost:8080/on-wake"},
			},
		}
		hooks := p.GetHooks()
		if hooks.OnWake == nil || hooks.OnWake.URL != "http://localhost:8080/on-wake" {
			t.Errorf("Expected GetHooks to return policy hooks, got %+v", hooks)
		}
	})

	t.Run("GetTimeout_Defaults_And_Custom", func(t *testing.T) {
		var nilEp *WebhookEndpoint
		if d := nilEp.GetTimeout(); d != 3000*time.Millisecond {
			t.Errorf("Expected 3000ms for nil endpoint, got %v", d)
		}

		epZero := &WebhookEndpoint{TimeoutMs: 0}
		if d := epZero.GetTimeout(); d != 3000*time.Millisecond {
			t.Errorf("Expected 3000ms for TimeoutMs=0, got %v", d)
		}

		epNeg := &WebhookEndpoint{TimeoutMs: -500}
		if d := epNeg.GetTimeout(); d != 3000*time.Millisecond {
			t.Errorf("Expected 3000ms for negative TimeoutMs, got %v", d)
		}

		epCustom := &WebhookEndpoint{TimeoutMs: 5000}
		if d := epCustom.GetTimeout(); d != 5000*time.Millisecond {
			t.Errorf("Expected 5000ms for TimeoutMs=5000, got %v", d)
		}
	})

	t.Run("GetOnTimeout_Defaults_And_Normalization", func(t *testing.T) {
		var nilEp *WebhookEndpoint
		if a := nilEp.GetOnTimeout("default_act"); a != "default_act" {
			t.Errorf("Expected default_act for nil endpoint, got %q", a)
		}

		epEmpty := &WebhookEndpoint{OnTimeoutAction: ""}
		if a := epEmpty.GetOnTimeout("fallback"); a != "fallback" {
			t.Errorf("Expected fallback for empty action, got %q", a)
		}

		epWhitespace := &WebhookEndpoint{OnTimeoutAction: "   \t  "}
		if a := epWhitespace.GetOnTimeout("fallback"); a != "fallback" {
			t.Errorf("Expected fallback for whitespace action, got %q", a)
		}

		epTrimLower := &WebhookEndpoint{OnTimeoutAction: "  RETRY  "}
		if a := epTrimLower.GetOnTimeout("drop"); a != "retry" {
			t.Errorf("Expected normalized 'retry', got %q", a)
		}

		epWake := &WebhookEndpoint{OnTimeoutAction: "WaKe"}
		if a := epWake.GetOnTimeout("drop"); a != "wake" {
			t.Errorf("Expected normalized 'wake', got %q", a)
		}
	})

	t.Run("ResolveChannelPolicy_And_Cloning", func(t *testing.T) {
		channels := map[string]ChannelPolicy{
			"default": {
				Mode: "threads",
				Hooks: ChannelHooksConfig{
					OnWake: &WebhookEndpoint{
						URL:             "http://default-sidecar/wake",
						TimeoutMs:       2000,
						OnTimeoutAction: "classify",
					},
					PreTurn: &WebhookEndpoint{
						URL:             "http://default-sidecar/pre",
						TimeoutMs:       3000,
						OnTimeoutAction: "drop",
					},
				},
			},
			"custom-channel": {
				Mode: "channel",
				Hooks: ChannelHooksConfig{
					PreTurn: &WebhookEndpoint{
						URL:             "http://custom-sidecar/pre",
						TimeoutMs:       1000,
						OnTimeoutAction: "retry",
					},
					PostTurn: &WebhookEndpoint{
						URL:             "http://custom-sidecar/post",
						TimeoutMs:       4000,
						OnTimeoutAction: "proceed",
					},
				},
			},
		}

		// 1. Resolve custom channel: should inherit OnWake from default, keep its own PreTurn and PostTurn
		resolvedCustom := ResolveChannelPolicy(channels, "", "custom-channel")
		if resolvedCustom.Hooks.OnWake == nil || resolvedCustom.Hooks.OnWake.URL != "http://default-sidecar/wake" {
			t.Errorf("Expected inherited OnWake from default, got %+v", resolvedCustom.Hooks.OnWake)
		}
		if resolvedCustom.Hooks.PreTurn == nil || resolvedCustom.Hooks.PreTurn.URL != "http://custom-sidecar/pre" {
			t.Errorf("Expected custom PreTurn, got %+v", resolvedCustom.Hooks.PreTurn)
		}
		if resolvedCustom.Hooks.PostTurn == nil || resolvedCustom.Hooks.PostTurn.URL != "http://custom-sidecar/post" {
			t.Errorf("Expected custom PostTurn, got %+v", resolvedCustom.Hooks.PostTurn)
		}

		// Verify deep copy isolation: mutating resolvedCustom does not mutate source
		resolvedCustom.Hooks.OnWake.URL = "http://mutated-wake"
		if channels["default"].Hooks.OnWake.URL == "http://mutated-wake" {
			t.Errorf("Pointer aliasing detected: default OnWake was mutated")
		}

		// 2. Resolve default channel directly
		resolvedDef := ResolveChannelPolicy(channels, "", "default")
		if resolvedDef.Hooks.OnWake == nil || resolvedDef.Hooks.OnWake.URL != "http://default-sidecar/wake" {
			t.Errorf("Expected default OnWake, got %+v", resolvedDef.Hooks.OnWake)
		}

		// 3. cloneChannelPolicy deep copy verification
		origPolicy := ChannelPolicy{
			Mode: "channel",
			Hooks: ChannelHooksConfig{
				OnWake:   &WebhookEndpoint{URL: "http://orig/wake", TimeoutMs: 100},
				PreTurn:  &WebhookEndpoint{URL: "http://orig/pre", TimeoutMs: 200},
				PostTurn: &WebhookEndpoint{URL: "http://orig/post", TimeoutMs: 300},
			},
		}
		clonedPolicy := cloneChannelPolicy(origPolicy)
		clonedPolicy.Hooks.OnWake.URL = "http://cloned/wake"
		clonedPolicy.Hooks.PreTurn.TimeoutMs = 999
		clonedPolicy.Hooks.PostTurn.OnTimeoutAction = "mutated"

		if origPolicy.Hooks.OnWake.URL != "http://orig/wake" {
			t.Errorf("cloneChannelPolicy failed to deep clone OnWake")
		}
		if origPolicy.Hooks.PreTurn.TimeoutMs != 200 {
			t.Errorf("cloneChannelPolicy failed to deep clone PreTurn")
		}
		if origPolicy.Hooks.PostTurn.OnTimeoutAction != "" {
			t.Errorf("cloneChannelPolicy failed to deep clone PostTurn")
		}
	})

	t.Run("Validation_Errors", func(t *testing.T) {
		tests := []struct {
			name        string
			yamlContent string
			wantErr     string
		}{
			{
				name: "valid_full_hooks",
				yamlContent: `
model: "test-model"
channels:
  default:
    mode: "threads"
    hooks:
      on_wake:
        url: "http://127.0.0.1:8080/on-wake"
        timeout_ms: 1000
      pre_turn:
        url: "https://example.com/api/pre"
        timeout_ms: 2000
      post_turn:
        url: "http://localhost:9000/post"
`,
				wantErr: "",
			},
			{
				name: "default_channel_empty_url",
				yamlContent: `
channels:
  default:
    mode: "threads"
    hooks:
      on_wake:
        url: "   "
`,
				wantErr: "channel \"default\" on_wake webhook url is required",
			},
			{
				name: "default_channel_invalid_url_scheme",
				yamlContent: `
channels:
  default:
    mode: "threads"
    hooks:
      pre_turn:
        url: "ftp://example.com/hook"
`,
				wantErr: "channel \"default\" pre_turn webhook url must be a valid http or https URL",
			},
			{
				name: "default_channel_malformed_url",
				yamlContent: `
channels:
  default:
    mode: "threads"
    hooks:
      post_turn:
        url: "://invalid-url"
`,
				wantErr: "channel \"default\" post_turn webhook url must be a valid http or https URL",
			},
			{
				name: "default_channel_negative_timeout",
				yamlContent: `
channels:
  default:
    mode: "threads"
    hooks:
      on_wake:
        url: "http://localhost:8080/wake"
        timeout_ms: -1
`,
				wantErr: "channel \"default\" on_wake webhook timeout_ms cannot be negative",
			},
			{
				name: "custom_channel_empty_url",
				yamlContent: `
channels:
  default:
    mode: "threads"
  custom:
    mode: "channel"
    hooks:
      pre_turn:
        url: ""
`,
				wantErr: "channel \"custom\" pre_turn webhook url is required",
			},
			{
				name: "custom_channel_invalid_url_host",
				yamlContent: `
channels:
  default:
    mode: "threads"
  custom:
    mode: "channel"
    hooks:
      post_turn:
        url: "http://"
`,
				wantErr: "channel \"custom\" post_turn webhook url must be a valid http or https URL",
			},
			{
				name: "custom_channel_negative_timeout",
				yamlContent: `
channels:
  default:
    mode: "threads"
  custom:
    mode: "channel"
    hooks:
      on_wake:
        url: "https://example.com/wake"
        timeout_ms: -100
`,
				wantErr: "channel \"custom\" on_wake webhook timeout_ms cannot be negative",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				tmpDir := t.TempDir()
				cfgPath := filepath.Join(tmpDir, "config.yaml")
				if err := os.WriteFile(cfgPath, []byte(tc.yamlContent), 0644); err != nil {
					t.Fatalf("Failed to write temp config: %v", err)
				}

				_, err := LoadConfigFromLookup(func(string) string { return "" }, cfgPath)
				if tc.wantErr == "" {
					if err != nil {
						t.Fatalf("Expected valid config load, got error: %v", err)
					}
				} else {
					if err == nil {
						t.Fatalf("Expected error containing %q, got nil", tc.wantErr)
					}
					if !strings.Contains(err.Error(), tc.wantErr) {
						t.Errorf("Expected error to contain %q, got %q", tc.wantErr, err.Error())
					}
				}
			})
		}
	})
}

func TestConfig_DaemonParameters(t *testing.T) {
	yamlContent := `
daemon_idle_timeout: 12h
max_concurrent_daemons: 25
max_background_task_duration: 90m
`
	cfg, err := LoadConfigFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("LoadConfigFromBytes failed: %v", err)
	}

	data := cfg.Get()
	if data.DaemonIdleTimeout != 12*time.Hour {
		t.Errorf("expected 12h daemon idle timeout, got %v", data.DaemonIdleTimeout)
	}
	if data.MaxConcurrentDaemons != 25 {
		t.Errorf("expected 25 max concurrent daemons, got %d", data.MaxConcurrentDaemons)
	}
	if data.MaxBackgroundTaskDuration != 90*time.Minute {
		t.Errorf("expected 90m max background task duration, got %v", data.MaxBackgroundTaskDuration)
	}

	// Also verify defaults when omitted
	defaultCfg, err := LoadConfigFromBytes([]byte("{}"))
	if err != nil {
		t.Fatalf("LoadConfigFromBytes on empty failed: %v", err)
	}
	defData := defaultCfg.Get()
	if defData.DaemonIdleTimeout != 24*time.Hour {
		t.Errorf("expected 24h default daemon idle timeout, got %v", defData.DaemonIdleTimeout)
	}
	if defData.MaxConcurrentDaemons != 40 {
		t.Errorf("expected 40 default max concurrent daemons, got %d", defData.MaxConcurrentDaemons)
	}
	if defData.MaxBackgroundTaskDuration != 2*time.Hour {
		t.Errorf("expected 2h default max background task duration, got %v", defData.MaxBackgroundTaskDuration)
	}
}

func TestConfig_ClassifierURLAndModel_YAML(t *testing.T) {
	t.Parallel()

	yamlContent := `
classifier_url: http://192.168.1.70:11434
classifier_model: qwen2.5:3b
classifier_protocol: systemone
`
	cfg, err := LoadConfigFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("LoadConfigFromBytes failed: %v", err)
	}

	data := cfg.Get()
	if data.ClassifierURL != "http://192.168.1.70:11434" {
		t.Errorf("expected ClassifierURL=http://192.168.1.70:11434, got %q", data.ClassifierURL)
	}
	if data.ClassifierModel != "qwen2.5:3b" {
		t.Errorf("expected ClassifierModel=qwen2.5:3b, got %q", data.ClassifierModel)
	}
	if data.ClassifierProtocol != "systemone" {
		t.Errorf("expected ClassifierProtocol=systemone, got %q", data.ClassifierProtocol)
	}

	// Verify defaults when omitted
	defaultCfg, err := LoadConfigFromBytes([]byte("{}"))
	if err != nil {
		t.Fatalf("LoadConfigFromBytes on empty failed: %v", err)
	}
	defData := defaultCfg.Get()
	if defData.ClassifierURL != "" {
		t.Errorf("expected empty default ClassifierURL, got %q", defData.ClassifierURL)
	}
	if defData.ClassifierModel != "" {
		t.Errorf("expected empty default ClassifierModel, got %q", defData.ClassifierModel)
	}
	if defData.ClassifierProtocol != "" {
		t.Errorf("expected empty default ClassifierProtocol, got %q", defData.ClassifierProtocol)
	}
}

func TestConfig_ClassifierProtocol_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		yaml        string
		expectError bool
	}{
		{
			name: "valid systemone with url",
			yaml: `
channels:
  default:
    mode: "threads"
classifier_protocol: "systemone"
classifier_url: "http://192.168.1.70:8000/v1/systemone"
`,
			expectError: false,
		},
		{
			name: "valid ollama with url",
			yaml: `
channels:
  default:
    mode: "threads"
classifier_protocol: "ollama"
classifier_url: "http://192.168.1.70:11434"
`,
			expectError: false,
		},
		{
			name: "valid ollama without url",
			yaml: `
channels:
  default:
    mode: "threads"
classifier_protocol: "ollama"
`,
			expectError: false,
		},
		{
			name: "invalid protocol name",
			yaml: `
channels:
  default:
    mode: "threads"
classifier_protocol: "unsupported"
`,
			expectError: true,
		},
		{
			name: "systemone missing url",
			yaml: `
channels:
  default:
    mode: "threads"
classifier_protocol: "systemone"
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.yaml")
			if err := os.WriteFile(configPath, []byte(tt.yaml), 0644); err != nil {
				t.Fatalf("failed to write config: %v", err)
			}
			lookup := func(key string) string { return "" }
			cfg, err := LoadConfigFromLookup(lookup, configPath)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected validation error, got nil (cfg=%+v)", cfg.Current())
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected validation error: %v", err)
				}
			}
		})
	}
}

func TestConfig_ClassifierProtocol_Env(t *testing.T) {
	t.Parallel()

	data := DefaultConfigData()
	lookup := func(key string) string {
		switch key {
		case "CLASSIFIER_PROTOCOL":
			return "SYSTEMONE"
		default:
			return ""
		}
	}

	applyEnvironmentOverrides(data, lookup)
	if data.ClassifierProtocol != "systemone" {
		t.Errorf("expected ClassifierProtocol=systemone, got %q", data.ClassifierProtocol)
	}
}

func TestConfig_ThreadTitleURLAndModel_YAML(t *testing.T) {
	t.Parallel()

	yamlContent := `
thread_title_url: http://192.168.1.70:11434
thread_title_model: qwen2.5:3b
`
	cfg, err := LoadConfigFromBytes([]byte(yamlContent))
	if err != nil {
		t.Fatalf("LoadConfigFromBytes failed: %v", err)
	}

	data := cfg.Get()
	if data.ThreadTitleURL != "http://192.168.1.70:11434" {
		t.Errorf("expected ThreadTitleURL=http://192.168.1.70:11434, got %q", data.ThreadTitleURL)
	}
	if data.ThreadTitleModel != "qwen2.5:3b" {
		t.Errorf("expected ThreadTitleModel=qwen2.5:3b, got %q", data.ThreadTitleModel)
	}

	// Verify defaults when omitted
	defaultCfg, err := LoadConfigFromBytes([]byte("{}"))
	if err != nil {
		t.Fatalf("LoadConfigFromBytes on empty failed: %v", err)
	}
	defData := defaultCfg.Get()
	if defData.ThreadTitleURL != "" {
		t.Errorf("expected empty default ThreadTitleURL, got %q", defData.ThreadTitleURL)
	}
	if defData.ThreadTitleModel != "" {
		t.Errorf("expected empty default ThreadTitleModel, got %q", defData.ThreadTitleModel)
	}
}

func TestConfig_ThreadTitleURLAndModel_Env(t *testing.T) {
	t.Parallel()

	data := DefaultConfigData()
	lookup := func(key string) string {
		switch key {
		case "THREAD_TITLE_URL":
			return "http://edge.lan:11434"
		case "THREAD_TITLE_MODEL":
			return "llama3.2:1b"
		default:
			return ""
		}
	}

	applyEnvironmentOverrides(data, lookup)
	if data.ThreadTitleURL != "http://edge.lan:11434" {
		t.Errorf("expected ThreadTitleURL=http://edge.lan:11434, got %q", data.ThreadTitleURL)
	}
	if data.ThreadTitleModel != "llama3.2:1b" {
		t.Errorf("expected ThreadTitleModel=llama3.2:1b, got %q", data.ThreadTitleModel)
	}
}

func TestTargetMcpConfig_UnmarshalAndValidation(t *testing.T) {
	t.Run("ValidCategorizedTargets", func(t *testing.T) {
		yamlContent := `
channels:
  default:
    mode: "threads"
mcp_servers:
  common:
    scheduler:
      serverUrl: "http://scheduler:8080/mcp"
  discord:
    docker:
      serverUrl: "http://docker:4002/mcp"
  voice:
    kiosk:
      serverUrl: "http://kiosk:4003/mcp"
  ephemeral:
    custom_ephemeral:
      serverUrl: "http://custom-ephemeral:4005/mcp"
`
		cfg, err := LoadConfigFromBytes([]byte(yamlContent))
		if err != nil {
			t.Fatalf("unexpected error parsing categorized mcp_servers: %v", err)
		}
		cur := cfg.Current()
		if len(cur.McpServers.Common) != 1 || cur.McpServers.Common["scheduler"] == nil {
			t.Errorf("expected common scheduler server, got: %v", cur.McpServers.Common)
		}
		if len(cur.McpServers.Discord) != 1 || cur.McpServers.Discord["docker"] == nil {
			t.Errorf("expected discord docker server, got: %v", cur.McpServers.Discord)
		}
		if len(cur.McpServers.Voice) != 1 || cur.McpServers.Voice["kiosk"] == nil {
			t.Errorf("expected voice kiosk server, got: %v", cur.McpServers.Voice)
		}
		if len(cur.McpServers.Ephemeral) != 1 || cur.McpServers.Ephemeral["custom_ephemeral"] == nil {
			t.Errorf("expected ephemeral custom_ephemeral server, got: %v", cur.McpServers.Ephemeral)
		}
	})

	t.Run("NullCategoryHandling", func(t *testing.T) {
		yamlContent := `
channels:
  default:
    mode: "threads"
mcp_servers:
  common: null
  discord: null
  voice: null
  ephemeral: null
`
		cfg, err := LoadConfigFromBytes([]byte(yamlContent))
		if err != nil {
			t.Fatalf("unexpected error parsing null categorized mcp_servers: %v", err)
		}
		cur := cfg.Current()
		if cur.McpServers.Common == nil || len(cur.McpServers.Common) != 0 {
			t.Errorf("expected initialized empty common map, got: %v", cur.McpServers.Common)
		}
		if cur.McpServers.Discord == nil || len(cur.McpServers.Discord) != 0 {
			t.Errorf("expected initialized empty discord map, got: %v", cur.McpServers.Discord)
		}
		if cur.McpServers.Voice == nil || len(cur.McpServers.Voice) != 0 {
			t.Errorf("expected initialized empty voice map, got: %v", cur.McpServers.Voice)
		}
		if cur.McpServers.Ephemeral == nil || len(cur.McpServers.Ephemeral) != 0 {
			t.Errorf("expected initialized empty ephemeral map, got: %v", cur.McpServers.Ephemeral)
		}
	})

	t.Run("RejectLegacyFlatKeys", func(t *testing.T) {
		yamlContent := `
channels:
  default:
    mode: "threads"
mcp_servers:
  weather:
    serverUrl: "http://weather:8080/mcp"
`
		_, err := LoadConfigFromBytes([]byte(yamlContent))
		if err == nil {
			t.Fatal("expected error parsing flat legacy mcp_servers, got nil")
		}
		if !strings.Contains(err.Error(), "weather") && !strings.Contains(err.Error(), "unrecognized target category") {
			t.Errorf("expected error mentioning unrecognized target category or weather, got: %v", err)
		}
		if !strings.Contains(err.Error(), "ephemeral") {
			t.Errorf("expected error message to mention 'ephemeral' allowed category, got: %v", err)
		}
	})

	t.Run("RejectNonMappingCategory", func(t *testing.T) {
		yamlContent := `
channels:
  default:
    mode: "threads"
mcp_servers:
  common: "invalid-string"
`
		_, err := LoadConfigFromBytes([]byte(yamlContent))
		if err == nil {
			t.Fatal("expected error parsing non-mapping category, got nil")
		}
	})

	t.Run("DeepCloneIndependence", func(t *testing.T) {
		orig := &ConfigData{
			McpServers: TargetMcpConfig{
				Common:    map[string]json.RawMessage{"srv1": json.RawMessage(`{"url":"http://c"}`)},
				Discord:   map[string]json.RawMessage{"srv2": json.RawMessage(`{"url":"http://d"}`)},
				Voice:     map[string]json.RawMessage{"srv3": json.RawMessage(`{"url":"http://v"}`)},
				Ephemeral: map[string]json.RawMessage{"srv4": json.RawMessage(`{"url":"http://e"}`)},
			},
		}

		cloned := cloneConfigData(orig)

		// Mutate original maps
		orig.McpServers.Common["new"] = json.RawMessage(`{}`)
		orig.McpServers.Discord["new"] = json.RawMessage(`{}`)
		orig.McpServers.Voice["new"] = json.RawMessage(`{}`)
		orig.McpServers.Ephemeral["new"] = json.RawMessage(`{}`)

		if _, exists := cloned.McpServers.Common["new"]; exists {
			t.Errorf("OCP violation: Common map was shallow-copied")
		}
		if _, exists := cloned.McpServers.Discord["new"]; exists {
			t.Errorf("OCP violation: Discord map was shallow-copied")
		}
		if _, exists := cloned.McpServers.Voice["new"]; exists {
			t.Errorf("OCP violation: Voice map was shallow-copied")
		}
		if _, exists := cloned.McpServers.Ephemeral["new"]; exists {
			t.Errorf("OCP violation: Ephemeral map was shallow-copied")
		}

		// Mutate byte slice
		orig.McpServers.Common["srv1"][0] = 'X'
		if cloned.McpServers.Common["srv1"][0] == 'X' {
			t.Errorf("OCP violation: RawMessage byte slice backing array was not cloned")
		}
	})

	t.Run("DeepCloneNilSafety", func(t *testing.T) {
		orig := &ConfigData{
			McpServers: TargetMcpConfig{
				Common:    nil,
				Discord:   nil,
				Voice:     nil,
				Ephemeral: nil,
			},
		}
		cloned := cloneConfigData(orig)
		if cloned == nil {
			t.Fatal("expected non-nil clone")
		}
		if cloned.McpServers.Ephemeral != nil {
			t.Errorf("expected nil Ephemeral map for nil src, got: %v", cloned.McpServers.Ephemeral)
		}
	})
}

func TestExampleConfig_Valid(t *testing.T) {
	t.Setenv("AGY_MODEL", "")
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	var examplePath string
	for {
		candidate := filepath.Join(dir, "config.example.yaml")
		if _, err := os.Stat(candidate); err == nil {
			examplePath = candidate
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find config.example.yaml in parent directories from %s", dir)
		}
		dir = parent
	}

	cfg, err := LoadConfigFromPaths(examplePath)
	if err != nil {
		t.Fatalf("failed to load config.example.yaml: %v", err)
	}
	if cfg == nil || cfg.Current() == nil {
		t.Fatalf("expected non-nil config loaded from config.example.yaml")
	}
	if cfg.Current().Model != "gemini-2.5-flash" {
		t.Errorf("expected model 'gemini-2.5-flash', got %q", cfg.Current().Model)
	}
	if cfg.Current().SystemChannel != "aerial-dev" {
		t.Errorf("expected system channel 'aerial-dev', got %q", cfg.Current().SystemChannel)
	}
	if defPolicy, ok := cfg.Current().Channels["default"]; !ok || defPolicy.Mode != "threads" {
		t.Errorf("expected channels.default.mode to be 'threads', got %+v", defPolicy)
	}
}

func TestVoiceConfig_UnmarshalAndValidation(t *testing.T) {
	t.Parallel()

	t.Run("ValidPrewarmedTargets", func(t *testing.T) {
		t.Parallel()
		yamlContent := `
voice:
  prewarmed_targets:
    - "kiosk"
    - " touch-kiosk-kitchen "
    - ""
    - "   "
    - "livingroom-dock"
`
		cfg, err := LoadConfigFromBytes([]byte(yamlContent))
		if err != nil {
			t.Fatalf("unexpected error parsing voice config: %v", err)
		}
		targets := cfg.VoicePrewarmedTargets()
		expected := []string{"kiosk", "touch-kiosk-kitchen", "livingroom-dock"}
		if len(targets) != len(expected) {
			t.Fatalf("expected %d targets, got %d: %v", len(expected), len(targets), targets)
		}
		for i, exp := range expected {
			if targets[i] != exp {
				t.Errorf("at index %d: expected %q, got %q", i, exp, targets[i])
			}
		}
	})

	t.Run("EmptyAndNilVoiceConfig", func(t *testing.T) {
		t.Parallel()
		// Omitted voice block defaults to kiosk for backwards compatibility
		cfgDefault, err := LoadConfigFromBytes([]byte("model: test-model\n"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		targetsDefault := cfgDefault.VoicePrewarmedTargets()
		if len(targetsDefault) != 1 || targetsDefault[0] != "kiosk" {
			t.Errorf("expected default ['kiosk'] targets for unspecified voice config, got: %v", targetsDefault)
		}

		// Explicitly empty prewarmed_targets yields empty slice
		cfgEmpty, err := LoadConfigFromBytes([]byte("model: test-model\nvoice:\n  prewarmed_targets: []\n"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		targetsEmpty := cfgEmpty.VoicePrewarmedTargets()
		if len(targetsEmpty) != 0 {
			t.Errorf("expected empty targets for explicit empty list, got: %v", targetsEmpty)
		}

		var nilCfg *Config
		if nilTargets := nilCfg.VoicePrewarmedTargets(); len(nilTargets) != 0 {
			t.Errorf("expected empty targets for nil *Config, got: %v", nilTargets)
		}
	})

	t.Run("DeepCloneIndependence", func(t *testing.T) {
		t.Parallel()
		orig := &ConfigData{
			Voice: VoiceConfig{
				Engine:           "gemini_api",
				Model:            "gemini-2.5-flash",
				PrewarmedTargets: []string{"kiosk", "dock"},
			},
		}
		cloned := cloneConfigData(orig)
		if len(cloned.Voice.PrewarmedTargets) != 2 {
			t.Fatalf("expected 2 cloned targets, got %d", len(cloned.Voice.PrewarmedTargets))
		}
		if cloned.Voice.Engine != "gemini_api" {
			t.Fatalf("expected cloned engine 'gemini_api', got %q", cloned.Voice.Engine)
		}
		if cloned.Voice.Model != "gemini-2.5-flash" {
			t.Fatalf("expected cloned model 'gemini-2.5-flash', got %q", cloned.Voice.Model)
		}

		// Mutate cloned slice and fields
		cloned.Voice.PrewarmedTargets[0] = "mutated"
		cloned.Voice.PrewarmedTargets = append(cloned.Voice.PrewarmedTargets, "extra")
		cloned.Voice.Engine = "agy"
		cloned.Voice.Model = "gemini-2.5-pro"

		if orig.Voice.PrewarmedTargets[0] != "kiosk" {
			t.Errorf("OCP violation: mutating cloned targets affected original[0]: got %q, want 'kiosk'", orig.Voice.PrewarmedTargets[0])
		}
		if len(orig.Voice.PrewarmedTargets) != 2 {
			t.Errorf("OCP violation: appending to cloned targets affected original length: got %d, want 2", len(orig.Voice.PrewarmedTargets))
		}
		if orig.Voice.Engine != "gemini_api" {
			t.Errorf("OCP violation: mutating cloned engine affected original: got %q, want 'gemini_api'", orig.Voice.Engine)
		}
		if orig.Voice.Model != "gemini-2.5-flash" {
			t.Errorf("OCP violation: mutating cloned model affected original: got %q, want 'gemini-2.5-flash'", orig.Voice.Model)
		}
	})

	t.Run("DefensiveGetterCopy", func(t *testing.T) {
		t.Parallel()
		yamlContent := `
voice:
  prewarmed_targets:
    - "kiosk"
`
		cfg, err := LoadConfigFromBytes([]byte(yamlContent))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		t1 := cfg.VoicePrewarmedTargets()
		t1[0] = "modified"

		t2 := cfg.VoicePrewarmedTargets()
		if t2[0] != "kiosk" {
			t.Errorf("expected getter to return defensive copy: got %q, want 'kiosk'", t2[0])
		}
	})

	t.Run("AllowedTools_UnmarshalCloneAndGetter", func(t *testing.T) {
		t.Parallel()
		yamlContent := `
voice:
  allowed_tools:
    - "ha_call_read_tool"
    - "ha_call_write_tool"
    - "get_events"
`
		cfg, err := LoadConfigFromBytes([]byte(yamlContent))
		if err != nil {
			t.Fatalf("unexpected error parsing allowed_tools: %v", err)
		}
		tools := cfg.VoiceAllowedTools()
		if len(tools) != 3 {
			t.Fatalf("expected 3 allowed tools, got %d: %v", len(tools), tools)
		}
		if tools[0] != "ha_call_read_tool" || tools[1] != "ha_call_write_tool" || tools[2] != "get_events" {
			t.Errorf("unexpected allowed tools: %v", tools)
		}

		// Defensive copy check
		tools[0] = "mutated"
		tools2 := cfg.VoiceAllowedTools()
		if tools2[0] != "ha_call_read_tool" {
			t.Errorf("mutating getter return mutated internal config: got %q, want 'ha_call_read_tool'", tools2[0])
		}

		// Nil config returns empty slice
		var nilCfg *Config
		if nilTools := nilCfg.VoiceAllowedTools(); len(nilTools) != 0 {
			t.Errorf("expected empty tools for nil *Config, got: %v", nilTools)
		}

		// Deep clone independence
		orig := &ConfigData{
			Voice: VoiceConfig{
				AllowedTools: []string{"tool1", "tool2"},
			},
		}
		cloned := cloneConfigData(orig)
		if len(cloned.Voice.AllowedTools) != 2 {
			t.Fatalf("expected 2 cloned allowed tools, got %d", len(cloned.Voice.AllowedTools))
		}
		cloned.Voice.AllowedTools[0] = "mutated_tool"
		cloned.Voice.AllowedTools = append(cloned.Voice.AllowedTools, "extra_tool")
		if orig.Voice.AllowedTools[0] != "tool1" {
			t.Errorf("mutating cloned AllowedTools affected original: got %q, want 'tool1'", orig.Voice.AllowedTools[0])
		}
		if len(orig.Voice.AllowedTools) != 2 {
			t.Errorf("appending to cloned AllowedTools affected original: got %d, want 2", len(orig.Voice.AllowedTools))
		}
	})
}

func TestVoiceEngine(t *testing.T) {
	t.Parallel()

	t.Run("NilConfig", func(t *testing.T) {
		t.Parallel()
		var cfg *Config
		if got := cfg.VoiceEngine(); got != "agy" {
			t.Errorf("expected VoiceEngine() for nil *Config to be 'agy', got %q", got)
		}

		var data *ConfigData
		if got := data.VoiceEngine(); got != "agy" {
			t.Errorf("expected VoiceEngine() for nil *ConfigData to be 'agy', got %q", got)
		}
	})

	t.Run("TableDrivenResolution", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			yamlContent string
			cfgData     *ConfigData
			expected    string
		}{
			{
				name:        "omitted_voice_block",
				yamlContent: "model: test-model\n",
				expected:    "agy",
			},
			{
				name:        "empty_voice_block",
				yamlContent: "model: test-model\nvoice: {}\n",
				expected:    "agy",
			},
			{
				name:        "empty_engine_string",
				yamlContent: "model: test-model\nvoice:\n  engine: \"\"\n",
				expected:    "agy",
			},
			{
				name:        "whitespace_engine_string",
				yamlContent: "model: test-model\nvoice:\n  engine: \"   \"\n",
				expected:    "agy",
			},
			{
				name:        "explicit_agy_lowercase",
				yamlContent: "voice:\n  engine: \"agy\"\n",
				expected:    "agy",
			},
			{
				name:        "explicit_agy_uppercase",
				yamlContent: "voice:\n  engine: \"AGY\"\n",
				expected:    "agy",
			},
			{
				name:        "explicit_agy_with_spaces",
				yamlContent: "voice:\n  engine: \"  agy  \"\n",
				expected:    "agy",
			},
			{
				name:        "gemini_api_exact",
				yamlContent: "voice:\n  engine: \"gemini_api\"\n",
				expected:    "gemini_api",
			},
			{
				name:        "gemini_api_uppercase",
				yamlContent: "voice:\n  engine: \"GEMINI_API\"\n",
				expected:    "gemini_api",
			},
			{
				name:        "gemini_api_with_spaces",
				yamlContent: "voice:\n  engine: \" gemini_api \"\n",
				expected:    "gemini_api",
			},
			{
				name:        "gemini_alias",
				yamlContent: "voice:\n  engine: \"gemini\"\n",
				expected:    "gemini_api",
			},
			{
				name:        "gemini_alias_uppercase",
				yamlContent: "voice:\n  engine: \"GEMINI\"\n",
				expected:    "gemini_api",
			},
			{
				name:        "gemini_alias_with_spaces",
				yamlContent: "voice:\n  engine: \"  gemini  \"\n",
				expected:    "gemini_api",
			},
			{
				name:        "direct_api_alias",
				yamlContent: "voice:\n  engine: \"direct_api\"\n",
				expected:    "gemini_api",
			},
			{
				name:        "direct_api_alias_uppercase",
				yamlContent: "voice:\n  engine: \"DIRECT_API\"\n",
				expected:    "gemini_api",
			},
			{
				name:        "direct_api_alias_with_spaces",
				yamlContent: "voice:\n  engine: \" direct_api \"\n",
				expected:    "gemini_api",
			},
			{
				name:        "unrecognized_fallback_custom",
				yamlContent: "voice:\n  engine: \"custom_engine\"\n",
				expected:    "agy",
			},
			{
				name:        "unrecognized_fallback_openai",
				yamlContent: "voice:\n  engine: \"openai\"\n",
				expected:    "agy",
			},
			{
				name:        "unrecognized_fallback_numeric",
				yamlContent: "voice:\n  engine: \"12345\"\n",
				expected:    "agy",
			},
			{
				name:        "default_config_data",
				cfgData:     DefaultConfigData(),
				expected:    "agy",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				var cfg *Config
				if tc.cfgData != nil {
					cfg = NewFromData(tc.cfgData)
				} else {
					var err error
					cfg, err = LoadConfigFromBytes([]byte(tc.yamlContent))
					if err != nil {
						t.Fatalf("failed to load config from bytes: %v", err)
					}
				}

				if got := cfg.VoiceEngine(); got != tc.expected {
					t.Errorf("cfg.VoiceEngine() = %q, want %q", got, tc.expected)
				}
				if got := cfg.Current().VoiceEngine(); got != tc.expected {
					t.Errorf("cfg.Current().VoiceEngine() = %q, want %q", got, tc.expected)
				}
			})
		}
	})

	t.Run("CloningPreservesEngineWithoutCrossMutation", func(t *testing.T) {
		t.Parallel()

		orig := &ConfigData{
			Voice: VoiceConfig{
				Engine:           "gemini_api",
				PrewarmedTargets: []string{"kiosk"},
			},
		}

		cloned := cloneConfigData(orig)
		if cloned.Voice.Engine != "gemini_api" {
			t.Fatalf("expected cloned.Voice.Engine to be 'gemini_api', got %q", cloned.Voice.Engine)
		}

		// Mutate cloned struct
		cloned.Voice.Engine = "agy"
		if orig.Voice.Engine != "gemini_api" {
			t.Errorf("OCP violation: mutating cloned.Voice.Engine altered orig.Voice.Engine: got %q, want 'gemini_api'", orig.Voice.Engine)
		}

		// Mutate orig struct
		orig.Voice.Engine = "direct_api"
		if cloned.Voice.Engine != "agy" {
			t.Errorf("OCP violation: mutating orig.Voice.Engine altered cloned.Voice.Engine: got %q, want 'agy'", cloned.Voice.Engine)
		}

		// Test through Config wrapper
		cfg := NewFromData(orig)
		if cfg.VoiceEngine() != "gemini_api" {
			t.Fatalf("expected cfg.VoiceEngine() to reflect updated orig 'gemini_api', got %q", cfg.VoiceEngine())
		}

		clonedCfg := cfg.Clone()
		if clonedCfg.VoiceEngine() != "gemini_api" {
			t.Fatalf("expected clonedCfg.VoiceEngine() to be 'gemini_api', got %q", clonedCfg.VoiceEngine())
		}

		// Update clonedCfg with new data
		clonedCfg.Update(&ConfigData{
			Voice: VoiceConfig{
				Engine: "agy",
			},
		})
		if clonedCfg.VoiceEngine() != "agy" {
			t.Errorf("expected clonedCfg.VoiceEngine() to be 'agy' after update, got %q", clonedCfg.VoiceEngine())
		}
		if cfg.VoiceEngine() != "gemini_api" {
			t.Errorf("OCP violation: updating clonedCfg affected original cfg: got %q, want 'gemini_api'", cfg.VoiceEngine())
		}
	})
}

func TestVoiceModel(t *testing.T) {
	t.Parallel()

	t.Run("NilConfig", func(t *testing.T) {
		t.Parallel()
		var cfg *Config
		if got := cfg.VoiceModel(); got != "" {
			t.Errorf("expected VoiceModel() for nil *Config to be empty, got %q", got)
		}

		var data *ConfigData
		if got := data.VoiceModel(); got != "" {
			t.Errorf("expected VoiceModel() for nil *ConfigData to be empty, got %q", got)
		}
	})

	t.Run("ConfiguredModel", func(t *testing.T) {
		t.Parallel()
		yamlContent := `
voice:
  model: " gemini-2.5-flash "
`
		cfg, err := LoadConfigFromBytes([]byte(yamlContent))
		if err != nil {
			t.Fatalf("failed to load config: %v", err)
		}
		if got := cfg.VoiceModel(); got != "gemini-2.5-flash" {
			t.Errorf("cfg.VoiceModel() = %q, want 'gemini-2.5-flash'", got)
		}
		if got := cfg.Current().VoiceModel(); got != "gemini-2.5-flash" {
			t.Errorf("cfg.Current().VoiceModel() = %q, want 'gemini-2.5-flash'", got)
		}
	})
}

func TestChannelPolicy_CanonicalModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		mode              string
		wakeMode          string
		wantThread        bool
		wantIgnored       bool
		wantWakeMode      string
		wantThresholdZero bool
	}{
		{
			name:              "canonical_thread",
			mode:              "thread",
			wantThread:        true,
			wantIgnored:       false,
			wantWakeMode:      "all",
			wantThresholdZero: true,
		},
		{
			name:              "canonical_classifier",
			mode:              "classifier",
			wantThread:        false,
			wantIgnored:       false,
			wantWakeMode:      "classifier",
			wantThresholdZero: false,
		},
		{
			name:              "canonical_mention",
			mode:              "mention",
			wantThread:        false,
			wantIgnored:       false,
			wantWakeMode:      "mention",
			wantThresholdZero: true,
		},
		{
			name:              "canonical_ignore",
			mode:              "ignore",
			wantThread:        false,
			wantIgnored:       true,
			wantWakeMode:      "ignore",
			wantThresholdZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := ChannelPolicy{Mode: tt.mode, WakeMode: tt.wakeMode}
			if got := p.IsThread(); got != tt.wantThread {
				t.Errorf("IsThread() = %v, want %v", got, tt.wantThread)
			}
			if got := p.IsIgnored(); got != tt.wantIgnored {
				t.Errorf("IsIgnored() = %v, want %v", got, tt.wantIgnored)
			}
			if got := p.GetWakeMode(); got != tt.wantWakeMode {
				t.Errorf("GetWakeMode() = %q, want %q", got, tt.wantWakeMode)
			}
			thresh := p.GetAmbientWakeThreshold()
			if tt.wantThresholdZero && thresh != 0.0 {
				t.Errorf("GetAmbientWakeThreshold() = %f, want 0.0", thresh)
			}
			if !tt.wantThresholdZero && thresh != 0.80 {
				t.Errorf("GetAmbientWakeThreshold() = %f, want 0.80", thresh)
			}
		})
	}
}

func TestChannelPolicy_Normalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    ChannelPolicy
		expected ChannelPolicy
	}{
		{
			name:     "legacy_threads",
			input:    ChannelPolicy{Mode: "threads"},
			expected: ChannelPolicy{Mode: "thread"},
		},
		{
			name:     "legacy_channel_mention",
			input:    ChannelPolicy{Mode: "channel", WakeMode: "mention"},
			expected: ChannelPolicy{Mode: "mention", WakeMode: "mention"},
		},
		{
			name:     "legacy_channel_classifier",
			input:    ChannelPolicy{Mode: "channel", WakeMode: "classifier"},
			expected: ChannelPolicy{Mode: "classifier", WakeMode: "classifier"},
		},
		{
			name:     "legacy_channel_default_wake",
			input:    ChannelPolicy{Mode: "channel"},
			expected: ChannelPolicy{Mode: "classifier"},
		},
		{
			name:     "legacy_disabled",
			input:    ChannelPolicy{Mode: "disabled"},
			expected: ChannelPolicy{Mode: "ignore"},
		},
		{
			name:     "canonical_thread",
			input:    ChannelPolicy{Mode: "thread"},
			expected: ChannelPolicy{Mode: "thread"},
		},
		{
			name:     "canonical_mention",
			input:    ChannelPolicy{Mode: "mention"},
			expected: ChannelPolicy{Mode: "mention"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := tt.input
			p.Normalize()
			if p.Mode != tt.expected.Mode {
				t.Errorf("Normalize() Mode = %q, want %q", p.Mode, tt.expected.Mode)
			}
		})
	}
}

func TestAmbientContext_YAMLUnmarshal(t *testing.T) {
	t.Parallel()
	yamlContent := `
voice:
  engine: "gemini_api"
  ambient_context:
    cache_ttl: "45s"
    timeout_ms: 300
    template_path: "/path/to/template.tmpl"
    template: "Voice template: {{.now}}"
    tools:
      - name: "ha"
        server: "ha-mcp"
        tool: "get_state"
        arguments:
          entity_id: "light.living_room"
channels:
  home:
    mode: "thread"
    ambient_context:
      cache_ttl: "1m"
      timeout_ms: 500
      template: "Channel template: {{.now}}"
      tools:
        - name: "weather"
          tool: "current_weather"
          arguments:
            city: "San Francisco"
`
	var cfg ConfigData
	if err := yaml.Unmarshal([]byte(yamlContent), &cfg); err != nil {
		t.Fatalf("yaml.Unmarshal failed: %v", err)
	}

	// Verify voice ambient_context
	if cfg.Voice.AmbientContext == nil {
		t.Fatal("expected cfg.Voice.AmbientContext to be non-nil")
	}
	vac := cfg.Voice.AmbientContext
	if vac.CacheTTL != "45s" {
		t.Errorf("expected CacheTTL '45s', got %q", vac.CacheTTL)
	}
	if vac.TimeoutMs != 300 {
		t.Errorf("expected TimeoutMs 300, got %d", vac.TimeoutMs)
	}
	if vac.TemplatePath != "/path/to/template.tmpl" {
		t.Errorf("expected TemplatePath '/path/to/template.tmpl', got %q", vac.TemplatePath)
	}
	if vac.Template != "Voice template: {{.now}}" {
		t.Errorf("expected Template 'Voice template: {{.now}}', got %q", vac.Template)
	}
	if len(vac.Tools) != 1 {
		t.Fatalf("expected 1 tool in Voice AmbientContext, got %d", len(vac.Tools))
	}
	if vac.Tools[0].Name != "ha" || vac.Tools[0].Server != "ha-mcp" || vac.Tools[0].Tool != "get_state" {
		t.Errorf("unexpected tool config: %+v", vac.Tools[0])
	}
	if vac.Tools[0].Arguments["entity_id"] != "light.living_room" {
		t.Errorf("unexpected tool argument: %+v", vac.Tools[0].Arguments)
	}

	// Verify channels ambient_context
	homeCh, ok := cfg.Channels["home"]
	if !ok {
		t.Fatal("expected channel 'home' in cfg.Channels")
	}
	if homeCh.AmbientContext == nil {
		t.Fatal("expected homeCh.AmbientContext to be non-nil")
	}
	cac := homeCh.AmbientContext
	if cac.CacheTTL != "1m" || cac.TimeoutMs != 500 {
		t.Errorf("unexpected channel ambient context: %+v", cac)
	}
	if len(cac.Tools) != 1 || cac.Tools[0].Name != "weather" {
		t.Errorf("unexpected channel tools: %+v", cac.Tools)
	}
	if cac.Tools[0].Arguments["city"] != "San Francisco" {
		t.Errorf("unexpected channel tool argument: %+v", cac.Tools[0].Arguments)
	}
}

func TestAmbientContext_DeepCloneIndependence(t *testing.T) {
	t.Parallel()
	src := &AmbientContextConfig{
		CacheTTL:  "20s",
		TimeoutMs: 150,
		Template:  "Test {{.now}}",
		Tools: []AmbientToolConfig{
			{
				Name:   "ha",
				Server: "ha-mcp",
				Tool:   "get_state",
				Arguments: map[string]interface{}{
					"entity_id": "light.kitchen",
					"nested": map[string]interface{}{
						"key": "val",
					},
					"tags": []interface{}{"a", "b"},
				},
			},
		},
	}

	cloned := cloneAmbientContextConfig(src)
	if cloned == nil {
		t.Fatal("expected non-nil clone")
	}

	// Mutate clone
	cloned.CacheTTL = "99s"
	cloned.Tools[0].Name = "mutated"
	cloned.Tools[0].Arguments["entity_id"] = "light.bedroom"
	cloned.Tools[0].Arguments["new_key"] = 123
	nestedMap := cloned.Tools[0].Arguments["nested"].(map[string]interface{})
	nestedMap["key"] = "mutated_val"
	tagsSlice := cloned.Tools[0].Arguments["tags"].([]interface{})
	tagsSlice[0] = "mutated_tag"

	// Verify src was untouched
	if src.CacheTTL != "20s" {
		t.Errorf("src.CacheTTL mutated: got %q, want '20s'", src.CacheTTL)
	}
	if src.Tools[0].Name != "ha" {
		t.Errorf("src.Tools[0].Name mutated: got %q, want 'ha'", src.Tools[0].Name)
	}
	if src.Tools[0].Arguments["entity_id"] != "light.kitchen" {
		t.Errorf("src.Tools[0].Arguments entity_id mutated: got %v", src.Tools[0].Arguments["entity_id"])
	}
	if _, ok := src.Tools[0].Arguments["new_key"]; ok {
		t.Errorf("src.Tools[0].Arguments contains new_key from clone")
	}
	srcNested := src.Tools[0].Arguments["nested"].(map[string]interface{})
	if srcNested["key"] != "val" {
		t.Errorf("src nested map mutated: got %v, want 'val'", srcNested["key"])
	}
	srcTags := src.Tools[0].Arguments["tags"].([]interface{})
	if srcTags[0] != "a" {
		t.Errorf("src tags slice mutated: got %v, want 'a'", srcTags[0])
	}

	// Test cloneChannelPolicy
	cp := ChannelPolicy{
		Mode:           "thread",
		AmbientContext: src,
	}
	clonedCP := cloneChannelPolicy(cp)
	clonedCP.AmbientContext.CacheTTL = "88s"
	if src.CacheTTL != "20s" {
		t.Errorf("src.CacheTTL mutated via cloneChannelPolicy: got %q", src.CacheTTL)
	}

	// Test cloneConfigData
	cd := &ConfigData{
		Voice: VoiceConfig{
			AmbientContext: src,
		},
	}
	clonedCD := cloneConfigData(cd)
	clonedCD.Voice.AmbientContext.CacheTTL = "77s"
	if src.CacheTTL != "20s" {
		t.Errorf("src.CacheTTL mutated via cloneConfigData: got %q", src.CacheTTL)
	}

	// Test ResolveChannelPolicy inheritance
	channels := map[string]ChannelPolicy{
		"default": {
			Mode:           "thread",
			AmbientContext: src,
		},
	}
	resolved := ResolveChannelPolicy(channels, "custom", "custom")
	if resolved.AmbientContext == nil {
		t.Fatal("expected resolved.AmbientContext to inherit from default")
	}
	resolved.AmbientContext.CacheTTL = "66s"
	if src.CacheTTL != "20s" {
		t.Errorf("src.CacheTTL mutated via ResolveChannelPolicy: got %q", src.CacheTTL)
	}
}

func TestAmbientContext_GetterNilSafety(t *testing.T) {
	t.Parallel()

	// Nil *Config
	var nilCfg *Config
	if nilCfg.VoiceAmbientContext() != nil {
		t.Errorf("expected nil for nilCfg.VoiceAmbientContext()")
	}

	// Nil *ConfigData
	var nilData *ConfigData
	if nilData.VoiceAmbientContext() != nil {
		t.Errorf("expected nil for nilData.VoiceAmbientContext()")
	}

	// Empty Config / ConfigData without Voice.AmbientContext
	cfgData := &ConfigData{}
	if cfgData.VoiceAmbientContext() != nil {
		t.Errorf("expected nil for empty ConfigData.VoiceAmbientContext()")
	}

	cfg := &Config{}
	cfg.current.Store(cfgData)
	if cfg.VoiceAmbientContext() != nil {
		t.Errorf("expected nil for empty Config.VoiceAmbientContext()")
	}

	// ConfigData with Voice.AmbientContext set
	cfgData.Voice.AmbientContext = &AmbientContextConfig{
		CacheTTL: "10s",
		Template: "Hello",
	}
	ret := cfgData.VoiceAmbientContext()
	if ret == nil || ret.CacheTTL != "10s" {
		t.Fatalf("unexpected return from cfgData.VoiceAmbientContext(): %+v", ret)
	}

	// Defensive copy check
	ret.CacheTTL = "mutated"
	ret2 := cfgData.VoiceAmbientContext()
	if ret2.CacheTTL != "10s" {
		t.Errorf("VoiceAmbientContext did not return a defensive copy: got %q", ret2.CacheTTL)
	}

	// Config wrapper check
	cfgRet := cfg.VoiceAmbientContext()
	if cfgRet == nil || cfgRet.CacheTTL != "10s" {
		t.Fatalf("unexpected return from cfg.VoiceAmbientContext(): %+v", cfgRet)
	}
	cfgRet.CacheTTL = "mutated2"
	cfgRet2 := cfg.VoiceAmbientContext()
	if cfgRet2.CacheTTL != "10s" {
		t.Errorf("cfg.VoiceAmbientContext did not return a defensive copy: got %q", cfgRet2.CacheTTL)
	}
}

func TestCloneAmbientContextConfig_NestedArgumentsAndSlices(t *testing.T) {
	src := &AmbientContextConfig{
		CacheTTL: "30s",
		Tools: []AmbientToolConfig{
			{
				Name: "test_tool",
				Arguments: map[string]interface{}{
					"list": []interface{}{"a", map[string]interface{}{"nested": 123}},
					"val":  42,
				},
			},
		},
	}
	dst := cloneAmbientContextConfig(src)
	if dst == nil || len(dst.Tools) != 1 {
		t.Fatalf("expected cloned config with 1 tool")
	}
	src.Tools[0].Arguments["val"] = 99
	if dst.Tools[0].Arguments["val"] == 99 {
		t.Errorf("mutation leaked into cloned arguments map")
	}
	if cloneAmbientContextConfig(nil) != nil {
		t.Errorf("expected nil for nil input")
	}
	if cloneInterfaceMap(nil) != nil {
		t.Errorf("expected nil for nil interface map")
	}
}

func TestConfig_CloneAndNilAccessors(t *testing.T) {
	var nilCfg *Config
	if nilCfg.Clone() != nil {
		t.Errorf("expected nil from nil Config.Clone()")
	}
	if nilCfg.VoiceAmbientContext() != nil {
		t.Errorf("expected nil from nil Config.VoiceAmbientContext()")
	}

	data := &ConfigData{
		Model: "gemini-test",
		Voice: VoiceConfig{
			Engine: "agy",
		},
	}
	clonedData := data.Clone()
	if clonedData == nil || clonedData.Model != "gemini-test" {
		t.Fatalf("unexpected clone from ConfigData.Clone()")
	}

	cfg := NewFromData(data)
	clonedCfg := cfg.Clone()
	if clonedCfg == nil || clonedCfg.Current().Model != "gemini-test" {
		t.Fatalf("unexpected clone from Config.Clone()")
	}
}

func TestConfigSearchPaths_Precedence(t *testing.T) {
	if len(ConfigSearchPaths) == 0 {
		t.Fatalf("expected non-empty ConfigSearchPaths")
	}
	if ConfigSearchPaths[0] != "/local/config.yaml" {
		t.Errorf("expected /local/config.yaml to be first search path, got %q", ConfigSearchPaths[0])
	}
	for _, p := range ConfigSearchPaths {
		if strings.Contains(p, ".lkgc") {
			t.Errorf("expected no .lkgc paths in ConfigSearchPaths, found %q", p)
		}
	}
}




func TestLoadScopeInstructions(t *testing.T) {
	tmpDir := t.TempDir()
	oldDirs := ScopeInstructionsDirs
	oldChanDirs := ChannelInstructionsDirs
	ScopeInstructionsDirs = []string{tmpDir}
	ChannelInstructionsDirs = []string{tmpDir}
	defer func() {
		ScopeInstructionsDirs = oldDirs
		ChannelInstructionsDirs = oldChanDirs
	}()

	if LoadScopeInstructions("") != "" || LoadScopeInstructions("missing-scope") != "" {
		t.Errorf("expected empty instructions for empty or missing scope")
	}

	content := "You are running on touch-kiosk-kitchen. Be brief and spoken."
	if err := os.WriteFile(filepath.Join(tmpDir, "touch-kiosk-kitchen.md"), []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write node instructions: %v", err)
	}

	if got := LoadScopeInstructions("touch-kiosk-kitchen"); got != content {
		t.Errorf("expected %q, got %q", content, got)
	}
	if got := LoadScopeInstructions("touch kiosk kitchen"); got != content {
		t.Errorf("expected space-to-hyphen fallback %q, got %q", content, got)
	}

	if err := os.WriteFile(filepath.Join(tmpDir, "living room.md"), []byte("spaced file"), 0644); err != nil {
		t.Fatalf("Failed to write spaced file: %v", err)
	}
	if got := LoadScopeInstructions("living-room"); got != "spaced file" {
		t.Errorf("expected hyphen-to-space fallback, got %q", got)
	}

	tagContent := "Instruction with </SCOPE_INSTRUCTIONS> attempt"
	cockpitPath := filepath.Join(tmpDir, "cockpit.md")
	if err := os.WriteFile(cockpitPath, []byte(tagContent), 0644); err != nil {
		t.Fatalf("Failed to write cockpit.md: %v", err)
	}
	gotCockpit := LoadScopeInstructions("cockpit")
	if !strings.Contains(gotCockpit, "<\\/SCOPE_INSTRUCTIONS>") {
		t.Errorf("expected escaped tag in %q", gotCockpit)
	}
	if err := os.WriteFile(cockpitPath, []byte("   "), 0644); err != nil {
		t.Fatalf("Failed to truncate cockpit.md: %v", err)
	}
	if gotCached := LoadChannelInstructions("cockpit"); gotCached != gotCockpit {
		t.Errorf("expected cached fallback %q, got %q", gotCockpit, gotCached)
	}
}
