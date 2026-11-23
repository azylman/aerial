package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/sanitizer"
)

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
			got := LoadScopeInstructions(tc.target)
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

// channelHooksSummary renders the resolved hooks through the accessor methods, so nil endpoints, timeout
// defaults and on_timeout normalization all show up in one comparable string.
func channelHooksSummary(p ChannelPolicy) string {
	parts := make([]string, 0, 3)
	for _, h := range []struct {
		name string
		ep   *WebhookEndpoint
	}{{"on_wake", p.Hooks.OnWake}, {"pre_turn", p.Hooks.PreTurn}, {"post_turn", p.Hooks.PostTurn}} {
		url := "-"
		if h.ep != nil {
			url = h.ep.URL
		}
		parts = append(parts, fmt.Sprintf("%s=%s,%v,%s", h.name, url, h.ep.GetTimeout(), h.ep.GetOnTimeout("drop")))
	}
	return fmt.Sprintf("%s no_build_updates=%v", strings.Join(parts, " "), p.NoBuildUpdates)
}

func TestChannelHooks_Validation(t *testing.T) {
	const def = "channels:\n  default:\n    mode: threads\n"
	tests := []struct {
		name    string
		yaml    string
		channel string
		want    string
		wantErr string
	}{
		{
			name: "valid",
			yaml: def + "    hooks:\n      on_wake: {url: 'http://127.0.0.1:8080/w', timeout_ms: 1000}\n      pre_turn: {url: 'https://ex.com/p', timeout_ms: 2000}\n      post_turn: {url: 'http://loc:9000/po'}",
			want: "on_wake=http://127.0.0.1:8080/w,1s,drop pre_turn=https://ex.com/p,2s,drop post_turn=http://loc:9000/po,3s,drop no_build_updates=false",
		},
		{
			name: "timeout defaults and on_timeout normalization",
			yaml: def + "    no_build_updates: true\n    hooks:\n      on_wake: {url: 'http://h/w', timeout_ms: 0, on_timeout: '   '}\n" +
				"      pre_turn: {url: 'http://h/p', timeout_ms: 5000, on_timeout: '  RETRY  '}\n      post_turn: {url: 'http://h/po', timeout_ms: 2500, on_timeout: WaKe}",
			want: "on_wake=http://h/w,3s,drop pre_turn=http://h/p,5s,retry post_turn=http://h/po,2.5s,wake no_build_updates=true",
		},
		{name: "no hooks resolve to nil endpoints", yaml: def, want: "on_wake=-,3s,drop pre_turn=-,3s,drop post_turn=-,3s,drop no_build_updates=false"},
		{
			name:    "custom inherits missing default hooks",
			yaml:    def + "    hooks: {on_wake: {url: 'http://def/wake'}, pre_turn: {url: 'http://def/pre'}}\n  custom:\n    mode: channel\n    hooks: {pre_turn: {url: 'http://cust/pre'}, post_turn: {url: 'http://cust/post'}}",
			channel: "custom",
			want:    "on_wake=http://def/wake,3s,drop pre_turn=http://cust/pre,3s,drop post_turn=http://cust/post,3s,drop no_build_updates=false",
		},
		{name: "default empty url", yaml: def + "    hooks:\n      on_wake: {url: '   '}", wantErr: `channel "default" on_wake webhook url is required`},
		{name: "default invalid scheme", yaml: def + "    hooks:\n      pre_turn: {url: 'ftp://ex.com/h'}", wantErr: `channel "default" pre_turn webhook url must be a valid http or https URL`},
		{name: "default malformed url", yaml: def + "    hooks:\n      post_turn: {url: '://invalid'}", wantErr: `channel "default" post_turn webhook url must be a valid http or https URL`},
		{name: "default neg timeout", yaml: def + "    hooks:\n      on_wake: {url: 'http://loc:8080/w', timeout_ms: -1}", wantErr: `channel "default" on_wake webhook timeout_ms cannot be negative`},
		{name: "custom empty url", yaml: def + "  custom:\n    mode: channel\n    hooks:\n      pre_turn: {url: ''}", wantErr: `channel "custom" pre_turn webhook url is required`},
		{
			name:    "custom invalid host",
			yaml:    def + "  custom:\n    mode: channel\n    hooks:\n      post_turn: {url: 'http://'}",
			wantErr: `channel "custom" post_turn webhook url must be a valid http or https URL`,
		},
		{
			name:    "custom neg timeout",
			yaml:    def + "  custom:\n    mode: channel\n    hooks:\n      on_wake: {url: 'https://ex.com/w', timeout_ms: -100}",
			wantErr: `channel "custom" on_wake webhook timeout_ms cannot be negative`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfigFromLookup(mapLookup(nil), writeTempYAML(t, tc.yaml))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got err=%v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			channels := cfg.Current().Channels
			res := ResolveChannelPolicy(channels, "", tc.channel)
			if got := channelHooksSummary(res); got != tc.want {
				t.Fatalf("resolved hooks = %q, want %q", got, tc.want)
			}
			// The resolved policy is a deep copy: mutating it must not leak back into the source channels.
			for _, ep := range []*WebhookEndpoint{res.Hooks.OnWake, res.Hooks.PreTurn, res.Hooks.PostTurn} {
				if ep != nil {
					ep.URL = "http://mutated"
				}
			}
			if got := channelHooksSummary(ResolveChannelPolicy(channels, "", tc.channel)); got != tc.want {
				t.Errorf("after mutating the resolved copy, hooks = %q, want %q", got, tc.want)
			}
		})
	}
}

type daemonClassifierCase struct {
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
}

func loadDaemonClassifierData(t *testing.T, tc daemonClassifierCase, lookup func(string) string) *ConfigData {
	t.Helper()
	if tc.yaml == "" {
		data := DefaultConfigData()
		applyEnvironmentOverrides(data, lookup)
		return data
	}
	cfg, err := LoadConfigFromLookup(lookup, writeTempYAML(t, tc.yaml))
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		return nil
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return cfg.Current()
}

func assertDaemonClassifierCase(t *testing.T, tc daemonClassifierCase) {
	t.Helper()
	lookup := func(k string) string {
		if tc.env != nil {
			return tc.env[k]
		}
		return ""
	}
	data := loadDaemonClassifierData(t, tc, lookup)
	if data == nil {
		return
	}
	if (tc.wantProtocol != "" && data.ClassifierProtocol != tc.wantProtocol) ||
		(tc.wantClassURL != "" && data.ClassifierURL != tc.wantClassURL) ||
		(tc.wantTitleURL != "" && data.ThreadTitleURL != tc.wantTitleURL) ||
		(tc.wantModel != "" && data.Model != tc.wantModel) ||
		(tc.wantChannel != "" && data.SystemChannel != tc.wantChannel) ||
		(tc.wantIdle != 0 && data.DaemonIdleTimeout != tc.wantIdle) {
		t.Fatalf("field expectation failed for %s: %+v", tc.name, data)
	}
}

func daemonClassifierCases() []daemonClassifierCase {
	return []daemonClassifierCase{
		{name: "daemon explicit", yaml: "channels: {default: {mode: threads}}\ndaemon_idle_timeout: 12h\nmax_concurrent_daemons: 25\nmax_background_task_duration: 90m\n", wantIdle: 12 * time.Hour},
		{name: "daemon defaults", yaml: "channels: {default: {mode: threads}}\n", wantIdle: 24 * time.Hour},
		{name: "classifier yaml", yaml: "channels: {default: {mode: threads}}\nclassifier_url: http://systemone.test:11434\nclassifier_model: qwen2.5:3b\nclassifier_protocol: systemone\n", wantProtocol: "systemone", wantClassURL: "http://systemone.test:11434"},
		{name: "classifier sysone valid", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: systemone\nclassifier_url: http://systemone.test:8000/v1/systemone\n", wantProtocol: "systemone", wantClassURL: "http://systemone.test:8000/v1/systemone"},
		{name: "classifier ollama with url", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: ollama\nclassifier_url: http://systemone.test:11434\n", wantProtocol: "ollama", wantClassURL: "http://systemone.test:11434"},
		{name: "classifier ollama no url", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: ollama\n", wantProtocol: "ollama"},
		{name: "classifier sysone missing url", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: systemone\n", wantErr: true},
		{name: "classifier invalid protocol", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: unsupported\n", wantErr: true},
		{name: "classifier env", env: map[string]string{"CLASSIFIER_PROTOCOL": "SYSTEMONE"}, wantProtocol: "systemone"},
		{name: "thread title yaml", yaml: "channels: {default: {mode: threads}}\nthread_title_url: http://systemone.test:11434\nthread_title_model: qwen2.5:3b\n", wantTitleURL: "http://systemone.test:11434"},
		{name: "thread title env", env: map[string]string{"THREAD_TITLE_URL": "http://edge.lan:11434", "THREAD_TITLE_MODEL": "llama3.2:1b"}, wantTitleURL: "http://edge.lan:11434"},
		{name: "example config", yaml: "model: gemini-2.5-flash\nsystem_channel: aerial-dev\nchannels: {default: {mode: threads}}\n", wantModel: "gemini-2.5-flash", wantChannel: "aerial-dev", wantIdle: 24 * time.Hour},
	}
}

func TestConfig_DaemonClassifierAndThreadTitle(t *testing.T) {
	tests := daemonClassifierCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertDaemonClassifierCase(t, tc)
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

func runVoiceEngineTests(t *testing.T) {
	t.Helper()
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

	processEngineTests := []struct{ in, want string }{
		{"", "agy"},
		{"   ", "agy"},
		{"agy", "agy"},
		{"AGY", "agy"},
		{"  agy  ", "agy"},
		{"claude", "claude"},
		{"CLAUDE", "claude"},
		{" Claude ", "claude"},
		{"codex", "codex"},
		{"CODEX", "codex"},
		{"  codex  ", "codex"},
		{"muse", "muse"},
		{"MUSE", "muse"},
		{"  muse  ", "muse"},
		{"other", "agy"},
	}
	for _, tc := range processEngineTests {
		t.Run("process_engine_"+tc.in, func(t *testing.T) {
			cfg := NewTestConfig(func(c *ConfigData) { c.Engine = tc.in })
			if got := cfg.Engine(); got != tc.want {
				t.Fatalf("Engine(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if got := cfg.Current().GetEngine(); got != tc.want {
				t.Fatalf("Current().GetEngine(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
	if (*Config)(nil).Engine() != "agy" || (*ConfigData)(nil).GetEngine() != "agy" {
		t.Fatal("nil Config/ConfigData Engine accessor failed")
	}
}

func runVoiceFeatureTests(t *testing.T) {
	t.Helper()
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
}

func runVoiceCloneTests(t *testing.T) {
	t.Helper()
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

func TestVoiceConfigAndEngine(t *testing.T) {
	runVoiceEngineTests(t)
	runVoiceFeatureTests(t)
	runVoiceCloneTests(t)
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

func runAmbientContextCloneAndPolicyTests(t *testing.T, src *AmbientContextConfig) {
	t.Helper()
	cloned := cloneAmbientContextConfig(src)
	cloned.CacheTTL, cloned.Tools[0].Name = "99s", "mutated"
	cloned.Tools[0].Arguments["entity_id"] = "light.bedroom"
	nested, ok := cloned.Tools[0].Arguments["nested"].(map[string]any)
	if !ok {
		t.Fatalf("cloned nested argument is %T, want map[string]any", cloned.Tools[0].Arguments["nested"])
	}
	nested["key"] = "mutated_val"
	tags, ok := cloned.Tools[0].Arguments["tags"].([]any)
	if !ok {
		t.Fatalf("cloned tags argument is %T, want []any", cloned.Tools[0].Arguments["tags"])
	}
	tags[0] = "mutated_tag"
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
}

func runAmbientContextAccessorsTests(t *testing.T, src *AmbientContextConfig) {
	t.Helper()
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
	runAmbientContextCloneAndPolicyTests(t, src)
	runAmbientContextAccessorsTests(t, src)
}

func TestRegisterConfigTokens(t *testing.T) {
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)
	tests := []struct {
		name       string
		initialCfg *Config
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
		{
			name: "purges removed config secrets on reload",
			initialCfg: NewFromData(&ConfigData{
				DiscordToken: "secret_discord_token_initial_12345",
				APIKey:       "secret_api_key_initial_888888",
			}),
			cfg: NewFromData(&ConfigData{
				DiscordToken: "secret_discord_token_initial_12345",
			}),
			input:      "sec: secret_api_key_initial_888888, preserved: secret_discord_token_initial_12345",
			wantResult: "sec: secret_api_key_initial_888888, preserved: [REDACTED]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.initialCfg != nil {
				RegisterConfigTokens(tc.initialCfg)
			}
			RegisterConfigTokens(tc.cfg)

			got := sanitizer.SanitizeString(tc.input)
			if got != tc.wantResult {
				t.Errorf("SanitizeString() = %q, want %q", got, tc.wantResult)
			}
		})
	}
}

func TestWithSharedSecretsLookup_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	writeSecrets := func(name, body string) string {
		p := filepath.Join(tmpDir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
		return p
	}
	sharedFile := writeSecrets("shared.json", `{"SECRET_ONLY": "secret-val", "OVERRIDDEN": "shared-val", "POSTGRES_PASSWORD": "secret-pg-pass", "FOO": "  bar  ",
		"DISCORD_BOT_TOKEN": "bot-tok-123", "1PASSWORD_SERVICE_ACCOUNT_TOKEN": "op-tok-789", "GEMINI_HARNESS_API_KEY": "harness-key-abc", "UNIFI_PASS": "unifi-pass-def"}`)
	customHostFile := writeSecrets("custom-host.json", `{"POSTGRES_HOST": "shared-db.internal", "POSTGRES_PASSWORD": "secret-pg-pass"}`)
	noPassFile := writeSecrets("no-pass.json", `{"OTHER": "val"}`)
	stalePrimaryFile := writeSecrets("stale-primary.json", `{"DISCORD_TOKEN": "primary-tok", "DISCORD_BOT_TOKEN": "fallback-tok"}`)
	invalidFile := writeSecrets("invalid.json", `{not valid json`)

	secretsDir := filepath.Join(tmpDir, "secrets_dir")
	for _, sub := range []string{"..data", "subdir"} {
		if err := os.MkdirAll(filepath.Join(secretsDir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, body := range map[string]string{
		"..data/IGNORE_ME": "ignored", ".hidden": "ignored", "subdir/IGNORE_SUB": "ignored",
		"DISCORD_BOT_TOKEN": "dir-bot-token\n", "POSTGRES_PASSWORD": "  dir-pg-pass  ", "EMPTY_KEY": "  ",
	} {
		writeSecrets(filepath.Join("secrets_dir", name), body)
	}

	tests := []struct {
		name    string
		primary map[string]string
		path    string
		key     string
		want    string
	}{
		{name: "primary wins over shared secret", primary: map[string]string{"OVERRIDDEN": "primary-val"}, path: sharedFile, key: "OVERRIDDEN", want: "primary-val"},
		{name: "fallback to shared secret when unset in primary", primary: map[string]string{}, path: sharedFile, key: "SECRET_ONLY", want: "secret-val"},
		{name: "nil primary handled safely", path: sharedFile, key: "SECRET_ONLY", want: "secret-val"},
		{name: "postgres host defaulted when password present in shared secrets", path: sharedFile, key: "POSTGRES_HOST", want: "postgres"},
		{name: "postgres host preserved from primary", primary: map[string]string{"POSTGRES_HOST": "primary-db.internal"}, path: sharedFile, key: "POSTGRES_HOST", want: "primary-db.internal"},
		{name: "postgres host preserved from shared secrets if specified", path: customHostFile, key: "POSTGRES_HOST", want: "shared-db.internal"},
		{name: "postgres host empty when password unset", path: noPassFile, key: "POSTGRES_HOST", want: ""},
		{name: "missing key returns empty", path: sharedFile, key: "NONEXISTENT", want: ""},
		{name: "shared values are trimmed", path: sharedFile, key: "FOO", want: "bar"},
		{name: "discord bot token alias fallback", path: sharedFile, key: "DISCORD_TOKEN", want: "bot-tok-123"},
		{name: "1password service account alias fallback", path: sharedFile, key: "OP_SERVICE_ACCOUNT_TOKEN", want: "op-tok-789"},
		{name: "gemini harness api key is not aliased to agy gemini api key", path: sharedFile, key: "AGY_GEMINI_API_KEY", want: ""},
		{name: "unifi pass alias fallback", path: sharedFile, key: "UNIFI_PASSWORD", want: "unifi-pass-def"},
		{name: "file primary key not overwritten by stale alias", path: stalePrimaryFile, key: "DISCORD_TOKEN", want: "primary-tok"},
		{name: "directory mount file-only secret is trimmed", path: secretsDir, key: "POSTGRES_PASSWORD", want: "dir-pg-pass"},
		{name: "directory mount alias fallback", path: secretsDir, key: "DISCORD_TOKEN", want: "dir-bot-token"},
		{name: "directory mount skips dotfiles", path: secretsDir, key: ".hidden", want: ""},
		{name: "directory mount skips subdirectories", path: secretsDir, key: "IGNORE_SUB", want: ""},
		{name: "invalid json yields no secrets", path: invalidFile, key: "POSTGRES_HOST", want: ""},
		{name: "missing file yields no secrets", path: filepath.Join(tmpDir, "nonexistent.json"), key: "SECRET_ONLY", want: ""},
		{name: "empty path yields no secrets", primary: map[string]string{"OTHER": "primary"}, key: "OTHER", want: "primary"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var prim func(string) string
			if tc.primary != nil {
				prim = mapLookup(tc.primary)
			}
			if got := WithSharedSecretsLookup(prim, tc.path)(tc.key); got != tc.want {
				t.Errorf("lookup(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestRegisterConfigTokens_SharedSecrets_TableDriven(t *testing.T) {
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)
	origSharedPath := SharedSecretsPath
	defer func() {
		SharedSecretsPath = origSharedPath
	}()

	tmpDir := t.TempDir()
	secretsFile := filepath.Join(tmpDir, "shared-secrets.json")
	_ = os.WriteFile(secretsFile, []byte(`{
		"DYNAMIC_SHARED_SECRET": "dynamic_shared_secret_token_val_12345"
	}`), 0o600)

	emptySecretsFile := filepath.Join(tmpDir, "empty-secrets.json")
	_ = os.WriteFile(emptySecretsFile, []byte(`{}`), 0o600)

	secretsDir := filepath.Join(tmpDir, "secrets_dir")
	_ = os.MkdirAll(secretsDir, 0o755)
	_ = os.WriteFile(filepath.Join(secretsDir, "DYNAMIC_DIR_SECRET"), []byte("dynamic_dir_secret_token_val_67890"), 0o600)

	tests := []struct {
		name        string
		initialPath string
		path        string
		cfg         *Config
		input       string
		wantResult  string
	}{
		{
			name:       "shared secrets token registered and redacted from json file",
			path:       secretsFile,
			cfg:        nil,
			input:      "alert with dynamic_shared_secret_token_val_12345 in message",
			wantResult: "alert with [REDACTED] in message",
		},
		{
			name:       "shared secrets token registered and redacted from directory mount",
			path:       secretsDir,
			cfg:        nil,
			input:      "alert with dynamic_dir_secret_token_val_67890 in message",
			wantResult: "alert with [REDACTED] in message",
		},
		{
			name:        "shared secrets purged on reload when secret removed from json file",
			initialPath: secretsFile,
			path:        emptySecretsFile,
			cfg:         nil,
			input:       "alert with dynamic_shared_secret_token_val_12345 in message",
			wantResult:  "alert with dynamic_shared_secret_token_val_12345 in message",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.initialPath != "" {
				SharedSecretsPath = tc.initialPath
				RegisterConfigTokens(nil)
			}
			SharedSecretsPath = tc.path
			RegisterConfigTokens(tc.cfg)
			got := sanitizer.SanitizeString(tc.input)
			if got != tc.wantResult {
				t.Errorf("SanitizeString() = %q, want %q", got, tc.wantResult)
			}
		})
	}
}

func TestCredentialSourcesValidate(t *testing.T) {
	t.Parallel()
	oauthPath, codexPath, musePath := CredentialSource{Path: "/fake/oauth"}, CredentialSource{Path: "/fake/codex"}, CredentialSource{Path: "/fake/muse"}
	off := CredentialSource{Disabled: true}
	const oauthErr = "credential_sources.oauth must specify exactly one nonblank path or disabled: true"
	const codexErr = "credential_sources.codex must specify exactly one nonblank path or disabled: true"
	const museErr = "credential_sources.muse must specify exactly one nonblank path or disabled: true"
	tests := []struct {
		name    string
		yaml    string
		want    CredentialSources
		wantErr string
	}{
		{name: "omitted has no defaults", yaml: "model: fake", wantErr: oauthErr},
		{name: "paths", yaml: "credential_sources: {oauth: {path: /fake/oauth}, codex: {path: /fake/codex}, muse: {path: /fake/muse}}", want: CredentialSources{oauthPath, codexPath, musePath}},
		{name: "disabled", yaml: "credential_sources: {oauth: {disabled: true}, codex: {disabled: true}, muse: {disabled: true}}", want: CredentialSources{off, off, off}},
		{name: "mixed", yaml: "credential_sources: {oauth: {path: /fake/oauth}, codex: {disabled: true}}", want: CredentialSources{OAuth: oauthPath, Codex: off}},
		{name: "oauth missing", yaml: "credential_sources: {codex: {disabled: true}}", want: CredentialSources{Codex: off}, wantErr: oauthErr},
		{name: "codex missing", yaml: "credential_sources: {oauth: {disabled: true}}", want: CredentialSources{OAuth: off}, wantErr: codexErr},
		{
			name:    "oauth both",
			yaml:    "credential_sources: {oauth: {path: /fake/oauth, disabled: true}, codex: {disabled: true}}",
			want:    CredentialSources{OAuth: CredentialSource{Path: "/fake/oauth", Disabled: true}, Codex: off},
			wantErr: oauthErr,
		},
		{
			name:    "codex both",
			yaml:    "credential_sources: {oauth: {disabled: true}, codex: {path: /fake/codex, disabled: true}}",
			want:    CredentialSources{OAuth: off, Codex: CredentialSource{Path: "/fake/codex", Disabled: true}},
			wantErr: codexErr,
		},
		{name: "oauth whitespace", yaml: "credential_sources: {oauth: {path: \" \\t\\n\"}, codex: {disabled: true}}", want: CredentialSources{OAuth: CredentialSource{Path: " \t\n"}, Codex: off}, wantErr: oauthErr},
		{name: "codex whitespace", yaml: "credential_sources: {oauth: {disabled: true}, codex: {path: \" \\t\"}}", want: CredentialSources{OAuth: off, Codex: CredentialSource{Path: " \t"}}, wantErr: codexErr},
		{name: "muse path valid", yaml: "credential_sources: {oauth: {disabled: true}, codex: {disabled: true}, muse: {path: /fake/muse}}", want: CredentialSources{off, off, musePath}},
		{
			name:    "muse both",
			yaml:    "credential_sources: {oauth: {disabled: true}, codex: {disabled: true}, muse: {path: /fake/muse, disabled: true}}",
			want:    CredentialSources{off, off, CredentialSource{Path: "/fake/muse", Disabled: true}},
			wantErr: museErr,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := DefaultConfigData()
			if err := yaml.Unmarshal([]byte(tc.yaml), cfg); err != nil {
				t.Fatal(err)
			}
			if cfg.CredentialSources != tc.want {
				t.Fatalf("sources = %+v, want %+v", cfg.CredentialSources, tc.want)
			}
			err := cfg.CredentialSources.Validate()
			if got := fmt.Sprint(err); (tc.wantErr == "" && err != nil) || (tc.wantErr != "" && got != tc.wantErr) {
				t.Fatalf("Validate error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
