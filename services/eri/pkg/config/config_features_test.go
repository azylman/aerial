package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

	p := ChannelPolicy{Mode: "threads", NoBuildUpdates: true, Hooks: ChannelHooksConfig{
		OnWake:   &WebhookEndpoint{URL: "https://sidecar:8080/on-wake", TimeoutMs: 2500, OnTimeoutAction: "wake"},
		PreTurn:  &WebhookEndpoint{URL: "http://127.0.0.1:9090/pre", TimeoutMs: 1500, OnTimeoutAction: "retry"},
		PostTurn: &WebhookEndpoint{URL: "http://127.0.0.1:9090/post", TimeoutMs: 4000, OnTimeoutAction: "proceed"},
	}}

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
	if p.Hooks.OnWake.URL == "http://cloned/wake" || !cloned.NoBuildUpdates {
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

func daemonClassifierCases(exampleContent string) []daemonClassifierCase {
	return []daemonClassifierCase{
		{name: "daemon explicit", yaml: "channels: {default: {mode: threads}}\ndaemon_idle_timeout: 12h\nmax_concurrent_daemons: 25\nmax_background_task_duration: 90m\n", wantIdle: 12 * time.Hour},
		{name: "daemon defaults", yaml: "channels: {default: {mode: threads}}\n", wantIdle: 24 * time.Hour},
		{name: "classifier yaml", yaml: "channels: {default: {mode: threads}}\nclassifier_url: http://192.168.1.70:11434\nclassifier_model: qwen2.5:3b\nclassifier_protocol: systemone\n", wantProtocol: "systemone", wantClassURL: "http://192.168.1.70:11434"},
		{name: "classifier sysone valid", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: systemone\nclassifier_url: http://192.168.1.70:8000/v1/systemone\n", wantProtocol: "systemone", wantClassURL: "http://192.168.1.70:8000/v1/systemone"},
		{name: "classifier ollama with url", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: ollama\nclassifier_url: http://192.168.1.70:11434\n", wantProtocol: "ollama", wantClassURL: "http://192.168.1.70:11434"},
		{name: "classifier ollama no url", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: ollama\n", wantProtocol: "ollama"},
		{name: "classifier sysone missing url", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: systemone\n", wantErr: true},
		{name: "classifier invalid protocol", yaml: "channels: {default: {mode: threads}}\nclassifier_protocol: unsupported\n", wantErr: true},
		{name: "classifier env", env: map[string]string{"CLASSIFIER_PROTOCOL": "SYSTEMONE"}, wantProtocol: "systemone"},
		{name: "thread title yaml", yaml: "channels: {default: {mode: threads}}\nthread_title_url: http://192.168.1.70:11434\nthread_title_model: qwen2.5:3b\n", wantTitleURL: "http://192.168.1.70:11434"},
		{name: "thread title env", env: map[string]string{"THREAD_TITLE_URL": "http://edge.lan:11434", "THREAD_TITLE_MODEL": "llama3.2:1b"}, wantTitleURL: "http://edge.lan:11434"},
		{name: "example config", yaml: exampleContent, wantModel: "gemini-2.5-flash", wantChannel: "aerial-dev", wantIdle: 24 * time.Hour},
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

	tests := daemonClassifierCases(exampleContent)
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

func TestLoadSharedSecrets_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()

	validJSON := filepath.Join(tmpDir, "valid.json")
	_ = os.WriteFile(validJSON, []byte(`{
		"FOO": "  bar  ",
		"DISCORD_BOT_TOKEN": "bot-tok-123",
		"1PASSWORD_SERVICE_ACCOUNT_TOKEN": "op-tok-789",
		"GEMINI_HARNESS_API_KEY": "harness-key-abc",
		"UNIFI_PASS": "unifi-pass-def"
	}`), 0o600)

	existingPrimaryJSON := filepath.Join(tmpDir, "existing-primary.json")
	_ = os.WriteFile(existingPrimaryJSON, []byte(`{
		"DISCORD_TOKEN": "primary-tok",
		"DISCORD_BOT_TOKEN": "fallback-tok"
	}`), 0o600)

	invalidJSON := filepath.Join(tmpDir, "invalid.json")
	_ = os.WriteFile(invalidJSON, []byte(`{not valid json`), 0o600)

	secretsDir := filepath.Join(tmpDir, "secrets_dir")
	_ = os.MkdirAll(filepath.Join(secretsDir, "..data"), 0o755)
	_ = os.WriteFile(filepath.Join(secretsDir, "..data", "IGNORE_ME"), []byte("ignored"), 0o600)
	_ = os.WriteFile(filepath.Join(secretsDir, ".hidden"), []byte("ignored"), 0o600)
	_ = os.MkdirAll(filepath.Join(secretsDir, "subdir"), 0o755)
	_ = os.WriteFile(filepath.Join(secretsDir, "subdir", "IGNORE_SUB"), []byte("ignored"), 0o600)
	_ = os.WriteFile(filepath.Join(secretsDir, "DISCORD_BOT_TOKEN"), []byte("dir-bot-token\n"), 0o600)
	_ = os.WriteFile(filepath.Join(secretsDir, "POSTGRES_PASSWORD"), []byte("  dir-pg-pass  "), 0o600)
	_ = os.WriteFile(filepath.Join(secretsDir, "EMPTY_KEY"), []byte("  "), 0o600)

	tests := []struct {
		name         string
		filePath     string
		wantKey      string
		wantVal      string
		wantMapEmpty bool
	}{
		{
			name:         "empty path returns empty map",
			filePath:     "",
			wantMapEmpty: true,
		},
		{
			name:         "missing file returns empty map",
			filePath:     filepath.Join(tmpDir, "nonexistent.json"),
			wantMapEmpty: true,
		},
		{
			name:         "invalid json returns empty map",
			filePath:     invalidJSON,
			wantMapEmpty: true,
		},
		{
			name:     "trimmed values parsed correctly",
			filePath: validJSON,
			wantKey:  "FOO",
			wantVal:  "bar",
		},
		{
			name:     "discord bot token alias fallback",
			filePath: validJSON,
			wantKey:  "DISCORD_TOKEN",
			wantVal:  "bot-tok-123",
		},
		{
			name:     "1password service account alias fallback",
			filePath: validJSON,
			wantKey:  "OP_SERVICE_ACCOUNT_TOKEN",
			wantVal:  "op-tok-789",
		},
		{
			name:     "gemini harness api key is not aliased to agy gemini api key",
			filePath: validJSON,
			wantKey:  "AGY_GEMINI_API_KEY",
			wantVal:  "",
		},
		{
			name:     "unifi pass alias fallback",
			filePath: validJSON,
			wantKey:  "UNIFI_PASSWORD",
			wantVal:  "unifi-pass-def",
		},
		{
			name:     "primary key not overwritten by alias fallback",
			filePath: existingPrimaryJSON,
			wantKey:  "DISCORD_TOKEN",
			wantVal:  "primary-tok",
		},
		{
			name:     "directory mount reads individual files",
			filePath: secretsDir,
			wantKey:  "POSTGRES_PASSWORD",
			wantVal:  "dir-pg-pass",
		},
		{
			name:     "directory mount alias fallback",
			filePath: secretsDir,
			wantKey:  "DISCORD_TOKEN",
			wantVal:  "dir-bot-token",
		},
		{
			name:     "directory mount skips dotfiles",
			filePath: secretsDir,
			wantKey:  ".hidden",
			wantVal:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := LoadSharedSecrets(tc.filePath)
			if tc.wantMapEmpty {
				if len(res) != 0 {
					t.Errorf("LoadSharedSecrets() returned non-empty map: %v", res)
				}
				return
			}
			if got := res[tc.wantKey]; got != tc.wantVal {
				t.Errorf("LoadSharedSecrets()[%q] = %q, want %q", tc.wantKey, got, tc.wantVal)
			}
		})
	}
}

func TestWithSharedSecretsLookup_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()

	sharedFile := filepath.Join(tmpDir, "shared.json")
	_ = os.WriteFile(sharedFile, []byte(`{
		"SECRET_ONLY": "secret-val",
		"OVERRIDDEN": "shared-val",
		"POSTGRES_PASSWORD": "secret-pg-pass"
	}`), 0o600)

	customHostFile := filepath.Join(tmpDir, "custom-host.json")
	_ = os.WriteFile(customHostFile, []byte(`{
		"POSTGRES_HOST": "shared-db.internal",
		"POSTGRES_PASSWORD": "secret-pg-pass"
	}`), 0o600)

	noPassFile := filepath.Join(tmpDir, "no-pass.json")
	_ = os.WriteFile(noPassFile, []byte(`{
		"OTHER": "val"
	}`), 0o600)

	tests := []struct {
		name       string
		primaryMap map[string]string
		isNilPrim  bool
		filePath   string
		queryKey   string
		expected   string
	}{
		{
			name:       "primary wins over shared secret",
			primaryMap: map[string]string{"OVERRIDDEN": "primary-val"},
			filePath:   sharedFile,
			queryKey:   "OVERRIDDEN",
			expected:   "primary-val",
		},
		{
			name:       "fallback to shared secret when unset in primary",
			primaryMap: map[string]string{},
			filePath:   sharedFile,
			queryKey:   "SECRET_ONLY",
			expected:   "secret-val",
		},
		{
			name:      "nil primary handled safely",
			isNilPrim: true,
			filePath:  sharedFile,
			queryKey:  "SECRET_ONLY",
			expected:  "secret-val",
		},
		{
			name:       "postgres host defaulted when password present in shared secrets",
			primaryMap: map[string]string{},
			filePath:   sharedFile,
			queryKey:   "POSTGRES_HOST",
			expected:   "postgres",
		},
		{
			name:       "postgres host preserved from primary even if password in shared secrets",
			primaryMap: map[string]string{"POSTGRES_HOST": "primary-db.internal"},
			filePath:   sharedFile,
			queryKey:   "POSTGRES_HOST",
			expected:   "primary-db.internal",
		},
		{
			name:       "postgres host preserved from shared secrets if specified",
			primaryMap: map[string]string{},
			filePath:   customHostFile,
			queryKey:   "POSTGRES_HOST",
			expected:   "shared-db.internal",
		},
		{
			name:       "postgres host empty when password unset",
			primaryMap: map[string]string{},
			filePath:   noPassFile,
			queryKey:   "POSTGRES_HOST",
			expected:   "",
		},
		{
			name:       "missing key returns empty",
			primaryMap: map[string]string{},
			filePath:   sharedFile,
			queryKey:   "NONEXISTENT",
			expected:   "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var prim func(string) string
			if !tc.isNilPrim {
				prim = mapLookup(tc.primaryMap)
			}
			lookup := WithSharedSecretsLookup(prim, tc.filePath)
			if got := lookup(tc.queryKey); got != tc.expected {
				t.Errorf("lookup(%q) = %q, want %q", tc.queryKey, got, tc.expected)
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
