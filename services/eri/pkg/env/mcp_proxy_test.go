package env

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func TestLoadTargetMCPConfig_MCPProxy(t *testing.T) {
	t.Parallel()
	servers := map[string]json.RawMessage{
		"docket":  json.RawMessage(`{"serverUrl":"http://docket.aerial/mcp?team=1","headers":{"Authorization":"Bearer tok"}}`),
		"kernel":  json.RawMessage(`{"url":"http://kernel.aerial/mcp","type":"http"}`),
		"github":  json.RawMessage(`{"url":"http://github.aerial/mcp","type":"http","instructions":"Use github sparingly","enabled_tools":["issue_read","search_code"],"disabled_tools":["delete_repo"],"response_filters":{"search_code":".items[] | {name: .name, path: .path}","bad_filter":"[invalid jq"}}`),
		"discord": json.RawMessage(`{"url":"http://discord.aerial/mcp","type":"http","instructions":"Use discord carefully","enabledTools":["send_message"],"disabledTools":["kick_member"],"responseFilters":{"send_message":"."}}`),
		"legacy":  json.RawMessage(`{"url":"http://legacy.aerial/sse","type":"sse"}`),
		"browser": json.RawMessage(`{"command":"npx","args":["browser-mcp"]}`),
	}
	verbatimAgy := `{"mcpServers":{
		"docket":{"serverUrl":"http://docket.aerial/mcp?team=1","headers":{"Authorization":"Bearer tok"}},
		"kernel":{"url":"http://kernel.aerial/mcp","type":"http"},
		"github":{"url":"http://github.aerial/mcp","type":"http"},
		"discord":{"url":"http://discord.aerial/mcp","type":"http"},
		"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
		"browser":{"command":"npx","args":["browser-mcp"]}}}`
	verbatimClaude := `{"mcpServers":{
		"docket":{"type":"http","url":"http://docket.aerial/mcp?team=1","headers":{"Authorization":"Bearer tok"}},
		"kernel":{"url":"http://kernel.aerial/mcp","type":"http"},
		"github":{"type":"http","url":"http://github.aerial/mcp"},
		"discord":{"type":"http","url":"http://discord.aerial/mcp"},
		"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
		"browser":{"command":"npx","args":["browser-mcp"]}}}`
	tests := []struct {
		name          string
		proxyPort     string
		wantAgy       string
		wantClaude    string
		wantUpstreams map[string]string
	}{
		{name: "tracing disabled writes configs verbatim and strips policy fields", wantAgy: verbatimAgy, wantClaude: verbatimClaude},
		{
			name:      "tracing enabled routes http servers through the proxy",
			proxyPort: "8080",
			wantAgy: `{"mcpServers":{
				"docket":{"serverUrl":"http://127.0.0.1:8080/internal/mcp-proxy/voice/docket","headers":{"Authorization":"Bearer tok"}},
				"kernel":{"url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/kernel","type":"http"},
				"github":{"url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/github","type":"http"},
				"discord":{"url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/discord","type":"http"},
				"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
				"browser":{"command":"npx","args":["browser-mcp"]}}}`,
			wantClaude: `{"mcpServers":{
				"docket":{"type":"http","url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/docket","headers":{"Authorization":"Bearer tok","X-Eri-Daemon":"${ERI_DAEMON_ID:-}"}},
				"kernel":{"url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/kernel","type":"http","headers":{"X-Eri-Daemon":"${ERI_DAEMON_ID:-}"}},
				"github":{"type":"http","url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/github","headers":{"X-Eri-Daemon":"${ERI_DAEMON_ID:-}"}},
				"discord":{"type":"http","url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/discord","headers":{"X-Eri-Daemon":"${ERI_DAEMON_ID:-}"}},
				"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
				"browser":{"command":"npx","args":["browser-mcp"]}}}`,
			wantUpstreams: map[string]string{
				"docket":  "http://docket.aerial/mcp?team=1",
				"kernel":  "http://kernel.aerial/mcp",
				"github":  "http://github.aerial/mcp",
				"discord": "http://discord.aerial/mcp",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newTestConfig(func(c *config.ConfigData) { c.McpServers.Voice = servers })
			var routes *MCPProxyRoutes
			if tc.proxyPort != "" {
				routes = NewMCPProxyRoutes(tc.proxyPort)
			}
			p := newDisabledProvisioner(t, t.TempDir(), "").WithMCPProxy(routes)
			raw := p.LoadTargetMCPConfig(cfg, TargetVoice)
			assertJSONEqual(t, "agy config", raw, tc.wantAgy)

			claudeDir := t.TempDir()
			if err := p.EnsureTargetClaudeMcpConfig(claudeDir, raw); err != nil {
				t.Fatalf("EnsureTargetClaudeMcpConfig: %v", err)
			}
			claude, err := os.ReadFile(filepath.Join(claudeDir, "mcp_config.json"))
			if err != nil {
				t.Fatalf("read claude config: %v", err)
			}
			assertJSONEqual(t, "claude config", claude, tc.wantClaude)

			for _, name := range []string{"docket", "kernel", "github", "discord", "legacy", "browser"} {
				got, ok := routes.Upstream(string(TargetVoice), name)
				if want, wantOK := tc.wantUpstreams[name]; got != want || ok != wantOK {
					t.Errorf("Upstream(voice, %s) = %q, %v; want %q, %v", name, got, ok, want, wantOK)
				}
			}

			if tc.proxyPort != "" {
				assertPolicyExpectations(t, routes)
			}
		})
	}
}

func assertPolicyExpectations(t *testing.T, routes *MCPProxyRoutes) {
	t.Helper()
	var nilRoutes *MCPProxyRoutes
	if _, ok := nilRoutes.Policy(string(TargetVoice), "github"); ok {
		t.Errorf("nil routes Policy should return false")
	}
	if _, ok := routes.Policy("unknown_target", "github"); ok {
		t.Errorf("unknown target Policy should return false")
	}

	ghPolicy, ok := routes.Policy(string(TargetVoice), "github")
	if !ok {
		t.Fatalf("Policy(voice, github) not found")
	}
	if ghPolicy.Instructions != "Use github sparingly" {
		t.Errorf("github instructions = %q, want %q", ghPolicy.Instructions, "Use github sparingly")
	}
	if !ghPolicy.EnabledTools["issue_read"] || !ghPolicy.EnabledTools["search_code"] {
		t.Errorf("github enabled tools = %+v, want issue_read and search_code", ghPolicy.EnabledTools)
	}
	if !ghPolicy.DisabledTools["delete_repo"] {
		t.Errorf("github disabled tools = %+v, want delete_repo", ghPolicy.DisabledTools)
	}
	if !ghPolicy.DisabledTools["bad_filter"] {
		t.Errorf("github disabled tools = %+v, want bad_filter disabled due to invalid filter", ghPolicy.DisabledTools)
	}
	if ghPolicy.ResponseFilters["search_code"] == nil {
		t.Errorf("github search_code filter missing")
	}
	if ghPolicy.ResponseFilters["bad_filter"] != nil {
		t.Errorf("github bad_filter unexpectedly compiled")
	}

	dcPolicy, ok := routes.Policy(string(TargetVoice), "discord")
	if !ok {
		t.Fatalf("Policy(voice, discord) not found")
	}
	if dcPolicy.Instructions != "Use discord carefully" {
		t.Errorf("discord instructions = %q, want %q", dcPolicy.Instructions, "Use discord carefully")
	}
	if !dcPolicy.EnabledTools["send_message"] {
		t.Errorf("discord enabled tools = %+v, want send_message", dcPolicy.EnabledTools)
	}
	if !dcPolicy.DisabledTools["kick_member"] {
		t.Errorf("discord disabled tools = %+v, want kick_member", dcPolicy.DisabledTools)
	}
	if dcPolicy.ResponseFilters["send_message"] == nil {
		t.Errorf("discord send_message filter missing")
	}

	rawMap := map[string]any{
		"slice_server": map[string]any{
			"url":              "http://slice.aerial/mcp",
			"enabled_tools":    []string{"slice_tool", " "},
			"disabled_tools":   []string{"bad_slice"},
			"response_filters": map[string]string{"slice_tool": "."},
		},
		"invalid_entry": "not_map",
		"empty_url":     map[string]any{"name": "no_url"},
	}
	routes.rewrite(TargetVoice, rawMap)
	StripProxyPolicyFields(rawMap)
	slicePolicy, ok := routes.Policy(string(TargetVoice), "slice_server")
	if !ok || !slicePolicy.EnabledTools["slice_tool"] || !slicePolicy.DisabledTools["bad_slice"] || slicePolicy.ResponseFilters["slice_tool"] == nil {
		t.Errorf("slice_server policy not extracted properly: %+v", slicePolicy)
	}
}

func TestEnsureTargetCodexConfigTOML_MCPProxy(t *testing.T) {
	t.Parallel()
	servers := map[string]json.RawMessage{
		"docket":  json.RawMessage(`{"serverUrl":"http://docket.aerial/mcp?team=1","headers":{"Authorization":"Bearer tok"}}`),
		"kernel":  json.RawMessage(`{"url":"http://kernel.aerial/mcp","type":"http"}`),
		"browser": json.RawMessage(`{"command":"npx","args":["browser-mcp"]}`),
	}
	tests := []struct {
		name          string
		proxyPort     string
		wantCodexSubs []string
	}{
		{
			name: "tracing disabled writes verbatim url and headers",
			wantCodexSubs: []string{
				"[features]\nhooks = true",
				`url = "http://docket.aerial/mcp?team=1"`,
				`http_headers = { "Authorization" = "Bearer tok" }`,
				`url = "http://kernel.aerial/mcp"`,
				`command = "npx"`,
				`args = ["browser-mcp"]`,
			},
		},
		{
			name:      "tracing enabled routes url through proxy with env_http_headers",
			proxyPort: "8080",
			wantCodexSubs: []string{
				"[otel]",
				`log_user_prompt = false`,
				`log_agent_responses = false`,
				`log_sse_deltas = false`,
				`log_guardian_assessments = false`,
				`tool_result = { max_bytes = 0 }`,
				`endpoint = "http://127.0.0.1:8080/internal/otel/v1/logs/voice"`,
				`url = "http://127.0.0.1:8080/internal/mcp-proxy/voice/docket"`,
				`env_http_headers = { "X-Eri-Daemon" = "ERI_DAEMON_ID" }`,
				`url = "http://127.0.0.1:8080/internal/mcp-proxy/voice/kernel"`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newTestConfig(func(c *config.ConfigData) { c.McpServers.Voice = servers })
			var routes *MCPProxyRoutes
			if tc.proxyPort != "" {
				routes = NewMCPProxyRoutes(tc.proxyPort)
			}
			p := newDisabledProvisioner(t, t.TempDir(), "").WithMCPProxy(routes)
			raw := p.LoadTargetMCPConfig(cfg, TargetVoice)

			codexDir := t.TempDir()
			if err := p.EnsureTargetCodexConfigTOML(codexDir, raw); err != nil {
				t.Fatalf("EnsureTargetCodexConfigTOML: %v", err)
			}
			codexBytes, err := os.ReadFile(filepath.Join(codexDir, "config.toml"))
			if err != nil {
				t.Fatalf("read codex config: %v", err)
			}
			codexStr := string(codexBytes)
			for _, sub := range tc.wantCodexSubs {
				if !strings.Contains(codexStr, sub) {
					t.Errorf("codex config missing substring %q, got:\n%s", sub, codexStr)
				}
			}

			museDir := t.TempDir()
			if mErr := p.EnsureTargetMuseSettingsJSON(museDir, raw); mErr != nil {
				t.Fatalf("EnsureTargetMuseSettingsJSON: %v", mErr)
			}
			museBytes, mErr := os.ReadFile(filepath.Join(museDir, "settings.json"))
			if mErr != nil {
				t.Fatalf("read muse config: %v", mErr)
			}
			museStr := string(museBytes)
			if tc.proxyPort != "" && !strings.Contains(museStr, `"${ERI_DAEMON_ID:-}"`) {
				t.Errorf("muse config missing X-Eri-Daemon header: %s", museStr)
			}
		})
	}
}

func TestFormatCodexConfigTOML(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       json.RawMessage
		proxyBase string
		contains  []string
		excludes  []string
	}{
		{
			name:     "empty or null raw config",
			raw:      json.RawMessage(`null`),
			contains: []string{"[features]\nhooks = true\n"},
		},
		{
			name:     "nil raw config",
			raw:      nil,
			contains: []string{"[features]\nhooks = true\n"},
		},
		{
			name:     "quoted empty string",
			raw:      json.RawMessage(`""`),
			contains: []string{"[features]\nhooks = true\n"},
		},
		{
			name:     "string encoded json",
			raw:      json.RawMessage(`"{\"mcpServers\":{\"srv\":{\"url\":\"http://test\"}}}"`),
			contains: []string{"[mcp_servers.srv]", `url = "http://test"`},
		},
		{
			name:     "invalid json returns default features",
			raw:      json.RawMessage(`{not-json`),
			contains: []string{"[features]\nhooks = true\n"},
		},
		{
			name: "server with http_headers and headers",
			raw: json.RawMessage(`{"mcpServers":{
				"h1":{"url":"http://h1","http_headers":{"H1":"V1"}},
				"h2":{"url":"http://h2","headers":{"H2":"V2"}}
			}}`),
			contains: []string{
				`[mcp_servers.h1]`,
				`http_headers = { "H1" = "V1" }`,
				`[mcp_servers.h2]`,
				`http_headers = { "H2" = "V2" }`,
			},
		},
		{
			name:      "proxyBase matching url sets env_http_headers and otel block",
			raw:       json.RawMessage(`{"mcpServers":{"proxied":{"url":"http://127.0.0.1:8080/mcp"}}}`),
			proxyBase: "http://127.0.0.1:8080",
			contains: []string{
				`[otel]`,
				`log_user_prompt = false`,
				`log_agent_responses = false`,
				`tool_result = { max_bytes = 0 }`,
				`endpoint = "http://127.0.0.1:8080/internal/otel/v1/logs"`,
				`[mcp_servers.proxied]`,
				`env_http_headers = { "X-Eri-Daemon" = "ERI_DAEMON_ID" }`,
			},
		},
		{
			name:      "proxyBase with target suffix in path",
			raw:       json.RawMessage(`null`),
			proxyBase: "http://127.0.0.1:8080/internal/mcp-proxy/voice",
			contains: []string{
				`[otel]`,
				`endpoint = "http://127.0.0.1:8080/internal/otel/v1/logs/voice"`,
			},
		},
		{
			name: "command with args and env",
			raw:  json.RawMessage(`{"mcpServers":{"local":{"command":"cli","args":["--flag","val"],"env":{"KEY":"VAL"}}}}`),
			contains: []string{
				`[mcp_servers.local]`,
				`command = "cli"`,
				`args = ["--flag", "val"]`,
				`[mcp_servers.local.env]`,
				`KEY = "VAL"`,
			},
		},
		{
			name: "server name and env key with dots and spaces quoted",
			raw:  json.RawMessage(`{"mcpServers":{"my.server":{"command":"cli","env":{"MY.ENV KEY":"VAL"}}}}`),
			contains: []string{
				`[mcp_servers."my.server"]`,
				`[mcp_servers."my.server".env]`,
				`"MY.ENV KEY" = "VAL"`,
			},
		},
		{
			name: "empty server name key quoted",
			raw:  json.RawMessage(`{"mcpServers":{"":{"command":"cli"}}}`),
			contains: []string{
				`[mcp_servers.""]`,
			},
		},
		{
			name: "command without command field and non-map entry",
			raw: json.RawMessage(`{"mcpServers":{
				"nocmd":{"args":["arg1"]},
				"invalid_entry":"not-a-map"
			}}`),
			contains: []string{
				`[mcp_servers.nocmd]`,
				`command = ""`,
				`args = ["arg1"]`,
			},
			excludes: []string{
				`[mcp_servers.invalid_entry]`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := FormatCodexConfigTOML(tc.raw, tc.proxyBase)
			for _, want := range tc.contains {
				if !strings.Contains(out, want) {
					t.Errorf("FormatCodexConfigTOML() missing %q, got:\n%s", want, out)
				}
			}
			for _, dontWant := range tc.excludes {
				if strings.Contains(out, dontWant) {
					t.Errorf("FormatCodexConfigTOML() unexpectedly contained %q, got:\n%s", dontWant, out)
				}
			}
		})
	}
}

func TestFormatMuseSettingsJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		raw       json.RawMessage
		proxyBase string
		contains  []string
		excludes  []string
	}{
		{
			name:     "empty or null raw config",
			raw:      json.RawMessage(`null`),
			contains: []string{`"schema_version": 1`, `"mcpServers": {}`, `"/usr/local/bin/hook-guard pre-invocation"`},
		},
		{
			name:     "nil raw config",
			raw:      nil,
			contains: []string{`"schema_version": 1`, `"mcpServers": {}`, `"/usr/local/bin/hook-guard all"`},
		},
		{
			name:     "string encoded json",
			raw:      json.RawMessage(`"{\"mcpServers\":{\"srv\":{\"url\":\"http://test\"}}}"`),
			contains: []string{`"type": "streamable-http"`, `"url": "http://test"`},
		},
		{
			name: "server with http_headers and headers",
			raw: json.RawMessage(`{"mcpServers":{
				"h1":{"url":"http://h1","http_headers":{"H1":"V1"}},
				"h2":{"url":"http://h2","headers":{"H2":"V2"}}
			}}`),
			contains: []string{
				`"h1"`,
				`"type": "streamable-http"`,
				`"url": "http://h1"`,
				`"H1": "V1"`,
				`"h2"`,
				`"url": "http://h2"`,
				`"H2": "V2"`,
			},
		},
		{
			name:      "proxyBase matching url sets X-Eri-Daemon",
			raw:       json.RawMessage(`{"mcpServers":{"proxied":{"url":"http://127.0.0.1:8080/mcp"}}}`),
			proxyBase: "http://127.0.0.1:8080",
			contains: []string{
				`"proxied"`,
				`"type": "streamable-http"`,
				`"X-Eri-Daemon": "${ERI_DAEMON_ID:-}"`,
			},
		},
		{
			name: "command with args and env",
			raw:  json.RawMessage(`{"mcpServers":{"local":{"command":"cli","args":["--flag","val"],"env":{"KEY":"VAL"}}}}`),
			contains: []string{
				`"local"`,
				`"type": "stdio"`,
				`"command": "cli"`,
				`"--flag"`,
				`"val"`,
				`"KEY": "VAL"`,
			},
		},
		{
			name: "hooks canonical structure verified",
			raw:  json.RawMessage(`null`),
			contains: []string{
				`"UserPromptSubmit"`,
				`"/usr/local/bin/hook-guard pre-invocation"`,
				`"PreToolUse"`,
				`".*"`,
				`"/usr/local/bin/hook-guard all"`,
				`"PostToolUse"`,
				`"edit_file|write_file|Edit|MultiEdit|Write"`,
				`"/usr/local/bin/hook-guard post-batch"`,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := FormatMuseSettingsJSON(tc.raw, tc.proxyBase)
			for _, want := range tc.contains {
				if !strings.Contains(out, want) {
					t.Errorf("FormatMuseSettingsJSON() missing %q, got:\n%s", want, out)
				}
			}
			for _, dontWant := range tc.excludes {
				if strings.Contains(out, dontWant) {
					t.Errorf("FormatMuseSettingsJSON() unexpectedly contained %q, got:\n%s", dontWant, out)
				}
			}
		})
	}
}

func assertJSONEqual(t *testing.T, what string, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("%s: invalid JSON %s: %v", what, got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: invalid expectation: %v", what, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s = %s, want %s", what, got, want)
	}
}
