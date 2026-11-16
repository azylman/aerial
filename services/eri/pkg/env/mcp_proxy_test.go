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
		"legacy":  json.RawMessage(`{"url":"http://legacy.aerial/sse","type":"sse"}`),
		"browser": json.RawMessage(`{"command":"npx","args":["browser-mcp"]}`),
	}
	verbatimAgy := `{"mcpServers":{
		"docket":{"serverUrl":"http://docket.aerial/mcp?team=1","headers":{"Authorization":"Bearer tok"}},
		"kernel":{"url":"http://kernel.aerial/mcp","type":"http"},
		"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
		"browser":{"command":"npx","args":["browser-mcp"]}}}`
	verbatimClaude := `{"mcpServers":{
		"docket":{"type":"http","url":"http://docket.aerial/mcp?team=1","headers":{"Authorization":"Bearer tok"}},
		"kernel":{"url":"http://kernel.aerial/mcp","type":"http"},
		"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
		"browser":{"command":"npx","args":["browser-mcp"]}}}`
	tests := []struct {
		name          string
		proxyPort     string
		wantAgy       string
		wantClaude    string
		wantUpstreams map[string]string
	}{
		{name: "tracing disabled writes configs verbatim", wantAgy: verbatimAgy, wantClaude: verbatimClaude},
		{
			name:      "tracing enabled routes http servers through the proxy",
			proxyPort: "8080",
			wantAgy: `{"mcpServers":{
				"docket":{"serverUrl":"http://127.0.0.1:8080/internal/mcp-proxy/voice/docket","headers":{"Authorization":"Bearer tok"}},
				"kernel":{"url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/kernel","type":"http"},
				"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
				"browser":{"command":"npx","args":["browser-mcp"]}}}`,
			wantClaude: `{"mcpServers":{
				"docket":{"type":"http","url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/docket","headers":{"Authorization":"Bearer tok","X-Eri-Daemon":"${ERI_DAEMON_ID:-}"}},
				"kernel":{"url":"http://127.0.0.1:8080/internal/mcp-proxy/voice/kernel","type":"http","headers":{"X-Eri-Daemon":"${ERI_DAEMON_ID:-}"}},
				"legacy":{"url":"http://legacy.aerial/sse","type":"sse"},
				"browser":{"command":"npx","args":["browser-mcp"]}}}`,
			wantUpstreams: map[string]string{"docket": "http://docket.aerial/mcp?team=1", "kernel": "http://kernel.aerial/mcp"},
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
			p := New(t.TempDir(), "").WithMCPProxy(routes)
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

			for _, name := range []string{"docket", "kernel", "legacy", "browser"} {
				got, ok := routes.Upstream(string(TargetVoice), name)
				if want, wantOK := tc.wantUpstreams[name]; got != want || ok != wantOK {
					t.Errorf("Upstream(voice, %s) = %q, %v; want %q, %v", name, got, ok, want, wantOK)
				}
			}
		})
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
			p := New(t.TempDir(), "").WithMCPProxy(routes)
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
			name:      "proxyBase matching url sets env_http_headers",
			raw:       json.RawMessage(`{"mcpServers":{"proxied":{"url":"http://127.0.0.1:8080/mcp"}}}`),
			proxyBase: "http://127.0.0.1:8080",
			contains: []string{
				`[mcp_servers.proxied]`,
				`env_http_headers = { "X-Eri-Daemon" = "ERI_DAEMON_ID" }`,
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
