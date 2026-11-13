package env

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
