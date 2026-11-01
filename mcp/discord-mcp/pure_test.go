package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mkCfg(port, upstreamPort, nodeBin, appPath string) *Config {
	return &Config{Port: port, UpstreamPort: upstreamPort, NodeBin: nodeBin, AppPath: appPath}
}

func TestIsBlockedToolCall_TableDriven(t *testing.T) {
	tests := []struct {
		name          string
		rawReq        string
		blockedMap    map[string]bool
		expectBlocked bool
		expectTool    string
		expectErr     bool
	}{
		{"single call - blocked discord_send", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"discord_send"}}`, BlockedToolNames, true, "discord_send", false},
		{"single call - blocked send_message", `{"jsonrpc":"2.0","id":"two","method":"tools/call","params":{"name":"send_message"}}`, BlockedToolNames, true, "send_message", false},
		{"single call - case variant Discord_Send", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"Discord_Send"}}`, BlockedToolNames, true, "Discord_Send", false},
		{"single call - case variant SEND_MESSAGE with whitespace", `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"  SEND_MESSAGE  "}}`, BlockedToolNames, true, "  SEND_MESSAGE  ", false},
		{"single call - allowed tool discord_read_messages", `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"discord_read_messages"}}`, BlockedToolNames, false, "discord_read_messages", false},
		{"single call - non-tool method ping", `{"jsonrpc":"2.0","id":6,"method":"ping"}`, BlockedToolNames, false, "", false},
		{"single call - empty params", `{"jsonrpc":"2.0","id":7,"method":"tools/call"}`, BlockedToolNames, false, "", false},
		{"single call - malformed params scalar", `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":"not-an-object"}`, BlockedToolNames, false, "", true},
		{"single call - empty raw request", "", BlockedToolNames, false, "", true},
		{"single call - malformed json", `{invalid json`, BlockedToolNames, false, "", true},
		{"single call - nil blocked map uses default", `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"discord_create_thread"}}`, nil, true, "discord_create_thread", false},
		{"batch request - contains blocked tool", `[{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"discord_read_messages"}},{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"discord_send"}}]`, BlockedToolNames, true, "discord_send", false},
		{"batch request - contains case variant blocked tool", `[{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"DISCORD_CREATE_THREAD"}}]`, BlockedToolNames, true, "DISCORD_CREATE_THREAD", false},
		{"batch request - allowed tools only", `[{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"discord_read_messages"}},{"jsonrpc":"2.0","id":14,"method":"ping"}]`, BlockedToolNames, false, "", false},
		{"batch request - empty array", `[]`, BlockedToolNames, false, "", false},
		{"batch request - malformed json array", `[{"jsonrpc":"2.0", invalid`, BlockedToolNames, false, "", true},
		{"batch request - malformed item params", `[{"jsonrpc":"2.0","id":15,"method":"tools/call","params":12345}]`, BlockedToolNames, false, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blocked, _, tool, err := IsBlockedToolCall([]byte(tt.rawReq), tt.blockedMap)
			if tt.expectErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if blocked != tt.expectBlocked {
				t.Errorf("expected blocked=%v, got %v", tt.expectBlocked, blocked)
			}
			if tt.expectBlocked && strings.TrimSpace(tool) != strings.TrimSpace(tt.expectTool) {
				t.Errorf("expected tool=%q, got %q", tt.expectTool, tool)
			}
		})
	}
}

func TestFilterToolsResponse_TableDriven(t *testing.T) {
	tests := []struct {
		name          string
		rawResp       string
		blockedMap    map[string]bool
		wantUnchanged bool
		wantTools     []string
		wantErrorMsg  string
	}{
		{"standard response with blocked and allowed tools", `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"discord_send"},{"name":"discord_read_messages"},{"name":"send_message"}]}}`, BlockedToolNames, false, []string{"discord_read_messages"}, ""},
		{"preserves nextCursor and _meta in result", `{"jsonrpc":"2.0","id":2,"result":{"tools":[{"name":"discord_send"},{"name":"allowed_tool"}],"nextCursor":"cursor-token-xyz","_meta":{"version":"2024-11-05"}}}`, BlockedToolNames, false, []string{"allowed_tool"}, ""},
		{"case variant blocked tool filtered", `{"jsonrpc":"2.0","id":3,"result":{"tools":[{"name":"DISCORD_SEND"},{"name":"valid_tool"}]}}`, BlockedToolNames, false, []string{"valid_tool"}, ""},
		{"empty result preserves raw response", `{"jsonrpc":"2.0","id":4}`, BlockedToolNames, true, nil, ""},
		{"null result preserves raw response without shape mutation", `{"jsonrpc":"2.0","id":5,"result":null}`, BlockedToolNames, true, nil, ""},
		{"non-object result returns error", `{"jsonrpc":"2.0","id":6,"result":"hello"}`, BlockedToolNames, false, nil, "cannot unmarshal"},
		{"missing tools array in result preserves raw response", `{"jsonrpc":"2.0","id":7,"result":{"other":"data"}}`, BlockedToolNames, true, nil, ""},
		{"malformed top-level JSON returns error", `{invalid json`, BlockedToolNames, false, nil, "invalid"},
		{"malformed tools list JSON returns error", `{"jsonrpc":"2.0","id":9,"result":{"tools":"not-an-array"}}`, BlockedToolNames, false, nil, "cannot unmarshal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := FilterToolsResponse([]byte(tt.rawResp), tt.blockedMap)
			if tt.wantErrorMsg != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrorMsg) {
					t.Fatalf("expected error containing %q, got: %v", tt.wantErrorMsg, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantUnchanged {
				if string(out) != tt.rawResp {
					t.Errorf("expected unchanged response, got %s", string(out))
				}
				return
			}
			if tt.wantTools != nil {
				var parsed struct {
					Result struct {
						Tools []struct {
							Name string `json:"name"`
						} `json:"tools"`
						NextCursor string         `json:"nextCursor"`
						Meta       map[string]any `json:"_meta"`
					} `json:"result"`
				}
				if err := json.Unmarshal(out, &parsed); err != nil {
					t.Fatalf("failed to unmarshal output: %v", err)
				}
				var gotNames []string
				for _, tool := range parsed.Result.Tools {
					gotNames = append(gotNames, tool.Name)
				}
				if len(gotNames) != len(tt.wantTools) {
					t.Fatalf("expected %d tools, got %d (%v)", len(tt.wantTools), len(gotNames), gotNames)
				}
				for i, want := range tt.wantTools {
					if gotNames[i] != want {
						t.Errorf("tool[%d]: expected %q, got %q", i, want, gotNames[i])
					}
				}
				if strings.Contains(tt.rawResp, "nextCursor") && parsed.Result.NextCursor != "cursor-token-xyz" {
					t.Errorf("expected nextCursor to be preserved, got %q", parsed.Result.NextCursor)
				}
				if strings.Contains(tt.rawResp, "_meta") && parsed.Result.Meta["version"] != "2024-11-05" {
					t.Errorf("expected _meta to be preserved, got %v", parsed.Result.Meta)
				}
			}
		})
	}
}

func TestIsToolsListRequest_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		rawReq   string
		expected bool
	}{
		{"valid tools/list request", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, true},
		{"valid tools/list request with whitespace", `  {"jsonrpc":"2.0","id":1,"method":"  tools/list  "}  `, true},
		{"tools/call request returns false", `{"jsonrpc":"2.0","id":2,"method":"tools/call"}`, false},
		{"other method returns false", `{"jsonrpc":"2.0","id":3,"method":"ping"}`, false},
		{"empty method returns false", `{"jsonrpc":"2.0","id":4}`, false},
		{"empty body returns false", "", false},
		{"array returns false", `[{"jsonrpc":"2.0","method":"tools/list"}]`, false},
		{"invalid json returns false", `{invalid json`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsToolsListRequest([]byte(tt.rawReq)); got != tt.expected {
				t.Errorf("expected IsToolsListRequest=%v, got %v", tt.expected, got)
			}
		})
	}
}

func TestBuildJSONRPCErrorResponse_TableDriven(t *testing.T) {
	tests := []struct {
		name     string
		id       any
		code     int
		message  string
		wantCode int
		wantMsg  string
		wantID   any
	}{
		{"integer id", 42, -32601, "Tool disabled", -32601, "Tool disabled", float64(42)},
		{"string id", "req-uuid-123", -32700, "Parse error", -32700, "Parse error", "req-uuid-123"},
		{"nil id preserves id: null", nil, -32600, "Invalid request", -32600, "Invalid request", nil},
		{"unmarshallable channel id triggers fallback", make(chan int), -32603, "internal error", -32603, "internal error marshalling response", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := BuildJSONRPCErrorResponse(tt.id, tt.code, tt.message)
			var resp JSONRPCResponse
			if err := json.Unmarshal(out, &resp); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if resp.Error.Code != tt.wantCode {
				t.Errorf("code: got %d, want %d", resp.Error.Code, tt.wantCode)
			}
			if !strings.Contains(resp.Error.Message, tt.wantMsg) {
				t.Errorf("message: got %q, want containing %q", resp.Error.Message, tt.wantMsg)
			}
			if tt.wantID == nil {
				if !strings.Contains(string(out), `"id":null`) {
					t.Errorf("expected '\"id\":null' in error response, got %s", string(out))
				}
			} else if resp.ID != tt.wantID {
				t.Errorf("id: got %v, want %v", resp.ID, tt.wantID)
			}
		})
	}
}

func TestBuildHealthResponse_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		wrapper      string
		blockedTools []string
		wantStatus   string
		wantService  string
		wantTools    []string
	}{
		{"default wrapper and canonical blocked tools", "", DefaultBlockedToolList, "ok", "aerial-discord-mcp", []string{"discord_create_thread", "discord_send", "send_message"}},
		{"custom wrapper and unsorted input gets sorted", "custom-wrapper", []string{"z_tool", "a_tool", "m_tool"}, "ok", "custom-wrapper", []string{"a_tool", "m_tool", "z_tool"}},
		{"nil blocked tools serializes as empty slice", "test", nil, "ok", "test", []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := BuildHealthResponse(tt.wrapper, tt.blockedTools)
			var resp HealthResponse
			if err := json.Unmarshal(out, &resp); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if resp.Status != tt.wantStatus || resp.Wrapper != tt.wantService {
				t.Errorf("unexpected status/wrapper: got status=%q wrapper=%q, want status=%q wrapper=%q",
					resp.Status, resp.Wrapper, tt.wantStatus, tt.wantService)
			}
			if len(resp.BlockedTools) != len(tt.wantTools) {
				t.Fatalf("blocked tools length mismatch: got %d, want %d", len(resp.BlockedTools), len(tt.wantTools))
			}
			for i, tool := range tt.wantTools {
				if resp.BlockedTools[i] != tool {
					t.Errorf("blocked tool[%d]: got %q, want %q", i, resp.BlockedTools[i], tool)
				}
			}
		})
	}
}

func TestBuildHealthResponse_MarshalError(t *testing.T) {
	oldMarshal := marshalFn
	t.Cleanup(func() { marshalFn = oldMarshal })
	marshalFn = func(v any) ([]byte, error) {
		return nil, errors.New("simulated marshal error")
	}

	health := BuildHealthResponse("test-service", []string{"tool1"})
	if !strings.Contains(string(health), `"status":"error"`) {
		t.Errorf("expected fallback health response, got %s", string(health))
	}
}

func TestValidateConfig_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *Config
		expectErr   bool
		errContains string
	}{
		{"nil config", nil, true, "config cannot be nil"},
		{"empty port", mkCfg("", "4005", "node", "build/app.js"), true, "port cannot be empty"},
		{"whitespace port", mkCfg("   ", "4005", "node", "build/app.js"), true, "port cannot be empty"},
		{"empty upstream port", mkCfg("4001", "", "node", "build/app.js"), true, "upstreamPort cannot be empty"},
		{"whitespace upstream port", mkCfg("4001", "  ", "node", "build/app.js"), true, "upstreamPort cannot be empty"},
		{"empty node bin", mkCfg("4001", "4005", "", "build/app.js"), true, "nodeBin cannot be empty"},
		{"whitespace node bin", mkCfg("4001", "4005", "  ", "build/app.js"), true, "nodeBin cannot be empty"},
		{"empty app path", mkCfg("4001", "4005", "node", ""), true, "appPath cannot be empty"},
		{"whitespace app path", mkCfg("4001", "4005", "node", "  \t "), true, "appPath cannot be empty"},
		{"valid config with trimming", mkCfg(" 4001 ", " 4005 ", " node ", " app.js "), false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.cfg)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %v", tt.errContains, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tt.cfg.Port != "4001" || tt.cfg.UpstreamPort != "4005" || tt.cfg.NodeBin != "node" || tt.cfg.AppPath != "app.js" {
					t.Errorf("fields not trimmed cleanly: %+v", tt.cfg)
				}
			}
		})
	}
}

func TestBuildNodeEnv_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		environ      []string
		upstreamPort string
		wantContains []string
		wantAbsent   []string
	}{
		{"strips uppercase PORT and appends new", []string{"PORT=3000", "PATH=/usr/bin", "FOO=bar"}, "4005", []string{"PORT=4005", "PATH=/usr/bin", "FOO=bar"}, []string{"PORT=3000"}},
		{"case-insensitively strips port= and Port=", []string{"port=3000", "Port=9999", "OTHER=1"}, "4005", []string{"PORT=4005", "OTHER=1"}, []string{"port=3000", "Port=9999"}},
		{"empty input environment", nil, "4005", []string{"PORT=4005"}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := BuildNodeEnv(tt.environ, tt.upstreamPort)
			envMap := make(map[string]bool)
			for _, e := range env {
				envMap[e] = true
			}
			for _, want := range tt.wantContains {
				if !envMap[want] {
					t.Errorf("expected env to contain %q, got: %v", want, env)
				}
			}
			for _, absent := range tt.wantAbsent {
				if envMap[absent] {
					t.Errorf("expected env not to contain %q, got: %v", absent, env)
				}
			}
		})
	}
}

func TestFormatUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		upstreamPort string
		expected     string
	}{
		{"clean port", "4005", "http://127.0.0.1:4005"},
		{"port with whitespace", "  5001  ", "http://127.0.0.1:5001"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatUpstreamURL(tt.upstreamPort); got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestIsToolNameBlocked_MixedCase(t *testing.T) {
	blocked := map[string]bool{"  DISCORD_SEND  ": true}
	if !isToolNameBlocked("discord_send", blocked) {
		t.Error("expected true for mixed-case key in blocked map")
	}
}

func TestResolveConfigPath_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	localFile := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(localFile, []byte("port: '4001'\nupstream_port: '4025'\n"), 0o644); err != nil {
		t.Fatalf("failed to write temp local config: %v", err)
	}
	emptyFile := filepath.Join(tmpDir, "empty.yaml")
	if err := os.WriteFile(emptyFile, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write empty local config: %v", err)
	}

	fallback := "/config/config.yaml"
	tests := []struct {
		name      string
		fallback  string
		envVal    string
		localPath string
		want      string
	}{
		{"CONFIG_PATH env precedence", fallback, "/custom/override/config.yaml", localFile, "/custom/override/config.yaml"},
		{"local config non-zero size", fallback, "", localFile, localFile},
		{"local config zero size falls back", fallback, "", emptyFile, fallback},
		{"local config nonexistent falls back", fallback, "", filepath.Join(tmpDir, "nonexistent.yaml"), fallback},
		{"empty fallback invokes multi-path fallback", "", "", emptyFile, "/share/aerial-config/config.yaml"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", tc.envVal)
			oldLocal := localConfigPath
			localConfigPath = tc.localPath
			t.Cleanup(func() { localConfigPath = oldLocal })

			if got := ResolveConfigPath(tc.fallback); got != tc.want {
				t.Errorf("ResolveConfigPath got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestExtractSearchOffset_TableDriven(t *testing.T) {
	tests := []struct {
		name   string
		rawReq string
		want   int
	}{
		{"valid positive offset", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"discord_search_messages","arguments":{"guildId":"1","offset":25}}}`, 25},
		{"negative offset clamps to 0", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"discord_search_messages","arguments":{"guildId":"1","offset":-5}}}`, 0},
		{"missing arguments returns 0", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"discord_search_messages"}}`, 0},
		{"malformed params returns 0", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":"bad"}`, 0},
		{"malformed json returns 0", `{bad`, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ExtractSearchOffset([]byte(tc.rawReq)); got != tc.want {
				t.Errorf("ExtractSearchOffset(%s) = %d, want %d", tc.rawReq, got, tc.want)
			}
		})
	}
}

func TestShapeSearchMessagesResponse_TableDriven(t *testing.T) {
	nestedHitResp := `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"total_results\":46,\"messages\":[[{\"id\":\"m0\",\"content\":\"ctx\",\"hit\":false,\"author\":{\"username\":\"bot\"}},{\"id\":\"m1\",\"timestamp\":\"2026-10-26T00:00:00Z\",\"channel_id\":\"c1\",\"content\":\"hello\",\"hit\":true,\"author\":{\"id\":\"u1\",\"username\":\"user123\",\"global_name\":\"DisplayUser\"}}], [{\"id\":\"m2\",\"timestamp\":\"2026-10-26T00:01:00Z\",\"channel_id\":\"c1\",\"content\":\"world\",\"author\":{\"id\":\"u2\",\"username\":\"same\",\"global_name\":\"same\"}}], {\"id\":\"m3\",\"timestamp\":\"2026-10-26T00:02:00Z\",\"channel_id\":\"c1\",\"content\":\"flat\",\"author\":{\"id\":\"u3\",\"global_name\":\"OnlyGlobal\"}}],\"threads\":[{\"id\":\"t1\"}]}"}]}}`
	nestedNoHitsResp := `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"total_results\":1,\"messages\":[[{\"id\":\"m0\",\"content\":\"no-hit-group\",\"hit\":false}]]}"}]}}`
	mixedEntriesResp := `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"total_results\":2,\"messages\":[\" \", \"[bad\", \"{bad\", {\"id\":\"m4\",\"content\":\"valid\"}]}"}]}}`
	lastPageResp := `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"total_results\":0,\"messages\":[[{\"id\":\"m1\",\"content\":\"only\"}]]}"}]}}`

	tests := []struct {
		name       string
		rawResp    string
		offset     int
		wantSub    string
		wantAbsent string
		wantErr    bool
	}{
		{"nested hit group + flat + global_name formatting", nestedHitResp, 0, `\"has_more\":true`, "threads", false},
		{"nested group with no hits falls back to full group", nestedNoHitsResp, 0, `no-hit-group`, "", false},
		{"malformed and whitespace message entries safely ignored", mixedEntriesResp, 0, `valid`, "", false},
		{"author formatted with global_name and username", nestedHitResp, 0, `DisplayUser (user123)`, "ctx", false},
		{"last page has_more false and negative offset clamped", lastPageResp, -1, `\"has_more\":false`, "", false},
		{"non-search text payload left unchanged", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"other\":1}"}]}}`, 0, `\"other\"`, "", false},
		{"missing content array left unchanged", `{"jsonrpc":"2.0","id":1,"result":{"other":1}}`, 0, `"other":1`, "", false},
		{"empty result left unchanged", `{"jsonrpc":"2.0","id":1}`, 0, `"id":1`, "", false},
		{"invalid top-level json returns error", `{bad`, 0, "", "", true},
		{"invalid result json returns error", `{"jsonrpc":"2.0","id":1,"result":"bad"}`, 0, "", "", true},
		{"invalid content json returns error", `{"jsonrpc":"2.0","id":1,"result":{"content":"bad"}}`, 0, "", "", true},
		{"invalid inner text json returns error", `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{bad"}]}}`, 0, "", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := ShapeSearchMessagesResponse([]byte(tc.rawResp), tc.offset)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			outStr := string(out)
			if tc.wantSub != "" && !strings.Contains(outStr, tc.wantSub) {
				t.Errorf("expected %q in %s", tc.wantSub, outStr)
			}
			if tc.wantAbsent != "" && strings.Contains(outStr, tc.wantAbsent) {
				t.Errorf("expected %q absent from %s", tc.wantAbsent, outStr)
			}
		})
	}
}
