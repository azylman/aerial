package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestIsBlockedToolCall_TableDriven(t *testing.T) {
	tests := []struct {
		name          string
		rawReq        string
		blockedMap    map[string]bool
		expectBlocked bool
		expectTool    string
		expectErr     bool
	}{
		{
			name:          "single call - blocked discord_send",
			rawReq:        `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"discord_send"}}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: true,
			expectTool:    "discord_send",
			expectErr:     false,
		},
		{
			name:          "single call - blocked send_message",
			rawReq:        `{"jsonrpc":"2.0","id":"two","method":"tools/call","params":{"name":"send_message"}}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: true,
			expectTool:    "send_message",
			expectErr:     false,
		},
		{
			name:          "single call - case variant Discord_Send",
			rawReq:        `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"Discord_Send"}}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: true,
			expectTool:    "Discord_Send",
			expectErr:     false,
		},
		{
			name:          "single call - case variant SEND_MESSAGE with whitespace",
			rawReq:        `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"  SEND_MESSAGE  "}}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: true,
			expectTool:    "  SEND_MESSAGE  ",
			expectErr:     false,
		},
		{
			name:          "single call - allowed tool discord_read_messages",
			rawReq:        `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"discord_read_messages"}}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "discord_read_messages",
			expectErr:     false,
		},
		{
			name:          "single call - non-tool method ping",
			rawReq:        `{"jsonrpc":"2.0","id":6,"method":"ping"}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     false,
		},
		{
			name:          "single call - empty params",
			rawReq:        `{"jsonrpc":"2.0","id":7,"method":"tools/call"}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     false,
		},
		{
			name:          "single call - malformed params scalar",
			rawReq:        `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":"not-an-object"}`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     true,
		},
		{
			name:          "single call - empty raw request",
			rawReq:        "",
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     true,
		},
		{
			name:          "single call - malformed json",
			rawReq:        `{invalid json`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     true,
		},
		{
			name:          "single call - nil blocked map uses default",
			rawReq:        `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"discord_create_thread"}}`,
			blockedMap:    nil,
			expectBlocked: true,
			expectTool:    "discord_create_thread",
			expectErr:     false,
		},
		{
			name: "batch request - contains blocked tool",
			rawReq: `[
				{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"discord_read_messages"}},
				{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"discord_send"}}
			]`,
			blockedMap:    BlockedToolNames,
			expectBlocked: true,
			expectTool:    "discord_send",
			expectErr:     false,
		},
		{
			name: "batch request - contains case variant blocked tool",
			rawReq: `[
				{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"DISCORD_CREATE_THREAD"}}
			]`,
			blockedMap:    BlockedToolNames,
			expectBlocked: true,
			expectTool:    "DISCORD_CREATE_THREAD",
			expectErr:     false,
		},
		{
			name: "batch request - allowed tools only",
			rawReq: `[
				{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"discord_read_messages"}},
				{"jsonrpc":"2.0","id":14,"method":"ping"}
			]`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     false,
		},
		{
			name:          "batch request - empty array",
			rawReq:        `[]`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     false,
		},
		{
			name:          "batch request - malformed json array",
			rawReq:        `[{"jsonrpc":"2.0", invalid`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     true,
		},
		{
			name: "batch request - malformed item params",
			rawReq: `[
				{"jsonrpc":"2.0","id":15,"method":"tools/call","params":12345}
			]`,
			blockedMap:    BlockedToolNames,
			expectBlocked: false,
			expectTool:    "",
			expectErr:     true,
		},
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
		name         string
		rawResp      string
		blockedMap   map[string]bool
		expectErr    bool
		assertResult func(t *testing.T, out []byte)
	}{
		{
			name: "standard response with blocked and allowed tools",
			rawResp: `{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"tools": [
						{"name": "discord_send", "description": "Send a message"},
						{"name": "discord_read_messages", "description": "Read messages"},
						{"name": "send_message", "description": "Send another message"}
					]
				}
			}`,
			blockedMap: BlockedToolNames,
			expectErr:  false,
			assertResult: func(t *testing.T, out []byte) {
				var parsed map[string]interface{}
				_ = json.Unmarshal(out, &parsed)
				res := parsed["result"].(map[string]interface{})
				tools := res["tools"].([]interface{})
				if len(tools) != 1 {
					t.Fatalf("expected 1 tool remaining, got %d", len(tools))
				}
				item := tools[0].(map[string]interface{})
				if item["name"] != "discord_read_messages" {
					t.Errorf("expected remaining tool to be discord_read_messages, got %v", item["name"])
				}
			},
		},
		{
			name: "preserves nextCursor and _meta in result",
			rawResp: `{
				"jsonrpc": "2.0",
				"id": 2,
				"result": {
					"tools": [
						{"name": "discord_send"},
						{"name": "allowed_tool"}
					],
					"nextCursor": "cursor-token-xyz",
					"_meta": {"version": "2024-11-05"}
				}
			}`,
			blockedMap: BlockedToolNames,
			expectErr:  false,
			assertResult: func(t *testing.T, out []byte) {
				var parsed map[string]interface{}
				_ = json.Unmarshal(out, &parsed)
				res := parsed["result"].(map[string]interface{})
				if res["nextCursor"] != "cursor-token-xyz" {
					t.Errorf("expected nextCursor to be preserved, got %v", res["nextCursor"])
				}
				meta, ok := res["_meta"].(map[string]interface{})
				if !ok || meta["version"] != "2024-11-05" {
					t.Errorf("expected _meta to be preserved, got %v", res["_meta"])
				}
			},
		},
		{
			name: "case variant blocked tool filtered",
			rawResp: `{
				"jsonrpc": "2.0",
				"id": 3,
				"result": {
					"tools": [
						{"name": "DISCORD_SEND"},
						{"name": "valid_tool"}
					]
				}
			}`,
			blockedMap: BlockedToolNames,
			expectErr:  false,
			assertResult: func(t *testing.T, out []byte) {
				if strings.Contains(string(out), "DISCORD_SEND") {
					t.Errorf("expected DISCORD_SEND to be filtered out, got %s", string(out))
				}
			},
		},
		{
			name:       "empty result preserves raw response",
			rawResp:    `{"jsonrpc":"2.0","id":4}`,
			blockedMap: BlockedToolNames,
			expectErr:  false,
			assertResult: func(t *testing.T, out []byte) {
				if string(out) != `{"jsonrpc":"2.0","id":4}` {
					t.Errorf("expected unmodified raw response, got %s", string(out))
				}
			},
		},
		{
			name:       "null result preserves raw response without shape mutation",
			rawResp:    `{"jsonrpc":"2.0","id":5,"result":null}`,
			blockedMap: BlockedToolNames,
			expectErr:  false,
			assertResult: func(t *testing.T, out []byte) {
				if !strings.Contains(string(out), `"result":null`) {
					t.Errorf("expected result:null preserved, got %s", string(out))
				}
			},
		},
		{
			name:       "non-object result returns error",
			rawResp:    `{"jsonrpc":"2.0","id":6,"result":"hello"}`,
			blockedMap: BlockedToolNames,
			expectErr:  true,
		},
		{
			name:       "missing tools array in result preserves raw response",
			rawResp:    `{"jsonrpc":"2.0","id":7,"result":{"other":"data"}}`,
			blockedMap: BlockedToolNames,
			expectErr:  false,
			assertResult: func(t *testing.T, out []byte) {
				if !strings.Contains(string(out), `"other":"data"`) {
					t.Errorf("expected result preserved, got %s", string(out))
				}
			},
		},
		{
			name:       "malformed top-level JSON returns error",
			rawResp:    `{invalid json`,
			blockedMap: BlockedToolNames,
			expectErr:  true,
		},
		{
			name:       "malformed tools list JSON returns error",
			rawResp:    `{"jsonrpc":"2.0","id":9,"result":{"tools":"not-an-array"}}`,
			blockedMap: BlockedToolNames,
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := FilterToolsResponse([]byte(tt.rawResp), tt.blockedMap)
			if tt.expectErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.assertResult != nil {
				tt.assertResult(t, out)
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
		{
			name:     "valid tools/list request",
			rawReq:   `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
			expected: true,
		},
		{
			name:     "valid tools/list request with whitespace",
			rawReq:   `  {"jsonrpc":"2.0","id":1,"method":"  tools/list  "}  `,
			expected: true,
		},
		{
			name:     "tools/call request returns false",
			rawReq:   `{"jsonrpc":"2.0","id":2,"method":"tools/call"}`,
			expected: false,
		},
		{
			name:     "other method returns false",
			rawReq:   `{"jsonrpc":"2.0","id":3,"method":"ping"}`,
			expected: false,
		},
		{
			name:     "empty method returns false",
			rawReq:   `{"jsonrpc":"2.0","id":4}`,
			expected: false,
		},
		{
			name:     "empty body returns false",
			rawReq:   "",
			expected: false,
		},
		{
			name:     "array returns false",
			rawReq:   `[{"jsonrpc":"2.0","method":"tools/list"}]`,
			expected: false,
		},
		{
			name:     "invalid json returns false",
			rawReq:   `{invalid json`,
			expected: false,
		},
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
		name       string
		id         interface{}
		code       int
		message    string
		assertJSON func(t *testing.T, out []byte)
	}{
		{
			name:    "integer id",
			id:      42,
			code:    -32601,
			message: "Tool disabled",
			assertJSON: func(t *testing.T, out []byte) {
				var resp JSONRPCResponse
				if err := json.Unmarshal(out, &resp); err != nil {
					t.Fatalf("unmarshal failed: %v", err)
				}
				if resp.ID != float64(42) || resp.Error.Code != -32601 || resp.Error.Message != "Tool disabled" {
					t.Errorf("unexpected response: %+v", resp)
				}
			},
		},
		{
			name:    "string id",
			id:      "req-uuid-123",
			code:    -32700,
			message: "Parse error",
			assertJSON: func(t *testing.T, out []byte) {
				var resp JSONRPCResponse
				if err := json.Unmarshal(out, &resp); err != nil {
					t.Fatalf("unmarshal failed: %v", err)
				}
				if resp.ID != "req-uuid-123" {
					t.Errorf("expected string id, got %v", resp.ID)
				}
			},
		},
		{
			name:    "nil id preserves id: null",
			id:      nil,
			code:    -32600,
			message: "Invalid request",
			assertJSON: func(t *testing.T, out []byte) {
				if !strings.Contains(string(out), `"id":null`) {
					t.Errorf("expected '\"id\":null' in error response, got %s", string(out))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := BuildJSONRPCErrorResponse(tt.id, tt.code, tt.message)
			tt.assertJSON(t, out)
		})
	}
}

func TestBuildHealthResponse_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		wrapper      string
		blockedTools []string
		assertJSON   func(t *testing.T, out []byte)
	}{
		{
			name:         "default wrapper and canonical blocked tools",
			wrapper:      "",
			blockedTools: DefaultBlockedToolList,
			assertJSON: func(t *testing.T, out []byte) {
				var resp HealthResponse
				if err := json.Unmarshal(out, &resp); err != nil {
					t.Fatalf("unmarshal failed: %v", err)
				}
				if resp.Status != "ok" || resp.Wrapper != "aerial-discord-mcp" {
					t.Errorf("unexpected health status or wrapper: %+v", resp)
				}
				if len(resp.BlockedTools) != 3 || resp.BlockedTools[0] != "discord_create_thread" {
					t.Errorf("unexpected sorted blocked tools: %+v", resp.BlockedTools)
				}
			},
		},
		{
			name:         "custom wrapper and unsorted input gets sorted",
			wrapper:      "custom-wrapper",
			blockedTools: []string{"z_tool", "a_tool", "m_tool"},
			assertJSON: func(t *testing.T, out []byte) {
				var resp HealthResponse
				_ = json.Unmarshal(out, &resp)
				if resp.Wrapper != "custom-wrapper" {
					t.Errorf("expected custom-wrapper, got %s", resp.Wrapper)
				}
				if resp.BlockedTools[0] != "a_tool" || resp.BlockedTools[1] != "m_tool" || resp.BlockedTools[2] != "z_tool" {
					t.Errorf("expected sorted tools, got %v", resp.BlockedTools)
				}
			},
		},
		{
			name:         "nil blocked tools serializes as empty slice",
			wrapper:      "test",
			blockedTools: nil,
			assertJSON: func(t *testing.T, out []byte) {
				if !strings.Contains(string(out), `"blocked_tools":[]`) {
					t.Errorf("expected '\"blocked_tools\":[]', got %s", string(out))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := BuildHealthResponse(tt.wrapper, tt.blockedTools)
			tt.assertJSON(t, out)
		})
	}
}

func TestValidateConfig_TableDriven(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *Config
		expectErr   bool
		errContains string
	}{
		{
			name:        "nil config",
			cfg:         nil,
			expectErr:   true,
			errContains: "config cannot be nil",
		},
		{
			name:        "empty port",
			cfg:         &Config{Port: "", UpstreamPort: "4005", NodeBin: "node", AppPath: "build/app.js"},
			expectErr:   true,
			errContains: "port cannot be empty",
		},
		{
			name:        "whitespace port",
			cfg:         &Config{Port: "   ", UpstreamPort: "4005", NodeBin: "node", AppPath: "build/app.js"},
			expectErr:   true,
			errContains: "port cannot be empty",
		},
		{
			name:        "empty upstream port",
			cfg:         &Config{Port: "4001", UpstreamPort: "", NodeBin: "node", AppPath: "build/app.js"},
			expectErr:   true,
			errContains: "upstreamPort cannot be empty",
		},
		{
			name:        "whitespace upstream port",
			cfg:         &Config{Port: "4001", UpstreamPort: "  ", NodeBin: "node", AppPath: "build/app.js"},
			expectErr:   true,
			errContains: "upstreamPort cannot be empty",
		},
		{
			name:        "empty node bin",
			cfg:         &Config{Port: "4001", UpstreamPort: "4005", NodeBin: "", AppPath: "build/app.js"},
			expectErr:   true,
			errContains: "nodeBin cannot be empty",
		},
		{
			name:        "whitespace node bin",
			cfg:         &Config{Port: "4001", UpstreamPort: "4005", NodeBin: "  ", AppPath: "build/app.js"},
			expectErr:   true,
			errContains: "nodeBin cannot be empty",
		},
		{
			name:        "empty app path",
			cfg:         &Config{Port: "4001", UpstreamPort: "4005", NodeBin: "node", AppPath: ""},
			expectErr:   true,
			errContains: "appPath cannot be empty",
		},
		{
			name:        "whitespace app path",
			cfg:         &Config{Port: "4001", UpstreamPort: "4005", NodeBin: "node", AppPath: "  \t "},
			expectErr:   true,
			errContains: "appPath cannot be empty",
		},
		{
			name:      "valid config with trimming",
			cfg:       &Config{Port: " 4001 ", UpstreamPort: " 4005 ", NodeBin: " node ", AppPath: " app.js "},
			expectErr: false,
		},
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
		assertEnv    func(t *testing.T, env []string)
	}{
		{
			name:         "strips uppercase PORT and appends new",
			environ:      []string{"PORT=3000", "PATH=/usr/bin", "FOO=bar"},
			upstreamPort: "4005",
			assertEnv: func(t *testing.T, env []string) {
				foundPort := false
				for _, e := range env {
					if strings.HasPrefix(e, "PORT=") {
						if foundPort {
							t.Errorf("duplicate PORT found: %s", e)
						}
						foundPort = true
						if e != "PORT=4005" {
							t.Errorf("expected PORT=4005, got %s", e)
						}
					}
				}
				if !foundPort {
					t.Error("PORT=4005 not found in env")
				}
			},
		},
		{
			name:         "case-insensitively strips port= and Port=",
			environ:      []string{"port=3000", "Port=9999", "OTHER=1"},
			upstreamPort: "4005",
			assertEnv: func(t *testing.T, env []string) {
				if len(env) != 2 {
					t.Fatalf("expected 2 items, got %d: %v", len(env), env)
				}
				if env[0] != "OTHER=1" || env[1] != "PORT=4005" {
					t.Errorf("unexpected env items: %v", env)
				}
			},
		},
		{
			name:         "empty input environment",
			environ:      nil,
			upstreamPort: "4005",
			assertEnv: func(t *testing.T, env []string) {
				if len(env) != 1 || env[0] != "PORT=4005" {
					t.Errorf("expected [PORT=4005], got %v", env)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := BuildNodeEnv(tt.environ, tt.upstreamPort)
			tt.assertEnv(t, env)
		})
	}
}

func TestFormatUpstreamURL_TableDriven(t *testing.T) {
	tests := []struct {
		name         string
		upstreamPort string
		expected     string
	}{
		{
			name:         "clean port",
			upstreamPort: "4005",
			expected:     "http://127.0.0.1:4005",
		},
		{
			name:         "port with whitespace",
			upstreamPort: "  5001  ",
			expected:     "http://127.0.0.1:5001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FormatUpstreamURL(tt.upstreamPort); got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}
