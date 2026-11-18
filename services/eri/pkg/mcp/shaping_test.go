package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/itchyny/gojq"

	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
)

func extractToolNames(t *testing.T, filtered []byte) []string {
	t.Helper()
	if len(bytes.TrimSpace(filtered)) == 0 {
		return nil
	}
	var root map[string]any
	if strings.HasPrefix(strings.TrimSpace(string(filtered)), "[") {
		var batch []map[string]any
		if err := json.Unmarshal(filtered, &batch); err != nil {
			t.Fatalf("unmarshal batch: %v", err)
		}
		if len(batch) > 0 {
			root = batch[0]
		}
	} else {
		if err := json.Unmarshal(filtered, &root); err != nil {
			t.Fatalf("unmarshal root: %v", err)
		}
	}
	result, _ := root["result"].(map[string]any)
	toolsList, _ := result["tools"].([]any)
	var gotNames []string
	for _, item := range toolsList {
		if m, ok := item.(map[string]any); ok {
			if name, ok := m["name"].(string); ok {
				gotNames = append(gotNames, name)
			}
		}
	}
	return gotNames
}

func TestFilterToolsResponse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		policy    env.ServerPolicy
		rawResp   string
		wantTools []string
		wantErr   bool
	}{
		{
			name:      "empty policy preserves all tools",
			policy:    env.ServerPolicy{},
			rawResp:   `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"t1"},{"name":"t2"}]}}`,
			wantTools: []string{"t1", "t2"},
		},
		{
			name:      "disabled tool filtered out",
			policy:    env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			rawResp:   `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"t1"},{"name":"t2"}]}}`,
			wantTools: []string{"t1"},
		},
		{
			name:      "disabled tool filtered out case-insensitively",
			policy:    env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			rawResp:   `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"t1"},{"name":"T2"}]}}`,
			wantTools: []string{"t1"},
		},
		{
			name:      "whitelist preserves only enabled tools",
			policy:    env.ServerPolicy{EnabledTools: map[string]bool{"t2": true}},
			rawResp:   `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"t1"},{"name":"t2"},{"name":"t3"}]}}`,
			wantTools: []string{"t2"},
		},
		{
			name:      "batch response tools filtered",
			policy:    env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			rawResp:   `[{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"t1"},{"name":"t2"}]}}]`,
			wantTools: []string{"t1"},
		},
		{
			name:    "invalid json returns error",
			policy:  env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			rawResp: `{not-json`,
			wantErr: true,
		},
		{
			name:      "response without tools returns empty tool list",
			policy:    env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			rawResp:   `{"jsonrpc":"2.0","id":1,"result":{}}`,
			wantTools: []string{},
		},
		{
			name:      "non-map tool item is dropped",
			policy:    env.ServerPolicy{DisabledTools: map[string]bool{"t1": true}},
			rawResp:   `{"jsonrpc":"2.0","id":1,"result":{"tools":["item"]}}`,
			wantTools: []string{},
		},
		{
			name:      "tool item without string name is dropped",
			policy:    env.ServerPolicy{DisabledTools: map[string]bool{"t1": true}},
			rawResp:   `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":99}]}}`,
			wantTools: []string{},
		},
		{
			name:      "empty raw response returns empty tool list",
			policy:    env.ServerPolicy{DisabledTools: map[string]bool{"t1": true}},
			rawResp:   "",
			wantTools: []string{},
		},
		{
			name:    "batch malformed json returns error",
			policy:  env.ServerPolicy{DisabledTools: map[string]bool{"t1": true}},
			rawResp: `[{bad`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			filtered, err := mcp.FilterToolsResponse([]byte(tc.rawResp), tc.policy)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			gotNames := extractToolNames(t, filtered)
			if gotNames == nil {
				gotNames = []string{}
			}
			if !stringsEqual(gotNames, tc.wantTools) {
				t.Errorf("got tools %v, want %v", gotNames, tc.wantTools)
			}
		})
	}
}

func stringsEqual(a, b []string) bool {
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

func TestInjectInstructions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		instructions string
		rawResp      string
		wantInst     string
		wantErr      bool
	}{
		{
			name:         "empty instructions leaves response untouched",
			instructions: "",
			rawResp:      `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}`,
			wantInst:     "",
		},
		{
			name:         "injects into empty result",
			instructions: "Strict mode only",
			rawResp:      `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}`,
			wantInst:     "Strict mode only",
		},
		{
			name:         "appends to existing instructions with double newline",
			instructions: "Second paragraph",
			rawResp:      `{"jsonrpc":"2.0","id":1,"result":{"instructions":"First paragraph"}}`,
			wantInst:     "First paragraph\n\nSecond paragraph",
		},
		{
			name:         "batch response injection",
			instructions: "Batch rule",
			rawResp:      `[{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}]`,
			wantInst:     "Batch rule",
		},
		{
			name:         "malformed json returns error",
			instructions: "Rule",
			rawResp:      `{bad`,
			wantErr:      true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			injected, err := mcp.InjectInstructions([]byte(tc.rawResp), tc.instructions)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			var root map[string]any
			if strings.HasPrefix(strings.TrimSpace(string(injected)), "[") {
				var batch []map[string]any
				if err := json.Unmarshal(injected, &batch); err != nil {
					t.Fatalf("unmarshal batch: %v", err)
				}
				if len(batch) > 0 {
					root = batch[0]
				}
			} else {
				if err := json.Unmarshal(injected, &root); err != nil {
					t.Fatalf("unmarshal root: %v", err)
				}
			}
			result, _ := root["result"].(map[string]any)
			gotInst, _ := result["instructions"].(string)
			if gotInst != tc.wantInst {
				t.Errorf("got instructions %q, want %q", gotInst, tc.wantInst)
			}
		})
	}
}

func extractFilteredContentText(t *testing.T, data []byte) string {
	t.Helper()
	var root map[string]any
	if strings.HasPrefix(strings.TrimSpace(string(data)), "[") {
		var batch []map[string]any
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Fatalf("unmarshal batch: %v", err)
		}
		if len(batch) == 0 {
			return ""
		}
		root = batch[0]
	} else {
		if err := json.Unmarshal(data, &root); err != nil {
			t.Fatalf("unmarshal filtered: %v", err)
		}
	}
	result, _ := root["result"].(map[string]any)
	content, _ := result["content"].([]any)
	if len(content) == 0 {
		return ""
	}
	first, _ := content[0].(map[string]any)
	txt, _ := first["text"].(string)
	return txt
}

type applyFilterTestCase struct {
	name     string
	jqQuery  string
	rawReq   string
	rawResp  string
	wantRaw  string
	wantText string
	wantErr  bool
}

func applyFilterTransformCases() []applyFilterTestCase {
	return []applyFilterTestCase{
		{
			name:     "nil code returns response verbatim",
			jqQuery:  "",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"search"}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":1}"}]}}`,
			wantRaw:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":1}"}]}}`,
			wantText: `{"count":1}`,
		},
		{
			name:     "empty response returns empty",
			jqQuery:  ".",
			rawReq:   "",
			rawResp:  "",
			wantRaw:  "",
			wantText: "",
		},
		{
			name:     "compacts JSON and transforms text with jq",
			jqQuery:  ".items[] | {name: .name, size: .size}",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"search","arguments":{"q":"test"}}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"items\":[{\"name\":\"file.go\",\"size\":100,\"extra\":\"ignore\"}]}"}]}}`,
			wantText: `{"name":"file.go","size":100}`,
		},
		{
			name:     "batch response transforms each item",
			jqQuery:  ".items[] | {name: .name}",
			rawReq:   `[{"jsonrpc":"2.0","method":"tools/call","params":{"name":"search","arguments":{"q":"batch"}}}]`,
			rawResp:  `[{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"items\":[{\"name\":\"batch_file\"}]}"}]}}]`,
			wantText: `{"name":"batch_file"}`,
		},
		{
			name:     "uses $args variable in jq expression",
			jqQuery:  "{query: $args.q, result: .count}",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"count","arguments":{"q":"foo"}}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":42}"}]}}`,
			wantText: `{"query":"foo","result":42}`,
		},
		{
			name:     "unwraps string-encoded JSON arguments into $args",
			jqQuery:  "{query: $args.q, result: .count}",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"count","arguments":"{\"q\":\"bar\"}"}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":99}"}]}}`,
			wantText: `{"query":"bar","result":99}`,
		},
		{
			name:     "string argument that is not JSON leaves $args empty",
			jqQuery:  "{has_arg: ($args | has(\"q\")), val: .count}",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"count","arguments":"not-json"}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":7}"}]}}`,
			wantText: `{"has_arg":false,"val":7}`,
		},
		{
			name:     "missing arguments defaults to empty map for $args",
			jqQuery:  "{has_arg: ($args | has(\"q\")), val: .count}",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":5}"}]}}`,
			wantText: `{"has_arg":false,"val":5}`,
		},
		{
			name:     "leaves error response untouched",
			jqQuery:  ".count",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"count","arguments":{}}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"isError":true,"content":[{"type":"text","text":"{\"count\":42}"}]}}`,
			wantRaw:  `{"id":1,"jsonrpc":"2.0","result":{"content":[{"text":"{\"count\":42}","type":"text"}],"isError":true}}`,
			wantText: `{"count":42}`,
		},
		{
			name:     "leaves non-JSON text content untouched",
			jqQuery:  ".count",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"count","arguments":{}}}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"plain text not json"}]}}`,
			wantRaw:  `{"id":1,"jsonrpc":"2.0","result":{"content":[{"text":"plain text not json","type":"text"}]}}`,
			wantText: `plain text not json`,
		},
		{
			name:     "response without result map left untouched",
			jqQuery:  ".count",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call"}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":"scalar"}`,
			wantRaw:  `{"id":1,"jsonrpc":"2.0","result":"scalar"}`,
			wantText: "",
		},
		{
			name:     "response without content array left untouched",
			jqQuery:  ".count",
			rawReq:   `{"jsonrpc":"2.0","method":"tools/call"}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":"not-slice"}}`,
			wantRaw:  `{"id":1,"jsonrpc":"2.0","result":{"content":"not-slice"}}`,
			wantText: "",
		},
	}
}

func applyFilterEdgeCases() []applyFilterTestCase {
	return []applyFilterTestCase{
		{
			name:    "invalid response JSON returns error",
			jqQuery: ".count",
			rawReq:  `{}`,
			rawResp: `{invalid`,
			wantErr: true,
		},
		{
			name:    "filter returning empty fails open",
			jqQuery: "empty",
			rawReq:  `{}`,
			rawResp: `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":1}"}]}}`,
			wantErr: true,
		},
		{
			name:    "filter error fails open",
			jqQuery: "error(\"jq-err\")",
			rawReq:  `{}`,
			rawResp: `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":1}"}]}}`,
			wantErr: true,
		},
		{
			name:    "batch response filter error fails open",
			jqQuery: "error(\"jq-err\")",
			rawReq:  `{}`,
			rawResp: `[{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"count\":1}"}]}}]`,
			wantErr: true,
		},
		{
			name:    "batch malformed json returns error",
			jqQuery: ".",
			rawReq:  `{}`,
			rawResp: `[{bad`,
			wantErr: true,
		},
		{
			name:    "batch item not a map returns error",
			jqQuery: ".",
			rawReq:  `{}`,
			rawResp: `["item"]`,
			wantErr: true,
		},
		{
			name:    "top level scalar returns error",
			jqQuery: ".",
			rawReq:  `{}`,
			rawResp: `"scalar"`,
			wantErr: true,
		},
		{
			name:     "content item not map left untouched",
			jqQuery:  ".",
			rawReq:   `{}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":["item"]}}`,
			wantRaw:  `{"id":1,"jsonrpc":"2.0","result":{"content":["item"]}}`,
			wantText: "",
		},
		{
			name:     "content type not text left untouched",
			jqQuery:  ".",
			rawReq:   `{}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"image"}]}}`,
			wantRaw:  `{"id":1,"jsonrpc":"2.0","result":{"content":[{"type":"image"}]}}`,
			wantText: "",
		},
		{
			name:     "content text not string left untouched",
			jqQuery:  ".",
			rawReq:   `{}`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":42}]}}`,
			wantRaw:  `{"id":1,"jsonrpc":"2.0","result":{"content":[{"text":42,"type":"text"}]}}`,
			wantText: "",
		},
		{
			name:     "batch request malformed with gojq variable",
			jqQuery:  "{val: .val, q: $args.q}",
			rawReq:   `[{bad`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"val\":1}"}]}}`,
			wantText: `{"q":null,"val":1}`,
		},
		{
			name:     "batch request scalar item with gojq variable",
			jqQuery:  "{val: .val, q: $args.q}",
			rawReq:   `["item"]`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"val\":2}"}]}}`,
			wantText: `{"q":null,"val":2}`,
		},
		{
			name:     "single request malformed with gojq variable",
			jqQuery:  "{val: .val, q: $args.q}",
			rawReq:   `{bad`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"val\":3}"}]}}`,
			wantText: `{"q":null,"val":3}`,
		},
		{
			name:     "request scalar with gojq variable",
			jqQuery:  "{val: .val, q: $args.q}",
			rawReq:   `"scalar"`,
			rawResp:  `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"{\"val\":4}"}]}}`,
			wantText: `{"q":null,"val":4}`,
		},
	}
}

func TestApplyResponseFilter(t *testing.T) {
	t.Parallel()
	tests := append(applyFilterTransformCases(), applyFilterEdgeCases()...)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var code *gojq.Code
			if tc.jqQuery != "" {
				code = compileJQ(t, tc.jqQuery)
			}
			filtered, err := mcp.ApplyResponseFilter(context.Background(), []byte(tc.rawResp), []byte(tc.rawReq), code)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if tc.wantRaw != "" || tc.wantText == "" {
				if string(filtered) != tc.wantRaw {
					t.Errorf("got raw %q, want %q", string(filtered), tc.wantRaw)
				}
			}
			if tc.wantText != "" {
				gotText := extractFilteredContentText(t, filtered)
				if gotText != tc.wantText {
					t.Errorf("got text %q, want %q", gotText, tc.wantText)
				}
			}
		})
	}
}

func TestShapeResponseBody(t *testing.T) {
	t.Parallel()
	filterCode := compileJQ(t, ".items[] | {name: .name}")
	tests := []struct {
		name        string
		contentType string
		rpcMethod   string
		tool        string
		reqBody     string
		respBody    string
		policy      env.ServerPolicy
		wantSub     string
		dontWantSub string
	}{
		{
			name:        "json tools/list filtered",
			contentType: "application/json",
			rpcMethod:   "tools/list",
			respBody:    `{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"t1"},{"name":"t2"}]}}`,
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			wantSub:     `"name":"t1"`,
			dontWantSub: `"name":"t2"`,
		},
		{
			name:        "json initialize instructions injected",
			contentType: "application/json",
			rpcMethod:   "initialize",
			respBody:    `{"jsonrpc":"2.0","id":2,"result":{"protocolVersion":"2024-11-05"}}`,
			policy:      env.ServerPolicy{Instructions: "Custom policy"},
			wantSub:     `"instructions":"Custom policy"`,
		},
		{
			name:        "json tools/call filtered with gojq",
			contentType: "application/json",
			rpcMethod:   "tools/call",
			tool:        "my_tool",
			reqBody:     `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"my_tool","arguments":{}}}`,
			respBody:    `{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"{\"items\":[{\"name\":\"item1\",\"junk\":true}]}"}]}}`,
			policy:      env.ServerPolicy{ResponseFilters: map[string]*gojq.Code{"my_tool": filterCode}},
			wantSub:     `\"name\":\"item1\"`,
			dontWantSub: `junk`,
		},
		{
			name:        "json tools/call unconfigured tool returns unchanged",
			contentType: "application/json",
			rpcMethod:   "tools/call",
			tool:        "other_tool",
			reqBody:     `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"other_tool"}}`,
			respBody:    `{"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"verbatim"}]}}`,
			policy:      env.ServerPolicy{ResponseFilters: map[string]*gojq.Code{"my_tool": filterCode}},
			wantSub:     `verbatim`,
		},
		{
			name:        "unknown rpc method returns unchanged",
			contentType: "application/json",
			rpcMethod:   "ping",
			respBody:    `{"jsonrpc":"2.0","id":4,"result":{}}`,
			wantSub:     `{"jsonrpc":"2.0","id":4,"result":{}}`,
		},
		{
			name:        "sse tools/list framed in data: prefix",
			contentType: "text/event-stream",
			rpcMethod:   "tools/list",
			respBody:    "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"t1\"},{\"name\":\"t2\"}]}}\n\n",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			wantSub:     `data: {"id":1,"jsonrpc":"2.0","result":{"tools":[{"name":"t1"}]}}`,
			dontWantSub: `"t2"`,
		},
		{
			name:        "sse tools/list framed in data: prefix without space",
			contentType: "text/event-stream",
			rpcMethod:   "tools/list",
			respBody:    "data:{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"tools\":[{\"name\":\"t1\"},{\"name\":\"t2\"}]}}\n\n",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			wantSub:     `data:{"id":1,"jsonrpc":"2.0","result":{"tools":[{"name":"t1"}]}}`,
			dontWantSub: `"t2"`,
		},
		{
			name:        "sse line without data: prefix preserved verbatim",
			contentType: "text/event-stream",
			rpcMethod:   "tools/list",
			respBody:    ":keepalive comment\nevent: ping\n\n",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			wantSub:     ":keepalive comment\nevent: ping",
		},
		{
			name:        "sse line with malformed json preserved verbatim",
			contentType: "text/event-stream",
			rpcMethod:   "tools/list",
			respBody:    "data: {bad json\n\n",
			policy:      env.ServerPolicy{DisabledTools: map[string]bool{"t2": true}},
			wantSub:     "data: {bad json",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shaped, err := mcp.ShapeResponseBody(context.Background(), []byte(tc.respBody), tc.contentType, tc.rpcMethod, tc.tool, []byte(tc.reqBody), tc.policy)
			if err != nil {
				t.Fatalf("ShapeResponseBody: %v", err)
			}
			s := string(shaped)
			if !strings.Contains(s, tc.wantSub) {
				t.Errorf("shaped missing substring %q, got: %s", tc.wantSub, s)
			}
			if tc.dontWantSub != "" && strings.Contains(s, tc.dontWantSub) {
				t.Errorf("shaped unexpectedly contained %q, got: %s", tc.dontWantSub, s)
			}
		})
	}
}

func TestShapeSSEBody_ScannerError(t *testing.T) {
	t.Parallel()
	longLine := bytes.Repeat([]byte("a"), 17*1024*1024)
	out, err := mcp.ShapeResponseBody(context.Background(), longLine, "text/event-stream", "tools/list", "", nil, env.ServerPolicy{})
	if err == nil {
		t.Fatalf("expected error for line exceeding max scan token size")
	}
	if len(out) != len(longLine) {
		t.Errorf("expected out to equal longLine on error")
	}
}

func TestBuildJSONRPCErrorResponse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		reqID   any
		code    int
		msg     string
		wantSub string
	}{
		{
			name:    "numeric id",
			reqID:   42,
			code:    -32601,
			msg:     "disabled",
			wantSub: `"id":42`,
		},
		{
			name:    "string id",
			reqID:   "req-1",
			code:    -32601,
			msg:     "disabled",
			wantSub: `"id":"req-1"`,
		},
		{
			name:    "unmarshalable id falls back to null id",
			reqID:   math.NaN(),
			code:    -32601,
			msg:     "disabled",
			wantSub: `"id":null`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mcp.BuildJSONRPCErrorResponse(tc.reqID, tc.code, tc.msg)
			if !strings.Contains(string(got), tc.wantSub) {
				t.Errorf("got %s, want substring %s", string(got), tc.wantSub)
			}
		})
	}
}
