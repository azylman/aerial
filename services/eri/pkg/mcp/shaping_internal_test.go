package mcp

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/itchyny/gojq"

	"github.com/azylman/aerial/services/eri/pkg/env"
)

func compileInternalJQ(t *testing.T, expr string) *gojq.Code {
	t.Helper()
	query, err := gojq.Parse(expr)
	if err != nil {
		t.Fatalf("parse jq %q: %v", expr, err)
	}
	code, err := gojq.Compile(query, gojq.WithVariables([]string{"$args"}))
	if err != nil {
		t.Fatalf("compile jq %q: %v", expr, err)
	}
	return code
}

func TestNeedsResponseShaping(t *testing.T) {
	t.Parallel()
	code := compileInternalJQ(t, ".")
	tests := []struct {
		name       string
		httpMethod string
		rpcMethod  string
		tool       string
		policy     env.ServerPolicy
		want       bool
	}{
		{
			name:       "non-POST returns false",
			httpMethod: http.MethodGet,
			rpcMethod:  mcpMethodToolsList,
			policy:     env.ServerPolicy{DisabledTools: map[string]bool{"t1": true}},
			want:       false,
		},
		{
			name:       "unknown rpc method returns false",
			httpMethod: http.MethodPost,
			rpcMethod:  "unknown/method",
			policy:     env.ServerPolicy{DisabledTools: map[string]bool{"t1": true}},
			want:       false,
		},
		{
			name:       "tools/list with enabled tools returns true",
			httpMethod: http.MethodPost,
			rpcMethod:  mcpMethodToolsList,
			policy:     env.ServerPolicy{EnabledTools: map[string]bool{"t1": true}},
			want:       true,
		},
		{
			name:       "tools/list without tool policy returns false",
			httpMethod: http.MethodPost,
			rpcMethod:  mcpMethodToolsList,
			policy:     env.ServerPolicy{Instructions: "rule"},
			want:       false,
		},
		{
			name:       "initialize without instructions returns false",
			httpMethod: http.MethodPost,
			rpcMethod:  mcpMethodInitialize,
			policy:     env.ServerPolicy{DisabledTools: map[string]bool{"t1": true}},
			want:       false,
		},
		{
			name:       "tools/call without matching filter returns false",
			httpMethod: http.MethodPost,
			rpcMethod:  mcpMethodToolsCall,
			tool:       "other",
			policy:     env.ServerPolicy{ResponseFilters: map[string]*gojq.Code{"target": code}},
			want:       false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := needsResponseShaping(tc.httpMethod, tc.rpcMethod, tc.tool, tc.policy)
			if got != tc.want {
				t.Errorf("needsResponseShaping() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMCPToolCallID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "malformed json returns empty", body: "{not-json", want: ""},
		{name: "valid json without meta returns empty", body: `{"params":{}}`, want: ""},
		{name: "valid json with claudecode toolUseId returns id", body: `{"params":{"_meta":{"claudecode/toolUseId":"tool-xyz"}}}`, want: "tool-xyz"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mcpToolCallID([]byte(tc.body))
			if got != tc.want {
				t.Errorf("mcpToolCallID() = %q, want %q", got, tc.want)
			}
		})
	}
}

type shapeResponseCase struct {
	name        string
	body        []byte
	encoding    string
	contentType string
	rpcMethod   string
	tool        string
	reqBody     string
	policy      env.ServerPolicy
	wantSub     string
}

func TestShapeResponse(t *testing.T) {
	t.Parallel()
	filterCode := compileInternalJQ(t, ".count")
	tests := []shapeResponseCase{
		{
			name:        "invalid gzip leaves bytes unchanged",
			body:        []byte("not-gzip"),
			encoding:    "gzip",
			contentType: "application/json",
			rpcMethod:   mcpMethodToolsCall,
			tool:        "filter",
			reqBody:     `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"filter"}}`,
			policy:      env.ServerPolicy{ResponseFilters: map[string]*gojq.Code{"filter": filterCode}},
			wantSub:     "not-gzip",
		},
		{
			name:        "shape error falls back to original response bytes",
			body:        []byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"invalid-json"}]}}`),
			contentType: "application/json",
			rpcMethod:   mcpMethodToolsCall,
			tool:        "filter",
			reqBody:     `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"filter"}}`,
			policy:      env.ServerPolicy{ResponseFilters: map[string]*gojq.Code{"filter": filterCode}},
			wantSub:     "invalid-json",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tp := &TraceProxy{}
			resp := &http.Response{
				Header:        make(http.Header),
				Body:          io.NopCloser(bytes.NewReader(tc.body)),
				ContentLength: int64(len(tc.body)),
			}
			if tc.encoding != "" {
				resp.Header.Set("Content-Encoding", tc.encoding)
			}
			if tc.contentType != "" {
				resp.Header.Set("Content-Type", tc.contentType)
			}
			err := tp.shapeResponse(context.Background(), resp, tc.rpcMethod, tc.tool, []byte(tc.reqBody), tc.policy)
			if err != nil {
				t.Fatalf("shapeResponse: %v", err)
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !strings.Contains(string(data), tc.wantSub) {
				t.Errorf("got body %q, want substring %q", string(data), tc.wantSub)
			}
		})
	}
}
