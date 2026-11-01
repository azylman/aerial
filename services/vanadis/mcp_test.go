package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestVanadisMCP_HTTPTransport(t *testing.T) {
	memStore := newMemoryVanadisStore()
	srv := NewVanadisServer(Config{Port: "0"}, memStore, nil, nil)
	mux := srv.Routes()

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "get ready banner on /mcp",
			method:     http.MethodGet,
			path:       "/mcp",
			wantStatus: http.StatusOK,
			wantSubstr: "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n",
		},
		{
			name:       "get ready banner on /sse",
			method:     http.MethodGet,
			path:       "/sse",
			wantStatus: http.StatusOK,
			wantSubstr: "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n",
		},
		{
			name:       "method not allowed on delete",
			method:     http.MethodDelete,
			path:       "/mcp",
			wantStatus: http.StatusMethodNotAllowed,
			wantSubstr: "Method not allowed",
		},
		{
			name:       "empty body rejected",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       "   ",
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Empty request body",
		},
		{
			name:       "initialize rpc",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"protocolVersion"`,
		},
		{
			name:       "tools list rpc",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
			wantStatus: http.StatusOK,
			wantSubstr: "get_build",
		},
		{
			name:       "tools list rpc required fields",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"required":["build_id"]`,
		},
		{
			name:       "call tool missing required build_id returns schema error",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"update_build","arguments":{"title":"New Title"}}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `missing properties: [\"build_id\"]`,
		},
		{
			name:       "vanadis_ prefix normalization on tools call",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"vanadis_get_build","arguments":{"build_id":"nonexistent"}}}`,
			wantStatus: http.StatusOK,
			wantSubstr: "isError",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			}
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, bodyReader)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			if tc.method == http.MethodPost {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			bodyBytes, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, tc.wantStatus, string(bodyBytes))
			}
			if tc.wantSubstr != "" && !strings.Contains(string(bodyBytes), tc.wantSubstr) {
				t.Fatalf("body %q does not contain expected substring %q", string(bodyBytes), tc.wantSubstr)
			}
		})
	}
}

func TestVanadisMCP_Tools(t *testing.T) {
	now := time.Now().UTC()
	b1 := mkBuild("build-mcp-1", "1234567890", "Build 1 Title", "Objective 1", "running")
	b1.SharedContext = "Context 1"
	b1.CreatedAt = now.Add(-10 * time.Minute)

	b2 := mkBuild("build-mcp-2", "1234567890", "Build 2 Title", "Objective 2", "completed")
	b2.CreatedAt = now.Add(-5 * time.Minute)

	b3 := mkBuild("build-mcp-3", "9876543210", "Build 3 Title", "Objective 3", "completed")
	b3.CreatedAt = now.Add(-1 * time.Minute)

	c1_0 := mkChunk("chunk-1", "build-mcp-1", 0, "Chunk 1", "Inst 1", "medium", "running")
	c1_0.Repo = "azylman/aerial"
	c1_0.PRNumber = 101

	c1_1 := mkChunk("chunk-2", "build-mcp-1", 1, "Chunk 2", "Inst 2", "high", "pending")
	c1_1.Repo = "azylman/aerial"

	setupStore := func() *memoryVanadisStore {
		store := newMemoryVanadisStore()
		_ = store.CreateBuild(context.Background(), b1, []VanadisChunk{c1_0, c1_1})
		_ = store.CreateBuild(context.Background(), b2, []VanadisChunk{mkChunk("chunk-2-0", "build-mcp-2", 0, "C2", "I2", "low", "completed")})
		_ = store.CreateBuild(context.Background(), b3, []VanadisChunk{mkChunk("chunk-3-0", "build-mcp-3", 0, "C3", "I3", "low", "completed")})
		return store
	}

	tests := []struct {
		name        string
		nilStore    bool
		listErr     error
		getErr      error
		toolName    string
		args        string
		wantToolErr bool
		wantSubstr  string
	}{
		// get_build
		{
			name:        "get_build nil store",
			nilStore:    true,
			toolName:    "get_build",
			args:        `{"build_id":"build-mcp-1"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "get_build missing both ids",
			toolName:    "get_build",
			args:        `{}`,
			wantToolErr: true,
			wantSubstr:  "build_id or thread_id is required",
		},
		{
			name:        "get_build by build_id success",
			toolName:    "get_build",
			args:        `{"build_id":"build-mcp-1"}`,
			wantToolErr: false,
			wantSubstr:  `"title":"Build 1 Title"`,
		},
		{
			name:        "get_build by thread_id picks running",
			toolName:    "get_build",
			args:        `{"thread_id":"stateful_medium:discord-1234567890"}`,
			wantToolErr: false,
			wantSubstr:  `"id":"build-mcp-1"`,
		},
		{
			name:        "get_build by thread_id fallback to completed if no running",
			toolName:    "get_build",
			args:        `{"thread_id":"9876543210"}`,
			wantToolErr: false,
			wantSubstr:  `"id":"build-mcp-3"`,
		},
		{
			name:        "get_build by thread_id not found",
			toolName:    "get_build",
			args:        `{"thread_id":"5555555555"}`,
			wantToolErr: true,
			wantSubstr:  "no build found for thread",
		},
		{
			name:        "get_build list builds store error",
			listErr:     errors.New("simulated list failure"),
			toolName:    "get_build",
			args:        `{"thread_id":"1234567890"}`,
			wantToolErr: true,
			wantSubstr:  "simulated list failure",
		},
		{
			name:        "get_build get store error",
			getErr:      errors.New("simulated get failure"),
			toolName:    "get_build",
			args:        `{"build_id":"build-mcp-1"}`,
			wantToolErr: true,
			wantSubstr:  "simulated get failure",
		},

		// get_chunk
		{
			name:        "get_chunk nil store",
			nilStore:    true,
			toolName:    "get_chunk",
			args:        `{"chunk_id":"chunk-1"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "get_chunk by chunk_id success",
			toolName:    "get_chunk",
			args:        `{"chunk_id":"chunk-1"}`,
			wantToolErr: false,
			wantSubstr:  `"chunk":{"acceptance_criteria":[]`,
		},
		{
			name:        "get_chunk by chunk_id not found",
			toolName:    "get_chunk",
			args:        `{"chunk_id":"nonexistent"}`,
			wantToolErr: true,
			wantSubstr:  "chunk not found",
		},
		{
			name:        "get_chunk by chunk_id store error",
			getErr:      errors.New("simulated get chunk failure"),
			toolName:    "get_chunk",
			args:        `{"chunk_id":"chunk-1"}`,
			wantToolErr: true,
			wantSubstr:  "simulated get chunk failure",
		},
		{
			name:        "get_chunk missing args",
			toolName:    "get_chunk",
			args:        `{}`,
			wantToolErr: true,
			wantSubstr:  "chunk_id or both build_id and chunk_ref are required",
		},
		{
			name:        "get_chunk missing chunk_ref when build_id provided",
			toolName:    "get_chunk",
			args:        `{"build_id":"build-mcp-1"}`,
			wantToolErr: true,
			wantSubstr:  "chunk_id or both build_id and chunk_ref are required",
		},
		{
			name:        "get_chunk by build_id and chunk_ref string match success",
			toolName:    "get_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"chunk-2"}`,
			wantToolErr: false,
			wantSubstr:  `"title":"Chunk 2"`,
		},
		{
			name:        "get_chunk by build_id and chunk_ref index match success",
			toolName:    "get_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"0"}`,
			wantToolErr: false,
			wantSubstr:  `"title":"Chunk 1"`,
		},
		{
			name:        "get_chunk by build_id and chunk_ref not found",
			toolName:    "get_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"99"}`,
			wantToolErr: true,
			wantSubstr:  "chunk ref 99 not found",
		},
		{
			name:        "get_chunk by build_id get build error",
			getErr:      errors.New("simulated get build failure"),
			toolName:    "get_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"0"}`,
			wantToolErr: true,
			wantSubstr:  "simulated get build failure",
		},

		// update_build
		{
			name:        "update_build nil store",
			nilStore:    true,
			toolName:    "update_build",
			args:        `{"build_id":"build-mcp-1","title":"New Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "update_build missing build_id",
			toolName:    "update_build",
			args:        `{"build_id":"   ","title":"New Title"}`,
			wantToolErr: true,
			wantSubstr:  "build_id is required",
		},
		{
			name:        "update_build success with title and widget sync",
			toolName:    "update_build",
			args:        `{"build_id":"build-mcp-1","title":"Updated MCP Title"}`,
			wantToolErr: false,
			wantSubstr:  `"status":"updated"`,
		},
		{
			name:        "update_build conflict status",
			toolName:    "update_build",
			args:        `{"build_id":"build-mcp-2","title":"Conflict Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis build state conflict",
		},

		// update_chunk
		{
			name:        "update_chunk nil store",
			nilStore:    true,
			toolName:    "update_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"1","title":"New Chunk Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "update_chunk missing build_id or chunk_ref",
			toolName:    "update_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":""}`,
			wantToolErr: true,
			wantSubstr:  "build_id and chunk_ref are required",
		},
		{
			name:        "update_chunk running chunk conflict",
			toolName:    "update_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"0","title":"New Chunk Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis build state conflict",
		},
		{
			name:        "update_chunk pending chunk success",
			toolName:    "update_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"1","title":"Updated Chunk 2 Title","effort":"high"}`,
			wantToolErr: false,
			wantSubstr:  `"status":"updated"`,
		},

		// append_chunk
		{
			name:        "append_chunk nil store",
			nilStore:    true,
			toolName:    "append_chunk",
			args:        `{"build_id":"build-mcp-1","chunks":[{"title":"C3","instructions":"I3"}]}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "append_chunk missing build_id",
			toolName:    "append_chunk",
			args:        `{"build_id":"","chunks":[{"title":"C3","instructions":"I3"}]}`,
			wantToolErr: true,
			wantSubstr:  "build_id is required",
		},
		{
			name:        "append_chunk validation failure",
			toolName:    "append_chunk",
			args:        `{"build_id":"build-mcp-1","chunks":[]}`,
			wantToolErr: true,
			wantSubstr:  "invalid vanadis build input",
		},
		{
			name:        "append_chunk success",
			toolName:    "append_chunk",
			args:        `{"build_id":"build-mcp-1","chunks":[{"title":"Part 3","instructions":"Do part 3","effort":"medium"}]}`,
			wantToolErr: false,
			wantSubstr:  `"status":"appended"`,
		},

		// remove_chunk
		{
			name:        "remove_chunk nil store",
			nilStore:    true,
			toolName:    "remove_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"1"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "remove_chunk missing args",
			toolName:    "remove_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":""}`,
			wantToolErr: true,
			wantSubstr:  "build_id and chunk_ref are required",
		},
		{
			name:        "remove_chunk running chunk conflict",
			toolName:    "remove_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"0"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis build state conflict",
		},
		{
			name:        "remove_chunk pending chunk success",
			toolName:    "remove_chunk",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"1"}`,
			wantToolErr: false,
			wantSubstr:  `"status":"removed"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := setupStore()
			if tc.listErr != nil {
				store.listErr = tc.listErr
			}
			if tc.getErr != nil {
				store.getErr = tc.getErr
			}
			var srvStore VanadisStore = store
			if tc.nilStore {
				srvStore = nil
			}

			var widgetSyncedBuildID string
			syncWidgetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req map[string]string
				_ = json.NewDecoder(r.Body).Decode(&req)
				widgetSyncedBuildID = req["build_id"]
				w.WriteHeader(http.StatusOK)
			}))
			defer syncWidgetSrv.Close()

			srv := NewVanadisServer(Config{Port: "0", SynapseURL: syncWidgetSrv.URL}, srvStore, nil, syncWidgetSrv.Client())
			handler := srv.newMCPHandler()

			reqBody := `{"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"` + tc.toolName + `","arguments":` + tc.args + `}}`
			req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(reqBody))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			handler(rec, req)

			respBytes := rec.Body.Bytes()
			var rpcResp struct {
				ID     int             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(respBytes, &rpcResp); err != nil {
				t.Fatalf("failed to decode JSON-RPC response: %v, raw: %s", err, string(respBytes))
			}

			if tc.wantToolErr {
				var toolResult struct {
					IsError bool `json:"isError"`
				}
				_ = json.Unmarshal(rpcResp.Result, &toolResult)
				isErr := toolResult.IsError || rpcResp.Error != nil
				if !isErr {
					t.Fatalf("expected tool or RPC error, got success response: %s", string(respBytes))
				}
				if tc.wantSubstr != "" && !strings.Contains(string(respBytes), tc.wantSubstr) {
					t.Fatalf("response %s does not contain expected error substring %q", string(respBytes), tc.wantSubstr)
				}
			} else {
				if rpcResp.Error != nil {
					t.Fatalf("unexpected RPC error: %+v", rpcResp.Error)
				}
				var toolResult struct {
					IsError bool `json:"isError"`
				}
				_ = json.Unmarshal(rpcResp.Result, &toolResult)
				if toolResult.IsError {
					t.Fatalf("unexpected tool error in result: %s", string(rpcResp.Result))
				}
				if tc.wantSubstr != "" && !strings.Contains(string(respBytes), tc.wantSubstr) {
					t.Fatalf("response %s does not contain expected substring %q", string(respBytes), tc.wantSubstr)
				}
			}
			_ = widgetSyncedBuildID
		})
	}
}
