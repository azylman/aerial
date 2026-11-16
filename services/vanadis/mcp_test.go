package main

import (
	"bytes"
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
	srv := NewVanadisServer(Config{Port: "0"}, memStore, nil)
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

// vanadisMCPToolCase is one tools/call invocation against the seeded MCP fixture store.
type vanadisMCPToolCase struct {
	name             string
	nilStore         bool
	actionGeneration *int
	listErr          error
	getErr           error
	args             string
	wantToolErr      bool
	wantSubstr       string
}

// newMCPToolsStore seeds two builds on thread 1234567890 (one running with a
// running and a pending chunk, one completed) and one completed build on thread
// 9876543210, then applies the injected list/get store errors.
func newMCPToolsStore(listErr, getErr error) *memoryVanadisStore {
	now := time.Now().UTC()
	b1 := mkBuild("build-mcp-1", "1234567890", "Build 1 Title", "Objective 1", "running")
	b1.SharedContext = "Context 1"
	b1.CreatedAt = now.Add(-10 * time.Minute)

	b2 := mkBuild("build-mcp-2", "1234567890", "Build 2 Title", "Objective 2", "completed")
	b2.CreatedAt = now.Add(-5 * time.Minute)

	b3 := mkBuild("build-mcp-3", "9876543210", "Build 3 Title", "Objective 3", "completed")
	b3.CreatedAt = now.Add(-1 * time.Minute)

	c1_0 := mkChunk("chunk-1", "build-mcp-1", 0, "Chunk 1", "Inst 1", "medium", "running")
	c1_0.Repo = normalizeRepo("aerial")
	c1_0.PRNumber = 101

	c1_1 := mkChunk("chunk-2", "build-mcp-1", 1, "Chunk 2", "Inst 2", "high", "pending")
	c1_1.Repo = normalizeRepo("aerial")
	c1_1.DependsOn = []string{"chunk-1"}

	store := newMemoryVanadisStore()
	_ = store.CreateBuild(context.Background(), b1, []VanadisChunk{c1_0, c1_1})
	_ = store.CreateBuild(context.Background(), b2, []VanadisChunk{mkChunk("chunk-2-0", "build-mcp-2", 0, "C2", "I2", "low", "completed")})
	_ = store.CreateBuild(context.Background(), b3, []VanadisChunk{mkChunk("chunk-3-0", "build-mcp-3", 0, "C3", "I3", "low", "completed")})
	store.listErr = listErr
	store.getErr = getErr
	return store
}

func handleMCPMockFailChunk(w http.ResponseWriter, r *http.Request, actionGen *int) {
	var failReq FailChunkRequest
	_ = json.NewDecoder(r.Body).Decode(&failReq)
	if failReq.ChunkID == "nonexistent-chunk" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"chunk not found"}`))
		return
	}
	failed := actionGen == nil || (failReq.DispatchID != "vanadis-chunk-1-turn-0" && failReq.DispatchID != "")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(FailChunkResponse{
		Failed:      failed,
		BuildID:     "build-mcp-1",
		ChunkID:     failReq.ChunkID,
		BuildStatus: "failed",
		ChunkStatus: "failed",
	})
}

func mockMCPRestateHandler(actionGen *int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		var bodyBytes []byte
		if r.Body != nil {
			bodyBytes, _ = io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
		bodyStr := string(bodyBytes)

		if strings.Contains(path, "nonexistent") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"chunk not found"}`))
			return
		}
		if strings.Contains(path, "build-mcp-2") || strings.Contains(bodyStr, `"chunk_ref":"0"`) {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"message":"vanadis build state conflict"}`))
			return
		}
		if strings.Contains(bodyStr, `"chunks":[]`) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"invalid vanadis build input: at least one chunk is required"}`))
			return
		}
		if strings.Contains(path, "RouteFailChunk") {
			handleMCPMockFailChunk(w, r, actionGen)
			return
		}
		if strings.Contains(path, "UpdateBuild") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(VanadisBuild{ID: "build-mcp-1", Status: "running"})
			return
		}
		if strings.Contains(path, "UpdateChunk") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(VanadisChunk{ID: "chunk-2", Status: "pending"})
			return
		}
		if strings.Contains(path, "AppendChunks") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]VanadisChunk{{ID: "chunk-3", Status: "pending"}})
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// callVanadisMCPTool sends one JSON-RPC tools/call request for toolName to a
// fresh MCP handler backed by the seeded fixture store (or no store) and
// returns the raw response body.
func callVanadisMCPTool(t *testing.T, tc vanadisMCPToolCase, toolName string) []byte {
	t.Helper()
	mem := newMCPToolsStore(tc.listErr, tc.getErr)
	var store VanadisStore = mem
	if tc.actionGeneration != nil {
		c := &mem.chunks["build-mcp-1"][0]
		c.Mode, c.PRNumber, c.StallNudges = "action", 0, *tc.actionGeneration
	}
	if tc.nilStore {
		store = nil
	}

	restateServer := httptest.NewServer(mockMCPRestateHandler(tc.actionGeneration))
	defer restateServer.Close()

	srv := NewVanadisServer(Config{Port: "0"}, store, restateServer.Client())
	if !tc.nilStore {
		srv.restateCfg.RestateIngressURL = restateServer.URL
	}
	handler := srv.newMCPHandler()

	reqBody := `{"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"` + toolName + `","arguments":` + tc.args + `}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec.Body.Bytes()
}

func TestVanadisMCP_GetBuild(t *testing.T) {
	t.Parallel()
	tests := []vanadisMCPToolCase{
		{
			name:        "get_build nil store",
			nilStore:    true,
			args:        `{"build_id":"build-mcp-1"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "get_build missing both ids",
			args:        `{}`,
			wantToolErr: true,
			wantSubstr:  "build_id or thread_id is required",
		},
		{
			name:       "get_build by build_id success",
			args:       `{"build_id":"build-mcp-1"}`,
			wantSubstr: `"title":"Build 1 Title"`,
		},
		{
			name:       "get_build by thread_id picks running",
			args:       `{"thread_id":"stateful_medium:discord-1234567890"}`,
			wantSubstr: `"id":"build-mcp-1"`,
		},
		{
			name:       "get_build by thread_id fallback to completed if no running",
			args:       `{"thread_id":"9876543210"}`,
			wantSubstr: `"id":"build-mcp-3"`,
		},
		{
			name:        "get_build by thread_id not found",
			args:        `{"thread_id":"5555555555"}`,
			wantToolErr: true,
			wantSubstr:  "no build found for thread",
		},
		{
			name:        "get_build list builds store error",
			listErr:     errors.New("simulated list failure"),
			args:        `{"thread_id":"1234567890"}`,
			wantToolErr: true,
			wantSubstr:  "simulated list failure",
		},
		{
			name:        "get_build get store error",
			getErr:      errors.New("simulated get failure"),
			args:        `{"build_id":"build-mcp-1"}`,
			wantToolErr: true,
			wantSubstr:  "simulated get failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertVanadisMCPToolResult(t, callVanadisMCPTool(t, tc, "get_build"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestVanadisMCP_GetChunk(t *testing.T) {
	t.Parallel()
	tests := []vanadisMCPToolCase{
		{
			name:        "get_chunk nil store",
			nilStore:    true,
			args:        `{"chunk_id":"chunk-1"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:       "get_chunk by chunk_id success",
			args:       `{"chunk_id":"chunk-1"}`,
			wantSubstr: `"chunk":{"acceptance_criteria":[]`,
		},
		{
			name:        "get_chunk by chunk_id not found",
			args:        `{"chunk_id":"nonexistent"}`,
			wantToolErr: true,
			wantSubstr:  "chunk not found",
		},
		{
			name:        "get_chunk by chunk_id store error",
			getErr:      errors.New("simulated get chunk failure"),
			args:        `{"chunk_id":"chunk-1"}`,
			wantToolErr: true,
			wantSubstr:  "simulated get chunk failure",
		},
		{
			name:        "get_chunk missing args",
			args:        `{}`,
			wantToolErr: true,
			wantSubstr:  "chunk_id or both build_id and chunk_ref are required",
		},
		{
			name:        "get_chunk missing chunk_ref when build_id provided",
			args:        `{"build_id":"build-mcp-1"}`,
			wantToolErr: true,
			wantSubstr:  "chunk_id or both build_id and chunk_ref are required",
		},
		{
			name:       "get_chunk by build_id and chunk_ref string match success",
			args:       `{"build_id":"build-mcp-1","chunk_ref":"chunk-2"}`,
			wantSubstr: `"title":"Chunk 2"`,
		},
		{
			name:       "get_chunk by build_id and chunk_ref index match success",
			args:       `{"build_id":"build-mcp-1","chunk_ref":"0"}`,
			wantSubstr: `"title":"Chunk 1"`,
		},
		{
			name:        "get_chunk by build_id and chunk_ref not found",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"99"}`,
			wantToolErr: true,
			wantSubstr:  "chunk ref 99 not found",
		},
		{
			name:        "get_chunk by build_id get build error",
			getErr:      errors.New("simulated get build failure"),
			args:        `{"build_id":"build-mcp-1","chunk_ref":"0"}`,
			wantToolErr: true,
			wantSubstr:  "simulated get build failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertVanadisMCPToolResult(t, callVanadisMCPTool(t, tc, "get_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestVanadisMCP_UpdateBuild(t *testing.T) {
	t.Parallel()
	tests := []vanadisMCPToolCase{
		{
			name:        "update_build nil store",
			nilStore:    true,
			args:        `{"build_id":"build-mcp-1","title":"New Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "update_build missing build_id",
			args:        `{"build_id":"   ","title":"New Title"}`,
			wantToolErr: true,
			wantSubstr:  "build_id is required",
		},
		{
			name:       "update_build success with title and widget sync",
			args:       `{"build_id":"build-mcp-1","title":"Updated MCP Title"}`,
			wantSubstr: `"status":"updated"`,
		},
		{
			name:        "update_build conflict status",
			args:        `{"build_id":"build-mcp-2","title":"Conflict Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis build state conflict",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertVanadisMCPToolResult(t, callVanadisMCPTool(t, tc, "update_build"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestVanadisMCP_UpdateChunk(t *testing.T) {
	t.Parallel()
	tests := []vanadisMCPToolCase{
		{
			name:        "update_chunk nil store",
			nilStore:    true,
			args:        `{"build_id":"build-mcp-1","chunk_ref":"1","title":"New Chunk Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "update_chunk missing build_id or chunk_ref",
			args:        `{"build_id":"build-mcp-1","chunk_ref":""}`,
			wantToolErr: true,
			wantSubstr:  "build_id and chunk_ref are required",
		},
		{
			name:        "update_chunk running chunk conflict",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"0","title":"New Chunk Title"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis build state conflict",
		},
		{
			name:       "update_chunk pending chunk success",
			args:       `{"build_id":"build-mcp-1","chunk_ref":"1","title":"Updated Chunk 2 Title","effort":"high"}`,
			wantSubstr: `"status":"updated"`,
		},
		{
			name:       "update_chunk mode update success",
			args:       `{"build_id":"build-mcp-1","chunk_ref":"1","mode":"action","title":"Switched to Action","instructions":"Run action"}`,
			wantSubstr: `"status":"updated"`,
		},
		{
			name:       "update_chunk empty mode treated as unset",
			args:       `{"build_id":"build-mcp-1","chunk_ref":"1","mode":"","title":"Updated Chunk 2 Title Again"}`,
			wantSubstr: `"status":"updated"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertVanadisMCPToolResult(t, callVanadisMCPTool(t, tc, "update_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestVanadisMCP_AppendChunk(t *testing.T) {
	t.Parallel()
	tests := []vanadisMCPToolCase{
		{
			name:        "append_chunk nil store",
			nilStore:    true,
			args:        `{"build_id":"build-mcp-1","chunks":[{"title":"C3","instructions":"I3","depends_on":["chunk-2"]}]}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "append_chunk missing build_id",
			args:        `{"build_id":"","chunks":[{"title":"C3","instructions":"I3","depends_on":["chunk-2"]}]}`,
			wantToolErr: true,
			wantSubstr:  "build_id is required",
		},
		{
			name:        "append_chunk validation failure",
			args:        `{"build_id":"build-mcp-1","chunks":[]}`,
			wantToolErr: true,
			wantSubstr:  "invalid vanadis build input",
		},
		{
			name:       "append_chunk success",
			args:       `{"build_id":"build-mcp-1","chunks":[{"repo":"aerial","title":"Part 3","instructions":"Do part 3","effort":"medium","depends_on":["chunk-2"]}]}`,
			wantSubstr: `"status":"appended"`,
		},
		{
			name:       "append_chunk action mode success",
			args:       `{"build_id":"build-mcp-1","chunks":[{"mode":"action","title":"Action Step","instructions":"Run migration","effort":"low","depends_on":["chunk-2"]}]}`,
			wantSubstr: `"status":"appended"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertVanadisMCPToolResult(t, callVanadisMCPTool(t, tc, "append_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestVanadisMCP_RemoveChunk(t *testing.T) {
	t.Parallel()
	tests := []vanadisMCPToolCase{
		{
			name:        "remove_chunk nil store",
			nilStore:    true,
			args:        `{"build_id":"build-mcp-1","chunk_ref":"1"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "remove_chunk missing args",
			args:        `{"build_id":"build-mcp-1","chunk_ref":""}`,
			wantToolErr: true,
			wantSubstr:  "build_id and chunk_ref are required",
		},
		{
			name:        "remove_chunk running chunk conflict",
			args:        `{"build_id":"build-mcp-1","chunk_ref":"0"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis build state conflict",
		},
		{
			name:       "remove_chunk pending chunk success",
			args:       `{"build_id":"build-mcp-1","chunk_ref":"1"}`,
			wantSubstr: `"status":"removed"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertVanadisMCPToolResult(t, callVanadisMCPTool(t, tc, "remove_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestVanadisMCP_FailChunk(t *testing.T) {
	t.Parallel()
	retryGeneration := 1
	tests := []vanadisMCPToolCase{
		{name: "action retry current identity", actionGeneration: &retryGeneration, args: `{"chunk_id":"chunk-1","reason":"failure","dispatch_id":"vanadis-chunk-1-turn-1"}`, wantSubstr: `"failed":true`},
		{name: "action retry missing identity", actionGeneration: &retryGeneration, args: `{"chunk_id":"chunk-1","reason":"failure"}`, wantSubstr: `"failed":false`},
		{name: "action retry stale identity", actionGeneration: &retryGeneration, args: `{"chunk_id":"chunk-1","reason":"failure","dispatch_id":"vanadis-chunk-1-turn-0"}`, wantSubstr: `"failed":false`},

		{
			name:        "fail_chunk nil store",
			nilStore:    true,
			args:        `{"chunk_id":"chunk-1","reason":"unrecoverable failure"}`,
			wantToolErr: true,
			wantSubstr:  "vanadis store not configured",
		},
		{
			name:        "fail_chunk missing chunk_id",
			args:        `{"chunk_id":"","reason":"unrecoverable failure"}`,
			wantToolErr: true,
			wantSubstr:  "chunk_id and reason are required",
		},
		{
			name:        "fail_chunk missing reason",
			args:        `{"chunk_id":"chunk-1","reason":""}`,
			wantToolErr: true,
			wantSubstr:  "chunk_id and reason are required",
		},
		{
			name:        "fail_chunk chunk not found",
			args:        `{"chunk_id":"nonexistent-chunk","reason":"unrecoverable failure"}`,
			wantToolErr: true,
			wantSubstr:  "chunk not found",
		},
		{
			name:       "fail_chunk success",
			args:       `{"chunk_id":"chunk-1","reason":"unrecoverable action error"}`,
			wantSubstr: `"status":"failed"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertVanadisMCPToolResult(t, callVanadisMCPTool(t, tc, "fail_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func assertVanadisMCPToolResult(t *testing.T, respBytes []byte, wantToolErr bool, wantSubstr string) {
	t.Helper()
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
	var toolResult struct {
		IsError bool `json:"isError"`
	}
	_ = json.Unmarshal(rpcResp.Result, &toolResult)

	if wantToolErr {
		isErr := toolResult.IsError || rpcResp.Error != nil
		if !isErr {
			t.Fatalf("expected tool or RPC error, got success response: %s", string(respBytes))
		}
		if wantSubstr != "" && !strings.Contains(string(respBytes), wantSubstr) {
			t.Fatalf("response %s does not contain expected error substring %q", string(respBytes), wantSubstr)
		}
		return
	}

	if rpcResp.Error != nil {
		t.Fatalf("unexpected RPC error: %+v", rpcResp.Error)
	}
	if toolResult.IsError {
		t.Fatalf("unexpected tool error in result: %s", string(rpcResp.Result))
	}
	if wantSubstr != "" && !strings.Contains(string(respBytes), wantSubstr) {
		t.Fatalf("response %s does not contain expected substring %q", string(respBytes), wantSubstr)
	}
}

func TestToMCPChunkBriefs(t *testing.T) {
	t.Parallel()

	chunks := []VanadisChunk{
		{ID: "c1", DependsOn: nil},
	}
	got := toMCPChunkBriefs(chunks)
	if len(got) != 1 || got[0].ID != "c1" || got[0].DependsOn == nil {
		t.Fatalf("expected non-nil DependsOn, got %+v", got)
	}
	b, err := json.Marshal(got[0])
	if err != nil {
		t.Fatalf("failed to marshal brief: %v", err)
	}
	if !strings.Contains(string(b), `"depends_on":[]`) {
		t.Fatalf("expected \"depends_on\":[] in JSON, got %s", string(b))
	}
}
