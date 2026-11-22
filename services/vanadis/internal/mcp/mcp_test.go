package mcp

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/services/vanadis/internal/config"
	"github.com/azylman/aerial/services/vanadis/internal/model"
	"github.com/azylman/aerial/services/vanadis/internal/restate"
	"github.com/azylman/aerial/services/vanadis/internal/store"
	"github.com/azylman/aerial/services/vanadis/internal/storetest"
)

func newTestHandler(st store.VanadisStore, ingressURL string, enabled bool) *Handler {
	return NewHandler(st, restate.NewClient(config.RestateConfig{RestateIngressURL: ingressURL}, nil), enabled)
}

func TestMCP_HTTPTransport(t *testing.T) {
	t.Parallel()
	handler := newTestHandler(storetest.New(), "", true).NewHTTPHandler()
	ts := httptest.NewServer(handler)
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
			name:       "get ready banner on post path",
			method:     http.MethodGet,
			path:       "/",
			wantStatus: http.StatusOK,
			wantSubstr: "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n",
		},
		{
			name:       "method not allowed on delete",
			method:     http.MethodDelete,
			path:       "/",
			wantStatus: http.StatusMethodNotAllowed,
			wantSubstr: "Method not allowed",
		},
		{
			name:       "empty body rejected",
			method:     http.MethodPost,
			path:       "/",
			body:       "   ",
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Empty request body",
		},
		{
			name:       "initialize rpc",
			method:     http.MethodPost,
			path:       "/",
			body:       `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"protocolVersion"`,
		},
		{
			name:       "tools list rpc",
			method:     http.MethodPost,
			path:       "/",
			body:       `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
			wantStatus: http.StatusOK,
			wantSubstr: "get_build",
		},
		{
			name:       "tools list rpc required fields",
			method:     http.MethodPost,
			path:       "/",
			body:       `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"required":["build_id"]`,
		},
		{
			name:       "call tool missing required build_id returns schema error",
			method:     http.MethodPost,
			path:       "/",
			body:       `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"update_build","arguments":{"title":"New Title"}}}`,
			wantStatus: http.StatusOK,
			wantSubstr: `missing properties: [\"build_id\"]`,
		},
		{
			name:       "vanadis_ prefix normalization on tools call",
			method:     http.MethodPost,
			path:       "/",
			body:       `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"vanadis_get_build","arguments":{"build_id":"nonexistent"}}}`,
			wantStatus: http.StatusOK,
			wantSubstr: "isError",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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

func TestServerInstructions(t *testing.T) {
	t.Parallel()
	handler := newTestHandler(storetest.New(), "", true).NewHTTPHandler()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	tests := []struct {
		name     string
		wantTool string
	}{
		{"initialize advertises workflow instructions", "fail_chunk"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := mcp.NewClient(&mcp.Implementation{Name: "instructions-test", Version: "1.0.0"}, nil)
			session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
				Endpoint:             ts.URL,
				HTTPClient:           ts.Client(),
				DisableStandaloneSSE: true,
			}, nil)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer session.Close()

			got := session.InitializeResult().Instructions
			if got == "" || len(got) > 1024 {
				t.Fatalf("instructions length = %d, want 1..1024", len(got))
			}
			if !strings.Contains(got, tc.wantTool) {
				t.Errorf("instructions missing tool name %q: %q", tc.wantTool, got)
			}
		})
	}
}

// mcpToolCase is one tools/call invocation against the seeded fixture store.
type mcpToolCase struct {
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
func newMCPToolsStore(listErr, getErr error) *storetest.Store {
	now := time.Now().UTC()
	b1 := storetest.Build("build-mcp-1", "1234567890", "Build 1 Title", "Objective 1", "running")
	b1.SharedContext = "Context 1"
	b1.CreatedAt = now.Add(-10 * time.Minute)

	b2 := storetest.Build("build-mcp-2", "1234567890", "Build 2 Title", "Objective 2", "completed")
	b2.CreatedAt = now.Add(-5 * time.Minute)

	b3 := storetest.Build("build-mcp-3", "9876543210", "Build 3 Title", "Objective 3", "completed")
	b3.CreatedAt = now.Add(-1 * time.Minute)

	c1_0 := storetest.Chunk("chunk-1", "build-mcp-1", 0, "Chunk 1", "Inst 1", "medium", "running")
	c1_0.PRNumber = 101

	c1_1 := storetest.Chunk("chunk-2", "build-mcp-1", 1, "Chunk 2", "Inst 2", "high", "pending")
	c1_1.DependsOn = []string{"chunk-1"}

	st := storetest.New()
	st.Builds["build-mcp-1"], st.Chunks["build-mcp-1"] = b1, []model.VanadisChunk{c1_0, c1_1}
	st.Builds["build-mcp-2"], st.Chunks["build-mcp-2"] = b2, []model.VanadisChunk{storetest.Chunk("chunk-2-0", "build-mcp-2", 0, "C2", "I2", "low", "completed")}
	st.Builds["build-mcp-3"], st.Chunks["build-mcp-3"] = b3, []model.VanadisChunk{storetest.Chunk("chunk-3-0", "build-mcp-3", 0, "C3", "I3", "low", "completed")}
	b4 := storetest.Build("build-mcp-4", "4444444444", "Build 4 Title", "Objective 4", "running")
	b4.Agent = "agent-alpha"
	b4.CreatedAt = now.Add(-20 * time.Minute)
	st.Builds["build-mcp-4"], st.Chunks["build-mcp-4"] = b4, []model.VanadisChunk{
		storetest.Chunk("chunk-4-0", "build-mcp-4", 0, "C4", "I4", "low", "running"),
		storetest.Chunk("chunk-4-1", "build-mcp-4", 1, "C4b", "I4b", "low", "pending"),
	}
	st.ListErr = listErr
	st.GetErr = getErr
	return st
}

func handleMCPMockFailChunk(w http.ResponseWriter, r *http.Request, actionGen *int) {
	var failReq failChunkPayload
	_ = json.NewDecoder(r.Body).Decode(&failReq)
	if failReq.ChunkID == "nonexistent-chunk" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"chunk not found"}`))
		return
	}
	failed := actionGen == nil || (failReq.DispatchID != "vanadis-chunk-1-turn-0" && failReq.DispatchID != "")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(failChunkResponse{
		Failed:      failed,
		BuildID:     "build-mcp-1",
		ChunkID:     failReq.ChunkID,
		BuildStatus: "failed",
		ChunkStatus: "failed",
	})
}

func handleMCPMockCompleteChunk(w http.ResponseWriter, r *http.Request, body []byte) {
	var req completeChunkPayload
	_ = json.Unmarshal(body, &req)
	if req.DispatchID == "stale-dispatch" {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":"vanadis build state conflict"}`))
		return
	}
	if r.Header.Get("Idempotency-Key") != "complete:"+req.DispatchID {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"bad idempotency key"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(completeChunkResponse{Status: "completed", BuildID: req.BuildID, ChunkID: req.ChunkID, Completed: true})
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

		// Build object calls must reach the canonical Build object.
		if !strings.HasPrefix(path, "/BuildRouter/") {
			want := "/Build/"
			if !strings.HasPrefix(path, want) {
				w.WriteHeader(http.StatusTeapot)
				_, _ = w.Write([]byte(`{"message":"misrouted build object"}`))
				return
			}
		}

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
		if strings.Contains(path, "CompleteChunk") {
			handleMCPMockCompleteChunk(w, r, bodyBytes)
			return
		}
		if strings.Contains(path, "UpdateBuild") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(model.VanadisBuild{ID: "build-mcp-1", Status: "running"})
			return
		}
		if strings.Contains(path, "UpdateChunk") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(model.VanadisChunk{ID: "chunk-2", Status: "pending"})
			return
		}
		if strings.Contains(path, "AppendChunks") {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]model.VanadisChunk{{ID: "chunk-3", Status: "pending"}})
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// callMCPTool sends one JSON-RPC tools/call request for toolName to a
// fresh MCP handler backed by the seeded fixture store (or no store) and
// returns the raw response body.
func callMCPTool(t *testing.T, tc mcpToolCase, toolName string) []byte {
	t.Helper()
	mem := newMCPToolsStore(tc.listErr, tc.getErr)
	var st store.VanadisStore = mem
	if tc.actionGeneration != nil {
		c := &mem.Chunks["build-mcp-1"][0]
		c.Mode, c.PRNumber, c.StallNudges = "action", 0, *tc.actionGeneration
	}
	if tc.nilStore {
		st = nil
	}

	restateServer := httptest.NewServer(mockMCPRestateHandler(tc.actionGeneration))
	defer restateServer.Close()

	ingressURL := ""
	if !tc.nilStore {
		ingressURL = restateServer.URL
	}
	h := NewHandler(st, restate.NewClient(config.RestateConfig{RestateIngressURL: ingressURL}, restateServer.Client()), true)
	handler := h.NewHTTPHandler()

	reqBody := `{"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"` + toolName + `","arguments":` + tc.args + `}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec.Body.Bytes()
}

func TestMCP_GetBuild(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
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
			assertMCPToolResult(t, callMCPTool(t, tc, "get_build"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestMCP_GetChunk(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
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
			assertMCPToolResult(t, callMCPTool(t, tc, "get_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestMCP_UpdateBuild(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
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
			name:       "update_build routes to the build runner",
			args:       `{"build_id":"build-mcp-4","title":"Updated Runner Title"}`,
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
			assertMCPToolResult(t, callMCPTool(t, tc, "update_build"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestMCP_UpdateChunk(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
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
		{
			name:       "update_chunk routes to the build runner",
			args:       `{"build_id":"build-mcp-4","chunk_ref":"1","title":"Runner Chunk"}`,
			wantSubstr: `"status":"updated"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertMCPToolResult(t, callMCPTool(t, tc, "update_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestMCP_AppendChunk(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
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
		{
			name:       "append_chunk routes to the build runner",
			args:       `{"build_id":"build-mcp-4","chunks":[{"repo":"aerial","title":"Runner Part","instructions":"Do it","effort":"low","depends_on":[]}]}`,
			wantSubstr: `"status":"appended"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertMCPToolResult(t, callMCPTool(t, tc, "append_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestMCP_RemoveChunk(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
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
		{
			name:       "remove_chunk routes to the build runner",
			args:       `{"build_id":"build-mcp-4","chunk_ref":"1"}`,
			wantSubstr: `"removed_chunk_id":"chunk-4-1"`,
		},
		{
			name:        "remove_chunk store read error refuses to guess the runner",
			getErr:      errors.New("simulated get failure"),
			args:        `{"build_id":"build-mcp-1","chunk_ref":"1"}`,
			wantToolErr: true,
			wantSubstr:  "simulated get failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertMCPToolResult(t, callMCPTool(t, tc, "remove_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestMCP_FailChunk(t *testing.T) {
	t.Parallel()
	retryGeneration := 1
	tests := []mcpToolCase{
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
			assertMCPToolResult(t, callMCPTool(t, tc, "fail_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func TestMCP_CompleteChunk(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
		{name: "nil store", nilStore: true, args: `{"chunk_id":"chunk-1","dispatch_id":"d1","summary":"ok"}`, wantToolErr: true, wantSubstr: "vanadis store not configured"},
		{name: "missing chunk_id", args: `{"chunk_id":"","dispatch_id":"d1","summary":"ok"}`, wantToolErr: true, wantSubstr: "chunk_id, dispatch_id and summary are required"},
		{name: "missing dispatch_id", args: `{"chunk_id":"chunk-1","dispatch_id":"","summary":"ok"}`, wantToolErr: true, wantSubstr: "chunk_id, dispatch_id and summary are required"},
		{name: "missing summary", args: `{"chunk_id":"chunk-1","dispatch_id":"d1","summary":" "}`, wantToolErr: true, wantSubstr: "chunk_id, dispatch_id and summary are required"},
		{name: "chunk not found", args: `{"chunk_id":"nonexistent-chunk","dispatch_id":"d1","summary":"ok"}`, wantToolErr: true, wantSubstr: "chunk not found"},
		{name: "stale dispatch conflict", args: `{"chunk_id":"chunk-1","dispatch_id":"stale-dispatch","summary":"ok"}`, wantToolErr: true, wantSubstr: "conflict"},
		{name: "success with idempotency key", args: `{"chunk_id":"chunk-1","dispatch_id":"vanadis-chunk-1-turn-0","summary":"all green"}`, wantSubstr: `"completed":true`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertMCPToolResult(t, callMCPTool(t, tc, "complete_chunk"), tc.wantToolErr, tc.wantSubstr)
		})
	}
}

func assertMCPToolResult(t *testing.T, respBytes []byte, wantToolErr bool, wantSubstr string) {
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
	tests := []struct {
		name        string
		chunks      []model.VanadisChunk
		wantLen     int
		wantDepends []string
		wantJSON    string
	}{
		{name: "nil projects to empty briefs", chunks: nil, wantLen: 0, wantDepends: nil, wantJSON: `[]`},
		{name: "nil depends_on normalizes to empty", chunks: []model.VanadisChunk{{ID: "c1"}}, wantLen: 1, wantDepends: []string{}, wantJSON: `"depends_on":[]`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ToMCPChunkBriefs(tc.chunks)
			if len(got) != tc.wantLen {
				t.Fatalf("len = %d, want %d", len(got), tc.wantLen)
			}
			b, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("failed to marshal briefs: %v", err)
			}
			if !strings.Contains(string(b), tc.wantJSON) {
				t.Fatalf("expected %q in JSON, got %s", tc.wantJSON, string(b))
			}
			if tc.wantLen > 0 && (got[0].DependsOn == nil) != (tc.wantDepends == nil) {
				t.Fatalf("DependsOn nil = %v, want nil = %v", got[0].DependsOn == nil, tc.wantDepends == nil)
			}
		})
	}
}

func TestStoreErrorStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "timeout maps to gateway timeout", err: restate.ErrRestateTimeout, wantStatus: http.StatusGatewayTimeout},
		{name: "invalid input maps to bad request", err: model.ErrInvalidBuildInput, wantStatus: http.StatusBadRequest},
		{name: "build not found maps to not found", err: model.ErrBuildNotFound, wantStatus: http.StatusNotFound},
		{name: "chunk not found maps to not found", err: model.ErrChunkNotFound, wantStatus: http.StatusNotFound},
		{name: "conflict maps to conflict", err: model.ErrBuildConflict, wantStatus: http.StatusConflict},
		{name: "unknown maps to internal error", err: errors.New("boom"), wantStatus: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := storeErrorStatus(tc.err); got != tc.wantStatus {
				t.Fatalf("status = %d, want %d", got, tc.wantStatus)
			}
			rec := httptest.NewRecorder()
			writeStoreError(context.Background(), rec, tc.err)
			if rec.Code != tc.wantStatus {
				t.Fatalf("http status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}

type retryIngressFixture struct {
	status    int
	body      string
	request   RetryActionChunkRequest
	key, path string
}

func (f *retryIngressFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.key = r.Header.Get("Idempotency-Key")
	f.path = r.URL.Path
	_ = json.NewDecoder(r.Body).Decode(&f.request)
	status := f.status
	if status == 0 {
		status = 500
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(f.body))
}

func TestRetryActionIntake(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body                string
		ingressStatus, wantStatus int
		ingressBody               string
		buildStatus               string
		generation                int
		prNumber                  int
		wantDispatch              string
		noIngress                 bool
		disabled                  bool
	}{
		{name: "missing replacement", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry","expected_instructions":"old"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 400},
		{name: "missing expected", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry","replacement_instructions":"new"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 400},
		{name: "blank expected", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry","expected_instructions":" ","replacement_instructions":"new"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 400},
		{name: "blank replacement", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry","expected_instructions":"old","replacement_instructions":" "}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 400},
		{name: "instruction mismatch", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry","expected_instructions":"old","replacement_instructions":"new"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 409},
		{name: "changed correction replay", generation: 1, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry","expected_instructions":"different","replacement_instructions":"ignored"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "correction", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry","expected_instructions":"verify","replacement_instructions":"new"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "accepted", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "completed replay", buildStatus: "completed", generation: 1, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "failed replay", buildStatus: "failed", generation: 1, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "cancelled replay", buildStatus: "cancelled", generation: 1, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "older replay", buildStatus: "failed", generation: 2, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "attached PR retained acceptance", buildStatus: "failed", generation: 1, prNumber: 123, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`, wantStatus: 200},
		{name: "attached PR new retry accepted", buildStatus: "failed", generation: 1, prNumber: 123, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-1","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-2","accepted":true}`, wantStatus: 200, wantDispatch: "vanadis-c1-turn-2"},
		{name: "missing ingress", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, noIngress: true, wantStatus: 503},
		{name: "disabled ingress", disabled: true, body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, noIngress: false, wantStatus: 503},
		{name: "missing identity", body: `{"build_id":"b1","chunk_id":"c1","reason":"retry"}`, wantStatus: 400},
		{name: "wrong build", body: `{"build_id":"b2","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, wantStatus: 400},
		{name: "invalid json", body: `{`, wantStatus: 400},
		{name: "stale", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 409, wantStatus: 409},
		{name: "transient", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 503, wantStatus: 500},
		{name: "invalid acknowledgement", body: `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, ingressStatus: 200, ingressBody: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"","accepted":false}`, wantStatus: 500},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &retryIngressFixture{status: tc.ingressStatus, body: tc.ingressBody}
			ts := httptest.NewServer(f)
			defer ts.Close()
			status := tc.buildStatus
			if status == "" {
				status = "failed"
			}
			var rc *restate.Client
			if !tc.noIngress {
				rc = restate.NewClient(config.RestateConfig{RestateIngressURL: ts.URL}, ts.Client())
			}
			h := &Handler{Store: &retryReadFixture{status: status, generation: tc.generation, prNumber: tc.prNumber}, Restate: rc, RestateEnabled: !tc.disabled}
			r := httptest.NewRequest(http.MethodPost, "/builds/b1/retry-action", strings.NewReader(tc.body))
			r.SetPathValue("id", "b1")
			w := httptest.NewRecorder()
			h.HandleRetryActionChunk(w, r)
			if w.Code != tc.wantStatus {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if (tc.wantStatus == 400 || tc.name == "instruction mismatch") && f.key != "" {
				t.Fatal("rejected request consumed durable key")
			}
			if tc.wantStatus != 200 {
				return
			}
			if f.path != "/Build/b1/RetryActionChunk" || f.key == "" {
				t.Fatalf("durable path=%s key=%s", f.path, f.key)
			}
			var sent RetryActionChunkRequest
			_ = json.Unmarshal([]byte(tc.body), &sent)
			if !reflect.DeepEqual(f.request, sent) {
				t.Fatal("correction not forwarded")
			}
			var result RetryActionChunkResult
			wantDispatch := cmp.Or(tc.wantDispatch, "vanadis-c1-turn-1")
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || !result.Accepted || result.DispatchID != wantDispatch {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestRetryActionIdempotencyKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		req   RetryActionChunkRequest
		equal bool
	}{
		{"different correction", RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", ExpectedInstructions: new("before"), ReplacementInstructions: new("after")}, true},
		{"different reason", RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "different"}, true},
		{"different build", RetryActionChunkRequest{BuildID: "b2", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0"}, false},
		{"different chunk", RetryActionChunkRequest{BuildID: "b1", ChunkID: "c2", ExpectedDispatchID: "vanadis-c1-turn-0"}, false},
		{"different generation", RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-1"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := retryActionIdempotencyKey(RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"})
			if (a == retryActionIdempotencyKey(tc.req)) != tc.equal {
				t.Fatal("idempotency identity mismatch")
			}
		})
	}
}

func TestRetryActionHTTPAndMCP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		direct     bool
		body, want string
	}{
		{"http", true, `{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}`, `"dispatch_id":"vanadis-c1-turn-1"`},
		{"mcp", false, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"retry_action_chunk","arguments":{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}}}`, `"accepted":true`},
		{"mcp invalid", false, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"retry_action_chunk","arguments":{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"","reason":"retry"}}}`, `"isError":true`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &retryIngressFixture{status: 200, body: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`}
			ts := httptest.NewServer(f)
			defer ts.Close()
			h := &Handler{Store: &retryReadFixture{status: "failed"}, Restate: restate.NewClient(config.RestateConfig{RestateIngressURL: ts.URL}, ts.Client()), RestateEnabled: true}
			var rec *httptest.ResponseRecorder
			if tc.direct {
				r := httptest.NewRequest(http.MethodPost, "/builds/b1/retry-action", strings.NewReader(tc.body))
				r.SetPathValue("id", "b1")
				rec = httptest.NewRecorder()
				h.HandleRetryActionChunk(rec, r)
			} else {
				r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(tc.body))
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("Accept", "application/json, text/event-stream")
				rec = httptest.NewRecorder()
				h.NewHTTPHandler()(rec, r)
			}
			if rec.Code != 200 || !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

type retryReadFixture struct {
	store.VanadisStore

	status     string
	generation int
	prNumber   int
}

func (f *retryReadFixture) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: "b1", Status: f.status}, []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: f.status, StallNudges: f.generation, PRNumber: f.prNumber, Instructions: "verify"}}, nil
}

func TestRetryPrematureRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		initial string
	}{
		{name: "running chunk becomes eligible on failure", initial: "running"},
		{name: "pending chunk becomes eligible on failure", initial: "pending"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &retryIngressFixture{status: 200, body: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`}
			ts := httptest.NewServer(f)
			defer ts.Close()
			st := &retryReadFixture{status: tc.initial}
			h := &Handler{Store: st, Restate: restate.NewClient(config.RestateConfig{RestateIngressURL: ts.URL}, ts.Client()), RestateEnabled: true}
			req := RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"}
			if _, err := h.RetryActionChunk(context.Background(), req); !errors.Is(err, model.ErrBuildConflict) {
				t.Fatalf("premature error=%v", err)
			}
			if f.key != "" {
				t.Fatal("premature request consumed durable key")
			}
			st.status = "failed"
			got, err := h.RetryActionChunk(context.Background(), req)
			if err != nil || !got.Accepted {
				t.Fatalf("eligible retry=%+v error=%v", got, err)
			}
		})
	}
}

type retryEligibilityFixture struct {
	store.VanadisStore

	build  *model.VanadisBuild
	chunks []model.VanadisChunk
	err    error
}

func (f retryEligibilityFixture) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return f.build, f.chunks, f.err
}

func TestRetryEligibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, buildStatus, chunkStatus, identity string
		generation                               int
		sibling                                  *model.VanadisChunk
		wantErr                                  bool
	}{
		{name: "uncertain accepted replay", buildStatus: "running", chunkStatus: "running", identity: "vanadis-c1-turn-0", generation: 1},
		{name: "negative identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c1-turn--1", generation: 1, wantErr: true},
		{name: "noncanonical identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c1-turn-00", generation: 1, wantErr: true},
		{name: "foreign identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c2-turn-0", generation: 1, wantErr: true},
		{name: "future identity", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-1", wantErr: true},
		{name: "retired generation fence", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-2147483647", generation: 2147483647, wantErr: true},
		{name: "cancelled", buildStatus: "cancelled", chunkStatus: "failed", identity: "vanadis-c1-turn-0", generation: 1},
		{name: "other failure accepted", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &model.VanadisChunk{ID: "c2", Status: "failed"}},
		{name: "running sibling accepted", buildStatus: "running", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &model.VanadisChunk{ID: "c2", Status: "running"}},
		{name: "running descendant rejected", buildStatus: "running", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &model.VanadisChunk{ID: "c2", Status: "running", DependsOn: []string{"c1"}}, wantErr: true},
		{name: "completed build rejected", buildStatus: "completed", chunkStatus: "failed", identity: "vanadis-c1-turn-0", wantErr: true},
		{name: "completed descendant", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &model.VanadisChunk{ID: "c2", Status: "completed", DependsOn: []string{"c1"}}, wantErr: true},
		{name: "pending descendant", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-0", sibling: &model.VanadisChunk{ID: "c2", Status: "pending", DependsOn: []string{"c1"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: tc.chunkStatus, StallNudges: tc.generation}}
			if tc.sibling != nil {
				chunks = append(chunks, *tc.sibling)
			}
			h := &Handler{Store: retryEligibilityFixture{build: &model.VanadisBuild{ID: "b1", Status: tc.buildStatus}, chunks: chunks}}
			err := h.ValidateRetryEligibility(context.Background(), RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: tc.identity})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

type eligibilityEdgeCase struct {
	name      string
	nilStore  bool
	storeErr  error
	build     *model.VanadisBuild
	chunks    []model.VanadisChunk
	wantErr   bool
	wantErrIs error
	wantSub   string
}

func TestValidateRetryEligibilityEdges(t *testing.T) {
	t.Parallel()
	tests := []eligibilityEdgeCase{
		{name: "nil store errors", nilStore: true, wantErr: true, wantSub: "vanadis store not configured"},
		{name: "store error propagates", storeErr: errors.New("db down"), wantErr: true, wantSub: "db down"},
		{name: "nil build maps to not found", build: nil, wantErrIs: model.ErrBuildNotFound},
		{name: "missing chunk maps to not found", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, chunks: []model.VanadisChunk{{ID: "c9", BuildID: "b1", Mode: "action", Status: "failed"}}, wantErrIs: model.ErrChunkNotFound},
		{name: "non-action mode conflicts", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed"}}, wantErrIs: model.ErrBuildConflict},
		{name: "unready dependency conflicts", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, {ID: "c0", BuildID: "b1", Mode: "action", Status: "pending"}}, wantErrIs: model.ErrBuildConflict},
		{name: "ready dependency passes", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", DependsOn: []string{"c0"}}, {ID: "c0", BuildID: "b1", Mode: "action", Status: "completed"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var h *Handler
			if !tc.nilStore {
				h = &Handler{Store: retryEligibilityFixture{build: tc.build, chunks: tc.chunks, err: tc.storeErr}}
			} else {
				h = &Handler{}
			}
			err := h.ValidateRetryEligibility(context.Background(), RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: "vanadis-c1-turn-0", Reason: "retry"})
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want %v", err, tc.wantErrIs)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
					t.Errorf("err %q does not contain %q", err.Error(), tc.wantSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
