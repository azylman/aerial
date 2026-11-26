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

	"github.com/azylman/aerial/pkg/k8sauth"
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

	const initialize = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}`
	tests := []struct {
		name, method, body string
		wantStatus         int
		wantSubstr         string
	}{
		{"ready banner on get", http.MethodGet, "", http.StatusOK, "Aerial Vanadis MCP Server (Streamable HTTP) Ready\n"},
		{"initialize advertises workflow instructions", http.MethodPost, initialize, http.StatusOK, "fail_chunk"},
		{"tools list", http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`, http.StatusOK, "get_build"},
		{"tools list required fields", http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`, http.StatusOK, `"required":["build_id"]`},
		{"missing required build_id schema error", http.MethodPost, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"update_build","arguments":{"title":"New Title"}}}`, http.StatusOK, `missing properties: [\"build_id\"]`},
		{"vanadis_ prefix normalization", http.MethodPost, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"vanadis_get_build","arguments":{"build_id":"nonexistent"}}}`, http.StatusOK, "isError"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := http.NewRequest(tc.method, ts.URL+"/", strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, tc.wantStatus, string(bodyBytes))
			}
			if !strings.Contains(string(bodyBytes), tc.wantSubstr) {
				t.Fatalf("body %q does not contain expected substring %q", string(bodyBytes), tc.wantSubstr)
			}
			// Any response that carries server instructions must keep them
			// within the 1..1024 characters clients reliably accept.
			var rpc struct {
				Result struct {
					Instructions *string `json:"instructions"`
				} `json:"result"`
			}
			if json.Unmarshal(bodyBytes, &rpc) == nil && rpc.Result.Instructions != nil {
				if n := len(*rpc.Result.Instructions); n == 0 || n > 1024 {
					t.Fatalf("instructions length = %d, want 1..1024", n)
				}
			}
		})
	}
}

// mcpToolFixture selects how the seeded fixture store is altered for one row.
// actionGeneration > 0 turns chunk-1 into an action chunk at that generation.
type mcpToolFixture struct {
	nilStore         bool
	actionGeneration int
	listErr, getErr  error
}

var (
	fxSeeded      = mcpToolFixture{}
	fxNilStore    = mcpToolFixture{nilStore: true}
	fxListErr     = mcpToolFixture{listErr: errors.New("simulated list failure")}
	fxGetErr      = mcpToolFixture{getErr: errors.New("simulated get failure")}
	fxActionRetry = mcpToolFixture{actionGeneration: 1}
)

// mcpToolCase is one tools/call invocation against the seeded fixture store.
type mcpToolCase struct {
	tool, name, args string
	wantToolErr      bool
	wantSubstr       string
	fx               mcpToolFixture
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

func handleMCPMockFailChunk(w http.ResponseWriter, r *http.Request, action bool) {
	var failReq failChunkPayload
	if err := json.NewDecoder(r.Body).Decode(&failReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if failReq.ChunkID == "nonexistent-chunk" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"chunk not found"}`))
		return
	}
	failed := !action || (failReq.DispatchID != "vanadis-chunk-1-turn-0" && failReq.DispatchID != "")
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
	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
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

func handleMCPMockCreateBuild(w http.ResponseWriter, body []byte) {
	var createReq model.CreateBuildRequest
	if err := json.Unmarshal(body, &createReq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(model.VanadisBuild{
		ID:       createReq.ID,
		ThreadID: createReq.ThreadID,
		Tier:     createReq.Tier,
		Status:   "running",
	}); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func handleMCPMockCancelBuild(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(model.VanadisBuild{ID: "build-mcp-1", Status: "cancelled"}); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func mockMCPCheckPreconditions(w http.ResponseWriter, path, bodyStr string) bool {
	if !strings.HasPrefix(path, "/BuildRouter/") && !strings.HasPrefix(path, "/Build/") {
		w.WriteHeader(http.StatusTeapot)
		if _, err := w.Write([]byte(`{"message":"misrouted build object"}`)); err != nil {
			return false
		}
		return false
	}
	if strings.Contains(path, "nonexistent") {
		w.WriteHeader(http.StatusNotFound)
		if _, err := w.Write([]byte(`{"message":"chunk not found"}`)); err != nil {
			return false
		}
		return false
	}
	if strings.Contains(path, "build-mcp-2") || (!strings.Contains(path, "SteerChunk") && strings.Contains(bodyStr, `"chunk_ref":"0"`)) {
		status := http.StatusConflict
		msg := `{"message":"vanadis build state conflict"}`
		if strings.Contains(path, "SteerChunk") {
			status = http.StatusPreconditionFailed
			msg = `{"message":"chunk cannot be steered in current status"}`
		}
		w.WriteHeader(status)
		if _, err := w.Write([]byte(msg)); err != nil {
			return false
		}
		return false
	}
	if strings.Contains(bodyStr, `"chunks":[]`) {
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte(`{"message":"invalid vanadis build input: at least one chunk is required"}`)); err != nil {
			return false
		}
		return false
	}
	return true
}

func mockMCPRestateHandler(action bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		var bodyBytes []byte
		if r.Body != nil {
			var err error
			bodyBytes, err = io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
		if !mockMCPCheckPreconditions(w, path, string(bodyBytes)) {
			return
		}

		switch {
		case strings.Contains(path, "RouteFailChunk"):
			handleMCPMockFailChunk(w, r, action)
		case strings.Contains(path, "CompleteChunk"):
			handleMCPMockCompleteChunk(w, r, bodyBytes)
		case strings.Contains(path, "Create"):
			handleMCPMockCreateBuild(w, bodyBytes)
		case strings.Contains(path, "Cancel"):
			handleMCPMockCancelBuild(w)
		case strings.Contains(path, "UpdateBuild"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(model.VanadisBuild{ID: "build-mcp-1", Status: "running"})
		case strings.Contains(path, "UpdateChunk"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(model.VanadisChunk{ID: "chunk-2", Status: "pending"})
		case strings.Contains(path, "AppendChunks"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]model.VanadisChunk{{ID: "chunk-3", Status: "pending"}})
		case strings.Contains(path, "SteerChunk"):
			var steerReq model.SteerChunkRequest
			if err := json.Unmarshal(bodyBytes, &steerReq); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(model.SteerChunkResult{
				Accepted:     true,
				BuildID:      steerReq.BuildID,
				ChunkID:      steerReq.ChunkRef,
				ChunkStatus:  "running",
				Instructions: "steered instructions",
			})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}
}

// callMCPTool sends one JSON-RPC tools/call request for tc.tool to a fresh MCP
// handler backed by the seeded fixture store (or no store) and returns the raw
// response body. A non-nil auth enables RBAC; a non-nil caller is attached to
// the request context the way the server's auth middleware does.
func callMCPTool(t *testing.T, tc mcpToolCase, auth *k8sauth.Authenticator, caller *k8sauth.CallerIdentity) []byte {
	t.Helper()
	mem := newMCPToolsStore(tc.fx.listErr, tc.fx.getErr)
	var st store.VanadisStore = mem
	if tc.fx.actionGeneration > 0 {
		c := &mem.Chunks["build-mcp-1"][0]
		c.Mode, c.PRNumber, c.StallNudges = "action", 0, tc.fx.actionGeneration
	}
	if tc.fx.nilStore {
		st = nil
	}

	restateServer := httptest.NewServer(mockMCPRestateHandler(tc.fx.actionGeneration > 0))
	defer restateServer.Close()

	ingressURL := ""
	if !tc.fx.nilStore {
		ingressURL = restateServer.URL
	}
	h := NewHandler(st, restate.NewClient(config.RestateConfig{RestateIngressURL: ingressURL}, restateServer.Client()), true, auth)
	handler := h.NewHTTPHandler()

	reqBody := `{"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":"` + tc.tool + `","arguments":` + tc.args + `}}`
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(reqBody))
	if caller != nil {
		req = req.WithContext(k8sauth.WithCallerIdentity(req.Context(), *caller))
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec.Body.Bytes()
}

// TestMCP_Tools exercises every tool's argument validation, store wiring and
// Restate routing through the JSON-RPC path.
func TestMCP_Tools(t *testing.T) {
	t.Parallel()
	tests := []mcpToolCase{
		{"get_build", "nil store", `{"build_id":"build-mcp-1"}`, true, "vanadis store not configured", fxNilStore},
		{"get_build", "missing both ids", `{}`, true, "build_id or thread_id is required", fxSeeded},
		{"get_build", "by build_id", `{"build_id":"build-mcp-1"}`, false, `"title":"Build 1 Title"`, fxSeeded},
		{"get_build", "by thread_id picks running", `{"thread_id":"stateful_medium:discord-1234567890"}`, false, `"id":"build-mcp-1"`, fxSeeded},
		{"get_build", "by thread_id falls back to completed", `{"thread_id":"9876543210"}`, false, `"id":"build-mcp-3"`, fxSeeded},
		{"get_build", "by thread_id not found", `{"thread_id":"5555555555"}`, true, "no build found for thread", fxSeeded},
		{"get_build", "list store error", `{"thread_id":"1234567890"}`, true, "simulated list failure", fxListErr},
		{"get_build", "get store error", `{"build_id":"build-mcp-1"}`, true, "simulated get failure", fxGetErr},

		{"get_chunk", "nil store", `{"chunk_id":"chunk-1"}`, true, "vanadis store not configured", fxNilStore},
		{"get_chunk", "by chunk_id", `{"chunk_id":"chunk-1"}`, false, `"chunk":{"acceptance_criteria":[]`, fxSeeded},
		{"get_chunk", "by chunk_id not found", `{"chunk_id":"nonexistent"}`, true, "chunk not found", fxSeeded},
		{"get_chunk", "by chunk_id store error", `{"chunk_id":"chunk-1"}`, true, "simulated get failure", fxGetErr},
		{"get_chunk", "missing args", `{}`, true, "chunk_id or both build_id and chunk_ref are required", fxSeeded},
		{"get_chunk", "build_id without chunk_ref", `{"build_id":"build-mcp-1"}`, true, "chunk_id or both build_id and chunk_ref are required", fxSeeded},
		{"get_chunk", "chunk_ref id match", `{"build_id":"build-mcp-1","chunk_ref":"chunk-2"}`, false, `"title":"Chunk 2"`, fxSeeded},
		{"get_chunk", "chunk_ref index match", `{"build_id":"build-mcp-1","chunk_ref":"0"}`, false, `"title":"Chunk 1"`, fxSeeded},
		{"get_chunk", "chunk_ref not found", `{"build_id":"build-mcp-1","chunk_ref":"99"}`, true, "chunk ref 99 not found", fxSeeded},
		{"get_chunk", "chunk_ref get build error", `{"build_id":"build-mcp-1","chunk_ref":"0"}`, true, "simulated get failure", fxGetErr},

		{"update_build", "nil store", `{"build_id":"build-mcp-1","title":"New Title"}`, true, "vanadis store not configured", fxNilStore},
		{"update_build", "blank build_id", `{"build_id":"   ","title":"New Title"}`, true, "build_id is required", fxSeeded},
		{"update_build", "title update", `{"build_id":"build-mcp-1","title":"Updated MCP Title"}`, false, `"status":"updated"`, fxSeeded},
		{"update_build", "routes to the build runner", `{"build_id":"build-mcp-4","title":"Updated Runner Title"}`, false, `"status":"updated"`, fxSeeded},
		{"update_build", "conflict status", `{"build_id":"build-mcp-2","title":"Conflict Title"}`, true, "vanadis build state conflict", fxSeeded},

		{"update_chunk", "nil store", `{"build_id":"build-mcp-1","chunk_ref":"1","title":"New Chunk Title"}`, true, "vanadis store not configured", fxNilStore},
		{"update_chunk", "empty chunk_ref", `{"build_id":"build-mcp-1","chunk_ref":""}`, true, "build_id and chunk_ref are required", fxSeeded},
		{"update_chunk", "running chunk conflict", `{"build_id":"build-mcp-1","chunk_ref":"0","title":"New"}`, true, "vanadis build state conflict", fxSeeded},
		{"update_chunk", "pending chunk", `{"build_id":"build-mcp-1","chunk_ref":"1","title":"Updated","effort":"high"}`, false, `"status":"updated"`, fxSeeded},
		{"update_chunk", "mode update", `{"build_id":"build-mcp-1","chunk_ref":"1","mode":"action","instructions":"Run action"}`, false, `"status":"updated"`, fxSeeded},
		{"update_chunk", "empty mode treated as unset", `{"build_id":"build-mcp-1","chunk_ref":"1","mode":"","title":"Again"}`, false, `"status":"updated"`, fxSeeded},
		{"update_chunk", "routes to the build runner", `{"build_id":"build-mcp-4","chunk_ref":"1","title":"Runner Chunk"}`, false, `"status":"updated"`, fxSeeded},

		{"append_chunk", "nil store", `{"build_id":"build-mcp-1","chunks":[{"title":"C3","instructions":"I3","depends_on":[]}]}`, true, "vanadis store not configured", fxNilStore},
		{"append_chunk", "empty build_id", `{"build_id":"","chunks":[{"title":"C3","instructions":"I3","depends_on":[]}]}`, true, "build_id is required", fxSeeded},
		{"append_chunk", "validation failure", `{"build_id":"build-mcp-1","chunks":[]}`, true, "invalid vanadis build input", fxSeeded},
		{"append_chunk", "pr chunk", `{"build_id":"build-mcp-1","chunks":[{"repo":"aerial","title":"P3","instructions":"Do","effort":"medium","depends_on":["chunk-2"]}]}`, false, `"status":"appended"`, fxSeeded},
		{"append_chunk", "action chunk", `{"build_id":"build-mcp-1","chunks":[{"mode":"action","title":"Act","instructions":"Run","depends_on":["chunk-2"]}]}`, false, `"status":"appended"`, fxSeeded},
		{"append_chunk", "routes to the build runner", `{"build_id":"build-mcp-4","chunks":[{"repo":"aerial","title":"Runner Part","instructions":"Do it","depends_on":[]}]}`, false, `"status":"appended"`, fxSeeded},

		{"remove_chunk", "nil store", `{"build_id":"build-mcp-1","chunk_ref":"1"}`, true, "vanadis store not configured", fxNilStore},
		{"remove_chunk", "empty chunk_ref", `{"build_id":"build-mcp-1","chunk_ref":""}`, true, "build_id and chunk_ref are required", fxSeeded},
		{"remove_chunk", "running chunk conflict", `{"build_id":"build-mcp-1","chunk_ref":"0"}`, true, "vanadis build state conflict", fxSeeded},
		{"remove_chunk", "pending chunk", `{"build_id":"build-mcp-1","chunk_ref":"1"}`, false, `"status":"removed"`, fxSeeded},
		{"remove_chunk", "routes to the build runner", `{"build_id":"build-mcp-4","chunk_ref":"1"}`, false, `"removed_chunk_id":"chunk-4-1"`, fxSeeded},
		{"remove_chunk", "store read error refuses to guess the runner", `{"build_id":"build-mcp-1","chunk_ref":"1"}`, true, "simulated get failure", fxGetErr},

		{"fail_chunk", "action retry current identity", `{"chunk_id":"chunk-1","reason":"failure","dispatch_id":"vanadis-chunk-1-turn-1"}`, false, `"failed":true`, fxActionRetry},
		{"fail_chunk", "action retry missing identity", `{"chunk_id":"chunk-1","reason":"failure"}`, false, `"failed":false`, fxActionRetry},
		{"fail_chunk", "action retry stale identity", `{"chunk_id":"chunk-1","reason":"failure","dispatch_id":"vanadis-chunk-1-turn-0"}`, false, `"failed":false`, fxActionRetry},
		{"fail_chunk", "nil store", `{"chunk_id":"chunk-1","reason":"unrecoverable failure"}`, true, "vanadis store not configured", fxNilStore},
		{"fail_chunk", "missing chunk_id", `{"chunk_id":"","reason":"unrecoverable failure"}`, true, "chunk_id and reason are required", fxSeeded},
		{"fail_chunk", "missing reason", `{"chunk_id":"chunk-1","reason":""}`, true, "chunk_id and reason are required", fxSeeded},
		{"fail_chunk", "chunk not found", `{"chunk_id":"nonexistent-chunk","reason":"unrecoverable failure"}`, true, "chunk not found", fxSeeded},
		{"fail_chunk", "fails the chunk", `{"chunk_id":"chunk-1","reason":"unrecoverable action error"}`, false, `"status":"failed"`, fxSeeded},

		{"complete_chunk", "nil store", `{"chunk_id":"chunk-1","dispatch_id":"d1","summary":"ok"}`, true, "vanadis store not configured", fxNilStore},
		{"complete_chunk", "missing chunk_id", `{"chunk_id":"","dispatch_id":"d1","summary":"ok"}`, true, "chunk_id, dispatch_id and summary are required", fxSeeded},
		{"complete_chunk", "missing dispatch_id", `{"chunk_id":"chunk-1","dispatch_id":"","summary":"ok"}`, true, "chunk_id, dispatch_id and summary are required", fxSeeded},
		{"complete_chunk", "blank summary", `{"chunk_id":"chunk-1","dispatch_id":"d1","summary":" "}`, true, "chunk_id, dispatch_id and summary are required", fxSeeded},
		{"complete_chunk", "chunk not found", `{"chunk_id":"nonexistent-chunk","dispatch_id":"d1","summary":"ok"}`, true, "chunk not found", fxSeeded},
		{"complete_chunk", "stale dispatch conflict", `{"chunk_id":"chunk-1","dispatch_id":"stale-dispatch","summary":"ok"}`, true, "conflict", fxSeeded},
		{"complete_chunk", "idempotency key", `{"chunk_id":"chunk-1","dispatch_id":"vanadis-chunk-1-turn-0","summary":"all green"}`, false, `"completed":true`, fxSeeded},

		{"retry_action_chunk", "nil store", `{"build_id":"build-mcp-1","chunk_id":"chunk-1","expected_dispatch_id":"d1","reason":"retry"}`, true, "vanadis store not configured", fxNilStore},

		{"create_build", "nil store", `{"thread_id":"1234567890","objective":"test obj","chunks":[{"title":"c1","instructions":"i1","depends_on":[]}]}`, true, "vanadis store not configured", fxNilStore},
		{"create_build", "invalid thread_id", `{"thread_id":"not-a-snowflake","objective":"test obj","chunks":[{"title":"c1","instructions":"i1","depends_on":[]}]}`, true, "invalid vanadis build input", fxSeeded},
		{"create_build", "empty objective", `{"thread_id":"1234567890","objective":"","chunks":[{"title":"c1","instructions":"i1","depends_on":[]}]}`, true, "invalid vanadis build input", fxSeeded},
		{"create_build", "empty chunks", `{"thread_id":"1234567890","objective":"test obj","chunks":[]}`, true, "invalid vanadis build input", fxSeeded},
		{"create_build", "invalid chunk instructions", `{"thread_id":"1234567890","objective":"test obj","chunks":[{"title":"c1","instructions":"","depends_on":[]}]}`, true, "instructions are required", fxSeeded},
		{"create_build", "valid create", `{"thread_id":"1234567890","objective":"test obj","chunks":[{"repo":"aerial","title":"c1","instructions":"i1","depends_on":[]}]}`, false, `"status":"accepted"`, fxSeeded},
		{"create_build", "custom id and tier", `{"id":"build-custom-1","thread_id":"1234567890","tier":3,"objective":"test obj","chunks":[{"repo":"aerial","title":"c1","instructions":"i1","depends_on":[]}]}`, false, `"build_id":"build-custom-1"`, fxSeeded},

		{"cancel_build", "nil store", `{"build_id":"build-mcp-1"}`, true, "vanadis store not configured", fxNilStore},
		{"cancel_build", "empty build_id", `{"build_id":""}`, true, "build_id is required", fxSeeded},
		{"cancel_build", "not found", `{"build_id":"nonexistent"}`, true, "build not found", fxSeeded},
		{"cancel_build", "conflict status", `{"build_id":"build-mcp-2"}`, true, "vanadis build state conflict", fxSeeded},
		{"cancel_build", "valid cancel", `{"build_id":"build-mcp-1"}`, false, `"status":"cancelled"`, fxSeeded},

		{"steer_chunk", "nil store", `{"build_id":"build-mcp-1","chunk_ref":"chunk-1","prompt":"steer"}`, true, "vanadis store not configured", fxNilStore},
		{"steer_chunk", "missing prompt schema error", `{}`, true, `missing properties: [\"prompt\"]`, fxSeeded},
		{"steer_chunk", "missing chunk ref", `{"prompt":"steer"}`, true, "chunk_id or chunk_ref is required", fxSeeded},
		{"steer_chunk", "empty prompt", `{"build_id":"build-mcp-1","chunk_ref":"chunk-1","prompt":""}`, true, "invalid vanadis build input", fxSeeded},
		{"steer_chunk", "running chunk steer", `{"build_id":"build-mcp-1","chunk_ref":"0","prompt":"steer running"}`, false, `"chunk_status":"running"`, fxSeeded},
		{"steer_chunk", "conflict status", `{"build_id":"build-mcp-2","chunk_ref":"0","prompt":"steer"}`, true, "vanadis build state conflict", fxSeeded},
		{"steer_chunk", "by thread_id", `{"thread_id":"stateful_medium:discord-1234567890","chunk_ref":"chunk-1","prompt":"steer thread"}`, false, `"status":"steered"`, fxSeeded},
		{"steer_chunk", "by chunk_id only", `{"chunk_id":"chunk-1","prompt":"steer chunk id"}`, false, `"build_id":"build-mcp-1"`, fxSeeded},
		{"steer_chunk", "chunk not found", `{"chunk_id":"nonexistent-chunk","prompt":"steer"}`, true, "chunk not found", fxSeeded},
	}
	for _, tc := range tests {
		t.Run(tc.tool+" "+tc.name, func(t *testing.T) {
			t.Parallel()
			assertMCPToolResult(t, callMCPTool(t, tc, nil, nil), tc.wantToolErr, tc.wantSubstr)
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
	if len(rpcResp.Result) > 0 {
		if err := json.Unmarshal(rpcResp.Result, &toolResult); err != nil {
			t.Fatalf("failed to decode tool result: %v, raw: %s", err, string(rpcResp.Result))
		}
	}

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

type retryIngressFixture struct {
	status    int
	body      string
	request   RetryActionChunkRequest
	key, path string
}

func (f *retryIngressFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.key = r.Header.Get("Idempotency-Key")
	f.path = r.URL.Path
	if err := json.NewDecoder(r.Body).Decode(&f.request); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	status := f.status
	if status == 0 {
		status = 500
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(f.body))
}

func mustUnmarshal(t *testing.T, data []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
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
			status := cmp.Or(tc.buildStatus, "failed")
			var rc *restate.Client
			if !tc.noIngress {
				rc = restate.NewClient(config.RestateConfig{RestateIngressURL: ts.URL}, ts.Client())
			}
			h := &Handler{Store: &retryReadFixture{status: status, generation: tc.generation, prNumber: tc.prNumber}, Restate: rc, RestateEnabled: !tc.disabled}
			var req RetryActionChunkRequest
			unmarshalErr := json.Unmarshal([]byte(tc.body), &req)
			var res *RetryActionChunkResult
			var err error
			if unmarshalErr != nil {
				err = unmarshalErr
			} else {
				res, err = h.RetryActionChunk(context.Background(), req)
			}
			if (err != nil) != (tc.wantStatus != 200) {
				t.Fatalf("err=%v, wantStatus=%d", err, tc.wantStatus)
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
			mustUnmarshal(t, []byte(tc.body), &sent)
			if !reflect.DeepEqual(f.request, sent) {
				t.Fatal("correction not forwarded")
			}
			wantDispatch := cmp.Or(tc.wantDispatch, "vanadis-c1-turn-1")
			if res == nil || !res.Accepted || res.DispatchID != wantDispatch {
				t.Fatalf("result=%+v err=%v", res, err)
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

func TestRetryActionMCP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body, want string
	}{
		{"mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"retry_action_chunk","arguments":{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"vanadis-c1-turn-0","reason":"retry"}}}`, `"accepted":true`},
		{"mcp invalid", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"retry_action_chunk","arguments":{"build_id":"b1","chunk_id":"c1","expected_dispatch_id":"","reason":"retry"}}}`, `"isError":true`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := &retryIngressFixture{status: 200, body: `{"build_id":"b1","chunk_id":"c1","dispatch_id":"vanadis-c1-turn-1","accepted":true}`}
			ts := httptest.NewServer(f)
			defer ts.Close()
			h := &Handler{Store: &retryReadFixture{status: "failed"}, Restate: restate.NewClient(config.RestateConfig{RestateIngressURL: ts.URL}, ts.Client()), RestateEnabled: true}
			r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "application/json, text/event-stream")
			rec := httptest.NewRecorder()
			h.NewHTTPHandler()(rec, r)
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

// TestRetryEligibility seeds build b1 (absent when noBuild) holding the target
// chunk (ID chunkID, default c1; mode default action) plus an optional sibling,
// and validates a retry of c1 at identity (default vanadis-c1-turn-0).
func TestRetryEligibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, buildStatus, chunkStatus, identity string
		chunkID, mode                            string
		generation                               int
		dependsOn                                []string
		sibling                                  *model.VanadisChunk
		nilStore, noBuild                        bool
		storeErr                                 error
		wantErr                                  bool
		wantErrIs                                error
		wantSub                                  string
	}{
		{name: "uncertain accepted replay", buildStatus: "running", chunkStatus: "running", identity: "vanadis-c1-turn-0", generation: 1},
		{name: "negative identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c1-turn--1", generation: 1, wantErr: true},
		{name: "noncanonical identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c1-turn-00", generation: 1, wantErr: true},
		{name: "foreign identity", buildStatus: "completed", chunkStatus: "completed", identity: "vanadis-c2-turn-0", generation: 1, wantErr: true},
		{name: "future identity", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-1", wantErr: true},
		{name: "retired generation fence", buildStatus: "failed", chunkStatus: "failed", identity: "vanadis-c1-turn-2147483647", generation: 2147483647, wantErr: true},
		{name: "cancelled", buildStatus: "cancelled", chunkStatus: "failed", identity: "vanadis-c1-turn-0", generation: 1},
		{name: "other failure accepted", buildStatus: "failed", chunkStatus: "failed", sibling: &model.VanadisChunk{ID: "c2", Status: "failed"}},
		{name: "running sibling accepted", buildStatus: "running", chunkStatus: "failed", sibling: &model.VanadisChunk{ID: "c2", Status: "running"}},
		{name: "running descendant rejected", buildStatus: "running", chunkStatus: "failed", sibling: &model.VanadisChunk{ID: "c2", Status: "running", DependsOn: []string{"c1"}}, wantErr: true},
		{name: "completed build rejected", buildStatus: "completed", chunkStatus: "failed", wantErr: true},
		{name: "completed descendant", buildStatus: "failed", chunkStatus: "failed", sibling: &model.VanadisChunk{ID: "c2", Status: "completed", DependsOn: []string{"c1"}}, wantErr: true},
		{name: "pending descendant", buildStatus: "failed", chunkStatus: "failed", sibling: &model.VanadisChunk{ID: "c2", Status: "pending", DependsOn: []string{"c1"}}},
		{name: "nil store errors", nilStore: true, wantErr: true, wantSub: "vanadis store not configured"},
		{name: "store error propagates", storeErr: errors.New("db down"), wantErr: true, wantSub: "db down"},
		{name: "nil build maps to not found", noBuild: true, wantErr: true, wantErrIs: model.ErrBuildNotFound},
		{name: "missing chunk maps to not found", buildStatus: "failed", chunkStatus: "failed", chunkID: "c9", wantErr: true, wantErrIs: model.ErrChunkNotFound},
		{name: "non-action mode conflicts", buildStatus: "failed", chunkStatus: "failed", mode: "pr", wantErr: true, wantErrIs: model.ErrBuildConflict},
		{name: "unready dependency conflicts", buildStatus: "failed", chunkStatus: "failed", dependsOn: []string{"c0"}, sibling: &model.VanadisChunk{ID: "c0", Mode: "action", Status: "pending"}, wantErr: true, wantErrIs: model.ErrBuildConflict},
		{name: "ready dependency passes", buildStatus: "failed", chunkStatus: "failed", dependsOn: []string{"c0"}, sibling: &model.VanadisChunk{ID: "c0", Mode: "action", Status: "completed"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunk := model.VanadisChunk{ID: cmp.Or(tc.chunkID, "c1"), BuildID: "b1", Mode: cmp.Or(tc.mode, "action"), Status: tc.chunkStatus, StallNudges: tc.generation, DependsOn: tc.dependsOn}
			chunks := []model.VanadisChunk{chunk}
			if tc.sibling != nil {
				chunks = append(chunks, *tc.sibling)
			}
			fixture := retryEligibilityFixture{build: &model.VanadisBuild{ID: "b1", Status: tc.buildStatus}, chunks: chunks, err: tc.storeErr}
			if tc.noBuild {
				fixture = retryEligibilityFixture{}
			}
			h := &Handler{Store: fixture}
			if tc.nilStore {
				h = &Handler{}
			}
			err := h.ValidateRetryEligibility(context.Background(), RetryActionChunkRequest{BuildID: "b1", ChunkID: "c1", ExpectedDispatchID: cmp.Or(tc.identity, "vanadis-c1-turn-0"), Reason: "retry"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
				t.Fatalf("err = %v, want %v", err, tc.wantErrIs)
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("err %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}
