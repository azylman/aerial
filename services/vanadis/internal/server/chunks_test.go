package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

type memoryVanadisStore struct {
	builds                     map[string]VanadisBuild
	chunks                     map[string][]VanadisChunk
	createErr, getErr, listErr error
}

func newMemoryVanadisStore() *memoryVanadisStore {
	return &memoryVanadisStore{builds: make(map[string]VanadisBuild), chunks: make(map[string][]VanadisChunk)}
}

func (m *memoryVanadisStore) CreateBuild(_ context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	if m.createErr != nil {
		return m.createErr
	}
	normChunks, err := normalizeAndValidateBuildDAG(chunks)
	if err != nil {
		return err
	}
	m.builds[build.ID], m.chunks[build.ID] = build, normChunks
	return nil
}

func (m *memoryVanadisStore) GetBuild(_ context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	b, ok := m.builds[buildID]
	if !ok {
		return nil, nil, ErrBuildNotFound
	}
	return &b, m.chunks[buildID], nil
}

func (m *memoryVanadisStore) ListBuilds(_ context.Context, limit int) ([]VanadisBuildEntry, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	bList := make([]VanadisBuild, 0, len(m.builds))
	for _, b := range m.builds {
		bList = append(bList, b)
	}
	sort.Slice(bList, func(i, j int) bool {
		return bList[i].CreatedAt.After(bList[j].CreatedAt) || (bList[i].CreatedAt.Equal(bList[j].CreatedAt) && bList[i].ID > bList[j].ID)
	})
	if len(bList) > limit {
		bList = bList[:limit]
	}
	entries := make([]VanadisBuildEntry, len(bList))
	for i, b := range bList {
		entries[i] = SummarizeBuildEntry(b, m.chunks[b.ID])
	}
	return entries, nil
}

func (m *memoryVanadisStore) GetChunk(_ context.Context, chunkID string) (*VanadisChunk, *VanadisBuild, error) {
	if m.getErr != nil {
		return nil, nil, m.getErr
	}
	for bID, list := range m.chunks {
		for _, c := range list {
			if c.ID == chunkID {
				b, ok := m.builds[bID]
				if !ok {
					return nil, nil, ErrBuildNotFound
				}
				return &c, &b, nil
			}
		}
	}
	return nil, nil, ErrChunkNotFound
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func mkBuild(id, threadID, title, objective, status string) VanadisBuild {
	return VanadisBuild{ID: id, ThreadID: threadID, Title: title, Objective: objective, Status: status, Tier: 2}
}

func mkChunk(id, buildID string, idx int, title, instr, effort, status string) VanadisChunk {
	if effort == "" {
		effort = "medium"
	}
	return VanadisChunk{ID: id, BuildID: buildID, ChunkIndex: idx, Mode: "pr", Repo: normalizeRepo("aerial"), Title: title, Instructions: instr, Effort: effort, Status: status, TargetFiles: []string{}, TargetSymbols: []string{}, AcceptanceCriteria: []string{}, DependsOn: []string{}}
}

func setupTestStoreWithBuild() (*memoryVanadisStore, string, []string) {
	s := newMemoryVanadisStore()
	buildID := "build-test-1"
	b := mkBuild(buildID, "123456789012345678", "Test Build Title", "Test Objective", "running")
	c1 := VanadisChunk{
		ID:         "chunk-1",
		BuildID:    buildID,
		ChunkIndex: 0,
		Mode:       "pr",
		Repo:       "aerial",
		Title:      "Chunk 1",
		Status:     "pending",
		Effort:     "low",
		DependsOn:  []string{},
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	c2 := VanadisChunk{
		ID:         "chunk-2",
		BuildID:    buildID,
		ChunkIndex: 1,
		Mode:       "action",
		Repo:       "aerial",
		Title:      "Chunk 2",
		Status:     "pending",
		Effort:     "medium",
		DependsOn:  []string{"chunk-1"},
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = s.CreateBuild(context.Background(), b, []VanadisChunk{c1, c2})
	return s, buildID, []string{"chunk-1", "chunk-2"}
}

func TestValidateUpdateChunkInput_Validation(t *testing.T) {
	t.Parallel()

	strPtr := func(s string) *string { return &s }

	// validateUpdateChunkInput tests
	_, err := validateUpdateChunkInput("", "c1", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "build id and chunk ref are required") {
		t.Fatalf("expected error on empty buildID, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "build id and chunk ref are required") {
		t.Fatalf("expected error on empty chunkRef, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "at least one updated field are required") {
		t.Fatalf("expected error on empty fields, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Mode: strPtr("  ")})
	if err == nil || !strings.Contains(err.Error(), "mode cannot be empty string") {
		t.Fatalf("expected error on empty mode string, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Mode: strPtr("invalid-mode")})
	if err == nil {
		t.Fatal("expected error on invalid mode")
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Effort: "huge"})
	if err == nil {
		t.Fatal("expected error on invalid effort")
	}
	effort, err := validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Effort: "high"})
	if err != nil || effort != "medium" {
		t.Fatalf("expected medium effort, got %q (err: %v)", effort, err)
	}
	effort, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Title: "New Title"})
	if err != nil || effort != "" {
		t.Fatalf("expected empty effort, got %q (err: %v)", effort, err)
	}
}

// seedMemoryStore returns nil when nilStore is set, otherwise an in-memory
// store holding seedBuild (if any) and its seedChunks.
func seedMemoryStore(nilStore bool, seedBuild *VanadisBuild, seedChunks []VanadisChunk) (VanadisStore, *memoryVanadisStore) {
	if nilStore {
		return nil, nil
	}
	mem := newMemoryVanadisStore()
	if seedBuild != nil {
		mem.builds[seedBuild.ID] = *seedBuild
	}
	if len(seedChunks) > 0 {
		mem.chunks[seedBuild.ID] = seedChunks
	}
	return mem, mem
}

// serveChunkRequest routes one request through srv's SetupMux. errBody swaps the
// request body for a reader that always fails.
func serveChunkRequest(srv *VanadisServer, method, path, body string, errBody bool) *httptest.ResponseRecorder {
	var req *http.Request
	switch {
	case errBody:
		req = httptest.NewRequest(method, path, errorReader{})
	case body != "":
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	default:
		req = httptest.NewRequest(method, path, http.NoBody)
	}
	rec := httptest.NewRecorder()
	SetupMux(srv).ServeHTTP(rec, req)
	return rec
}

// assertJSONFields checks that each wantPresent key in the JSON object body is
// set to a non-null, non-empty value, and that "status" equals wantStatus when
// wantStatus is non-empty.
func assertJSONFields(t *testing.T, body []byte, wantStatus string, wantPresent []string) {
	t.Helper()
	if wantStatus == "" && len(wantPresent) == 0 {
		return
	}
	var res map[string]any
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("decode response %s: %v", body, err)
	}
	if wantStatus != "" && res["status"] != wantStatus {
		t.Errorf("status = %v, want %q (response %+v)", res["status"], wantStatus, res)
	}
	for _, key := range wantPresent {
		if v, ok := res[key]; !ok || v == nil || v == "" {
			t.Errorf("response field %q missing or empty: %+v", key, res)
		}
	}
}

func TestVanadisServer_HandleChunkByID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		nilStore    bool
		method      string
		path        string
		wantCode    int
		wantPresent []string
	}{
		{"ok", false, http.MethodGet, "/chunks/chunk-1", http.StatusOK, []string{"chunk", "build"}},
		{"not_found", false, http.MethodGet, "/chunks/missing-chunk-id", http.StatusNotFound, nil},
		{"empty_id", false, http.MethodGet, "/chunks/", http.StatusBadRequest, nil},
		{"method_not_allowed", false, http.MethodPost, "/chunks/chunk-1", http.StatusMethodNotAllowed, nil},
		{"nil_store", true, http.MethodGet, "/chunks/chunk-1", http.StatusInternalServerError, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store VanadisStore
			if !tc.nilStore {
				store, _, _ = setupTestStoreWithBuild()
			}
			rec := serveChunkRequest(NewVanadisServer(Config{}, store, nil), tc.method, tc.path, "", false)
			if rec.Code != tc.wantCode {
				t.Fatalf("got status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			assertJSONFields(t, rec.Body.Bytes(), "", tc.wantPresent)
		})
	}
}

// mockRestateMisrouted rejects Build object calls that miss the build's
// runner: builds whose ID contains "runner-alpha" run on agent-alpha, Create
// must target the requested agent, and every other build runs on Build.
func mockRestateMisrouted(w http.ResponseWriter, path string, bodyBytes []byte) bool {
	if strings.HasPrefix(path, "/BuildRouter") {
		return false
	}
	want := "/Build/"
	if strings.HasSuffix(path, "/Create") {
		var createReq CreateBuildRequest
		_ = json.Unmarshal(bodyBytes, &createReq)
		want = "/" + BuildServiceName(createReq.Agent) + "/"
	} else if strings.Contains(path, "runner-alpha") {
		want = "/Build-agent-alpha/"
	}
	if strings.HasPrefix(path, want) {
		return false
	}
	w.WriteHeader(http.StatusTeapot)
	_, _ = w.Write([]byte(`{"message":"misrouted build object"}`))
	return true
}

func mockRestateCheckErrors(w http.ResponseWriter, path, bodyStr string) bool {
	if strings.Contains(path, "fail") {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"internal error"}`))
		return true
	}
	if strings.Contains(path, "missing") || strings.Contains(path, "c999") || strings.Contains(path, "unknown-id") || strings.Contains(bodyStr, "missing") {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"not found"}`))
		return true
	}
	if strings.Contains(path, "b-done") || strings.Contains(path, "conflict") || strings.Contains(bodyStr, "conflict") {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":"build state conflict"}`))
		return true
	}
	return false
}

func mockRestateRouteSuccess(w http.ResponseWriter, path string, bodyBytes []byte) {
	if strings.Contains(path, "RouteAttachPR") {
		var req AttachChunkPRRequest
		_ = json.Unmarshal(bodyBytes, &req)
		if req.ChunkID == "missing-chunk" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"chunk not found"}`))
			return
		}
		chunkID := firstNonEmpty(req.ChunkID, "chunk-standalone")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(AttachChunkPRResponse{
			Status:   "attached",
			BuildID:  "build-1",
			ChunkID:  chunkID,
			ThreadID: "thread-" + firstNonEmpty(req.Agent, "none"),
		})
		return
	}
	if strings.Contains(path, "UpdateBuild") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VanadisBuild{ID: "b1", Status: "running"})
		return
	}
	if strings.Contains(path, "UpdateChunk") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VanadisChunk{ID: "c1", Status: "pending"})
		return
	}
	if strings.Contains(path, "AppendChunks") {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]VanadisChunk{{ID: "c2", Status: "pending"}})
		return
	}
	if strings.Contains(path, "RemoveChunk") {
		w.WriteHeader(http.StatusOK)
		return
	}
	if strings.Contains(path, "Cancel") {
		parts := strings.Split(path, "/")
		buildID := "b1"
		if len(parts) >= 3 {
			buildID = parts[2]
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VanadisBuild{ID: buildID, Status: statusCancelled})
		return
	}
	if strings.Contains(path, "Create") {
		var createReq CreateBuildRequest
		_ = json.Unmarshal(bodyBytes, &createReq)
		buildID := firstNonEmpty(createReq.ID, "b1")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VanadisBuild{
			ID:              buildID,
			ThreadID:        createReq.ThreadID,
			Tier:            createReq.Tier,
			Title:           createReq.Title,
			Objective:       createReq.Objective,
			Status:          "running",
			StatusMessageID: "msg-1",
		})
		return
	}
	w.WriteHeader(http.StatusOK)
}

func newMockRestateServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		var bodyBytes []byte
		if r.Body != nil {
			bodyBytes, _ = io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
		if mockRestateMisrouted(w, path, bodyBytes) || mockRestateCheckErrors(w, path, string(bodyBytes)) {
			return
		}
		mockRestateRouteSuccess(w, path, bodyBytes)
	}))
}

func TestVanadisServer_HandleAttachChunkPR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilStore    bool
		errBody     bool
		method      string
		body        string
		wantCode    int
		wantStatus  string
		wantPresent []string
	}{
		{"attach_existing_chunk", false, false, http.MethodPost, `{"chunk_id":"chunk-1","pr_number":103,"repo":"aerial"}`, http.StatusOK, "attached", []string{"thread_id", "build_id", "chunk_id"}},
		{"attach_standalone", false, false, http.MethodPost, `{"chunk_id":"","repo":"aerial","pr_number":401,"branch":"feature-1","title":"Feature 1"}`, http.StatusOK, "attached", []string{"build_id", "chunk_id"}},
		{"missing_chunk", false, false, http.MethodPost, `{"chunk_id":"missing-chunk","pr_number":101,"repo":"aerial"}`, http.StatusNotFound, "", nil},
		{"body_read_error", false, true, http.MethodPost, "", http.StatusBadRequest, "", nil},
		{"invalid_json", false, false, http.MethodPost, `{invalid-json`, http.StatusBadRequest, "", nil},
		{"missing_chunk_id_and_repo", false, false, http.MethodPost, `{"chunk_id":"","repo":"","pr_number":101}`, http.StatusBadRequest, "", nil},
		{"zero_pr_number", false, false, http.MethodPost, `{"chunk_id":"chunk-1","pr_number":0}`, http.StatusBadRequest, "", nil},
		{"method_not_allowed", false, false, http.MethodGet, "", http.StatusMethodNotAllowed, "", nil},
		{"nil_store", true, false, http.MethodPost, `{}`, http.StatusInternalServerError, "", nil},
		{"invalid_agent", false, false, http.MethodPost, `{"repo":"aerial","pr_number":402,"agent":"BAD_SLUG!"}`, http.StatusBadRequest, "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			restateSrv := newMockRestateServer()
			defer restateSrv.Close()

			var store VanadisStore
			if !tc.nilStore {
				store, _, _ = setupTestStoreWithBuild()
			}
			srv := NewVanadisServer(Config{}, store, restateSrv.Client())
			if !tc.nilStore {
				srv.restateCfg.RestateIngressURL = restateSrv.URL
			}
			rec := serveChunkRequest(srv, tc.method, "/chunks/attach-pr", tc.body, tc.errBody)
			if rec.Code != tc.wantCode {
				t.Fatalf("got status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			assertJSONFields(t, rec.Body.Bytes(), tc.wantStatus, tc.wantPresent)
		})
	}
}

func TestServer_MutationEndpoints(t *testing.T) {
	restateSrv := newMockRestateServer()
	defer restateSrv.Close()

	tests := []struct {
		name       string
		nilStore   bool
		useErrBody bool
		seedBuild  *VanadisBuild
		seedChunks []VanadisChunk
		method     string
		path       string
		body       string
		wantCode   int
	}{
		// PATCH /builds/{id}
		{"patch_build_nil_store", true, false, nil, nil, http.MethodPatch, "/builds/b1", `{"title":"T"}`, http.StatusInternalServerError},
		{"patch_build_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", "", http.StatusBadRequest},
		{"patch_build_invalid_json", false, false, nil, nil, http.MethodPatch, "/builds/b1", `{invalid-json`, http.StatusBadRequest},
		{"patch_build_empty_fields", false, false, nil, nil, http.MethodPatch, "/builds/b1", `{"title":""}`, http.StatusBadRequest},
		{"patch_build_not_found", false, false, nil, nil, http.MethodPatch, "/builds/b-missing", `{"title":"T"}`, http.StatusNotFound},
		{"patch_build_conflict", false, false, &VanadisBuild{ID: "b-done", Status: "completed"}, nil, http.MethodPatch, "/builds/b-done", `{"title":"T"}`, http.StatusConflict},
		{"patch_build_ok_no_title", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", `{"objective":"New Obj"}`, http.StatusOK},
		{"patch_build_ok_with_title", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", `{"title":"New Title"}`, http.StatusOK},

		// GET /builds/{id}/chunks/{chunk_ref}
		{"get_chunk_nil_store", true, false, nil, nil, http.MethodGet, "/builds/b1/chunks/c1", "", http.StatusInternalServerError},
		{"get_chunk_build_not_found", false, false, nil, nil, http.MethodGet, "/builds/b-missing/chunks/c1", "", http.StatusNotFound},
		{"get_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1"}}, http.MethodGet, "/builds/b1/chunks/c999", "", http.StatusNotFound},
		{"get_chunk_ok_by_id", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Title: "C1"}}, http.MethodGet, "/builds/b1/chunks/c1", "", http.StatusOK},
		{"get_chunk_ok_by_index", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Title: "C1"}}, http.MethodGet, "/builds/b1/chunks/0", "", http.StatusOK},

		// PATCH /builds/{id}/chunks/{chunk_ref}
		{"patch_chunk_nil_store", true, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"T"}`, http.StatusInternalServerError},
		{"patch_chunk_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending"}}, http.MethodPatch, "/builds/b1/chunks/c1", "", http.StatusBadRequest},
		{"patch_chunk_invalid_json", false, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{invalid-json`, http.StatusBadRequest},
		{"patch_chunk_empty_update", false, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":""}`, http.StatusBadRequest},
		{"patch_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1/chunks/c-missing", `{"title":"T"}`, http.StatusNotFound},
		{"patch_chunk_conflict", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "running"}}, http.MethodPatch, "/builds/b1/chunks/c-conflict", `{"title":"T"}`, http.StatusConflict},
		{"patch_chunk_ok", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending", ChunkIndex: 0}}, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"New Chunk Title"}`, http.StatusOK},

		// POST /builds/{id}/chunks
		{"append_chunks_nil_store", true, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusInternalServerError},
		{"append_chunks_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPost, "/builds/b1/chunks", "", http.StatusBadRequest},
		{"append_chunks_invalid_json", false, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{invalid-json`, http.StatusBadRequest},
		{"append_chunks_empty_list", false, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[]}`, http.StatusBadRequest},
		{"append_chunks_build_not_found", false, false, nil, nil, http.MethodPost, "/builds/b-missing/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusNotFound},
		{"append_chunks_conflict", false, false, &VanadisBuild{ID: "b-done", Status: "completed"}, nil, http.MethodPost, "/builds/b-done/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusConflict},
		{"append_chunks_ok", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusOK},

		// DELETE /builds/{id}/chunks/{chunk_ref}
		{"delete_chunk_nil_store", true, false, nil, nil, http.MethodDelete, "/builds/b1/chunks/c1", "", http.StatusInternalServerError},
		{"delete_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodDelete, "/builds/b1/chunks/c-missing", "", http.StatusNotFound},
		{"delete_chunk_conflict_only_chunk", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending"}}, http.MethodDelete, "/builds/b1/chunks/c-conflict", "", http.StatusConflict},
		{"delete_chunk_ok", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{
			{ID: "c1", Status: "running", ChunkIndex: 0},
			{ID: "c2", Status: "pending", ChunkIndex: 1},
		}, http.MethodDelete, "/builds/b1/chunks/c2", "", http.StatusOK},

		// Per-build mutations route to the build's runner, not the server agent.
		{"patch_build_routes_to_runner", false, false, &VanadisBuild{ID: "b-runner-alpha", Agent: "agent-alpha", RunnerAgent: "agent-alpha", Status: "running"}, nil, http.MethodPatch, "/builds/b-runner-alpha", `{"title":"New Title"}`, http.StatusOK},
		{"patch_chunk_routes_to_runner", false, false, &VanadisBuild{ID: "b-runner-alpha", Agent: "agent-alpha", RunnerAgent: "agent-alpha", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending"}}, http.MethodPatch, "/builds/b-runner-alpha/chunks/c1", `{"title":"New Chunk Title"}`, http.StatusOK},
		{"append_chunks_routes_to_runner", false, false, &VanadisBuild{ID: "b-runner-alpha", Agent: "agent-alpha", RunnerAgent: "agent-alpha", Status: "running"}, nil, http.MethodPost, "/builds/b-runner-alpha/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusOK},
		{"delete_chunk_routes_to_runner", false, false, &VanadisBuild{ID: "b-runner-alpha", Agent: "agent-alpha", RunnerAgent: "agent-alpha", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "running", ChunkIndex: 0}, {ID: "c2", Status: "pending", ChunkIndex: 1}}, http.MethodDelete, "/builds/b-runner-alpha/chunks/c2", "", http.StatusOK},
		{"active_step_routes_to_runner", false, false, &VanadisBuild{ID: "b-runner-alpha", Agent: "agent-alpha", RunnerAgent: "agent-alpha", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "running"}}, http.MethodPost, "/builds/b-runner-alpha/active-step", `{"chunk_id":"c1","step":"testing"}`, http.StatusOK},
		{"sibling_owned_build_pinned_to_default_runner", false, false, &VanadisBuild{ID: "b1", Agent: "agent-alpha", RunnerAgent: "aerial", Status: "running"}, nil, http.MethodPatch, "/builds/b1", `{"title":"New Title"}`, http.StatusOK},

		// Method and path errors
		{"method_not_allowed_put_build", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPut, "/builds/b1", "", http.StatusMethodNotAllowed},
		{"method_not_allowed_get_chunks_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks", "", http.StatusMethodNotAllowed},
		{"method_not_allowed_post_chunk_ref", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1"}}, http.MethodPost, "/builds/b1/chunks/c1", "", http.StatusMethodNotAllowed},
		{"bad_request_empty_chunk_ref_with_slash_get", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/", "", http.StatusBadRequest},
		{"bad_request_empty_chunk_ref_space", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/%20", "", http.StatusBadRequest},
		{"bad_request_empty_build_id_chunks", false, false, nil, nil, http.MethodPost, "/builds/%20/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusBadRequest},
		{"not_found_unknown_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/unknown", "", http.StatusNotFound},
		{"not_found_deep_unknown_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/c1/deep", "", http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := seedMemoryStore(tc.nilStore, tc.seedBuild, tc.seedChunks)

			srv := NewVanadisServer(Config{}, store, restateSrv.Client())
			if !tc.nilStore {
				srv.restateCfg.RestateIngressURL = restateSrv.URL
			}
			rec := serveChunkRequest(srv, tc.method, tc.path, tc.body, tc.useErrBody)
			if rec.Code != tc.wantCode {
				t.Fatalf("got status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
		})
	}
}

func TestResolveAttachCallerAgent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		requested string
		want      string
		wantErr   bool
	}{
		{name: "empty defaults to the default agent", want: "aerial"},
		{name: "named agent is normalized", requested: " Agent-Alpha ", want: "agent-alpha"},
		{name: "invalid slug rejected", requested: "BAD_SLUG!", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewVanadisServer(Config{}, nil, nil)
			got, err := srv.resolveAttachCallerAgent(context.Background(), tc.requested)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("agent = %q, want %q", got, tc.want)
			}
		})
	}
}
