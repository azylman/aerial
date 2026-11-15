package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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
		req = httptest.NewRequest(method, path, nil)
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
		wantWidget  bool
	}{
		{"attach_existing_chunk", false, false, http.MethodPost, `{"chunk_id":"chunk-1","pr_number":103,"repo":"aerial"}`, http.StatusOK, "attached", []string{"thread_id", "build_id", "chunk_id"}, true},
		{"attach_standalone", false, false, http.MethodPost, `{"chunk_id":"","repo":"aerial","pr_number":401,"branch":"feature-1","title":"Feature 1"}`, http.StatusOK, "attached", []string{"build_id", "chunk_id"}, true},
		{"missing_chunk", false, false, http.MethodPost, `{"chunk_id":"missing-chunk","pr_number":101,"repo":"aerial"}`, http.StatusNotFound, "", nil, false},
		{"body_read_error", false, true, http.MethodPost, "", http.StatusBadRequest, "", nil, false},
		{"invalid_json", false, false, http.MethodPost, `{invalid-json`, http.StatusBadRequest, "", nil, false},
		{"missing_chunk_id_and_repo", false, false, http.MethodPost, `{"chunk_id":"","repo":"","pr_number":101}`, http.StatusBadRequest, "", nil, false},
		{"zero_pr_number", false, false, http.MethodPost, `{"chunk_id":"chunk-1","pr_number":0}`, http.StatusBadRequest, "", nil, false},
		{"method_not_allowed", false, false, http.MethodGet, "", http.StatusMethodNotAllowed, "", nil, false},
		{"nil_store", true, false, http.MethodPost, `{}`, http.StatusInternalServerError, "", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store VanadisStore
			if !tc.nilStore {
				store, _, _ = setupTestStoreWithBuild()
			}
			bus := &mockChunkBus{}
			srv := NewVanadisServer(Config{}, store, nil)
			srv.SetBus(bus)
			rec := serveChunkRequest(srv, tc.method, "/chunks/attach-pr", tc.body, tc.errBody)
			if rec.Code != tc.wantCode {
				t.Fatalf("got status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			assertJSONFields(t, rec.Body.Bytes(), tc.wantStatus, tc.wantPresent)
			if gotWidget := bus.widgetCalls > 0; gotWidget != tc.wantWidget {
				t.Errorf("widget sync published = %v, want %v", gotWidget, tc.wantWidget)
			}
		})
	}
}

func TestServer_MutationEndpoints(t *testing.T) {
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
		wantWidget bool
	}{
		// PATCH /builds/{id}
		{"patch_build_nil_store", true, false, nil, nil, http.MethodPatch, "/builds/b1", `{"title":"T"}`, http.StatusInternalServerError, false},
		{"patch_build_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", "", http.StatusBadRequest, false},
		{"patch_build_invalid_json", false, false, nil, nil, http.MethodPatch, "/builds/b1", `{invalid-json`, http.StatusBadRequest, false},
		{"patch_build_empty_fields", false, false, nil, nil, http.MethodPatch, "/builds/b1", `{"title":""}`, http.StatusBadRequest, false},
		{"patch_build_not_found", false, false, nil, nil, http.MethodPatch, "/builds/b-missing", `{"title":"T"}`, http.StatusNotFound, false},
		{"patch_build_conflict", false, false, &VanadisBuild{ID: "b-done", Status: "completed"}, nil, http.MethodPatch, "/builds/b-done", `{"title":"T"}`, http.StatusConflict, false},
		{"patch_build_ok_no_title", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", `{"objective":"New Obj"}`, http.StatusOK, false},
		{"patch_build_ok_with_title_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1", `{"title":"New Title"}`, http.StatusOK, true},
		{"patch_chunk_dag_mutation_notify_error", false, false, &VanadisBuild{ID: "b-mut-err", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending", ChunkIndex: 0}}, http.MethodPatch, "/builds/b-mut-err/chunks/c1", `{"title":"New Chunk Title"}`, http.StatusOK, true},

		// GET /builds/{id}/chunks/{chunk_ref}
		{"get_chunk_nil_store", true, false, nil, nil, http.MethodGet, "/builds/b1/chunks/c1", "", http.StatusInternalServerError, false},
		{"get_chunk_build_not_found", false, false, nil, nil, http.MethodGet, "/builds/b-missing/chunks/c1", "", http.StatusNotFound, false},
		{"get_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1"}}, http.MethodGet, "/builds/b1/chunks/c999", "", http.StatusNotFound, false},
		{"get_chunk_ok_by_id", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Title: "C1"}}, http.MethodGet, "/builds/b1/chunks/c1", "", http.StatusOK, false},
		{"get_chunk_ok_by_index", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Title: "C1"}}, http.MethodGet, "/builds/b1/chunks/0", "", http.StatusOK, false},

		// PATCH /builds/{id}/chunks/{chunk_ref}
		{"patch_chunk_nil_store", true, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"T"}`, http.StatusInternalServerError, false},
		{"patch_chunk_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending"}}, http.MethodPatch, "/builds/b1/chunks/c1", "", http.StatusBadRequest, false},
		{"patch_chunk_invalid_json", false, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{invalid-json`, http.StatusBadRequest, false},
		{"patch_chunk_empty_update", false, false, nil, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":""}`, http.StatusBadRequest, false},
		{"patch_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"T"}`, http.StatusNotFound, false},
		{"patch_chunk_conflict", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "running"}}, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"T"}`, http.StatusConflict, false},
		{"patch_chunk_ok_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending", ChunkIndex: 0}}, http.MethodPatch, "/builds/b1/chunks/c1", `{"title":"New Chunk Title"}`, http.StatusOK, true},

		// POST /builds/{id}/chunks
		{"append_chunks_nil_store", true, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusInternalServerError, false},
		{"append_chunks_err_reader", false, true, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPost, "/builds/b1/chunks", "", http.StatusBadRequest, false},
		{"append_chunks_invalid_json", false, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{invalid-json`, http.StatusBadRequest, false},
		{"append_chunks_empty_list", false, false, nil, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[]}`, http.StatusBadRequest, false},
		{"append_chunks_build_not_found", false, false, nil, nil, http.MethodPost, "/builds/b-missing/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusNotFound, false},
		{"append_chunks_conflict", false, false, &VanadisBuild{ID: "b-done", Status: "completed"}, nil, http.MethodPost, "/builds/b-done/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusConflict, false},
		{"append_chunks_ok_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPost, "/builds/b1/chunks", `{"chunks":[{"repo":"aerial","title":"T","instructions":"I"}]}`, http.StatusOK, true},

		// DELETE /builds/{id}/chunks/{chunk_ref}
		{"delete_chunk_nil_store", true, false, nil, nil, http.MethodDelete, "/builds/b1/chunks/c1", "", http.StatusInternalServerError, false},
		{"delete_chunk_not_found", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodDelete, "/builds/b1/chunks/c1", "", http.StatusNotFound, false},
		{"delete_chunk_conflict_only_chunk", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1", Status: "pending"}}, http.MethodDelete, "/builds/b1/chunks/c1", "", http.StatusConflict, false},
		{"delete_chunk_ok_syncs_widget", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{
			{ID: "c1", Status: "running", ChunkIndex: 0},
			{ID: "c2", Status: "pending", ChunkIndex: 1},
		}, http.MethodDelete, "/builds/b1/chunks/c2", "", http.StatusOK, true},

		// Method and path errors
		{"method_not_allowed_put_build", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodPut, "/builds/b1", "", http.StatusMethodNotAllowed, false},
		{"method_not_allowed_get_chunks_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks", "", http.StatusMethodNotAllowed, false},
		{"method_not_allowed_post_chunk_ref", false, false, &VanadisBuild{ID: "b1", Status: "running"}, []VanadisChunk{{ID: "c1"}}, http.MethodPost, "/builds/b1/chunks/c1", "", http.StatusMethodNotAllowed, false},
		{"bad_request_empty_chunk_ref_with_slash_get", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/", "", http.StatusBadRequest, false},
		{"bad_request_empty_chunk_ref_space", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/%20", "", http.StatusBadRequest, false},
		{"bad_request_empty_build_id_chunks", false, false, nil, nil, http.MethodPost, "/builds/%20/chunks", `{"chunks":[{"title":"T","instructions":"I"}]}`, http.StatusBadRequest, false},
		{"not_found_unknown_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/unknown", "", http.StatusNotFound, false},
		{"not_found_deep_unknown_subpath", false, false, &VanadisBuild{ID: "b1", Status: "running"}, nil, http.MethodGet, "/builds/b1/chunks/c1/deep", "", http.StatusNotFound, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := seedMemoryStore(tc.nilStore, tc.seedBuild, tc.seedChunks)

			bus := &mockChunkBus{}
			srv := NewVanadisServer(Config{}, store, nil)
			srv.SetBus(bus)
			rec := serveChunkRequest(srv, tc.method, tc.path, tc.body, tc.useErrBody)
			if rec.Code != tc.wantCode {
				t.Fatalf("got status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			if tc.wantWidget && bus.widgetCalls == 0 {
				t.Errorf("expected widget sync call, got 0")
			}
			if !tc.wantWidget && bus.widgetCalls > 0 {
				t.Errorf("unexpected widget sync call, got %d", bus.widgetCalls)
			}
		})
	}
}
