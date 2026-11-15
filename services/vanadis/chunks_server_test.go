package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/httpkit"
)

func TestServer_ChunkEndpoints(t *testing.T) {
	memStore, _, cIDs := setupTestStoreWithBuild()
	if _, _, _, err := memStore.AttachChunkPR(context.Background(), AttachChunkPRRequest{ChunkID: cIDs[0], PRNumber: 101, Repo: "aerial"}); err != nil {
		t.Fatalf("AttachChunkPR failed: %v", err)
	}

	tsBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/discord/widget" {
			httpkit.WriteJSON(w, http.StatusOK, map[string]string{"message_id": "msg-new-widget-1"})
			return
		}
		http.NotFound(w, r)
	}))
	defer tsBrain.Close()

	srv := NewVanadisServer(Config{BrainURL: tsBrain.URL}, memStore, tsBrain.Client())
	mux := SetupMux(srv)

	doReq := func(method, path string, body string) *httptest.ResponseRecorder {
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, r)
		return rec
	}

	t.Run("GET_ChunkByID", func(t *testing.T) {
		rec := doReq(http.MethodGet, "/chunks/"+cIDs[0], "")
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		var res map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if res["chunk"] == nil || res["build"] == nil {
			t.Errorf("missing chunk or build in response: %+v", res)
		}

		rec404 := doReq(http.MethodGet, "/chunks/missing-chunk-id", "")
		if rec404.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", rec404.Code)
		}

		rec400 := doReq(http.MethodGet, "/chunks/", "")
		if rec400.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for empty id, got %d", rec400.Code)
		}

		rec405 := doReq(http.MethodPost, "/chunks/"+cIDs[0], "")
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodGet, "/chunks/"+cIDs[0], nil))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})

	t.Run("POST_AttachChunkPR", func(t *testing.T) {
		sAttach, _, cAttachIDs := setupTestStoreWithBuild()
		srvAttach := NewVanadisServer(Config{}, sAttach, nil)
		muxAttach := SetupMux(srvAttach)
		doAttachReq := func(method, path string, body string) *httptest.ResponseRecorder {
			var r *http.Request
			if body != "" {
				r = httptest.NewRequest(method, path, strings.NewReader(body))
			} else {
				r = httptest.NewRequest(method, path, nil)
			}
			rec := httptest.NewRecorder()
			muxAttach.ServeHTTP(rec, r)
			return rec
		}

		rec := doAttachReq(http.MethodPost, "/chunks/attach-pr", fmt.Sprintf(`{"chunk_id":%q,"pr_number":103,"repo":"aerial"}`, cAttachIDs[0]))
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
		}
		var res map[string]string
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		if res["status"] != "attached" || res["thread_id"] == "" || res["build_id"] == "" || res["chunk_id"] == "" {
			t.Errorf("unexpected attach response: %+v", res)
		}

		// Standalone attach test
		recStandalone := doAttachReq(http.MethodPost, "/chunks/attach-pr", `{"chunk_id":"","repo":"aerial","pr_number":401,"branch":"feature-1","title":"Feature 1"}`)
		if recStandalone.Code != http.StatusOK {
			t.Fatalf("expected 200 for standalone attach, got %d: %s", recStandalone.Code, recStandalone.Body.String())
		}
		var saRes map[string]string
		_ = json.Unmarshal(recStandalone.Body.Bytes(), &saRes)
		if saRes["status"] != "attached" || saRes["build_id"] == "" || saRes["chunk_id"] == "" {
			t.Fatalf("unexpected standalone attach res: %+v", saRes)
		}

		rec404 := doAttachReq(http.MethodPost, "/chunks/attach-pr", `{"chunk_id":"missing-chunk","pr_number":101,"repo":"aerial"}`)
		if rec404.Code != http.StatusNotFound {
			t.Errorf("expected 404 for missing chunk, got %d: %s", rec404.Code, rec404.Body.String())
		}

		recErrReader := httptest.NewRecorder()
		muxAttach.ServeHTTP(recErrReader, httptest.NewRequest(http.MethodPost, "/chunks/attach-pr", errorReader{}))
		if recErrReader.Code != http.StatusBadRequest {
			t.Errorf("expected 400 on errorReader, got %d", recErrReader.Code)
		}

		for _, bad := range []string{
			`{invalid-json`,
			`{"chunk_id":"","repo":"","pr_number":101}`,
			`{"chunk_id":"chunk-1","pr_number":0}`,
		} {
			recBad := doAttachReq(http.MethodPost, "/chunks/attach-pr", bad)
			if recBad.Code != http.StatusBadRequest {
				t.Errorf("expected 400 for bad body %s, got %d", bad, recBad.Code)
			}
		}

		rec405 := doAttachReq(http.MethodGet, "/chunks/attach-pr", "")
		if rec405.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", rec405.Code)
		}

		srvNil := NewVanadisServer(Config{}, nil, nil)
		muxNil := SetupMux(srvNil)
		rec500 := httptest.NewRecorder()
		muxNil.ServeHTTP(rec500, httptest.NewRequest(http.MethodPost, "/chunks/attach-pr", strings.NewReader(`{}`)))
		if rec500.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 when store is nil, got %d", rec500.Code)
		}
	})
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
			var store VanadisStore
			if !tc.nilStore {
				mem := newMemoryVanadisStore()
				if tc.seedBuild != nil {
					mem.builds[tc.seedBuild.ID] = *tc.seedBuild
				}
				if len(tc.seedChunks) > 0 {
					mem.chunks[tc.seedBuild.ID] = tc.seedChunks
				}
				store = mem
			}

			bus := &mockChunkBus{}
			srv := NewVanadisServer(Config{}, store, nil)
			srv.SetBus(bus)
			mux := SetupMux(srv)

			var req *http.Request
			switch {
			case tc.useErrBody:
				req = httptest.NewRequest(tc.method, tc.path, errorReader{})
			case tc.body != "":
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", "application/json")
			default:
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
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
