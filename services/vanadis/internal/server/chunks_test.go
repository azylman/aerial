package server

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

	"github.com/azylman/aerial/pkg/buildplan"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis/internal/storetest"
)

// memoryVanadisStore adds DAG-normalizing CreateBuild seeding to storetest.Store.
type memoryVanadisStore struct{ *storetest.Store }

func newMemoryVanadisStore() *memoryVanadisStore { return &memoryVanadisStore{storetest.New()} }

func (m *memoryVanadisStore) CreateBuild(_ context.Context, build VanadisBuild, chunks []VanadisChunk) error {
	normChunks, err := buildplan.NormalizeAndValidateBuildDAG(chunks)
	if err != nil {
		return err
	}
	m.Builds[build.ID], m.Chunks[build.ID] = build, normChunks
	return nil
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read error") }

func setupTestStoreWithBuild() *memoryVanadisStore {
	s := newMemoryVanadisStore()
	buildID := "build-test-1"
	b := storetest.Build(buildID, "123456789012345678", "Test Build Title", "Test Objective", "running")
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
	if err := s.CreateBuild(context.Background(), b, []VanadisChunk{c1, c2}); err != nil {
		panic(err)
	}
	return s
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
				store = setupTestStoreWithBuild()
			}
			rec := serveChunkRequest(NewVanadisServer(Config{}, RestateConfig{}, store, nil), tc.method, tc.path, "", false)
			if rec.Code != tc.wantCode {
				t.Fatalf("got status %d, want %d: %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			assertJSONFields(t, rec.Body.Bytes(), "", tc.wantPresent)
		})
	}
}

// mockRestateMisrouted rejects Build object calls that miss the build's
// runner: builds whose ID contains "runner-alpha" run on agent-alpha, Create
// always targets canonical Build, and every other build runs on Build.
func mockRestateMisrouted(w http.ResponseWriter, path string, bodyBytes []byte) bool {
	if strings.HasPrefix(path, "/BuildRouter") {
		return false
	}
	want := "/Build/"
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
		if err := json.Unmarshal(bodyBytes, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.ChunkID == "missing-chunk" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"chunk not found"}`))
			return
		}
		chunkID := strutil.FirstNonEmpty(req.ChunkID, "chunk-standalone")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(AttachChunkPRResponse{
			Status:   "attached",
			BuildID:  "build-1",
			ChunkID:  chunkID,
			ThreadID: "thread-" + strutil.FirstNonEmpty(req.Agent, "none"),
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
		if err := json.Unmarshal(bodyBytes, &createReq); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		buildID := strutil.FirstNonEmpty(createReq.ID, "b1")
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
			var err error
			bodyBytes, err = io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
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
		name         string
		nilStore     bool
		errBody      bool
		preRecordErr error
		method       string
		body         string
		wantCode     int
		wantStatus   string
		wantPresent  []string
	}{
		{"attach_existing_chunk", false, false, nil, http.MethodPost, `{"chunk_id":"chunk-1","pr_number":103,"repo":"aerial"}`, http.StatusOK, "attached", []string{"thread_id", "build_id", "chunk_id"}},
		{"attach_prerecord_err_logs_and_continues", false, false, errors.New("prerecord failure"), http.MethodPost, `{"chunk_id":"chunk-1","pr_number":103,"repo":"aerial"}`, http.StatusOK, "attached", []string{"thread_id", "build_id", "chunk_id"}},
		{"attach_standalone", false, false, nil, http.MethodPost, `{"chunk_id":"","repo":"aerial","pr_number":401,"branch":"feature-1","title":"Feature 1"}`, http.StatusOK, "attached", []string{"build_id", "chunk_id"}},
		{"missing_chunk", false, false, nil, http.MethodPost, `{"chunk_id":"missing-chunk","pr_number":101,"repo":"aerial"}`, http.StatusNotFound, "", nil},
		{"body_read_error", false, true, nil, http.MethodPost, "", http.StatusBadRequest, "", nil},
		{"invalid_json", false, false, nil, http.MethodPost, `{invalid-json`, http.StatusBadRequest, "", nil},
		{"missing_chunk_id_and_repo", false, false, nil, http.MethodPost, `{"chunk_id":"","repo":"","pr_number":101}`, http.StatusBadRequest, "", nil},
		{"zero_pr_number", false, false, nil, http.MethodPost, `{"chunk_id":"chunk-1","pr_number":0}`, http.StatusBadRequest, "", nil},
		{"method_not_allowed", false, false, nil, http.MethodGet, "", http.StatusMethodNotAllowed, "", nil},
		{"nil_store", true, false, nil, http.MethodPost, `{}`, http.StatusInternalServerError, "", nil},
		{"invalid_agent", false, false, nil, http.MethodPost, `{"repo":"aerial","pr_number":402,"agent":"BAD_SLUG!"}`, http.StatusBadRequest, "", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			restateSrv := newMockRestateServer()
			defer restateSrv.Close()

			var store VanadisStore
			if !tc.nilStore {
				mem := setupTestStoreWithBuild()
				mem.PreRecordErr = tc.preRecordErr
				store = mem
			}
			srv := NewVanadisServer(Config{}, RestateConfig{}, store, restateSrv.Client())
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
		{name: "spaced name rejected not defaulted", requested: "Foo Bar", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewVanadisServer(Config{}, RestateConfig{}, nil, nil)
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
