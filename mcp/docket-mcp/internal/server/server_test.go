package server

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/mcp/docket-mcp/internal/config"
	"github.com/azylman/aerial/mcp/docket-mcp/internal/store"
	"github.com/azylman/aerial/pkg/httpkittest"
	"github.com/azylman/aerial/pkg/ollama"
)

type memoryTaskStore struct {
	mu                       sync.Mutex
	resolutions              map[int64]string
	claims                   map[int64]taskClaimRecord
	searchHits               []config.ScoredTaskSummary
	searchErr                error
	fetchErr                 error
	updateErr                error
	claimErr                 error
	unclaimErr               error
	embeddingResolutionCalls int
}

type taskClaimRecord struct {
	agent     string
	turnID    string
	claimedAt time.Time
}

func newMemoryTaskStore() *memoryTaskStore {
	return &memoryTaskStore{
		resolutions: map[int64]string{
			5: "completed",
		},
		claims: make(map[int64]taskClaimRecord),
		searchHits: []config.ScoredTaskSummary{
			{ID: 1, Title: "Top Task 1", Status: "open", Score: 0.95},
			{ID: 2, Title: "Subtask 2", Status: "open", Score: 0.85},
			{ID: 1000349, Title: "Million Task 317", Status: "open", Score: 0.75},
		},
	}
}

func (m *memoryTaskStore) UpdateTaskEmbedding(ctx context.Context, taskID int64, embedding []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.updateErr != nil {
		return m.updateErr
	}
	return nil
}

func (m *memoryTaskStore) UpdateTaskEmbeddingAndResolution(ctx context.Context, taskID int64, embedding []float32, resolution string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.embeddingResolutionCalls++
	if m.updateErr != nil {
		return m.updateErr
	}
	m.resolutions[taskID] = resolution
	return nil
}

func (m *memoryTaskStore) UpdateTaskResolutionBatch(ctx context.Context, taskIDs []int64, resolution string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.updateErr != nil {
		return m.updateErr
	}
	for _, id := range taskIDs {
		m.resolutions[id] = resolution
		delete(m.claims, id)
	}
	return nil
}

func (m *memoryTaskStore) FetchTaskResolutions(ctx context.Context, taskIDs []int64) (map[int64]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	res := make(map[int64]string, len(taskIDs))
	for _, id := range taskIDs {
		res[id] = m.resolutions[id]
	}
	return res, nil
}

func (m *memoryTaskStore) FetchTaskMeta(ctx context.Context, taskIDs []int64) (map[int64]config.TaskMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	res := make(map[int64]config.TaskMeta, len(taskIDs))
	for _, id := range taskIDs {
		meta := config.TaskMeta{
			Resolution: m.resolutions[id],
		}
		if claim, ok := m.claims[id]; ok {
			meta.ClaimedByAgent = claim.agent
			meta.ClaimedByTurnID = claim.turnID
			meta.ClaimedAt = &claim.claimedAt
		}
		res[id] = meta
	}
	return res, nil
}

func (m *memoryTaskStore) ClaimTask(ctx context.Context, taskID int64, agent, turnID string, claimedAt time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.claimErr != nil {
		return false, m.claimErr
	}
	if existing, ok := m.claims[taskID]; ok && (existing.agent != "" || existing.turnID != "") {
		return false, nil
	}
	m.claims[taskID] = taskClaimRecord{
		agent:     agent,
		turnID:    turnID,
		claimedAt: claimedAt,
	}
	return true, nil
}

func (m *memoryTaskStore) UnclaimTask(ctx context.Context, taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.unclaimErr != nil {
		return m.unclaimErr
	}
	delete(m.claims, taskID)
	return nil
}

func (m *memoryTaskStore) SearchTasksRRF(ctx context.Context, embedding []float32, queryText string, statusFilter string, limit int) ([]config.ScoredTaskSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	if queryText == "nomatch" {
		return nil, nil
	}
	return m.searchHits, nil
}

type fakeVikunjaServerState struct {
	mu                sync.Mutex
	recordedRelations []VikunjaRelationRecord
}

type VikunjaRelationRecord struct {
	TaskID       int64
	OtherTaskID  int64
	RelationKind string
}

const fakeVikunjaMaxPerPage = 4

func fixtureTasks() []config.VikunjaTask {
	rel := func(ids ...int64) []config.VikunjaTask {
		out := make([]config.VikunjaTask, len(ids))
		for i, id := range ids {
			out[i] = config.VikunjaTask{ID: id, Title: fmt.Sprintf("Neighbor %d", id)}
		}
		return out
	}
	return []config.VikunjaTask{
		{
			ID: 1, Title: "Top Task 1", Description: "Task description", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"subtask":  rel(2),
				"blocking": rel(3),
			},
		},
		{
			ID: 2, Title: "Subtask 2", Description: "Subtask description", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"parenttask": rel(1),
			},
		},
		{
			ID: 3, Title: "Blocked Task 3", Description: "Blocked by 1", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"blocked": rel(1),
			},
		},
		{
			ID: 4, Title: "Independent Task 4", Description: "Leaf task", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"related":  rel(1, 2),
				"blocking": rel(1),
			},
		},
		{ID: 5, Title: "Completed Task 5", Description: "Done", Done: true, ProjectID: 1},
		{
			ID: 6, Title: "Task 6", Description: "Parent of 7", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"subtask": rel(7),
			},
		},
		{
			ID: 7, Title: "Subtask 7", Description: "Child of 6", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"parenttask": rel(6),
				"subtask":    rel(8),
			},
		},
		{
			ID: 8, Title: "Subtask 8", Description: "Child of 7", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"parenttask": rel(7),
			},
		},
		{
			ID: 75, Title: "High Task 75", Description: "Beyond page 1", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"blocking": rel(76),
			},
		},
		{
			ID: 76, Title: "Task 76", Description: "Blocked by 75", Done: false, ProjectID: 1,
			RelatedTasks: map[string][]config.VikunjaTask{
				"blocked": rel(75),
				"related": {{ID: 60, Index: 1, Title: "Foreign Task 60", ProjectID: 2}},
			},
		},
		{ID: 1000349, Index: 317, Title: "Million Task 317", Description: "Global id drift", Done: false, ProjectID: 1},
		{ID: 8888, Index: 100, Title: "Index 100 Task", Description: "Has index 100 which matches put task global id", Done: false, ProjectID: 1},
	}
}

func handleFakeVikunjaListTasks(w http.ResponseWriter, r *http.Request, byID map[int64]config.VikunjaTask) {
	page := 1
	if raw := r.URL.Query().Get("page"); raw != "" {
		var err error
		page, err = strconv.Atoi(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	if page < 1 {
		page = 1
	}
	all := make([]config.VikunjaTask, 0, len(byID))
	for _, t := range byID {
		all = append(all, t)
	}
	slices.SortFunc(all, func(a, b config.VikunjaTask) int { return cmp.Compare(a.ID, b.ID) })
	start := (page - 1) * fakeVikunjaMaxPerPage
	if start >= len(all) {
		_ = json.NewEncoder(w).Encode([]config.VikunjaTask{})
		return
	}
	end := min(start+fakeVikunjaMaxPerPage, len(all))
	_ = json.NewEncoder(w).Encode(all[start:end])
}

func handleFakeVikunjaPutTask(w http.ResponseWriter, r *http.Request) {
	var body config.VikunjaTask
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body.ID = 100
	body.ProjectID = 1
	_ = json.NewEncoder(w).Encode(body)
}

func setupFakeVikunjaTasksHandler(byID map[int64]config.VikunjaTask, state *fakeVikunjaServerState) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			handleFakeVikunjaListTasks(w, r, byID)
		case http.MethodPut:
			handleFakeVikunjaPutTask(w, r)
		default:
			http.NotFound(w, r)
		}
	}
}

func handleFakeVikunjaComments(w http.ResponseWriter, r *http.Request, path string) {
	if strings.Contains(path, "/5/comments") {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"failed to list comments"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode([]config.VikunjaComment{
			{ID: 1, Comment: "Great work!"},
		})
		return
	}
	if r.Method == http.MethodPut {
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 2})
		return
	}
	http.NotFound(w, r)
}

func handleFakeVikunjaRelations(w http.ResponseWriter, r *http.Request, path string, state *fakeVikunjaServerState) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	taskID, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.Contains(path, "/888") || strings.Contains(path, "/relations/blocking/888") {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"relation failed"}`))
		return
	}
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
		return
	}
	var relReq config.VikunjaRelationReq
	if err := json.NewDecoder(r.Body).Decode(&relReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if relReq.OtherTaskID == 888 {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"relation failed"}`))
		return
	}
	state.mu.Lock()
	state.recordedRelations = append(state.recordedRelations, VikunjaRelationRecord{
		TaskID:       taskID,
		OtherTaskID:  relReq.OtherTaskID,
		RelationKind: relReq.RelationKind,
	})
	state.mu.Unlock()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{}`))
}

func handleFakeVikunjaGetTask(w http.ResponseWriter, path string, byID map[int64]config.VikunjaTask) {
	w.Header().Set("Content-Type", "application/json")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	id, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if id == 888 {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"upstream down"}`))
		return
	}
	if id == 60 {
		_ = json.NewEncoder(w).Encode(config.VikunjaTask{ID: 60, Title: "Foreign Task 60", ProjectID: 2})
		return
	}
	task, exists := byID[id]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"task not found"}`))
		return
	}
	_ = json.NewEncoder(w).Encode(task)
}

func handleFakeVikunjaPostTask(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "/888") {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"update failed"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	var body config.VikunjaTask
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

type testEnvironment struct {
	srv   *Server
	mux   *http.ServeMux
	state *fakeVikunjaServerState
}

func setupTestEnvironment(t *testing.T) *testEnvironment {
	t.Helper()
	state := &fakeVikunjaServerState{}
	byID := make(map[int64]config.VikunjaTask)
	for _, task := range fixtureTasks() {
		byID[task.ID] = task
	}

	vikunjaMux := http.NewServeMux()
	vikunjaMux.HandleFunc("/api/v1/login", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": "test-token"})
	})
	vikunjaMux.HandleFunc("/api/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]config.VikunjaProject{{ID: 1, Title: "Default"}})
	})
	vikunjaMux.HandleFunc("/api/v1/projects/1/tasks", setupFakeVikunjaTasksHandler(byID, state))
	vikunjaMux.HandleFunc("/api/v1/tasks/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/comments"):
			handleFakeVikunjaComments(w, r, path)
		case strings.Contains(path, "/relations"):
			handleFakeVikunjaRelations(w, r, path, state)
		case r.Method == http.MethodGet:
			handleFakeVikunjaGetTask(w, path, byID)
		case r.Method == http.MethodPost:
			handleFakeVikunjaPostTask(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	tsVikunja := httptest.NewServer(vikunjaMux)
	t.Cleanup(tsVikunja.Close)

	ollamaMux := http.NewServeMux()
	ollamaMux.HandleFunc("/api/embeddings", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"embedding": make([]float64, 384)})
	})
	tsOllama := httptest.NewServer(ollamaMux)
	t.Cleanup(tsOllama.Close)

	cfg := &config.Config{
		Port:            "0",
		VikunjaURL:      tsVikunja.URL,
		VikunjaUsername: "aerial",
		VikunjaPassword: "password",
		DatabaseURL:     "postgres://mock",
		Ollama: ollama.Config{
			BaseURL: tsOllama.URL,
			Model:   "test-model",
		},
	}

	client := store.NewVikunjaClient(cfg, tsVikunja.Client())
	taskStore := newMemoryTaskStore()
	olClient := ollama.NewDynamicClient(func() ollama.Config { return cfg.Ollama })
	srv := NewServer(cfg, client, taskStore, olClient)
	mux := NewHTTPMux(srv)

	return &testEnvironment{srv: srv, mux: mux, state: state}
}

func TestServer_HTTPRoutesTable(t *testing.T) {
	httpkittest.StubCoverageWriters(t, nil, nil)
	env := setupTestEnvironment(t)

	tests := []struct {
		name             string
		method           string
		target           string
		wantStatus       int
		wantSubstr       string
		wantCacheControl string
	}{
		{name: "health GET returns 200", method: http.MethodGet, target: "/health", wantStatus: http.StatusOK, wantSubstr: `"status":"ok"`},
		{name: "health HEAD returns 200", method: http.MethodHead, target: "/health", wantStatus: http.StatusOK},
		{name: "health POST returns 405", method: http.MethodPost, target: "/health", wantStatus: http.StatusMethodNotAllowed, wantSubstr: "Method not allowed"},
		{name: "coverage flush returns 200", method: http.MethodPost, target: "/debug/coverage/flush", wantStatus: http.StatusOK},
		{name: "mcp GET returns ready banner", method: http.MethodGet, target: "/mcp", wantStatus: http.StatusOK, wantSubstr: "docket-mcp ready"},
		{name: "root GET returns ready banner", method: http.MethodGet, target: "/", wantStatus: http.StatusOK, wantSubstr: "docket-mcp ready"},
		{name: "mcp empty POST returns 400", method: http.MethodPost, target: "/mcp", wantStatus: http.StatusBadRequest, wantSubstr: "Empty request body"},
		{name: "tasks list defaults to open", method: http.MethodGet, target: "/api/tasks", wantStatus: http.StatusOK, wantSubstr: `"Top Task 1"`, wantCacheControl: "no-cache"},
		{name: "tasks list status all includes completed", method: http.MethodGet, target: "/api/tasks?status=all", wantStatus: http.StatusOK, wantSubstr: `"Completed Task 5"`, wantCacheControl: "no-cache"},
		{name: "tasks list pages past the vikunja page cap", method: http.MethodGet, target: "/api/tasks", wantStatus: http.StatusOK, wantSubstr: `"Task 76"`, wantCacheControl: "no-cache"},
		{name: "tasks list with no matches returns empty array", method: http.MethodGet, target: "/api/tasks?status=wont_do", wantStatus: http.StatusOK, wantSubstr: `[]`, wantCacheControl: "no-cache"},
		{name: "tasks list bad status returns 400", method: http.MethodGet, target: "/api/tasks?status=bogus", wantStatus: http.StatusBadRequest, wantSubstr: "invalid status", wantCacheControl: "no-cache"},
		{name: "tasks list POST returns 405", method: http.MethodPost, target: "/api/tasks", wantStatus: http.StatusMethodNotAllowed, wantSubstr: `{"error":"method not allowed"}`, wantCacheControl: "no-cache"},
		{name: "task detail hydrates relations and comments", method: http.MethodGet, target: "/api/tasks/2", wantStatus: http.StatusOK, wantSubstr: `"Top Task 1"`, wantCacheControl: "no-cache"},
		{name: "task detail above first page hydrates full neighbor summaries", method: http.MethodGet, target: "/api/tasks/75", wantStatus: http.StatusOK, wantSubstr: `"Task 76"`, wantCacheControl: "no-cache"},
		{name: "task detail includes comments", method: http.MethodGet, target: "/api/tasks/1", wantStatus: http.StatusOK, wantSubstr: `"Great work!"`, wantCacheControl: "no-cache"},
		{name: "task detail non-numeric id returns 400", method: http.MethodGet, target: "/api/tasks/abc", wantStatus: http.StatusBadRequest, wantSubstr: `invalid task id`, wantCacheControl: "no-cache"},
		{name: "task detail missing id returns 400", method: http.MethodGet, target: "/api/tasks/", wantStatus: http.StatusBadRequest, wantSubstr: `invalid task id`, wantCacheControl: "no-cache"},
		{name: "task detail in another project returns 404", method: http.MethodGet, target: "/api/tasks/60", wantStatus: http.StatusNotFound, wantSubstr: `task 60 not found in project graph`, wantCacheControl: "no-cache"},
		{name: "task detail unknown to vikunja returns 404", method: http.MethodGet, target: "/api/tasks/999", wantStatus: http.StatusNotFound, wantSubstr: "failed to fetch task 999", wantCacheControl: "no-cache"},
		{name: "task detail upstream failure returns 502", method: http.MethodGet, target: "/api/tasks/888", wantStatus: http.StatusBadGateway, wantSubstr: "failed to fetch task 888", wantCacheControl: "no-cache"},
		{name: "task detail DELETE returns 405", method: http.MethodDelete, target: "/api/tasks/1", wantStatus: http.StatusMethodNotAllowed, wantSubstr: "method not allowed", wantCacheControl: "no-cache"},
		{name: "search returns scored hits with parent ids", method: http.MethodGet, target: "/api/search?q=subtask&limit=5", wantStatus: http.StatusOK, wantSubstr: `"Subtask 2"`, wantCacheControl: "no-cache"},
		{name: "search returns empty array when no matches", method: http.MethodGet, target: "/api/search?q=nomatch", wantStatus: http.StatusOK, wantSubstr: "[]", wantCacheControl: "no-cache"},
		{name: "search empty q returns 400", method: http.MethodGet, target: "/api/search?q=", wantStatus: http.StatusBadRequest, wantSubstr: "parameter 'q' is required", wantCacheControl: "no-cache"},
		{name: "search bad limit returns 400", method: http.MethodGet, target: "/api/search?q=x&limit=zero", wantStatus: http.StatusBadRequest, wantSubstr: `invalid limit`, wantCacheControl: "no-cache"},
		{name: "search bad status returns 400", method: http.MethodGet, target: "/api/search?q=x&status=closed", wantStatus: http.StatusBadRequest, wantSubstr: `invalid status`, wantCacheControl: "no-cache"},
		{name: "search PUT returns 405", method: http.MethodPut, target: "/api/search?q=x", wantStatus: http.StatusMethodNotAllowed, wantSubstr: "method not allowed", wantCacheControl: "no-cache"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			env.mux.ServeHTTP(w, httptest.NewRequest(tc.method, tc.target, http.NoBody))

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("body = %q, want substring %q", w.Body.String(), tc.wantSubstr)
			}
			if tc.wantCacheControl != "" {
				if got := w.Header().Get("Cache-Control"); got != tc.wantCacheControl {
					t.Fatalf("Cache-Control = %q, want %q", got, tc.wantCacheControl)
				}
				if got := w.Header().Get("Content-Type"); got != "application/json" {
					t.Fatalf("Content-Type = %q, want application/json", got)
				}
			}
		})
	}
}

func setupErrorEnvironment(t *testing.T) http.Handler {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/login" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"token": "test-token"})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"upstream down"}`))
	}))
	t.Cleanup(upstream.Close)

	cfg := &config.Config{
		Port:        "0",
		VikunjaURL:  upstream.URL,
		DatabaseURL: "postgres://mock",
		Ollama:      ollama.Config{BaseURL: upstream.URL, Model: "test-model"},
	}
	client := store.NewVikunjaClient(cfg, upstream.Client())
	failingStore := &memoryTaskStore{
		searchErr: errors.New("search db failed"),
		fetchErr:  errors.New("fetch db failed"),
	}
	olClient := ollama.NewDynamicClient(func() ollama.Config { return cfg.Ollama })
	srv := NewServer(cfg, client, failingStore, olClient)
	return NewHTTPMux(srv)
}

func TestServer_RESTUpstreamErrorsTable(t *testing.T) {
	t.Parallel()

	okEnv := setupTestEnvironment(t)
	errMux := setupErrorEnvironment(t)

	tests := []struct {
		name       string
		mux        http.Handler
		target     string
		wantStatus int
		wantSubstr string
	}{
		{name: "list tasks upstream failure returns 502", mux: errMux, target: "/api/tasks", wantStatus: http.StatusBadGateway, wantSubstr: "vikunja API returned status 500"},
		{name: "task detail upstream failure returns 502", mux: errMux, target: "/api/tasks/1", wantStatus: http.StatusBadGateway, wantSubstr: "vikunja API returned status 500"},
		{name: "task detail missing returns 404", mux: okEnv.mux, target: "/api/tasks/999", wantStatus: http.StatusNotFound, wantSubstr: "vikunja API returned status 404"},
		{name: "task in other project returns 404", mux: okEnv.mux, target: "/api/tasks/60", wantStatus: http.StatusNotFound, wantSubstr: "not found in project graph"},
		{name: "search upstream failure returns 502", mux: errMux, target: "/api/search?q=test", wantStatus: http.StatusBadGateway, wantSubstr: "search db failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, tc.target, http.NoBody)
			rec := httptest.NewRecorder()
			tc.mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body = %q, want substring %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func postMCP(t *testing.T, handler http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return postMCPWithHeaders(t, handler, path, body, nil)
}

func postMCPWithHeaders(t *testing.T, handler http.Handler, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

type toolCallResponse struct {
	Result struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	} `json:"result"`
	Error any `json:"error"`
}

func callTool(t *testing.T, handler http.Handler, name, args string) toolCallResponse {
	t.Helper()
	return callToolWithHeaders(t, handler, name, args, nil)
}

func callToolWithHeaders(t *testing.T, handler http.Handler, name, args string, headers map[string]string) toolCallResponse {
	t.Helper()
	body := `{"jsonrpc":"2.0","id":100,"method":"tools/call","params":{"name":` + strconv.Quote(name) + `,"arguments":` + args + `}}`
	w := postMCPWithHeaders(t, handler, "/mcp", body, headers)
	var resp toolCallResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unexpected response %q (status %d, err %v)", w.Body.String(), w.Code, err)
	}
	return resp
}

func TestServerInstructions(t *testing.T) {
	t.Parallel()

	env := setupTestEnvironment(t)
	ts := httptest.NewServer(env.mux)
	t.Cleanup(ts.Close)

	tests := []struct {
		name           string
		path           string
		wantServerName string
		wantTool       string
	}{
		{name: "initialize advertises server identity and task workflow instructions", path: "/mcp", wantServerName: "docket-mcp", wantTool: "blocked_by"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := mcp.NewClient(&mcp.Implementation{Name: "instructions-test", Version: "1.0.0"}, nil)
			session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
				Endpoint:             ts.URL + tc.path,
				HTTPClient:           ts.Client(),
				DisableStandaloneSSE: true,
			}, nil)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer func() { _ = session.Close() }()

			if got := session.InitializeResult().ServerInfo.Name; got != tc.wantServerName {
				t.Errorf("server name = %q, want %q", got, tc.wantServerName)
			}
			got := session.InitializeResult().Instructions
			if got == "" || len(got) > 1024 {
				t.Fatalf("instructions length = %d, want 1..1024", len(got))
			}
			if !strings.Contains(got, tc.wantTool) {
				t.Errorf("instructions missing %q: %q", tc.wantTool, got)
			}
		})
	}
}

// TestServer_ToolsListSchema locks the enum and default values tools/list advertises.
func TestServer_ToolsListSchema(t *testing.T) {
	t.Parallel()

	env := setupTestEnvironment(t)
	w := postMCP(t, env.mux, "/mcp", `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`)
	var resp struct {
		Result struct {
			Tools []struct {
				Name        string `json:"name"`
				InputSchema struct {
					Properties map[string]struct {
						Enum    []string `json:"enum"`
						Default any      `json:"default"`
					} `json:"properties"`
				} `json:"inputSchema"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode tools/list %q: %v", w.Body.String(), err)
	}
	enums := make(map[string]string)
	defaults := make(map[string]any)
	for _, tool := range resp.Result.Tools {
		for name, prop := range tool.InputSchema.Properties {
			if len(prop.Enum) > 0 {
				enums[tool.Name+"."+name] = strings.Join(prop.Enum, ",")
			}
			if prop.Default != nil {
				defaults[tool.Name+"."+name] = prop.Default
			}
		}
	}
	if len(resp.Result.Tools) != 11 {
		t.Fatalf("tools/list returned %d tools, want 11", len(resp.Result.Tools))
	}
	wantEnums := map[string]string{
		"list.status":      "open,completed,wont_do,all",
		"search.status":    "open,completed,wont_do,all",
		"link.type":        "parent,blocks,related",
		"close.resolution": "completed,wont_do",
	}
	if !maps.Equal(enums, wantEnums) {
		t.Errorf("enums = %v, want %v", enums, wantEnums)
	}
	wantDefaults := map[string]any{"list.status": "open", "search.status": "open", "search.limit": float64(10), "link.remove": false}
	if !maps.Equal(defaults, wantDefaults) {
		t.Errorf("defaults = %v, want %v", defaults, wantDefaults)
	}
}

func TestServer_ToolsExecutionTable(t *testing.T) {
	t.Parallel()

	type rel = VikunjaRelationRecord

	tests := []struct {
		name          string
		toolName      string
		args          string
		wantSubstr    string
		wantError     bool
		wantRelations []VikunjaRelationRecord
		vikunjaDown   bool
	}{
		{name: "ready top level tasks", toolName: "ready", args: `{}`, wantSubstr: `"id":1`},
		{name: "ready includes tasks past first page", toolName: "ready", args: `{}`, wantSubstr: `{"id":75,"title":"High Task 75","status":"open"}`},
		{name: "ready scoped parent tasks", toolName: "ready", args: `{"parent_id":1}`, wantSubstr: `"id":2`},
		{name: "list open tasks", toolName: "list", args: `{"status":"open"}`, wantSubstr: `"id":1`},
		{name: "list exposes per-project index not global id", toolName: "list", args: `{"status":"open"}`, wantSubstr: `{"id":317,"title":"Million Task 317","status":"open"}`},
		{name: "show resolves per-project index to global task", toolName: "show", args: `{"id":317}`, wantSubstr: `{"id":317,"title":"Million Task 317","status":"open"`},
		{name: "comment resolves per-project index", toolName: "comment", args: `{"id":317,"text":"on index"}`, wantSubstr: "comment added to task 317"},
		{name: "search exposes per-project index not global id", toolName: "search", args: `{"query":"million","status":"open","limit":5}`, wantSubstr: `{"id":317,"title":"Million Task 317","status":"open","score":0.75}`},
		{name: "update resolves per-project index", toolName: "update", args: `{"id":317,"title":"Renamed 317"}`, wantSubstr: `{"id":317,"title":"Renamed 317","status":"open"}`},
		{name: "link resolves per-project index to global relation", toolName: "link", args: `{"id":4,"target_id":317,"type":"blocks"}`, wantSubstr: "relation blocks between task 4 and 317 created", wantRelations: []rel{{4, 1000349, "blocking"}}},
		{name: "close resolves per-project index", toolName: "close", args: `{"id":317,"resolution":"completed","reason":"done"}`, wantSubstr: `{"id":317,"title":"Million Task 317","status":"completed"}`},
		{name: "create blocks resolves per-project index to global relation", toolName: "create", args: `{"title":"Blocks Million","blocks":[317]}`, wantSubstr: `"blocks":[317]`, wantRelations: []rel{{100, 1000349, "blocking"}}},
		{name: "link remove resolves per-project index", toolName: "link", args: `{"id":4,"target_id":317,"type":"blocks","remove":true}`, wantSubstr: "relation blocks between task 4 and 317 removed"},
		{name: "comment unknown index fails", toolName: "comment", args: `{"id":999,"text":"x"}`, wantSubstr: "failed to add comment to task 999", wantError: true},
		{name: "close wont_do resolves per-project index", toolName: "close", args: `{"id":317,"resolution":"wont_do","reason":"drop"}`, wantSubstr: `{"id":317,"title":"Million Task 317","status":"wont_do"}`},
		{name: "update unknown index fails", toolName: "update", args: `{"id":999,"title":"x"}`, wantSubstr: "failed to fetch task 999", wantError: true},
		{name: "link foreign task fails", toolName: "link", args: `{"id":4,"target_id":60,"type":"related"}`, wantSubstr: "task 60 not found in project graph", wantError: true},
		{name: "comment foreign task fails", toolName: "comment", args: `{"id":60,"text":"x"}`, wantSubstr: "task 60 not found in project graph", wantError: true},
		{name: "create task id collision with existing index does not resolve created id", toolName: "create", args: `{"title":"Created With Collision","blocks":[4]}`, wantSubstr: `"title":"Created With Collision"`, wantRelations: []rel{{100, 4, "blocking"}}},
		{name: "comment upstream comment failure", toolName: "comment", args: `{"id":5,"text":"x"}`, wantSubstr: "failed to add comment to task 5", wantError: true},
		{name: "link remove unknown target fails", toolName: "link", args: `{"id":1,"target_id":999,"type":"blocks","remove":true}`, wantSubstr: "failed to remove relation", wantError: true},
		{name: "link remove unknown source fails", toolName: "link", args: `{"id":999,"target_id":1,"type":"blocks","remove":true}`, wantSubstr: "failed to remove relation", wantError: true},
		{name: "link create unknown target fails", toolName: "link", args: `{"id":1,"target_id":999,"type":"related"}`, wantSubstr: "failed to create relation", wantError: true},
		{name: "link create unknown source fails", toolName: "link", args: `{"id":999,"target_id":1,"type":"related"}`, wantSubstr: "failed to create relation", wantError: true},
		{name: "close completed with comment failure still closes", toolName: "close", args: `{"id":5,"resolution":"completed","reason":"again"}`, wantSubstr: `{"id":5,"title":"Completed Task 5","status":"completed"}`},
		{name: "list all tasks", toolName: "list", args: `{"status":"all"}`, wantSubstr: `"id":5`},
		{name: "list includes tasks past first page", toolName: "list", args: `{"status":"open"}`, wantSubstr: `{"id":76,"title":"Task 76","status":"open","blocked_by":[75]}`},
		{name: "list scoped by parent", toolName: "list", args: `{"status":"open","parent_id":1}`, wantSubstr: `"id":2`},
		{name: "search tasks success", toolName: "search", args: `{"query":"test","status":"open","limit":5}`, wantSubstr: `"id":1`},
		{name: "search missing query error", toolName: "search", args: `{"query":""}`, wantSubstr: "parameter 'query' is required", wantError: true},
		{name: "show task details", toolName: "show", args: `{"id":1}`, wantSubstr: `"description":"Task description"`},
		{name: "show task with comments failure", toolName: "show", args: `{"id":5}`, wantSubstr: `{"id":5,"title":"Completed Task 5","status":"completed"`},
		{name: "show task with parent", toolName: "show", args: `{"id":2}`, wantSubstr: `"parent":{"id":1,"title":"Top Task 1","status":"open","blocks":[3]}`},
		{name: "show task with related", toolName: "show", args: `{"id":4}`, wantSubstr: `"related":[{"id":1`},
		{name: "show task with blocked_by", toolName: "show", args: `{"id":3}`, wantSubstr: `"blocked_by":[{"id":1,"title":"Top Task 1","status":"open","blocks":[3]}]`},
		{name: "show task above first page", toolName: "show", args: `{"id":75}`, wantSubstr: `"blocks":[{"id":76,"title":"Task 76","status":"open","blocked_by":[75]}]`},
		{name: "show subtask neighbor carries its parent", toolName: "show", args: `{"id":6}`, wantSubstr: `"subtasks":[{"id":7,"title":"Subtask 7","status":"open","parent_id":6}]`},
		{name: "show foreign neighbor falls back to embedded row", toolName: "show", args: `{"id":76}`, wantSubstr: `"related":[{"id":60,"title":"Foreign Task 60","status":"open"}]`},
		{name: "show invalid id error", toolName: "show", args: `{"id":-1}`, wantSubstr: "must be a positive integer", wantError: true},
		{name: "show task in another project not found", toolName: "show", args: `{"id":60}`, wantSubstr: "task 60 not found in project graph", wantError: true},
		{name: "show task unknown to vikunja", toolName: "show", args: `{"id":999}`, wantSubstr: "failed to fetch task 999", wantError: true},
		{name: "create task success", toolName: "create", args: `{"title":"New Task","description":"A brand new task","blocks":[4]}`, wantSubstr: `"title":"New Task"`, wantRelations: []rel{{100, 4, "blocking"}}},
		{name: "create task with parent, blocked_by, and related", toolName: "create", args: `{"title":"All Relations","parent_id":1,"blocked_by":[4],"related":[2]}`, wantSubstr: `"parent_id":1,"blocked_by":[4]`, wantRelations: []rel{{100, 1, "parenttask"}, {4, 100, "blocking"}, {100, 2, "related"}}},
		{name: "create blocked by task above first page", toolName: "create", args: `{"title":"After 75","blocked_by":[75],"blocks":[3]}`, wantSubstr: `"blocked_by":[75],"blocks":[3]`, wantRelations: []rel{{100, 3, "blocking"}, {75, 100, "blocking"}}},
		{name: "create forming blocking cycle rejected", toolName: "create", args: `{"title":"Cycle","blocks":[1],"blocked_by":[3]}`, wantSubstr: "cycle detected: task 1 already transitively blocks task 3", wantError: true},
		{name: "create with invalid parent_id", toolName: "create", args: `{"title":"Bad Parent","parent_id":999}`, wantSubstr: "parent task 999 not found", wantError: true},
		{name: "create with invalid blocks", toolName: "create", args: `{"title":"Bad Blocks","blocks":[999]}`, wantSubstr: "blocked task 999 not found", wantError: true},
		{name: "create with invalid related", toolName: "create", args: `{"title":"Bad Rel","related":[999]}`, wantSubstr: "related task 999 not found", wantError: true},
		{name: "create with blocker in another project", toolName: "create", args: `{"title":"Foreign","blocked_by":[60]}`, wantSubstr: "blocker task 60 not found", wantError: true},
		{name: "create nonexistent blocker error", toolName: "create", args: `{"title":"Task","blocked_by":[999]}`, wantSubstr: "blocker task 999 not found", wantError: true},
		{name: "create blocker fetch failure", toolName: "create", args: `{"title":"Task","blocked_by":[888]}`, wantSubstr: "failed to fetch task 888", wantError: true},
		{name: "create empty title error", toolName: "create", args: `{"title":""}`, wantSubstr: "cannot be empty", wantError: true},
		{name: "update task success", toolName: "update", args: `{"id":1,"title":"Updated Title"}`, wantSubstr: `"title":"Updated Title"`},
		{name: "update task description", toolName: "update", args: `{"id":1,"description":"Updated description text"}`, wantSubstr: `{"id":1,"title":"Top Task 1","status":"open"}`},
		{name: "update invalid id error", toolName: "update", args: `{"id":0}`, wantSubstr: "must be a positive integer", wantError: true},
		{name: "comment task success", toolName: "comment", args: `{"id":1,"text":"Nice progress"}`, wantSubstr: "comment added to task 1"},
		{name: "comment missing text error", toolName: "comment", args: `{"id":1,"text":""}`, wantSubstr: "required", wantError: true},
		{name: "link tasks blocks success", toolName: "link", args: `{"id":4,"target_id":2,"type":"blocks"}`, wantSubstr: "created", wantRelations: []rel{{4, 2, "blocking"}}},
		{name: "link blocks to task above first page", toolName: "link", args: `{"id":4,"target_id":75,"type":"blocks"}`, wantSubstr: "relation blocks between task 4 and 75 created", wantRelations: []rel{{4, 75, "blocking"}}},
		{name: "link blocks cycle above first page rejected", toolName: "link", args: `{"id":76,"target_id":75,"type":"blocks"}`, wantSubstr: "cycle detected: task 75 already transitively blocks task 76", wantError: true},
		{name: "link blocks cycle check fetch failure", toolName: "link", args: `{"id":4,"target_id":888,"type":"blocks"}`, wantSubstr: "failed to fetch task 888", wantError: true},
		{name: "link create error", toolName: "link", args: `{"id":4,"target_id":888,"type":"related"}`, wantSubstr: "failed to create relation", wantError: true},
		{name: "link remove error", toolName: "link", args: `{"id":4,"target_id":888,"type":"blocks","remove":true}`, wantSubstr: "failed to remove relation", wantError: true},
		{name: "link invalid id error", toolName: "link", args: `{"id":0,"target_id":2,"type":"blocks"}`, wantSubstr: "must be positive integers", wantError: true},
		{name: "link invalid relation type error", toolName: "link", args: `{"id":4,"target_id":2,"type":"invalid"}`, wantSubstr: "does not equal any of: [parent blocks related]", wantError: true},
		{name: "link tasks parent success", toolName: "link", args: `{"id":4,"target_id":1,"type":"parent"}`, wantSubstr: "created", wantRelations: []rel{{4, 1, "parenttask"}}},
		{name: "link tasks related success", toolName: "link", args: `{"id":4,"target_id":2,"type":"related"}`, wantSubstr: "created", wantRelations: []rel{{4, 2, "related"}}},
		{name: "link self-link rejected", toolName: "link", args: `{"id":1,"target_id":1,"type":"parent"}`, wantSubstr: "self-link cycle", wantError: true},
		{name: "link cycle rejected", toolName: "link", args: `{"id":1,"target_id":2,"type":"parent"}`, wantSubstr: "cycle detected: task 1 is already an ancestor of task 2", wantError: true},
		{name: "link remove relation", toolName: "link", args: `{"id":1,"target_id":2,"type":"parent","remove":true}`, wantSubstr: "removed"},
		{name: "close invalid id error", toolName: "close", args: `{"id":0,"resolution":"completed","reason":"done"}`, wantSubstr: "must be a positive integer", wantError: true},
		{name: "close completed rejected with open subtask", toolName: "close", args: `{"id":1,"resolution":"completed","reason":"done"}`, wantSubstr: "open subtasks: [2]", wantError: true},
		{name: "close completed rejected with open blocker", toolName: "close", args: `{"id":76,"resolution":"completed","reason":"done"}`, wantSubstr: "open blockers: [75]", wantError: true},
		{name: "close completed success on leaf task", toolName: "close", args: `{"id":4,"resolution":"completed","reason":"verified"}`, wantSubstr: `{"id":4,"title":"Independent Task 4","status":"completed"}`},
		{name: "close above first page unblocks dependent", toolName: "close", args: `{"id":75,"resolution":"completed","reason":"shipped"}`, wantSubstr: `{"id":75,"title":"High Task 75","status":"completed","unblocked_ids":[76]}`},
		{name: "close wont_do cascades", toolName: "close", args: `{"id":6,"resolution":"wont_do","reason":"abandoned"}`, wantSubstr: `{"id":6,"title":"Task 6","status":"wont_do"}`},
		{name: "close wont_do rejected when blocking open task", toolName: "close", args: `{"id":1,"resolution":"wont_do","reason":"abandoned"}`, wantSubstr: "blocks open tasks: [3]", wantError: true},
		{name: "close task in another project not found", toolName: "close", args: `{"id":60,"resolution":"completed","reason":"done"}`, wantSubstr: "task 60 not found in graph", wantError: true},
		{name: "close task unknown to vikunja not found", toolName: "close", args: `{"id":999,"resolution":"wont_do","reason":"done"}`, wantSubstr: "task 999 not found in graph", wantError: true},
		{name: "close fetch failure", toolName: "close", args: `{"id":888,"resolution":"completed","reason":"done"}`, wantSubstr: "failed to fetch task 888", wantError: true},
		{name: "close empty reason error", toolName: "close", args: `{"id":1,"resolution":"completed","reason":""}`, wantSubstr: "cannot be empty", wantError: true},
		{name: "list status defaults to open", toolName: "list", args: `{}`, wantSubstr: `[{"id":1,"title":"Top Task 1","status":"open","blocks":[3]},{"id":2,`},
		{name: "legacy docket_ prefix resolves tool", toolName: "docket_comment", args: `{"id":1,"text":"via alias"}`, wantSubstr: "comment added to task 1"},
		{name: "ready upstream failure", toolName: "ready", args: `{}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "list upstream failure", toolName: "list", args: `{}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "search store failure", toolName: "search", args: `{"query":"error test"}`, wantSubstr: "search db failed", wantError: true, vikunjaDown: true},
		{name: "show upstream failure", toolName: "show", args: `{"id":1}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "create upstream failure", toolName: "create", args: `{"title":"Task"}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "update upstream failure", toolName: "update", args: `{"id":1,"title":"Task"}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "comment upstream failure", toolName: "comment", args: `{"id":1,"text":"Hello"}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "link upstream failure", toolName: "link", args: `{"id":1,"target_id":2,"type":"blocks"}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "close upstream failure", toolName: "close", args: `{"id":1,"resolution":"completed","reason":"done"}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mux http.Handler
			state := &fakeVikunjaServerState{}
			if tc.vikunjaDown {
				mux = setupErrorEnvironment(t)
			} else {
				env := setupTestEnvironment(t)
				mux = env.mux
				state = env.state
			}
			resp := callTool(t, mux, tc.toolName, tc.args)
			if resp.Error != nil || len(resp.Result.Content) != 1 {
				t.Fatalf("unexpected response %+v", resp)
			}
			text := resp.Result.Content[0].Text
			if !strings.Contains(text, tc.wantSubstr) {
				t.Fatalf("result = %q, want substring %q", text, tc.wantSubstr)
			}
			if resp.Result.IsError != tc.wantError {
				t.Fatalf("isError = %v, want %v (result %q)", resp.Result.IsError, tc.wantError, text)
			}
			state.mu.Lock()
			defer state.mu.Unlock()
			if !slices.Equal(state.recordedRelations, tc.wantRelations) {
				t.Fatalf("recorded relations = %+v, want %+v", state.recordedRelations, tc.wantRelations)
			}
		})
	}
}

func TestServer_ClaimAndFilterTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		toolName    string
		args        string
		wantSubstr  string
		wantError   bool
		vikunjaDown bool
	}{
		{name: "claim upstream failure", toolName: "claim", args: `{"id":4,"agent":"eri"}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "unclaim upstream failure", toolName: "unclaim", args: `{"id":4}`, wantSubstr: "failed to get default project: vikunja API returned status 500", wantError: true, vikunjaDown: true},
		{name: "claim fetch failure", toolName: "claim", args: `{"id":888,"agent":"eri"}`, wantSubstr: "failed to fetch task 888", wantError: true},
		{name: "unclaim fetch failure", toolName: "unclaim", args: `{"id":888}`, wantSubstr: "failed to fetch task 888", wantError: true},
		{name: "claim with turn_id only", toolName: "claim", args: `{"id":4,"turn_id":"turn-only"}`, wantSubstr: `{"id":4,"title":"Independent Task 4"`},
		{name: "claim deep subtask rejected", toolName: "claim", args: `{"id":8,"agent":"eri"}`, wantSubstr: "task is a subtask (top-level parent is task 6; only top-level tasks can be claimed)", wantError: true},
		{name: "claim top level task", toolName: "claim", args: `{"id":4,"agent":"eri","turn_id":"turn-1"}`, wantSubstr: `{"id":4,"title":"Independent Task 4","status":"open"`},
		{name: "claim subtask rejected", toolName: "claim", args: `{"id":2,"agent":"eri","turn_id":"turn-1"}`, wantSubstr: "task is a subtask (top-level parent is task 1; only top-level tasks can be claimed)", wantError: true},
		{name: "claim closed task rejected", toolName: "claim", args: `{"id":5,"agent":"eri","turn_id":"turn-1"}`, wantSubstr: "cannot claim task 5: task is completed (must be open)", wantError: true},
		{name: "claim missing agent and turn_id rejected", toolName: "claim", args: `{"id":4}`, wantSubstr: "either agent or turn_id must be provided", wantError: true},
		{name: "claim invalid id error", toolName: "claim", args: `{"id":0,"agent":"eri"}`, wantSubstr: "must be a positive integer", wantError: true},
		{name: "claim nonexistent task rejected", toolName: "claim", args: `{"id":999,"agent":"eri"}`, wantSubstr: "task 999 not found", wantError: true},
		{name: "claim task in another project rejected", toolName: "claim", args: `{"id":60,"agent":"eri"}`, wantSubstr: "task 60 not found in project graph", wantError: true},
		{name: "unclaim task", toolName: "unclaim", args: `{"id":4}`, wantSubstr: `{"id":4,"title":"Independent Task 4","status":"open"`},
		{name: "unclaim invalid id error", toolName: "unclaim", args: `{"id":-1}`, wantSubstr: "must be a positive integer", wantError: true},
		{name: "unclaim nonexistent task rejected", toolName: "unclaim", args: `{"id":999}`, wantSubstr: "task 999 not found", wantError: true},
		{name: "list claimed false filter", toolName: "list", args: `{"claimed":false}`, wantSubstr: `"id":1`},
		{name: "list claimed true filter empty", toolName: "list", args: `{"claimed":true}`, wantSubstr: `[]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mux http.Handler
			if tc.vikunjaDown {
				mux = setupErrorEnvironment(t)
			} else {
				env := setupTestEnvironment(t)
				mux = env.mux
			}
			resp := callTool(t, mux, tc.toolName, tc.args)
			if resp.Error != nil || len(resp.Result.Content) != 1 {
				t.Fatalf("unexpected response %+v", resp)
			}
			text := resp.Result.Content[0].Text
			if !strings.Contains(text, tc.wantSubstr) {
				t.Fatalf("result = %q, want substring %q", text, tc.wantSubstr)
			}
			if resp.Result.IsError != tc.wantError {
				t.Fatalf("isError = %v, want %v (result %q)", resp.Result.IsError, tc.wantError, text)
			}
		})
	}
}

func TestServer_ClaimWithHeaders(t *testing.T) {
	t.Parallel()

	env := setupTestEnvironment(t)
	headers := map[string]string{
		"X-Aerial-Agent":   "eri-runner",
		"X-Aerial-Turn-Id": "turn-test-456",
	}
	resp := callToolWithHeaders(t, env.mux, "claim", `{"id":4}`, headers)
	if resp.Result.IsError || len(resp.Result.Content) != 1 {
		t.Fatalf("unexpected error response: %+v", resp)
	}
	text := resp.Result.Content[0].Text
	if !strings.Contains(text, `"claimed_by_agent":"eri-runner"`) || !strings.Contains(text, `"claimed_by_turn_id":"turn-test-456"`) {
		t.Fatalf("claim response missing header values: %s", text)
	}
	taskStore, ok := env.srv.store.(*memoryTaskStore)
	if !ok {
		t.Fatalf("srv.store = %T, want *memoryTaskStore", env.srv.store)
	}
	taskStore.mu.Lock()
	claim := taskStore.claims[4]
	taskStore.mu.Unlock()
	if claim.agent != "eri-runner" || claim.turnID != "turn-test-456" {
		t.Fatalf("store claim = %+v, want agent eri-runner, turn turn-test-456", claim)
	}
}

func TestServer_ClaimAlreadyClaimed(t *testing.T) {
	t.Parallel()

	env := setupTestEnvironment(t)
	resp1 := callTool(t, env.mux, "claim", `{"id":4,"agent":"eri-1","turn_id":"turn-1"}`)
	if resp1.Result.IsError {
		t.Fatalf("first claim failed: %+v", resp1)
	}

	resp2 := callTool(t, env.mux, "claim", `{"id":4,"agent":"eri-2","turn_id":"turn-2"}`)
	if !resp2.Result.IsError {
		t.Fatalf("second claim succeeded, want error: %+v", resp2)
	}
	if !strings.Contains(resp2.Result.Content[0].Text, "already claimed") {
		t.Fatalf("unexpected error text: %s", resp2.Result.Content[0].Text)
	}

	respUnclaim := callTool(t, env.mux, "unclaim", `{"id":4}`)
	if respUnclaim.Result.IsError {
		t.Fatalf("unclaim failed: %+v", respUnclaim)
	}

	resp3 := callTool(t, env.mux, "claim", `{"id":4,"agent":"eri-2","turn_id":"turn-2"}`)
	if resp3.Result.IsError {
		t.Fatalf("third claim after unclaim failed: %+v", resp3)
	}
}

type testConnector struct {
	conn *testConn
}

func (c *testConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

func (*testConnector) Driver() driver.Driver {
	return &testDriver{}
}

type testDriver struct{}

func (*testDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("open not supported")
}

type testConn struct{}

func (*testConn) Prepare(string) (driver.Stmt, error) { return nil, nil }
func (*testConn) Close() error                        { return nil }
func (*testConn) Begin() (driver.Tx, error)           { return nil, nil }

func TestRunServer_Execution(t *testing.T) {
	tests := []struct {
		name              string
		port              string
		databaseURL       string
		dbErr             error
		cancelInit        bool
		useDefaultSQLOpen bool
		wantErr           string
	}{
		{name: "clean shutdown on context cancel", port: "0", databaseURL: "postgres://mock", cancelInit: true},
		{name: "clean shutdown with default pgx sqlOpen", port: "0", databaseURL: "postgres://mock:5432/mock", cancelInit: true, useDefaultSQLOpen: true},
		{name: "unconfigured database URL returns error", port: "0", wantErr: "database URL is not configured"},
		{name: "database open error returned", port: "0", databaseURL: "postgres://mock", dbErr: errors.New("db open error"), wantErr: "db open error"},
		{name: "invalid port returns listen error", port: "-1", databaseURL: "postgres://mock", wantErr: "listen tcp: address -1: invalid port"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var sqlOpen func(string, string) (*sql.DB, error)
			if tc.databaseURL != "" && !tc.useDefaultSQLOpen {
				sqlOpen = func(string, string) (*sql.DB, error) {
					if tc.dbErr != nil {
						return nil, tc.dbErr
					}
					return sql.OpenDB(&testConnector{conn: &testConn{}}), nil
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelInit {
				cancel()
			}

			err := RunServer(ctx, &config.Config{Port: tc.port, DatabaseURL: tc.databaseURL}, sqlOpen)
			if errors.Is(err, http.ErrServerClosed) || errors.Is(err, context.Canceled) {
				err = nil
			}
			if got := errText(err); !strings.Contains(got, tc.wantErr) || (tc.wantErr == "") != (err == nil) {
				t.Fatalf("runServer err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func closeTestServer(t *testing.T, updateErr error) (*Server, *vikunjaNodeSource) {
	t.Helper()
	env := setupTestEnvironment(t)
	taskStore, ok := env.srv.store.(*memoryTaskStore)
	if !ok {
		t.Fatalf("store = %T, want *memoryTaskStore", env.srv.store)
	}
	taskStore.updateErr = updateErr
	src, err := env.srv.newNodeSource(context.Background())
	if err != nil {
		t.Fatalf("newNodeSource = %v", err)
	}
	return env.srv, src
}

func TestServer_CloseCompleted_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		taskID    int64
		updateErr error
		wantErr   string
	}{
		{"fetch failure returns error", 888, nil, `failed to get task: vikunja API returned status 500: {"error":"upstream down"}`},
		{"resolution update failure is logged only", 4, errors.New("resolution db failed"), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, src := closeTestServer(t, tc.updateErr)
			err := srv.closeCompleted(context.Background(), src, tc.taskID, "reason", time.Now().UTC())
			if got := errText(err); got != tc.wantErr {
				t.Fatalf("closeCompleted err = %q, want %q", got, tc.wantErr)
			}
		})
	}
}

func TestServer_CloseWontDo_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		taskID     int64
		openSubIDs []int64
		updateErr  error
		wantErr    string
	}{
		{"fetch failure returns error", 888, nil, nil, `failed to get task: vikunja API returned status 500: {"error":"upstream down"}`},
		{"resolution update failure is logged only", 4, nil, errors.New("resolution db failed"), ""},
		{"subtasks logged only", 4, []int64{999, 888}, errors.New("resolution db failed"), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, src := closeTestServer(t, tc.updateErr)
			err := srv.closeWontDo(context.Background(), src, tc.taskID, "reason", tc.openSubIDs, time.Now().UTC())
			if got := errText(err); got != tc.wantErr {
				t.Fatalf("closeWontDo err = %q, want %q", got, tc.wantErr)
			}
		})
	}
}

func TestServer_WireCreateRelations_Errors(t *testing.T) {
	t.Parallel()

	p888 := int64(888)
	tests := []struct {
		name          string
		taskGlobalID  int64
		parentID      *int64
		blocks        []int64
		blockedBy     []int64
		related       []int64
		wantRelations []VikunjaRelationRecord
	}{
		{
			name:         "resolve errors logged and skipped",
			taskGlobalID: 100,
			parentID:     &p888,
			blocks:       []int64{888},
			blockedBy:    []int64{888},
			related:      []int64{888},
		},
		{
			name:         "create relation failure logged",
			taskGlobalID: 888,
			blocks:       []int64{4},
		},
		{
			name:          "resolved relations recorded while unresolved are skipped",
			taskGlobalID:  100,
			parentID:      &p888,
			blocks:        []int64{4},
			blockedBy:     []int64{2},
			related:       []int64{1},
			wantRelations: []VikunjaRelationRecord{{100, 4, "blocking"}, {2, 100, "blocking"}, {100, 1, "related"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := setupTestEnvironment(t)
			src, err := env.srv.newNodeSource(context.Background())
			if err != nil {
				t.Fatalf("newNodeSource = %v", err)
			}
			env.srv.wireCreateRelations(context.Background(), src, tc.taskGlobalID, tc.parentID, tc.blocks, tc.blockedBy, tc.related)
			if !slices.Equal(env.state.recordedRelations, tc.wantRelations) {
				t.Fatalf("recorded relations = %+v, want %+v", env.state.recordedRelations, tc.wantRelations)
			}
		})
	}
}

func TestServer_CreateEmbeddingFailureLogged(t *testing.T) {
	t.Parallel()

	env := setupTestEnvironment(t)
	taskStore, ok := env.srv.store.(*memoryTaskStore)
	if !ok {
		t.Fatalf("store = %T, want *memoryTaskStore", env.srv.store)
	}
	taskStore.updateErr = errors.New("embedding db failed")
	got, isErr := env.srv.toolCreate(context.Background(), CreateArgs{Title: "Created Despite Embedding Failure"})
	if isErr || !strings.Contains(got, `"title":"Created Despite Embedding Failure"`) {
		t.Fatalf("toolCreate = %q, %v; want summary despite embedding store failure", got, isErr)
	}
	taskStore.mu.Lock()
	calls := taskStore.embeddingResolutionCalls
	taskStore.mu.Unlock()
	if calls != 1 {
		t.Fatalf("UpdateTaskEmbeddingAndResolution calls = %d, want 1 (embedding path reached)", calls)
	}
}

func TestFindTopParentID(t *testing.T) {
	t.Parallel()
	env := setupTestEnvironment(t)
	src, err := env.srv.newNodeSource(context.Background())
	if err != nil {
		t.Fatalf("newNodeSource: %v", err)
	}
	if got := findTopParentID(context.Background(), src, 1, nil); got != 1 {
		t.Fatalf("findTopParentID(nil) = %d, want 1", got)
	}
}

func TestVikunjaNodeSource_Methods(t *testing.T) {
	t.Parallel()
	env := setupTestEnvironment(t)
	src, err := env.srv.newNodeSource(context.Background())
	if err != nil {
		t.Fatalf("newNodeSource: %v", err)
	}

	_, err = src.ResolveGlobalID(context.Background(), 60)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("ResolveGlobalID(60) = %v, want not found", err)
	}

	_, err = src.ResolveGlobalID(context.Background(), 888)
	if err == nil {
		t.Fatalf("ResolveGlobalID(888) expected error, got nil")
	}

	_, err = src.GetTask(context.Background(), 888)
	if err == nil {
		t.Fatalf("GetTask(888) expected error, got nil")
	}

	task60, err := src.GetTask(context.Background(), 60)
	if err != nil || task60.ID != 60 {
		t.Fatalf("GetTask(60) = %+v, %v", task60, err)
	}

	// GetTask via globalToDisplay mapping
	task317, err := src.GetTask(context.Background(), 1000349)
	if err != nil || task317.ID != 1000349 {
		t.Fatalf("GetTask(1000349) = %+v, %v", task317, err)
	}

	// ResolveGlobalID when not in displayToGlobal cache
	delete(src.displayToGlobal, 75)
	global75, err := src.ResolveGlobalID(context.Background(), 75)
	if err != nil || global75 != 75 {
		t.Fatalf("ResolveGlobalID(75) = %d, %v, want 75", global75, err)
	}
}

func TestServer_ClaimStoreError(t *testing.T) {
	t.Parallel()
	env := setupTestEnvironment(t)
	taskStore, ok := env.srv.store.(*memoryTaskStore)
	if !ok {
		t.Fatalf("srv.store = %T, want *memoryTaskStore", env.srv.store)
	}
	taskStore.claimErr = errors.New("db claim error")
	resp := callTool(t, env.mux, "claim", `{"id":4,"agent":"eri"}`)
	if !resp.Result.IsError || !strings.Contains(resp.Result.Content[0].Text, "db claim error") {
		t.Fatalf("expected claim db error, got %+v", resp)
	}
}

func TestServer_UnclaimStoreError(t *testing.T) {
	t.Parallel()
	env := setupTestEnvironment(t)
	taskStore, ok := env.srv.store.(*memoryTaskStore)
	if !ok {
		t.Fatalf("srv.store = %T, want *memoryTaskStore", env.srv.store)
	}
	taskStore.unclaimErr = errors.New("db unclaim error")
	resp := callTool(t, env.mux, "unclaim", `{"id":4}`)
	if !resp.Result.IsError || !strings.Contains(resp.Result.Content[0].Text, "db unclaim error") {
		t.Fatalf("expected unclaim db error, got %+v", resp)
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
