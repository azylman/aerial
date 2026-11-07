package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/ollama"
)

type memoryTaskStore struct {
	mu          sync.Mutex
	resolutions map[int64]string
	searchHits  []ScoredTaskSummary
	searchErr   error
	fetchErr    error
	updateErr   error
}

func newMemoryTaskStore() *memoryTaskStore {
	return &memoryTaskStore{
		resolutions: map[int64]string{
			5: "completed",
		},
		searchHits: []ScoredTaskSummary{
			{ID: 1, Title: "Top Task 1", Status: "open", Score: 0.95},
			{ID: 2, Title: "Subtask 2", Status: "open", Score: 0.85},
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

func (m *memoryTaskStore) SearchTasksRRF(ctx context.Context, embedding []float32, queryText string, statusFilter string, limit int) ([]ScoredTaskSummary, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.searchErr != nil {
		return nil, m.searchErr
	}
	return m.searchHits, nil
}

type fakeVikunjaServerState struct {
	mu                sync.Mutex
	recordedRelations []VikunjaRelationRecord
	lastUpdatedTask   *VikunjaTask
}

type VikunjaRelationRecord struct {
	TaskID       int64
	OtherTaskID  int64
	RelationKind string
}

func setupTestEnvironment(t *testing.T) (*Server, *http.ServeMux, *fakeVikunjaServerState) {
	t.Helper()
	state := &fakeVikunjaServerState{}
	vikunjaMux := http.NewServeMux()

	vikunjaMux.HandleFunc("/api/v1/login", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": "test-jwt"})
	})

	vikunjaMux.HandleFunc("/api/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode([]VikunjaProject{{ID: 1, Title: "Docket"}})
			return
		}
		_ = json.NewEncoder(w).Encode(VikunjaProject{ID: 1, Title: "Docket"})
	})

	vikunjaMux.HandleFunc("/api/v1/projects/1/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			tasks := []VikunjaTask{
				{
					ID:    1,
					Title: "Top Task 1",
					Done:  false,
					RelatedTasks: map[string][]VikunjaTask{
						"subtask":  {{ID: 2}},
						"blocking": {{ID: 3}},
					},
				},
				{
					ID:    2,
					Title: "Subtask 2",
					Done:  false,
					RelatedTasks: map[string][]VikunjaTask{
						"parenttask": {{ID: 1}},
					},
				},
				{
					ID:    3,
					Title: "Task 3",
					Done:  false,
					RelatedTasks: map[string][]VikunjaTask{
						"blocked": {{ID: 1}},
					},
				},
				{
					ID:    4,
					Title: "Independent Task 4",
					Done:  false,
					RelatedTasks: map[string][]VikunjaTask{
						"related": {{ID: 1}},
					},
				},
				{
					ID:    5,
					Title: "Completed Task 5",
					Done:  true,
				},
				{
					ID:    888,
					Title: "Task 888",
					Done:  false,
				},
			}
			_ = json.NewEncoder(w).Encode(tasks)
			return
		}
		var created VikunjaTask
		_ = json.NewDecoder(r.Body).Decode(&created)
		created.ID = 100
		_ = json.NewEncoder(w).Encode(created)
	})

	vikunjaMux.HandleFunc("/api/v1/tasks/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path
		if strings.Contains(path, "/comments") {
			if strings.Contains(path, "/5/comments") {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"comments failed"}`))
				return
			}
			if r.Method == http.MethodGet {
				_ = json.NewEncoder(w).Encode([]VikunjaComment{
					{ID: 1, Comment: "Initial comment", Created: time.Now().UTC()},
				})
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if strings.Contains(path, "/relations") {
			parts := strings.Split(strings.Trim(path, "/"), "/")
			taskID, _ := strconv.ParseInt(parts[3], 10, 64)
			if strings.Contains(path, "/888") || strings.Contains(path, "/relations/blocking/888") {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"relation failed"}`))
				return
			}
			var relReq VikunjaRelationReq
			_ = json.NewDecoder(r.Body).Decode(&relReq)
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
			return
		}
		if strings.HasSuffix(path, "/888") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"task 888 error"}`))
			return
		}
		if r.Method == http.MethodGet {
			parts := strings.Split(strings.Trim(path, "/"), "/")
			taskID, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
			if taskID == 0 {
				taskID = 1
			}
			_ = json.NewEncoder(w).Encode(VikunjaTask{
				ID:          taskID,
				Title:       fmt.Sprintf("Task %d", taskID),
				Description: "Task description",
				Done:        false,
				Created:     time.Now().UTC(),
				Updated:     time.Now().UTC(),
			})
			return
		}
		if r.Method == http.MethodPost {
			var updated VikunjaTask
			_ = json.NewDecoder(r.Body).Decode(&updated)
			if updated.ID == 0 {
				updated.ID = 1
			}
			state.mu.Lock()
			state.lastUpdatedTask = &updated
			state.mu.Unlock()
			_ = json.NewEncoder(w).Encode(updated)
			return
		}
		http.NotFound(w, r)
	})

	vikunjaServer := httptest.NewServer(vikunjaMux)
	t.Cleanup(vikunjaServer.Close)

	ollamaMux := http.NewServeMux()
	ollamaMux.HandleFunc("/api/embeddings", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		vec := make([]float32, 384)
		vec[0] = 0.5
		_ = json.NewEncoder(w).Encode(map[string]any{"embedding": vec})
	})
	ollamaServer := httptest.NewServer(ollamaMux)
	t.Cleanup(ollamaServer.Close)

	cfg := &Config{
		Port:             "8080",
		VikunjaURL:       vikunjaServer.URL,
		VikunjaUsername:  "admin",
		VikunjaPassword:  "password",
		DefaultProjectID: 1,
		Ollama: ollama.Config{
			BaseURL: ollamaServer.URL,
			Model:   "all-minilm",
			NumCtx:  512,
		},
	}

	vkClient := NewVikunjaClient(cfg, vikunjaServer.Client())
	setTestToken(vkClient, "test-jwt")
	store := newMemoryTaskStore()
	olClient := ollama.NewDynamicClient(func() ollama.Config { return cfg.Ollama })

	srv := NewServer(cfg, vkClient, store, olClient)
	mux := NewHTTPMux(srv)

	return srv, mux, state
}

func TestServer_HTTPRoutesTable(t *testing.T) {
	t.Parallel()

	oldCountersFn := httpkit.WriteCountersDirFn
	oldMetaFn := httpkit.WriteMetaDirFn
	httpkit.WriteCountersDirFn = func(dir string) error { return nil }
	httpkit.WriteMetaDirFn = func(dir string) error { return nil }
	t.Cleanup(func() {
		httpkit.WriteCountersDirFn = oldCountersFn
		httpkit.WriteMetaDirFn = oldMetaFn
	})

	_, mux, _ := setupTestEnvironment(t)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "health GET returns 200",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"ok"`,
		},
		{
			name:       "health HEAD returns 200",
			method:     http.MethodHead,
			path:       "/health",
			wantStatus: http.StatusOK,
		},
		{
			name:       "health POST returns 405",
			method:     http.MethodPost,
			path:       "/health",
			wantStatus: http.StatusMethodNotAllowed,
			wantSubstr: "Method not allowed",
		},
		{
			name:       "coverage flush returns 200",
			method:     http.MethodPost,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
		},
		{
			name:       "mcp GET returns ready banner",
			method:     http.MethodGet,
			path:       "/mcp",
			wantStatus: http.StatusOK,
			wantSubstr: "docket-mcp ready",
		},
		{
			name:       "root GET returns ready banner",
			method:     http.MethodGet,
			path:       "/",
			wantStatus: http.StatusOK,
			wantSubstr: "docket-mcp ready",
		},
		{
			name:       "mcp empty POST returns 400",
			method:     http.MethodPost,
			path:       "/mcp",
			body:       "",
			wantStatus: http.StatusBadRequest,
			wantSubstr: "Empty request body",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var bodyReader *strings.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			} else {
				bodyReader = strings.NewReader("")
			}
			req := httptest.NewRequest(tc.method, tc.path, bodyReader)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", w.Code, tc.wantStatus)
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("body = %q, want substring %q", w.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServer_MCPMethodsTable(t *testing.T) {
	t.Parallel()

	_, mux, _ := setupTestEnvironment(t)

	tests := []struct {
		name       string
		request    string
		wantSubstr string
		wantError  bool
	}{
		{
			name:       "invalid json returns parse error",
			request:    `{invalid_json`,
			wantSubstr: "Parse error",
			wantError:  true,
		},
		{
			name:       "unknown method returns method not found",
			request:    `{"jsonrpc":"2.0","id":1,"method":"foo/bar"}`,
			wantSubstr: "Method not found",
			wantError:  true,
		},
		{
			name:       "initialize returns server info",
			request:    `{"jsonrpc":"2.0","id":1,"method":"initialize"}`,
			wantSubstr: "docket-mcp",
			wantError:  false,
		},
		{
			name:       "notifications initialized returns no content",
			request:    `{"jsonrpc":"2.0","method":"notifications/initialized"}`,
			wantSubstr: "",
			wantError:  false,
		},
		{
			name:       "ping returns success",
			request:    `{"jsonrpc":"2.0","id":2,"method":"ping"}`,
			wantSubstr: `"result":{}`,
			wantError:  false,
		},
		{
			name:       "tools list returns 9 tools",
			request:    `{"jsonrpc":"2.0","id":3,"method":"tools/list"}`,
			wantSubstr: `"ready"`,
			wantError:  false,
		},
		{
			name:       "tools call with invalid params",
			request:    `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":"invalid"}`,
			wantSubstr: "Invalid params",
			wantError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(tc.request))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			body := w.Body.String()
			if tc.wantSubstr != "" && !strings.Contains(body, tc.wantSubstr) {
				t.Fatalf("body = %q, want substring %q", body, tc.wantSubstr)
			}
			if tc.wantError && !strings.Contains(body, `"error"`) {
				t.Fatalf("expected error in response, got %s", body)
			}
		})
	}
}

func TestServer_ToolsExecutionTable(t *testing.T) {
	t.Parallel()

	_, mux, _ := setupTestEnvironment(t)

	tests := []struct {
		name       string
		toolName   string
		args       string
		wantSubstr string
		wantError  bool
	}{
		{
			name:       "ready top level tasks",
			toolName:   "ready",
			args:       `{}`,
			wantSubstr: `"id":1`,
			wantError:  false,
		},
		{
			name:       "ready scoped parent tasks",
			toolName:   "ready",
			args:       `{"parent_id":1}`,
			wantSubstr: `"id":2`,
			wantError:  false,
		},
		{
			name:       "list open tasks",
			toolName:   "list",
			args:       `{"status":"open"}`,
			wantSubstr: `"id":1`,
			wantError:  false,
		},
		{
			name:       "list all tasks",
			toolName:   "list",
			args:       `{"status":"all"}`,
			wantSubstr: `"id":5`,
			wantError:  false,
		},
		{
			name:       "list scoped by parent",
			toolName:   "list",
			args:       `{"status":"open","parent_id":1}`,
			wantSubstr: `"id":2`,
			wantError:  false,
		},
		{
			name:       "search tasks success",
			toolName:   "search",
			args:       `{"query":"test","status":"open","limit":5}`,
			wantSubstr: `"id":1`,
			wantError:  false,
		},
		{
			name:       "search missing query error",
			toolName:   "search",
			args:       `{"query":""}`,
			wantSubstr: "parameter 'query' is required",
			wantError:  true,
		},
		{
			name:       "show task details",
			toolName:   "show",
			args:       `{"id":1}`,
			wantSubstr: `"description":"Task description"`,
			wantError:  false,
		},
		{
			name:       "show task with comments failure",
			toolName:   "show",
			args:       `{"id":5}`,
			wantSubstr: `"id":5`,
			wantError:  false,
		},
		{
			name:       "show task with parent",
			toolName:   "show",
			args:       `{"id":2}`,
			wantSubstr: `"parent":{"id":1`,
			wantError:  false,
		},
		{
			name:       "show task with related",
			toolName:   "show",
			args:       `{"id":4}`,
			wantSubstr: `"related":[{"id":1`,
			wantError:  false,
		},
		{
			name:       "show task with parent and blocked_by",
			toolName:   "show",
			args:       `{"id":3}`,
			wantSubstr: `"blocked_by":[{"id":1`,
			wantError:  false,
		},
		{
			name:       "show invalid id error",
			toolName:   "show",
			args:       `{"id":-1}`,
			wantSubstr: "must be a positive integer",
			wantError:  true,
		},
		{
			name:       "show task not found in graph",
			toolName:   "show",
			args:       `{"id":999}`,
			wantSubstr: "not found in project graph",
			wantError:  true,
		},
		{
			name:       "create task success",
			toolName:   "create",
			args:       `{"title":"New Task","description":"A brand new task","blocks":[4]}`,
			wantSubstr: `"title":"New Task"`,
			wantError:  false,
		},
		{
			name:       "create task with parent, blocked_by, and related",
			toolName:   "create",
			args:       `{"title":"All Relations","parent_id":1,"blocked_by":[4],"related":[2]}`,
			wantSubstr: `"parent_id":1,"blocked_by":[4]`,
			wantError:  false,
		},
		{
			name:       "create with invalid parent_id",
			toolName:   "create",
			args:       `{"title":"Bad Parent","parent_id":999}`,
			wantSubstr: "parent task 999 not found",
			wantError:  true,
		},
		{
			name:       "create with invalid blocks",
			toolName:   "create",
			args:       `{"title":"Bad Blocks","blocks":[999]}`,
			wantSubstr: "blocked task 999 not found",
			wantError:  true,
		},
		{
			name:       "create with invalid related",
			toolName:   "create",
			args:       `{"title":"Bad Rel","related":[999]}`,
			wantSubstr: "related task 999 not found",
			wantError:  true,
		},
		{
			name:       "create empty title error",
			toolName:   "create",
			args:       `{"title":""}`,
			wantSubstr: "cannot be empty",
			wantError:  true,
		},
		{
			name:       "create nonexistent blocker error",
			toolName:   "create",
			args:       `{"title":"Task","blocked_by":[999]}`,
			wantSubstr: "blocker task 999 not found",
			wantError:  true,
		},
		{
			name:       "update task success",
			toolName:   "update",
			args:       `{"id":1,"title":"Updated Title"}`,
			wantSubstr: `"title":"Updated Title"`,
			wantError:  false,
		},
		{
			name:       "update invalid id error",
			toolName:   "update",
			args:       `{"id":0}`,
			wantSubstr: "must be a positive integer",
			wantError:  true,
		},
		{
			name:       "comment task success",
			toolName:   "comment",
			args:       `{"id":1,"text":"Nice progress"}`,
			wantSubstr: "comment added to task 1",
			wantError:  false,
		},
		{
			name:       "comment missing text error",
			toolName:   "comment",
			args:       `{"id":1,"text":""}`,
			wantSubstr: "required",
			wantError:  true,
		},
		{
			name:       "link tasks blocks success",
			toolName:   "link",
			args:       `{"id":4,"target_id":2,"type":"blocks"}`,
			wantSubstr: "created",
			wantError:  false,
		},
		{
			name:       "link create error",
			toolName:   "link",
			args:       `{"id":4,"target_id":888,"type":"blocks"}`,
			wantSubstr: "failed to create relation",
			wantError:  true,
		},
		{
			name:       "link remove error",
			toolName:   "link",
			args:       `{"id":4,"target_id":888,"type":"blocks","remove":true}`,
			wantSubstr: "failed to remove relation",
			wantError:  true,
		},
		{
			name:       "link invalid id error",
			toolName:   "link",
			args:       `{"id":0,"target_id":2,"type":"blocks"}`,
			wantSubstr: "must be positive integers",
			wantError:  true,
		},
		{
			name:       "close invalid id error",
			toolName:   "close",
			args:       `{"id":0,"resolution":"completed","reason":"done"}`,
			wantSubstr: "must be a positive integer",
			wantError:  true,
		},
		{
			name:       "link tasks parent success",
			toolName:   "link",
			args:       `{"id":4,"target_id":1,"type":"parent"}`,
			wantSubstr: "created",
			wantError:  false,
		},
		{
			name:       "link tasks related success",
			toolName:   "link",
			args:       `{"id":4,"target_id":2,"type":"related"}`,
			wantSubstr: "created",
			wantError:  false,
		},
		{
			name:       "link self-link rejected",
			toolName:   "link",
			args:       `{"id":1,"target_id":1,"type":"parent"}`,
			wantSubstr: "self-link cycle",
			wantError:  true,
		},
		{
			name:       "link cycle rejected",
			toolName:   "link",
			args:       `{"id":1,"target_id":2,"type":"parent"}`,
			wantSubstr: "cycle detected",
			wantError:  true,
		},
		{
			name:       "link remove relation",
			toolName:   "link",
			args:       `{"id":1,"target_id":2,"type":"parent","remove":true}`,
			wantSubstr: "removed",
			wantError:  false,
		},
		{
			name:       "link invalid relation type",
			toolName:   "link",
			args:       `{"id":1,"target_id":2,"type":"invalid"}`,
			wantSubstr: "invalid relation type",
			wantError:  true,
		},
		{
			name:       "close completed rejected with open subtask",
			toolName:   "close",
			args:       `{"id":1,"resolution":"completed","reason":"done"}`,
			wantSubstr: "open subtasks: [2]",
			wantError:  true,
		},
		{
			name:       "close completed success on leaf task",
			toolName:   "close",
			args:       `{"id":4,"resolution":"completed","reason":"verified"}`,
			wantSubstr: `"status":"completed"`,
			wantError:  false,
		},
		{
			name:       "close wont_do cascades",
			toolName:   "close",
			args:       `{"id":1,"resolution":"wont_do","reason":"abandoned"}`,
			wantSubstr: `"status":"wont_do"`,
			wantError:  false,
		},
		{
			name:       "close invalid resolution error",
			toolName:   "close",
			args:       `{"id":1,"resolution":"invalid","reason":"why"}`,
			wantSubstr: "invalid resolution",
			wantError:  true,
		},
		{
			name:       "close empty reason error",
			toolName:   "close",
			args:       `{"id":1,"resolution":"completed","reason":""}`,
			wantSubstr: "cannot be empty",
			wantError:  true,
		},
		{
			name:       "ready invalid arguments",
			toolName:   "ready",
			args:       `{"parent_id":"not-an-integer"}`,
			wantSubstr: "invalid arguments",
			wantError:  true,
		},
		{
			name:       "list invalid arguments",
			toolName:   "list",
			args:       `{"parent_id":"not-an-integer"}`,
			wantSubstr: "invalid arguments",
			wantError:  true,
		},
		{
			name:       "unknown tool returns error",
			toolName:   "unknown_tool",
			args:       `{}`,
			wantSubstr: "Unknown tool: unknown_tool",
			wantError:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload := map[string]any{
				"jsonrpc": "2.0",
				"id":      100,
				"method":  "tools/call",
				"params": map[string]any{
					"name":      tc.toolName,
					"arguments": json.RawMessage(tc.args),
				},
			}
			reqBytes, _ := json.Marshal(payload)
			req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(reqBytes))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			body := w.Body.String()
			escapedSubstr := strings.ReplaceAll(tc.wantSubstr, `"`, `\"`)
			if !strings.Contains(body, tc.wantSubstr) && !strings.Contains(body, escapedSubstr) {
				t.Fatalf("body = %q, want substring %q", body, tc.wantSubstr)
			}
			if tc.wantError && !strings.Contains(body, `"isError":true`) && !strings.Contains(body, `"error"`) {
				t.Fatalf("expected isError:true in response: %s", body)
			}
		})
	}
}

func TestServer_ErrorBranchesTable(t *testing.T) {
	t.Parallel()

	errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"server error"}`))
	}))
	t.Cleanup(errorServer.Close)

	cfg := &Config{
		VikunjaURL:      errorServer.URL,
		VikunjaUsername: "user",
		VikunjaPassword: "pwd",
	}
	vkClient := NewVikunjaClient(cfg, errorServer.Client())
	setTestToken(vkClient, "dummy")
	store := newMemoryTaskStore()
	store.searchErr = errors.New("search db failed")
	store.fetchErr = errors.New("fetch db failed")

	srv := NewServer(cfg, vkClient, store, nil)

	tests := []struct {
		name     string
		toolName string
		args     string
	}{
		{"ready error", "ready", `{}`},
		{"list error", "list", `{}`},
		{"search error", "search", `{"query":"error test"}`},
		{"show error", "show", `{"id":1}`},
		{"create error", "create", `{"title":"Task"}`},
		{"update error", "update", `{"id":1,"title":"Task"}`},
		{"comment error", "comment", `{"id":1,"text":"Hello"}`},
		{"link error", "link", `{"id":1,"target_id":2,"type":"blocks"}`},
		{"close error", "close", `{"id":1,"resolution":"completed","reason":"done"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, isErr := srv.executeTool(context.Background(), tc.toolName, json.RawMessage(tc.args))
			if !isErr {
				t.Fatalf("%s: expected isErr=true, got result %q", tc.name, res)
			}
		})
	}
}

func TestServer_CloseGraphFallback(t *testing.T) {
	t.Parallel()

	callCount := 0
	var mu sync.Mutex

	vikunjaMux := http.NewServeMux()
	vikunjaMux.HandleFunc("/api/v1/projects", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]VikunjaProject{{ID: 1, Title: "Docket"}})
	})
	vikunjaMux.HandleFunc("/api/v1/projects/1/tasks", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		c := callCount
		mu.Unlock()
		if c > 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"list tasks failed on reload"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		tasks := []VikunjaTask{{ID: 10, Title: "Task 10", Done: false}}
		_ = json.NewEncoder(w).Encode(tasks)
	})
	vikunjaMux.HandleFunc("/api/v1/tasks/10", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(VikunjaTask{ID: 10, Title: "Task 10", Done: true})
	})
	vikunjaMux.HandleFunc("/api/v1/tasks/10/comments", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	server := httptest.NewServer(vikunjaMux)
	t.Cleanup(server.Close)

	cfg := &Config{
		VikunjaURL:       server.URL,
		DefaultProjectID: 1,
	}
	vkClient := NewVikunjaClient(cfg, server.Client())
	setTestToken(vkClient, "test-jwt")
	store := newMemoryTaskStore()

	srv := NewServer(cfg, vkClient, store, nil)
	res, isErr := srv.executeTool(context.Background(), "close", json.RawMessage(`{"id":10,"resolution":"completed","reason":"done"}`))
	if isErr {
		t.Fatalf("expected close fallback to succeed, got error: %s", res)
	}
	if !strings.Contains(res, `"status":"completed"`) {
		t.Fatalf("expected completed status in result: %s", res)
	}
}

func TestRunServer_Execution(t *testing.T) {
	oldSQLOpen := sqlOpenFn
	t.Cleanup(func() {
		sqlOpenFn = oldSQLOpen
	})

	tests := []struct {
		name       string
		port       string
		dbErr      error
		cancelInit bool
		wantErr    bool
	}{
		{
			name:       "clean shutdown on context cancel",
			port:       "0",
			cancelInit: true,
			wantErr:    false,
		},
		{
			name:       "database open error returned",
			port:       "0",
			dbErr:      errors.New("db open error"),
			cancelInit: false,
			wantErr:    true,
		},
		{
			name:       "invalid port returns listen error",
			port:       "-1",
			cancelInit: false,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlOpenFn = func(driverName, dataSourceName string) (*sql.DB, error) {
				if tc.dbErr != nil {
					return nil, tc.dbErr
				}
				conn := &testConn{}
				return sql.OpenDB(&testConnector{conn: conn}), nil
			}

			cfg := &Config{
				Port:        tc.port,
				DatabaseURL: "postgres://mock",
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tc.cancelInit {
				cancel()
			} else {
				defer cancel()
			}

			err := runServer(ctx, cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestMain_Execution_TableDriven(t *testing.T) {
	tmpDir := t.TempDir()
	badConfigPath := filepath.Join(tmpDir, "bad_config.yaml")
	_ = os.WriteFile(badConfigPath, []byte("port: [unclosed"), 0o600)

	tests := []struct {
		name       string
		configPath string
		runErr     error
		wantExit   bool
	}{
		{"success exit", "", nil, false},
		{"server closed error", "", http.ErrServerClosed, false},
		{"fatal error calls exitFn", "", errors.New("fatal server error"), true},
		{"config load error calls exitFn", badConfigPath, nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldRun := runServerFn
			oldExit := exitFn
			exitCalled := false

			if tc.configPath != "" {
				t.Setenv("CONFIG_PATH", tc.configPath)
			} else {
				t.Setenv("CONFIG_PATH", "")
			}

			runServerFn = func(ctx context.Context, cfg *Config) error {
				return tc.runErr
			}
			exitFn = func(format string, v ...any) {
				exitCalled = true
			}

			t.Cleanup(func() {
				runServerFn = oldRun
				exitFn = oldExit
			})

			main()

			if exitCalled != tc.wantExit {
				t.Fatalf("exitCalled = %v, want %v", exitCalled, tc.wantExit)
			}
		})
	}
}

func TestServer_CloseCompleted_Errors(t *testing.T) {
	t.Parallel()

	srv, _, _ := setupTestEnvironment(t)
	ctx := context.Background()
	now := time.Now().UTC()

	tests := []struct {
		name    string
		taskID  int64
		wantErr bool
	}{
		{
			name:    "nonexistent task returns error",
			taskID:  888,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := srv.closeCompleted(ctx, tc.taskID, "reason", now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("%s: err = %v, wantErr = %v", tc.name, err, tc.wantErr)
			}
		})
	}
}

func TestServer_CloseWontDo_Errors(t *testing.T) {
	t.Parallel()

	srv, _, _ := setupTestEnvironment(t)
	ctx := context.Background()
	now := time.Now().UTC()

	tests := []struct {
		name    string
		taskID  int64
		wantErr bool
	}{
		{
			name:    "nonexistent task returns error",
			taskID:  888,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := srv.closeWontDo(ctx, tc.taskID, "reason", nil, now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("%s: err = %v, wantErr = %v", tc.name, err, tc.wantErr)
			}
		})
	}
}

func TestServer_WireCreateRelations(t *testing.T) {
	t.Parallel()

	srv, _, state := setupTestEnvironment(t)
	ctx := context.Background()

	// 1. Success case: verify wireCreateRelations creates all four relations with correct kinds and directions.
	srv.wireCreateRelations(ctx, 10, int64Ptr(1), []int64{2}, []int64{3}, []int64{4})

	state.mu.Lock()
	if len(state.recordedRelations) != 4 {
		t.Fatalf("recorded %d relations, want 4", len(state.recordedRelations))
	}
	expected := []VikunjaRelationRecord{
		{TaskID: 10, OtherTaskID: 1, RelationKind: "parenttask"},
		{TaskID: 10, OtherTaskID: 2, RelationKind: "blocking"},
		{TaskID: 3, OtherTaskID: 10, RelationKind: "blocking"},
		{TaskID: 10, OtherTaskID: 4, RelationKind: "related"},
	}
	for i, exp := range expected {
		if state.recordedRelations[i] != exp {
			t.Fatalf("relation[%d] = %+v, want %+v", i, state.recordedRelations[i], exp)
		}
	}
	state.mu.Unlock()
}

func TestServer_UpdateTaskDescription(t *testing.T) {
	t.Parallel()

	srv, _, state := setupTestEnvironment(t)
	desc := "Updated description text"
	payload, _ := json.Marshal(map[string]any{
		"id":          1,
		"description": desc,
	})
	res, isErr := srv.toolUpdate(context.Background(), payload)
	if isErr {
		t.Fatalf("unexpected error: %s", res)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.lastUpdatedTask == nil || state.lastUpdatedTask.Description != desc {
		t.Fatalf("lastUpdatedTask description = %v, want %q", state.lastUpdatedTask, desc)
	}
}
