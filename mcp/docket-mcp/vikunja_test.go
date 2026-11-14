package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func setTestToken(c *VikunjaClient, token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
}

type vikunjaAuthenticateTestCase struct {
	name           string
	loginStatus    int
	loginBody      string
	registerStatus int
	registerBody   string
	initialToken   string
	wantToken      string
	wantErr        string
}

func runVikunjaAuthenticateCase(t *testing.T, tc vikunjaAuthenticateTestCase) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/login" {
			if tc.loginStatus != 0 {
				w.WriteHeader(tc.loginStatus)
			}
			_, _ = w.Write([]byte(tc.loginBody))
			return
		}
		if r.URL.Path == "/api/v1/register" {
			if tc.registerStatus != 0 {
				w.WriteHeader(tc.registerStatus)
			}
			_, _ = w.Write([]byte(tc.registerBody))
			return
		}
		http.NotFound(w, r)
	})

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	cfg := &Config{
		VikunjaURL:      ts.URL,
		VikunjaUsername: "user",
		VikunjaPassword: "pass",
	}
	client := NewVikunjaClient(cfg, ts.Client())
	if tc.initialToken != "" {
		setTestToken(client, tc.initialToken)
	}

	err := client.authenticateLocked(context.Background())
	if tc.wantErr == "" && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
		}
		return
	}
	if client.getToken() != tc.wantToken {
		t.Fatalf("token = %q, want %q", client.getToken(), tc.wantToken)
	}
}

func TestVikunjaClient_Authenticate(t *testing.T) {
	t.Parallel()

	tests := []vikunjaAuthenticateTestCase{
		{
			name:        "login success on first try",
			loginStatus: http.StatusOK,
			loginBody:   `{"token":"test-token-123"}`,
			wantToken:   "test-token-123",
		},
		{
			name:           "login fails, register succeeds",
			loginStatus:    http.StatusUnauthorized,
			loginBody:      `{"message":"user not found"}`,
			registerStatus: http.StatusOK,
			registerBody:   `{"token":"reg-token-456"}`,
			wantToken:      "reg-token-456",
		},
		{
			name:        "login and register both fail",
			loginStatus: http.StatusInternalServerError,
			loginBody:   `{"error":"db failure"}`,
			wantErr:     "vikunja authentication failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVikunjaAuthenticateCase(t, tc)
		})
	}
}

type getDefaultProjectTestCase struct {
	name             string
	defaultProjectID int64
	projectsResponse string
	createResponse   string
	wantID           int64
	wantErr          string
}

func handleProjectsEndpoint(w http.ResponseWriter, r *http.Request, projectsResponse, createResponse string) {
	if r.Method == http.MethodGet {
		if strings.HasPrefix(projectsResponse, "500:") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(projectsResponse[4:]))
			return
		}
		_, _ = w.Write([]byte(projectsResponse))
		return
	}
	if r.Method == http.MethodPut {
		if strings.HasPrefix(createResponse, "500:") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(createResponse[4:]))
			return
		}
		_, _ = w.Write([]byte(createResponse))
		return
	}
}

func runGetDefaultProjectCase(t *testing.T, tc getDefaultProjectTestCase) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/projects" {
			handleProjectsEndpoint(w, r, tc.projectsResponse, tc.createResponse)
			return
		}
		http.NotFound(w, r)
	})

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	cfg := &Config{
		VikunjaURL:       ts.URL,
		DefaultProjectID: tc.defaultProjectID,
	}
	client := NewVikunjaClient(cfg, ts.Client())
	setTestToken(client, "dummy-token")

	gotID, err := client.GetDefaultProject(context.Background())
	if tc.wantErr == "" && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
		}
		return
	}
	if gotID != tc.wantID {
		t.Fatalf("GetDefaultProject = %d, want %d", gotID, tc.wantID)
	}
}

func TestVikunjaClient_GetDefaultProject(t *testing.T) {
	t.Parallel()

	tests := []getDefaultProjectTestCase{
		{
			name:             "configured default project returns immediately",
			defaultProjectID: 99,
			wantID:           99,
		},
		{
			name:             "lists existing project when default is zero",
			projectsResponse: `[{"id":55,"title":"Existing"}]`,
			wantID:           55,
		},
		{
			name:             "creates docket project when none exist",
			projectsResponse: `[]`,
			createResponse:   `{"id":77,"title":"Docket"}`,
			wantID:           77,
		},
		{
			name:             "list failure returns error",
			projectsResponse: `500:internal error`,
			wantErr:          "status 500",
		},
		{
			name:             "create failure returns error",
			projectsResponse: `[]`,
			createResponse:   `500:create error`,
			wantErr:          "status 500",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runGetDefaultProjectCase(t, tc)
		})
	}
}

func TestVikunjaClient_GetDefaultProject_Concurrent(t *testing.T) {
	t.Parallel()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/projects" && r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`[{"id":42,"title":"Docket"}]`))
			return
		}
		http.NotFound(w, r)
	})

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	cfg := &Config{
		VikunjaURL:       ts.URL,
		DefaultProjectID: 0,
	}
	client := NewVikunjaClient(cfg, ts.Client())
	setTestToken(client, "dummy-token")

	errCh := make(chan error, 5)
	for range 5 {
		go func() {
			id, err := client.GetDefaultProject(context.Background())
			if err != nil {
				errCh <- err
				return
			}
			if id != 42 {
				errCh <- fmt.Errorf("got id %d, want 42", id)
				return
			}
			errCh <- nil
		}()
	}
	for range 5 {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent GetDefaultProject error: %v", err)
		}
	}
}

type listProjectTasksTestCase struct {
	name       string
	totalTasks int
	maxPerPage int
	ignorePage bool
	serverErr  bool
	wantCount  int
	wantErr    string
}

func runListProjectTasksCase(t *testing.T, tc listProjectTasksTestCase) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if tc.serverErr {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"list failure"}`))
			return
		}
		perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		perPage = min(perPage, tc.maxPerPage)
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if tc.ignorePage {
			page = 1
		}
		tasks := []VikunjaTask{}
		for id := (page-1)*perPage + 1; id <= min(page*perPage, tc.totalTasks); id++ {
			tasks = append(tasks, VikunjaTask{ID: int64(id), Title: "Task"})
		}
		_ = json.NewEncoder(w).Encode(tasks)
	})

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	client := NewVikunjaClient(&Config{VikunjaURL: ts.URL}, ts.Client())
	setTestToken(client, "dummy")

	tasks, err := client.ListProjectTasks(context.Background(), 1)
	if tc.wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tasks) != tc.wantCount {
		t.Fatalf("task count = %d, want %d", len(tasks), tc.wantCount)
	}
	for i, task := range tasks {
		if task.ID != int64(i+1) {
			t.Fatalf("tasks[%d].ID = %d, want %d", i, task.ID, i+1)
		}
	}
}

func TestVikunjaClient_ListProjectTasks(t *testing.T) {
	t.Parallel()

	tests := []listProjectTasksTestCase{
		{name: "120 tasks capped at 50 per page returns all", totalTasks: 120, maxPerPage: 50, wantCount: 120},
		{name: "server capping pages at 20 still returns all", totalTasks: 120, maxPerPage: 20, wantCount: 120},
		{name: "exact multiple of page size returns all", totalTasks: 100, maxPerPage: 50, wantCount: 100},
		{name: "empty project returns no tasks", totalTasks: 0, maxPerPage: 50, wantCount: 0},
		{name: "server ignoring page param terminates after one page", totalTasks: 120, maxPerPage: 50, ignorePage: true, wantCount: 50},
		{name: "server error propagates", serverErr: true, wantErr: "status 500"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runListProjectTasksCase(t, tc)
		})
	}
}

type taskOperationsTestCase struct {
	name      string
	serverErr bool
	wantErr   string
}

func handleTaskOperationsServer(w http.ResponseWriter, r *http.Request, serverErr bool) {
	w.Header().Set("Content-Type", "application/json")
	if serverErr {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"server error"}`))
		return
	}

	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/api/v1/projects/1/tasks":
		_ = json.NewEncoder(w).Encode(VikunjaTask{ID: 10, Title: "Created"})
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks/10":
		_ = json.NewEncoder(w).Encode(VikunjaTask{ID: 10, Title: "Fetched"})
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks/10":
		_ = json.NewEncoder(w).Encode(VikunjaTask{ID: 10, Title: "Updated"})
	case r.Method == http.MethodPut && r.URL.Path == "/api/v1/tasks/10/relations":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/tasks/10/relations/blocking/2":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/tasks/10/comments":
		_ = json.NewEncoder(w).Encode([]VikunjaComment{{ID: 1, Comment: "Hello"}})
	case r.Method == http.MethodPut && r.URL.Path == "/api/v1/tasks/10/comments":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	default:
		http.NotFound(w, r)
	}
}

func runTaskOperationsCase(t *testing.T, tc taskOperationsTestCase) {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleTaskOperationsServer(w, r, tc.serverErr)
	})

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	client := NewVikunjaClient(&Config{VikunjaURL: ts.URL}, ts.Client())
	setTestToken(client, "dummy")

	ctx := context.Background()
	assertTaskCRUDFlow(ctx, t, client, tc.wantErr)
	assertTaskRelationsFlow(ctx, t, client, tc.wantErr)
	assertTaskCommentsFlow(ctx, t, client, tc.wantErr)
}

func assertTaskCRUDFlow(ctx context.Context, t *testing.T, client *VikunjaClient, wantErr string) {
	t.Helper()
	created, err := client.CreateTask(ctx, 1, "Created", "Desc")
	if wantErr == "" && (err != nil || created.ID != 10) {
		t.Fatalf("CreateTask err: %v, task: %+v", err, created)
	}
	if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
		t.Fatalf("CreateTask expected error %q, got %v", wantErr, err)
	}

	fetched, err := client.GetTask(ctx, 10)
	if wantErr == "" && (err != nil || fetched.ID != 10) {
		t.Fatalf("GetTask err: %v, task: %+v", err, fetched)
	}
	if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
		t.Fatalf("GetTask expected error %q, got %v", wantErr, err)
	}

	updated, err := client.UpdateTask(ctx, &VikunjaTask{ID: 10, Title: "Updated"})
	if wantErr == "" && (err != nil || updated.Title != "Updated") {
		t.Fatalf("UpdateTask err: %v, task: %+v", err, updated)
	}
	if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
		t.Fatalf("UpdateTask expected error %q, got %v", wantErr, err)
	}
}

func assertOperationError(t *testing.T, opName string, err error, wantErr string) {
	t.Helper()
	if wantErr == "" && err != nil {
		t.Fatalf("%s err: %v", opName, err)
	}
	if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
		t.Fatalf("%s expected error %q, got %v", opName, wantErr, err)
	}
}

func assertTaskRelationsFlow(ctx context.Context, t *testing.T, client *VikunjaClient, wantErr string) {
	t.Helper()
	assertOperationError(t, "CreateRelation", client.CreateRelation(ctx, 10, 2, "blocking"), wantErr)
	assertOperationError(t, "DeleteRelation", client.DeleteRelation(ctx, 10, 2, "blocking"), wantErr)
}

func assertTaskCommentsFlow(ctx context.Context, t *testing.T, client *VikunjaClient, wantErr string) {
	t.Helper()
	comments, err := client.ListComments(ctx, 10)
	if wantErr == "" && (err != nil || len(comments) != 1) {
		t.Fatalf("ListComments err: %v, len: %d", err, len(comments))
	}
	if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
		t.Fatalf("ListComments expected error %q, got %v", wantErr, err)
	}
	assertOperationError(t, "CreateComment", client.CreateComment(ctx, 10, "New comment"), wantErr)
}

func TestVikunjaClient_TaskOperations(t *testing.T) {
	t.Parallel()

	tests := []taskOperationsTestCase{
		{
			name: "success on all endpoints",
		},
		{
			name:      "server error on all endpoints",
			serverErr: true,
			wantErr:   "status 500",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTaskOperationsCase(t, tc)
		})
	}
}

type authAndRetryTestCase struct {
	name         string
	initialToken string
	unauthorized bool
	reauthFails  bool
	invalidJSON  bool
	networkError bool
	authError    bool
	wantErr      string
}

func handleAuthAndRetryRequest(w http.ResponseWriter, r *http.Request, tc authAndRetryTestCase, taskCalls *atomic.Int32) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/api/v1/login" {
		if tc.reauthFails || tc.authError {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"login failed"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"token": "refreshed-token"})
		return
	}
	if r.URL.Path == "/api/v1/register" && tc.authError {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"register failed"}`))
		return
	}
	if r.URL.Path == "/api/v1/tasks/10" {
		c := taskCalls.Add(1)
		if tc.unauthorized && c == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"token expired"}`))
			return
		}
		if tc.invalidJSON {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{not valid json`))
			return
		}
		_ = json.NewEncoder(w).Encode(VikunjaTask{ID: 10, Title: "Success"})
		return
	}
	http.NotFound(w, r)
}

func runAuthAndRetryCase(t *testing.T, tc authAndRetryTestCase) {
	t.Helper()
	var taskCalls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleAuthAndRetryRequest(w, r, tc, &taskCalls)
	})

	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	serverURL := ts.URL
	if tc.networkError {
		ts.Close()
	}

	client := NewVikunjaClient(&Config{VikunjaURL: serverURL, VikunjaUsername: "u", VikunjaPassword: "p"}, ts.Client())
	if tc.initialToken != "" {
		setTestToken(client, tc.initialToken)
	}

	task, err := client.GetTask(context.Background(), 10)
	if tc.wantErr == "" && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
		t.Fatalf("expected error %q, got %v", tc.wantErr, err)
	}
	if tc.wantErr == "" && task.ID != 10 {
		t.Fatalf("task.ID = %d, want 10", task.ID)
	}
}

func TestVikunjaClient_AuthAndRetry(t *testing.T) {
	t.Parallel()

	tests := []authAndRetryTestCase{
		{
			name:         "empty token bootstraps login",
			initialToken: "",
		},
		{
			name:         "401 unauthorized refreshes token and retries",
			initialToken: "stale-token",
			unauthorized: true,
		},
		{
			name:         "401 reauth fails returns error",
			initialToken: "stale-token",
			unauthorized: true,
			reauthFails:  true,
			wantErr:      "reauth on 401 failed",
		},
		{
			name:         "invalid json returns parse error",
			initialToken: "token",
			invalidJSON:  true,
			wantErr:      "failed to parse response JSON",
		},
		{
			name:         "network error returns http failure",
			initialToken: "token",
			networkError: true,
			wantErr:      "failed",
		},
		{
			name:         "bootstrap auth fails propagates error",
			initialToken: "",
			authError:    true,
			wantErr:      "vikunja authentication failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runAuthAndRetryCase(t, tc)
		})
	}
}
