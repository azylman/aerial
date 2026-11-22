package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/mcp/docket-mcp/internal/config"
)

func setTestToken(c *VikunjaClient, token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
}

// vikunjaResp is one scripted reply: status 0 means 200; hangup closes the
// connection without a response.
type vikunjaResp struct {
	status int
	body   string
	hangup bool
}

// vikunjaFake serves scripted replies keyed by "METHOD /path". Each hit
// consumes the head of the route's sequence and the last reply repeats;
// unrouted requests get 404. Every request is recorded in calls and its body
// in bodies.
type vikunjaFake struct {
	mu     sync.Mutex
	routes map[string][]vikunjaResp
	calls  []string
	bodies []string
}

func (f *vikunjaFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + r.URL.Path
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.calls = append(f.calls, key)
	f.bodies = append(f.bodies, string(body))
	seq := f.routes[key]
	if len(seq) > 1 {
		f.routes[key] = seq[1:]
	}
	f.mu.Unlock()
	if len(seq) == 0 {
		http.NotFound(w, r)
		return
	}
	resp := seq[0]
	if resp.hangup {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if resp.status != 0 {
		w.WriteHeader(resp.status)
	}
	_, _ = w.Write([]byte(resp.body))
}

func (f *vikunjaFake) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// newVikunjaFakeClient starts a vikunjaFake with a copy of routes and returns a
// client for it holding token. A non-empty url replaces the fake's URL.
func newVikunjaFakeClient(t *testing.T, routes map[string][]vikunjaResp, token, url string, projectID int64) (*VikunjaClient, *vikunjaFake) {
	t.Helper()
	fake := &vikunjaFake{routes: map[string][]vikunjaResp{}}
	for k, v := range routes {
		fake.routes[k] = slices.Clone(v)
	}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)
	if url == "" {
		url = ts.URL
	}
	cfg := &config.Config{VikunjaURL: url, VikunjaUsername: "user", VikunjaPassword: "pass", DefaultProjectID: projectID}
	client := NewVikunjaClient(cfg, ts.Client())
	setTestToken(client, token)
	return client, fake
}

func (f *vikunjaFake) recordedBodies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.bodies)
}

func assertVikunjaCalls(t *testing.T, fake *vikunjaFake, want []string) {
	t.Helper()
	if got := fake.recorded(); !slices.Equal(got, want) {
		t.Fatalf("calls = %q; want %q", got, want)
	}
}

const (
	vikunjaLogin    = "POST /api/v1/login"
	vikunjaRegister = "POST /api/v1/register"
	vikunjaGetTask  = "GET /api/v1/tasks/10"
	vikunjaProjects = "GET /api/v1/projects"
	vikunjaNewProj  = "PUT /api/v1/projects"
	vikunjaErrBody  = `{"error":"boom"}`
	vikunjaErr500   = `vikunja API returned status 500: {"error":"boom"}`
)

var vikunjaFail = []vikunjaResp{{status: http.StatusInternalServerError, body: vikunjaErrBody}}

func vikunjaOK(body string) []vikunjaResp { return []vikunjaResp{{body: body}} }

func assertErrString(t *testing.T, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("error = %v, want substring %q", err, wantErr)
	}
}

func TestVikunjaClient_Authenticate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		routes    map[string][]vikunjaResp
		wantToken string
		wantErr   string
		wantCalls []string
	}{
		{
			name:      "login success on first try",
			routes:    map[string][]vikunjaResp{vikunjaLogin: vikunjaOK(`{"token":"test-token-123"}`)},
			wantToken: "test-token-123", wantCalls: []string{vikunjaLogin},
		},
		{
			name: "login fails, register succeeds",
			routes: map[string][]vikunjaResp{
				vikunjaLogin:    {{status: http.StatusUnauthorized, body: `{"message":"user not found"}`}},
				vikunjaRegister: vikunjaOK(`{"token":"reg-token-456"}`),
			},
			wantToken: "reg-token-456", wantCalls: []string{vikunjaLogin, vikunjaRegister},
		},
		{
			name:    "login and register both fail",
			routes:  map[string][]vikunjaResp{vikunjaLogin: vikunjaFail},
			wantErr: "vikunja authentication failed: " + vikunjaErr500, wantCalls: []string{vikunjaLogin, vikunjaRegister, vikunjaLogin},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, tc.routes, "", "", 0)
			assertErrString(t, client.authenticateLocked(context.Background()), tc.wantErr)
			if got := client.getToken(); got != tc.wantToken {
				t.Fatalf("token = %q, want %q", got, tc.wantToken)
			}
			assertVikunjaCalls(t, fake, tc.wantCalls)
		})
	}
}

func TestVikunjaClient_GetDefaultProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		projectID int64
		routes    map[string][]vikunjaResp
		want      int64
		wantErr   string
		wantCalls []string
	}{
		{name: "configured default project returns immediately", projectID: 99, want: 99},
		{
			name:   "lists existing project when default is zero",
			routes: map[string][]vikunjaResp{vikunjaProjects: vikunjaOK(`[{"id":55,"title":"Existing"}]`)},
			want:   55, wantCalls: []string{vikunjaProjects},
		},
		{
			name:   "creates docket project when none exist",
			routes: map[string][]vikunjaResp{vikunjaProjects: vikunjaOK(`[]`), vikunjaNewProj: vikunjaOK(`{"id":77,"title":"Docket"}`)},
			want:   77, wantCalls: []string{vikunjaProjects, vikunjaNewProj},
		},
		{name: "list failure returns error", routes: map[string][]vikunjaResp{vikunjaProjects: vikunjaFail}, wantErr: vikunjaErr500, wantCalls: []string{vikunjaProjects}},
		{
			name:    "create failure returns error",
			routes:  map[string][]vikunjaResp{vikunjaProjects: vikunjaOK(`[]`), vikunjaNewProj: vikunjaFail},
			wantErr: vikunjaErr500, wantCalls: []string{vikunjaProjects, vikunjaNewProj},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, tc.routes, "dummy-token", "", tc.projectID)
			got, err := client.GetDefaultProject(context.Background())
			assertErrString(t, err, tc.wantErr)
			if got != tc.want {
				t.Fatalf("GetDefaultProject = %d, want %d", got, tc.want)
			}
			assertVikunjaCalls(t, fake, tc.wantCalls)
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

	cfg := &config.Config{
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

// pagedTasksServer serves total tasks for any project, capping per_page at
// maxPerPage; ignorePage always serves page 1; fail answers 500.
type pagedTasksServer struct {
	total, maxPerPage int
	ignorePage, fail  bool
}

func (s pagedTasksServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.fail {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(vikunjaErrBody))
		return
	}
	perPage, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	perPage = min(perPage, s.maxPerPage)
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if s.ignorePage {
		page = 1
	}
	tasks := []config.VikunjaTask{}
	for id := (page-1)*perPage + 1; id <= min(page*perPage, s.total); id++ {
		tasks = append(tasks, config.VikunjaTask{ID: int64(id), Title: "Task"})
	}
	_ = json.NewEncoder(w).Encode(tasks)
}

func TestVikunjaClient_ListProjectTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		server    pagedTasksServer
		wantCount int
		wantErr   string
	}{
		{"120 tasks capped at 50 per page returns all", pagedTasksServer{total: 120, maxPerPage: 50}, 120, ""},
		{"server capping pages at 20 still returns all", pagedTasksServer{total: 120, maxPerPage: 20}, 120, ""},
		{"exact multiple of page size returns all", pagedTasksServer{total: 100, maxPerPage: 50}, 100, ""},
		{"empty project returns no tasks", pagedTasksServer{total: 0, maxPerPage: 50}, 0, ""},
		{"server ignoring page param terminates after one page", pagedTasksServer{total: 120, maxPerPage: 50, ignorePage: true}, 50, ""},
		{"server error propagates", pagedTasksServer{fail: true}, 0, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(tc.server)
			t.Cleanup(ts.Close)
			client := NewVikunjaClient(&config.Config{VikunjaURL: ts.URL}, ts.Client())
			setTestToken(client, "dummy")

			tasks, err := client.ListProjectTasks(context.Background(), 1)
			assertErrString(t, err, tc.wantErr)
			if len(tasks) != tc.wantCount {
				t.Fatalf("task count = %d, want %d", len(tasks), tc.wantCount)
			}
			for i, task := range tasks {
				if task.ID != int64(i+1) {
					t.Fatalf("tasks[%d].ID = %d, want %d", i, task.ID, i+1)
				}
			}
		})
	}
}

func TestVikunjaClient_CreateTask(t *testing.T) {
	t.Parallel()

	const route = "PUT /api/v1/projects/1/tasks"
	tests := []struct {
		name    string
		resp    []vikunjaResp
		want    *config.VikunjaTask
		wantErr string
	}{
		{"created", vikunjaOK(`{"id":10,"title":"Created"}`), &config.VikunjaTask{ID: 10, Title: "Created"}, ""},
		{"server error", vikunjaFail, nil, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, map[string][]vikunjaResp{route: tc.resp}, "dummy", "", 0)
			got, err := client.CreateTask(context.Background(), 1, "Created", "Desc")
			assertErrString(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("CreateTask = %+v, want %+v", got, tc.want)
			}
			assertVikunjaCalls(t, fake, []string{route})
		})
	}
}

func TestVikunjaClient_GetTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		resp    []vikunjaResp
		want    *config.VikunjaTask
		wantErr string
	}{
		{"fetched", vikunjaOK(`{"id":10,"title":"Fetched"}`), &config.VikunjaTask{ID: 10, Title: "Fetched"}, ""},
		{"server error", vikunjaFail, nil, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, map[string][]vikunjaResp{vikunjaGetTask: tc.resp}, "dummy", "", 0)
			got, err := client.GetTask(context.Background(), 10)
			assertErrString(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("GetTask = %+v, want %+v", got, tc.want)
			}
			assertVikunjaCalls(t, fake, []string{vikunjaGetTask})
		})
	}
}

func TestVikunjaClient_UpdateTask(t *testing.T) {
	t.Parallel()

	const route = "POST /api/v1/tasks/10"
	tests := []struct {
		name    string
		resp    []vikunjaResp
		want    *config.VikunjaTask
		wantErr string
	}{
		{"updated", vikunjaOK(`{"id":10,"title":"Updated"}`), &config.VikunjaTask{ID: 10, Title: "Updated"}, ""},
		{"server error", vikunjaFail, nil, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, map[string][]vikunjaResp{route: tc.resp}, "dummy", "", 0)
			got, err := client.UpdateTask(context.Background(), &config.VikunjaTask{ID: 10, Title: "Updated"})
			assertErrString(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("UpdateTask = %+v, want %+v", got, tc.want)
			}
			assertVikunjaCalls(t, fake, []string{route})
		})
	}
}

func TestVikunjaClient_CreateRelation(t *testing.T) {
	t.Parallel()

	const route = "PUT /api/v1/tasks/10/relations"
	tests := []struct {
		name     string
		kind     string
		resp     []vikunjaResp
		wantBody string
		wantErr  string
	}{
		{"blocking created", "blocking", vikunjaOK(`{}`), `{"other_task_id":2,"relation_kind":"blocking"}`, ""},
		{"related created", "related", vikunjaOK(`{}`), `{"other_task_id":2,"relation_kind":"related"}`, ""},
		{"server error", "blocking", vikunjaFail, `{"other_task_id":2,"relation_kind":"blocking"}`, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, map[string][]vikunjaResp{route: tc.resp}, "dummy", "", 0)
			assertErrString(t, client.CreateRelation(context.Background(), 10, 2, tc.kind), tc.wantErr)
			assertVikunjaCalls(t, fake, []string{route})
			if got := fake.recordedBodies(); !slices.Equal(got, []string{tc.wantBody}) {
				t.Fatalf("bodies = %q; want [%q]", got, tc.wantBody)
			}
		})
	}
}

func TestVikunjaClient_DeleteRelation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		kind    string
		route   string
		resp    []vikunjaResp
		wantErr string
	}{
		{"blocking deleted", "blocking", "DELETE /api/v1/tasks/10/relations/blocking/2", vikunjaOK(`{}`), ""},
		{"related deleted", "related", "DELETE /api/v1/tasks/10/relations/related/2", vikunjaOK(`{}`), ""},
		{"server error", "blocking", "DELETE /api/v1/tasks/10/relations/blocking/2", vikunjaFail, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, map[string][]vikunjaResp{tc.route: tc.resp}, "dummy", "", 0)
			assertErrString(t, client.DeleteRelation(context.Background(), 10, 2, tc.kind), tc.wantErr)
			assertVikunjaCalls(t, fake, []string{tc.route})
		})
	}
}

func TestVikunjaClient_ListComments(t *testing.T) {
	t.Parallel()

	const route = "GET /api/v1/tasks/10/comments"
	tests := []struct {
		name    string
		resp    []vikunjaResp
		want    []config.VikunjaComment
		wantErr string
	}{
		{"listed", vikunjaOK(`[{"id":1,"comment":"Hello"}]`), []config.VikunjaComment{{ID: 1, Comment: "Hello"}}, ""},
		{"server error", vikunjaFail, nil, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, map[string][]vikunjaResp{route: tc.resp}, "dummy", "", 0)
			got, err := client.ListComments(context.Background(), 10)
			assertErrString(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ListComments = %+v, want %+v", got, tc.want)
			}
			assertVikunjaCalls(t, fake, []string{route})
		})
	}
}

func TestVikunjaClient_CreateComment(t *testing.T) {
	t.Parallel()

	const route = "PUT /api/v1/tasks/10/comments"
	tests := []struct {
		name    string
		resp    []vikunjaResp
		wantErr string
	}{
		{"created", vikunjaOK(`{}`), ""},
		{"server error", vikunjaFail, vikunjaErr500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, map[string][]vikunjaResp{route: tc.resp}, "dummy", "", 0)
			assertErrString(t, client.CreateComment(context.Background(), 10, "New comment"), tc.wantErr)
			assertVikunjaCalls(t, fake, []string{route})
		})
	}
}

// TestVikunjaClient_AuthAndRetry covers doJSON's token bootstrap, 401
// refresh-and-retry and transport/parse failures through GetTask.
func TestVikunjaClient_AuthAndRetry(t *testing.T) {
	t.Parallel()

	refreshed := vikunjaOK(`{"token":"refreshed-token"}`)
	task := vikunjaOK(`{"id":10,"title":"Success"}`)
	expired := vikunjaResp{status: http.StatusUnauthorized, body: `{"message":"token expired"}`}
	tests := []struct {
		name      string
		token     string
		url       string
		routes    map[string][]vikunjaResp
		want      *config.VikunjaTask
		wantErr   string
		wantCalls []string
	}{
		{name: "blank vikunja URL fails fast", url: "   ", wantErr: "vikunja URL is not configured"},
		{
			name:   "empty token bootstraps login",
			routes: map[string][]vikunjaResp{vikunjaLogin: refreshed, vikunjaGetTask: task},
			want:   &config.VikunjaTask{ID: 10, Title: "Success"}, wantCalls: []string{vikunjaLogin, vikunjaGetTask},
		},
		{
			name: "401 unauthorized refreshes token and retries", token: "stale-token",
			routes: map[string][]vikunjaResp{vikunjaLogin: refreshed, vikunjaGetTask: {expired, task[0]}},
			want:   &config.VikunjaTask{ID: 10, Title: "Success"}, wantCalls: []string{vikunjaGetTask, vikunjaLogin, vikunjaGetTask},
		},
		{
			name: "401 reauth fails returns error", token: "stale-token",
			routes:    map[string][]vikunjaResp{vikunjaLogin: vikunjaFail, vikunjaGetTask: {expired}},
			wantErr:   "reauth on 401 failed: vikunja authentication failed: " + vikunjaErr500,
			wantCalls: []string{vikunjaGetTask, vikunjaLogin, vikunjaRegister, vikunjaLogin},
		},
		{
			name: "invalid json returns parse error", token: "token",
			routes:  map[string][]vikunjaResp{vikunjaGetTask: vikunjaOK(`{not valid json`)},
			wantErr: "failed to parse response JSON", wantCalls: []string{vikunjaGetTask},
		},
		{
			name: "dropped connection returns http failure", token: "token",
			routes:  map[string][]vikunjaResp{vikunjaGetTask: {{hangup: true}}},
			wantErr: "http request GET /api/v1/tasks/10 failed", wantCalls: []string{vikunjaGetTask},
		},
		{
			name:      "bootstrap auth fails propagates error",
			routes:    map[string][]vikunjaResp{vikunjaLogin: vikunjaFail, vikunjaRegister: vikunjaFail},
			wantErr:   "vikunja authentication failed: " + vikunjaErr500,
			wantCalls: []string{vikunjaLogin, vikunjaRegister, vikunjaLogin},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, fake := newVikunjaFakeClient(t, tc.routes, tc.token, tc.url, 0)
			got, err := client.GetTask(context.Background(), 10)
			assertErrString(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("GetTask = %+v, want %+v", got, tc.want)
			}
			assertVikunjaCalls(t, fake, tc.wantCalls)
		})
	}
}
