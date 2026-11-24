package server

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/httpkittest"
)

func TestStatusHandler_TasksAndAgents(t *testing.T) {
	mockServer := newStatusMockServer(t)
	defer mockServer.Close()

	tests := []statusTasksTestCase{
		{name: "ActiveTasks_Success", brainPath: "/brain/ok", wantCode: http.StatusOK, wantTasks: 2, wantConfiguredAgents: []string{"aerial", "agent-alpha"}},
		{
			name: "ActiveTasks_FilterAgent_Aerial", brainPath: "/brain/ok", target: "/api/status?agent=aerial",
			wantCode: http.StatusOK, wantTasks: 1, wantTaskID: "task-abc", wantConfiguredAgents: []string{"aerial", "agent-alpha"},
		},
		{
			name: "ActiveTasks_FilterAgent_Other", brainPath: "/brain/ok", target: "/api/status?agent=agent-alpha",
			wantCode: http.StatusOK, wantTasks: 1, wantTaskID: "task-def", wantConfiguredAgents: []string{"aerial", "agent-alpha"},
		},
		{
			name: "ActiveTasks_FilterAgent_All", brainPath: "/brain/ok", target: "/api/status?agent=all",
			wantCode: http.StatusOK, wantTasks: 2, wantConfiguredAgents: []string{"aerial", "agent-alpha"},
		},
		{
			name: "ActiveTasks_EmptyTasks_UpstreamSchedulesAndBuildsDiscovery", brainPath: "/brain/empty", haroPath: "/haro/ok", vanadisPath: "/vanadis/ok",
			useMux: true, wantCode: http.StatusOK, wantTasks: 0, wantConfiguredAgents: []string{"aerial", "agent-beta", "agent-gamma"},
		},
		{name: "ActiveTasks_Degraded", brainPath: "/brain/offline", wantCode: http.StatusOK, wantTasks: 0},
		{
			name: "EnrichedFields", yamlContent: "console:\n  repos:\n    - \"example/aerial\"\n",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runStatusTasksCase(t, tc, mockServer.URL)
		})
	}
}

func TestStatusHandler_ClusterAndDeploys(t *testing.T) {
	k8sAPI := newFakeK8sAPI(t)

	tests := []statusClusterTestCase{
		{name: "Basic_NoClusterStateFallback", wantCode: http.StatusOK, wantMinServices: 1, wantOngoingDeploy: "idle"},
		{name: "DegradedClusterStatus", k8sPath: "/degraded", wantCode: http.StatusOK, wantClusterStatus: "degraded"},
		{
			name: "PollerRunBuilding",
			pollerRuns: []GitHubRun{
				{
					ID:        999,
					HeadSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					Status:    "in_progress",
					CreatedAt: time.Now().UTC().Add(-2 * time.Minute),
					UpdatedAt: time.Now().UTC().Add(-1 * time.Minute),
				},
			},
			pollerJobs: map[int64][]GitHubJob{
				999: {{ID: 1, Name: "Build Dashboard", Status: "in_progress"}},
			},
			yamlContent:       "console: {}\n",
			wantCode:          http.StatusOK,
			wantDeploys:       1,
			wantOngoingDeploy: "building",
			wantIsDeploying:   true,
		},
		{name: "MethodNotAllowed", method: http.MethodPost, wantCode: http.StatusMethodNotAllowed},
		{name: "K8s_State_Success", k8sPath: "/ok", wantCode: http.StatusOK, wantServiceName: "k8s-brain"},
		{name: "K8s_Error_FallbackServices", k8sPath: "/dep500", wantCode: http.StatusOK, wantMinServices: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runStatusClusterCase(t, tc, k8sAPI.URL)
		})
	}
}

func newStatusMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/brain/ok/tasks"):
			_, _ = w.Write([]byte(`{
				"status": "ok",
				"total": 2,
				"tasks": [
					{
						"id": "task-abc",
						"thread_id": "thread-123",
						"session_id": "session-456",
						"author_name": "UserA",
						"prompt": "Test execution prompt",
						"summary": "Test execution prompt summary",
						"status": "PROCESSING",
						"retry_count": 0,
						"trigger_type": "discord",
						"agent": "aerial",
						"created_at": "2026-08-30T12:00:00Z",
						"updated_at": "2026-08-30T12:00:10Z"
					},
					{
						"id": "task-def",
						"thread_id": "thread-124",
						"session_id": "session-457",
						"author_name": "UserB",
						"prompt": "Test execution prompt 2",
						"summary": "Test execution prompt summary 2",
						"status": "PROCESSING",
						"retry_count": 0,
						"trigger_type": "discord",
						"agent": "agent-alpha",
						"created_at": "2026-08-30T12:00:00Z",
						"updated_at": "2026-08-30T12:00:10Z"
					}
				]
			}`))
		case strings.HasPrefix(r.URL.Path, "/brain/empty/tasks"):
			_, _ = w.Write([]byte(`{
				"status": "ok",
				"total": 0,
				"tasks": []
			}`))
		case strings.HasPrefix(r.URL.Path, "/haro/ok/schedules"):
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Status: "ok",
				Crons: []CronSchedule{
					{ID: "cron-1", Agent: "agent-beta"},
				},
				OneShots: []OneShotSchedule{},
			})
		case strings.HasPrefix(r.URL.Path, "/vanadis/ok/builds"):
			_ = json.NewEncoder(w).Encode(BuildsAPIResponse{
				Status: "ok",
				Builds: []VanadisBuildDTO{
					{ID: "b-1", Agent: "agent-gamma"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
}

type statusTasksTestCase struct {
	name                 string
	brainPath            string
	haroPath             string
	vanadisPath          string
	target               string
	useMux               bool
	yamlContent          string
	wantCode             int
	wantTasks            int
	wantTaskID           string
	wantConfiguredAgents []string
}

type statusClusterTestCase struct {
	name              string
	method            string
	k8sPath           string
	pollerRuns        []GitHubRun
	pollerJobs        map[int64][]GitHubJob
	yamlContent       string
	wantCode          int
	wantClusterStatus string
	wantOngoingDeploy string
	wantIsDeploying   bool
	wantDeploys       int
	wantMinServices   int
	wantServiceName   string
}

func statusMockURL(mockServerURL, path string) string {
	if path == "" {
		return ""
	}
	return mockServerURL + path
}

func runStatusTasksCase(t *testing.T, tc statusTasksTestCase, mockServerURL string) {
	t.Helper()
	cfgFile := ""
	if tc.yamlContent != "" {
		cfgFile = filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(cfgFile, []byte(tc.yamlContent), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	brainURL := statusMockURL(mockServerURL, tc.brainPath)
	haroURL := statusMockURL(mockServerURL, tc.haroPath)
	vanadisURL := statusMockURL(mockServerURL, tc.vanadisPath)

	discoveredAgentsCache.reset()

	var h http.Handler
	if tc.useMux {
		cfg := ConsoleConfig{
			Port:       "8080",
			BrainURL:   brainURL,
			ConfigPath: cfgFile,
			GitCommit:  "testcommit",
			HaroURL:    haroURL,
			VanadisURL: vanadisURL,
		}
		h = SetupConsoleMux(cfg, nil)
	} else {
		h = statusHandler(brainURL, "testcommit", "", haroURL, vanadisURL)
	}

	target := tc.target
	if target == "" {
		target = "/api/status"
	}
	req := httptest.NewRequest(http.MethodGet, target, http.NoBody)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != tc.wantCode {
		t.Fatalf("expected status %d, got %d", tc.wantCode, rr.Code)
	}
	if tc.wantCode != http.StatusOK {
		return
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	assertTasksResponse(t, tc, resp)
}

func assertTasksResponse(t *testing.T, tc statusTasksTestCase, resp ClusterResponse) {
	t.Helper()
	if len(tc.wantConfiguredAgents) > 0 {
		if len(resp.ConfiguredAgents) != len(tc.wantConfiguredAgents) {
			t.Fatalf("expected %d configured agents, got %d: %+v", len(tc.wantConfiguredAgents), len(resp.ConfiguredAgents), resp.ConfiguredAgents)
		}
		for i, a := range tc.wantConfiguredAgents {
			if resp.ConfiguredAgents[i] != a {
				t.Errorf("expected configured agent %d to be %q, got %q", i, a, resp.ConfiguredAgents[i])
			}
		}
	}
	if resp.ActiveTasks == nil {
		t.Fatalf("expected non-nil ActiveTasks, got null")
	}
	if resp.ActiveTasksCount != tc.wantTasks || len(resp.ActiveTasks) != tc.wantTasks {
		t.Fatalf("expected %d active tasks, got %d: %+v", tc.wantTasks, resp.ActiveTasksCount, resp.ActiveTasks)
	}
	if len(resp.ActiveTasks) == 0 {
		return
	}
	task := resp.ActiveTasks[0]
	expectedID := "task-abc"
	if tc.wantTaskID != "" {
		expectedID = tc.wantTaskID
	}
	if task.ID != expectedID || task.TriggerType != "discord" || task.Status != "PROCESSING" {
		t.Errorf("unexpected task contents: %+v", task)
	}
}

func runStatusClusterCase(t *testing.T, tc statusClusterTestCase, k8sURL string) {
	t.Helper()
	if tc.pollerRuns != nil {
		oldPoller := globalGHPoller
		defer func() { globalGHPoller = oldPoller }()
		globalGHPoller = &GitHubPoller{
			cachedRuns: tc.pollerRuns,
			cachedJobs: tc.pollerJobs,
		}
	}

	if tc.yamlContent != "" {
		if err := os.WriteFile(filepath.Join(t.TempDir(), "config.yaml"), []byte(tc.yamlContent), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	kubeAPIServer := statusMockURL(k8sURL, tc.k8sPath)
	discoveredAgentsCache.reset()

	h := statusHandler("", "testcommit", kubeAPIServer, "", "")

	method := tc.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, "/api/status", http.NoBody)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != tc.wantCode {
		t.Fatalf("expected status %d, got %d", tc.wantCode, rr.Code)
	}
	if tc.wantCode != http.StatusOK {
		return
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	assertClusterResponse(t, tc, resp)
}

func assertClusterResponse(t *testing.T, tc statusClusterTestCase, resp ClusterResponse) {
	t.Helper()
	if tc.wantClusterStatus != "" && resp.ClusterStatus != tc.wantClusterStatus {
		t.Errorf("expected ClusterStatus %q, got %q", tc.wantClusterStatus, resp.ClusterStatus)
	}
	if tc.wantOngoingDeploy != "" && resp.OngoingDeploy != tc.wantOngoingDeploy {
		t.Errorf("expected OngoingDeploy %q, got %q", tc.wantOngoingDeploy, resp.OngoingDeploy)
	}
	if tc.wantIsDeploying != resp.IsDeploying {
		t.Errorf("expected IsDeploying %v, got %v", tc.wantIsDeploying, resp.IsDeploying)
	}
	if tc.wantDeploys > 0 {
		if len(resp.Deployments) != tc.wantDeploys {
			t.Fatalf("expected %d deployments, got %d", tc.wantDeploys, len(resp.Deployments))
		}
		stages := map[string]string{}
		for _, d := range resp.Deployments {
			stages[d.Commit] = d.Stage
		}
		if stages["bbbbbbb"] != "building" {
			t.Errorf("expected commit bbbbbbb building, got %q", stages["bbbbbbb"])
		}
	}
	if tc.wantMinServices > 0 && len(resp.Services) < tc.wantMinServices {
		t.Errorf("expected at least %d services, got %d", tc.wantMinServices, len(resp.Services))
	}
	for _, svc := range resp.Services {
		if svc.UptimeSeconds < 0 {
			t.Errorf("service %s has negative uptime: %d", svc.Name, svc.UptimeSeconds)
		}
	}
	if tc.wantServiceName != "" {
		found := false
		for _, s := range resp.Services {
			if s.Name == tc.wantServiceName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected service %q, got services: %+v", tc.wantServiceName, resp.Services)
		}
	}
}

func TestResolveStaticDir(t *testing.T) {
	tempDirWithIndex := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDirWithIndex, "index.html"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("failed to create test index.html: %v", err)
	}

	tempDirWithoutIndex := t.TempDir()

	tests := []struct {
		name       string
		candidates []string
		wantDir    string
	}{
		{name: "default_candidates_fallback_to_static", wantDir: "static"},
		{name: "custom_dir_with_index", candidates: []string{tempDirWithIndex}, wantDir: tempDirWithIndex},
		{name: "first_missing_second_exists", candidates: []string{tempDirWithoutIndex, tempDirWithIndex}, wantDir: tempDirWithIndex},
		{name: "none_matching_fallback_to_static", candidates: []string{tempDirWithoutIndex, "/nonexistent/static/dir"}, wantDir: "static"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveStaticDir(tc.candidates...)
			if got != tc.wantDir {
				t.Errorf("resolveStaticDir(%v) = %q, want %q", tc.candidates, got, tc.wantDir)
			}
		})
	}
}

func TestConsoleServerLifecycle(t *testing.T) {
	// 1. Bad port errors
	errBadPort := RunConsoleServer(context.Background(), ConsoleConfig{Port: "bad-port-string"})
	if errBadPort == nil {
		t.Errorf("expected error on bad port in RunConsoleServer")
	}

	errFatal := RunConsoleServer(context.Background(), ConsoleConfig{Port: "-1"})
	if errFatal == nil {
		t.Errorf("expected fatal error on invalid port -1")
	}

	// 2. Successful server startup with TCP probe and graceful shutdown via cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address is %T, want *net.TCPAddr", l.Addr())
	}
	port := strconv.Itoa(addr.Port)
	_ = l.Close()

	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
	}))
	defer ghMock.Close()

	cfg := ConsoleConfig{
		Port:       port,
		BrainURL:   downServerURL(t),
		GHRepo:     "azylman/aerial",
		GHToken:    "dummy",
		APIBaseURL: ghMock.URL,
		GitCommit:  "testsha",
	}

	sighupReady := make(chan chan<- os.Signal, 1)
	origSighupNotify := sighupNotifyFn
	sighupNotifyFn = func(c chan<- os.Signal) {
		sighupReady <- c
	}
	defer func() { sighupNotifyFn = origSighupNotify }()

	t.Cleanup(func() {
		globalGHPoller = nil
	})

	done := make(chan error, 1)
	go func() {
		done <- RunConsoleServer(ctx, cfg)
	}()

	probeAddr := "127.0.0.1:" + port
	deadline := time.Now().Add(2 * time.Second)
	started := false
	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", probeAddr, 15*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			started = true
			break
		}
		runtime.Gosched()
	}
	if !started {
		t.Fatalf("server failed to start on %s within deadline", probeAddr)
	}

	// Send SIGHUP via injected signal channel to exercise static asset reload
	capturedSighup := <-sighupReady
	capturedSighup <- syscall.SIGHUP

	// Verify server remains responsive after SIGHUP
	resp, err := http.Get("http://" + probeAddr + "/health")
	if err != nil {
		t.Errorf("server health check failed after SIGHUP: %v", err)
	} else {
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 OK after SIGHUP, got %d", resp.StatusCode)
		}
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunConsoleServer returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunConsoleServer did not shut down in time")
	}
}

func TestFetchActiveTasksFromBrain_Extended(t *testing.T) {
	origClient := brainHTTPClient
	defer func() { brainHTTPClient = origClient }()

	tests := []struct {
		name        string
		brainURL    string
		statusCode  int
		body        string
		wantTasks   int
		wantSummary string
		wantPrompt  string
		wantErr     bool
	}{
		{name: "EmptyURL"},
		{name: "HTTP500_Error", brainURL: "http://brain", statusCode: http.StatusInternalServerError, body: "error", wantErr: true},
		{name: "BadJSON_Error", brainURL: "http://brain", statusCode: http.StatusOK, body: "bad json", wantErr: true},
		{
			name:        "SummaryFallbackToPrompt",
			brainURL:    "http://brain",
			statusCode:  http.StatusOK,
			body:        `{"status":"ok","total":1,"tasks":[{"id":"t1","status":"PENDING","prompt":"do something","summary":"","created_at":"2026-08-30T12:00:00Z"}]}`,
			wantTasks:   1,
			wantSummary: "do something",
			wantPrompt:  "do something",
		},
		{
			name:        "PromptTruncationExceeds500Runes",
			brainURL:    "http://brain",
			statusCode:  http.StatusOK,
			body:        fmt.Sprintf(`{"status":"ok","total":1,"tasks":[{"id":"t2","status":"PENDING","prompt":%q,"summary":"explicit summary","created_at":"2026-08-30T12:00:00Z"}]}`, strings.Repeat("A", 600)),
			wantTasks:   1,
			wantSummary: "explicit summary",
			wantPrompt:  strings.Repeat("A", 500) + "...",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.statusCode != 0 || tc.body != "" {
				brainHTTPClient = &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							return &http.Response{
								StatusCode: tc.statusCode,
								Body:       io.NopCloser(strings.NewReader(tc.body)),
								Header:     make(http.Header),
							}, nil
						},
					},
				}
			}
			tasks, err := fetchActiveTasksFromBrain(context.Background(), tc.brainURL)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tasks) != tc.wantTasks {
				t.Errorf("expected %d tasks, got %d", tc.wantTasks, len(tasks))
			}
			if tc.wantSummary != "" && (len(tasks) == 0 || tasks[0].Summary != tc.wantSummary) {
				t.Errorf("expected task summary to fallback to prompt, got %+v", tasks)
			}
			if tc.wantPrompt != "" && (len(tasks) == 0 || tasks[0].Prompt != tc.wantPrompt) {
				t.Errorf("expected task prompt %q, got %+v", tc.wantPrompt, tasks)
			}
		})
	}
}

type roundTripperFunc struct {
	fn func(req *http.Request) (*http.Response, error)
}

func (r *roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.fn(req)
}

// trackedBody is a response body that records how much was left unread and whether it was closed.
type trackedBody struct {
	r        *strings.Reader
	readErr  error
	closeErr error
	closed   bool
}

func newTrackedBody(content string, readErr, closeErr error) *trackedBody {
	return &trackedBody{r: strings.NewReader(content), readErr: readErr, closeErr: closeErr}
}

func (b *trackedBody) Read(p []byte) (int, error) {
	if b.readErr != nil {
		return 0, b.readErr
	}
	return b.r.Read(p)
}

func (b *trackedBody) Close() error {
	b.closed = true
	return b.closeErr
}

func TestDrainAndClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          *trackedBody
		wantRemaining int
	}{
		{name: "small body fully drained and closed", body: newTrackedBody("clean body content", nil, nil), wantRemaining: 0},
		{name: "drain stops at 32KiB limit", body: newTrackedBody(strings.Repeat("x", 40000), nil, nil), wantRemaining: 7232},
		{name: "read error still closes body", body: newTrackedBody("unread", errors.New("simulated copy read error"), nil), wantRemaining: 6},
		{name: "close error after full drain", body: newTrackedBody("hello", nil, errors.New("simulated close error")), wantRemaining: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			drainAndClose(tc.body, tc.name)
			if got := tc.body.r.Len(); got != tc.wantRemaining {
				t.Errorf("unread bytes after drainAndClose = %d, want %d", got, tc.wantRemaining)
			}
			if !tc.body.closed {
				t.Error("drainAndClose did not close the body")
			}
		})
	}
}

func TestWriteJSON(t *testing.T) {
	tests := []struct {
		name           string
		acceptEncoding string
		status         int
		data           any
		wantCode       int
		wantType       string
		wantEncoding   string
		wantBody       string
	}{
		{name: "plain JSON keeps caller status", status: http.StatusCreated, data: map[string]string{"status": "ok"}, wantCode: http.StatusCreated, wantType: "application/json", wantBody: `{"status":"ok"}`},
		{name: "marshal error returns 500", status: http.StatusOK, data: make(chan int), wantCode: http.StatusInternalServerError, wantType: "text/plain; charset=utf-8", wantBody: `{"error":"Internal error"}` + "\n"},
		{name: "gzip when accepted", acceptEncoding: "gzip, deflate", status: http.StatusOK, data: map[string]string{"status": "ok"}, wantCode: http.StatusOK, wantType: "application/json", wantEncoding: "gzip", wantBody: `{"status":"ok"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/builds", http.NoBody)
			req.Header.Set("Accept-Encoding", tc.acceptEncoding)
			rr := httptest.NewRecorder()
			writeJSON(rr, req, tc.status, tc.data)
			if rr.Code != tc.wantCode {
				t.Errorf("code = %d, want %d", rr.Code, tc.wantCode)
			}
			if got := rr.Header().Get("Content-Type"); got != tc.wantType {
				t.Errorf("Content-Type = %q, want %q", got, tc.wantType)
			}
			if got := rr.Header().Get("Content-Encoding"); got != tc.wantEncoding {
				t.Fatalf("Content-Encoding = %q, want %q", got, tc.wantEncoding)
			}
			var body io.Reader = rr.Body
			if tc.wantEncoding == "gzip" {
				gz, err := gzip.NewReader(rr.Body)
				if err != nil {
					t.Fatalf("gzip.NewReader: %v", err)
				}
				defer gz.Close()
				body = gz
			}
			got, err := io.ReadAll(body)
			if err != nil || string(got) != tc.wantBody {
				t.Errorf("body = %q (err=%v), want %q", got, err, tc.wantBody)
			}
		})
	}
}

// assertTraceparentTrace fails unless traceparent is a W3C header carrying traceID.
func assertTraceparentTrace(t *testing.T, traceparent, traceID string) {
	t.Helper()
	if parts := strings.Split(traceparent, "-"); len(parts) != 4 || parts[1] != traceID {
		t.Errorf("upstream traceparent = %q, want trace %s", traceparent, traceID)
	}
}

func TestSetupConsoleMux_Tracing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		target   string
		traceID  string
		wantSpan string
	}{
		{name: "facts proxy via brain client", target: "/api/facts", traceID: "11111111111111111111111111111111", wantSpan: "GET /api/facts"},
		{name: "builds proxy via vanadis client", target: "/console/api/builds", traceID: "22222222222222222222222222222222", wantSpan: "GET /console/api/builds"},
		{name: "docket proxy via docket client", target: "/api/docket/tasks", traceID: "33333333333333333333333333333333", wantSpan: "GET /api/docket/tasks"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			upstreamTraceparent := make(chan string, 4)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamTraceparent <- r.Header.Get("Traceparent")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(upstream.Close)
			cfg := ConsoleConfig{BrainURL: upstream.URL, MemoryURL: upstream.URL, VanadisURL: upstream.URL, DocketURL: upstream.URL}
			req := httptest.NewRequest(http.MethodGet, tc.target, http.NoBody)
			req.Header.Set("Traceparent", "00-"+tc.traceID+"-00f067aa0ba902b7-01")
			SetupConsoleMux(cfg, nil).ServeHTTP(httptest.NewRecorder(), req)

			select {
			case got := <-upstreamTraceparent:
				assertTraceparentTrace(t, got, tc.traceID)
			default:
				t.Fatal("handler made no upstream call")
			}
			var server trace.SpanKind
			for _, s := range spanRecorder.Ended() {
				if s.SpanContext().TraceID().String() == tc.traceID && s.Name() == tc.wantSpan {
					server = s.SpanKind()
				}
			}
			if server != trace.SpanKindServer {
				t.Errorf("no server span %q in trace %s", tc.wantSpan, tc.traceID)
			}
		})
	}
}

// doStaticRoutingRequest sends method (GET when empty) to url and returns the status code,
// headers and fully read body.
func doStaticRoutingRequest(t *testing.T, client *http.Client, method, url string) (int, http.Header, []byte) {
	t.Helper()
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequest(method, url, http.NoBody)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}
	return resp.StatusCode, resp.Header, body
}

func TestSetupConsoleMux_StaticRouting(t *testing.T) {
	reg, err := NewAssetRegistry(fstest.MapFS{
		"index.html":          {Data: []byte(`<div id="root"></div>`)},
		"assets/index-Zz9.js": {Data: []byte(`export{}`)},
	})
	if err != nil {
		t.Fatalf("NewAssetRegistry failed: %v", err)
	}
	ts := httptest.NewServer(SetupConsoleMux(ConsoleConfig{}, reg))
	defer ts.Close()
	client := ts.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	tests := []struct {
		name           string
		method         string
		path           string
		wantCode       int
		wantLocation   string
		wantCache      string
		wantBodySubstr string
		wantHeaders    map[string]string
	}{
		{name: "console_shell", path: "/console/", wantCode: http.StatusOK, wantCache: "no-cache, must-revalidate", wantBodySubstr: `<div id="root"></div>`},
		{name: "root_shell", path: "/", wantCode: http.StatusOK, wantCache: "no-cache, must-revalidate", wantBodySubstr: `<div id="root"></div>`},
		{name: "console_deep_link_shell", path: "/console/schedules", wantCode: http.StatusOK, wantCache: "no-cache, must-revalidate", wantBodySubstr: `<div id="root"></div>`},
		{name: "console_hashed_asset", path: "/console/assets/index-Zz9.js", wantCode: http.StatusOK, wantCache: "public, max-age=31536000, immutable", wantBodySubstr: `export{}`},
		{name: "root_hashed_asset", path: "/assets/index-Zz9.js", wantCode: http.StatusOK, wantCache: "public, max-age=31536000, immutable", wantBodySubstr: `export{}`},
		{name: "nextgen_route_not_legacy", path: "/console/nextgen", wantCode: http.StatusOK, wantCache: "no-cache, must-revalidate", wantBodySubstr: `<div id="root"></div>`},
		{name: "api_routes_still_win", path: "/console/health", wantCode: http.StatusOK, wantBodySubstr: "OK"},
		{name: "root_health", path: "/health", wantCode: http.StatusOK, wantBodySubstr: "OK"},
		{name: "health_rejects_post", method: http.MethodPost, path: "/console/health", wantCode: http.StatusMethodNotAllowed, wantBodySubstr: "Method Not Allowed"},
		{name: "security_headers", path: "/console/", wantCode: http.StatusOK, wantHeaders: map[string]string{"X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "X-XSS-Protection": "1; mode=block"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, header, body := doStaticRoutingRequest(t, client, tc.method, ts.URL+tc.path)
			if status != tc.wantCode {
				t.Fatalf("GET %s status = %d, want %d", tc.path, status, tc.wantCode)
			}
			if got := header.Get("Location"); got != tc.wantLocation {
				t.Errorf("Location = %q, want %q", got, tc.wantLocation)
			}
			if tc.wantCache != "" && header.Get("Cache-Control") != tc.wantCache {
				t.Errorf("Cache-Control = %q, want %q", header.Get("Cache-Control"), tc.wantCache)
			}
			if !strings.Contains(string(body), tc.wantBodySubstr) {
				t.Errorf("body = %q, want substring %q", body, tc.wantBodySubstr)
			}
			for k, v := range tc.wantHeaders {
				if got := header.Get(k); got != v {
					t.Errorf("header %s = %q, want %q", k, got, v)
				}
			}
		})
	}
}

func TestCoverageFlushEndpoint(t *testing.T) {
	httpkittest.StubCoverageWriters(t, nil, nil)
	h := SetupConsoleMux(ConsoleConfig{}, nil)

	tests := []struct {
		name string
		path string
	}{
		{"root flush route wired", "/debug/coverage/flush"},
		{"console flush route wired", "/console/debug/coverage/flush"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.path, http.NoBody))
			if rr.Code != http.StatusOK || rr.Header().Get("Content-Type") != "application/x-tar" {
				t.Errorf("GET %s = %d %q, want 200 \"application/x-tar\"", tc.path, rr.Code, rr.Header().Get("Content-Type"))
			}
		})
	}
}

func TestCachedDiscoveredAgents_TableDriven(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		initial   []string
		queryTime time.Time
		wantHit   bool
		wantCount int
	}{
		{name: "empty cache misses", queryTime: now},
		{name: "fresh cache hits", initial: []string{"aerial", "agent-alpha"}, queryTime: now.Add(10 * time.Second), wantHit: true, wantCount: 2},
		{name: "expired cache misses", initial: []string{"aerial", "agent-alpha"}, queryTime: now.Add(31 * time.Second)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := &cachedDiscoveredAgents{ttl: 30 * time.Second}
			if len(tc.initial) > 0 {
				c.set(tc.initial, now)
			}
			got, ok := c.get(tc.queryTime)
			if ok != tc.wantHit {
				t.Fatalf("c.get() ok = %v, want %v", ok, tc.wantHit)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("c.get() len = %d, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func (c *cachedDiscoveredAgents) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agents = nil
	c.fetchedAt = time.Time{}
}

func TestFetchUpstreamDiscoveredAgents_TableDriven(t *testing.T) {
	t.Parallel()
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schedules":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Crons:    []CronSchedule{{ID: "c1", Agent: "agent-haro"}},
				OneShots: []OneShotSchedule{{ID: "o1", Agent: "agent-oneshot"}},
			})
		case "/builds":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(BuildsAPIResponse{
				Builds: []VanadisBuildDTO{{ID: "b1", Agent: "agent-vanadis"}},
			})
		case "/error/schedules", "/error/builds":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(mockServer.Close)

	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		haroURL    string
		vanadisURL string
		wantCount  int
		wantAgents []string
	}{
		{name: "empty urls return empty"},
		{name: "haro only discovers schedule agents", haroURL: mockServer.URL, wantCount: 2, wantAgents: []string{"agent-haro", "agent-oneshot"}},
		{name: "vanadis only discovers build agents", vanadisURL: mockServer.URL, wantCount: 1, wantAgents: []string{"agent-vanadis"}},
		{
			name:       "both upstream urls query concurrently",
			haroURL:    mockServer.URL,
			vanadisURL: mockServer.URL,
			wantCount:  3,
			wantAgents: []string{"agent-haro", "agent-oneshot", "agent-vanadis"},
		},
		{name: "upstream errors return empty gracefully", haroURL: mockServer.URL + "/error", vanadisURL: mockServer.URL + "/error"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cache := &cachedDiscoveredAgents{ttl: 30 * time.Second}
			got := fetchUpstreamDiscoveredAgentsWithCache(context.Background(), cache, tc.haroURL, tc.vanadisURL, now)
			if len(got) != tc.wantCount {
				t.Fatalf("fetchUpstreamDiscoveredAgentsWithCache() count = %d, want %d: %v", len(got), tc.wantCount, got)
			}
			for _, want := range tc.wantAgents {
				if !slices.Contains(got, want) {
					t.Errorf("expected agent %q in results %v", want, got)
				}
			}

			if tc.wantCount > 0 {
				cachedGot := fetchUpstreamDiscoveredAgentsWithCache(context.Background(), cache, "http://invalid-server-that-should-not-be-called:9999", "http://invalid-server-that-should-not-be-called:9999", now.Add(5*time.Second))
				if len(cachedGot) != tc.wantCount {
					t.Fatalf("cached call count = %d, want %d", len(cachedGot), tc.wantCount)
				}
			}
		})
	}
}

func TestDiscoverConfiguredAgents_TableDriven(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	discoveredAgentsCache.reset()
	tests := []struct {
		name       string
		tasks      []ActiveTaskStatus
		haroURL    string
		vanadisURL string
		want       []string
	}{
		{
			name:  "tasks with agents discovered",
			tasks: []ActiveTaskStatus{{ID: "t1", Agent: "agent-alpha"}, {ID: "t2", Agent: "agent-beta"}},
			want:  []string{"aerial", "agent-alpha", "agent-beta"},
		},
		{
			name:  "empty tasks discovers aerial",
			tasks: nil,
			want:  []string{"aerial"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := discoverConfiguredAgents(context.Background(), tc.tasks, tc.haroURL, tc.vanadisURL, now)
			if len(got) != len(tc.want) {
				t.Fatalf("discoverConfiguredAgents() len = %d, want %d: %v", len(got), len(tc.want), got)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("discoverConfiguredAgents()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func newEriTasksServer(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tasks" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func downServerURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	return u
}

func TestFetchActiveTasksFromAgents_TableDriven(t *testing.T) {
	t.Parallel()
	healthy := newEriTasksServer(t, `{"status":"ok","total":2,"tasks":[{"id":"m1","status":"PROCESSING"},{"id":"m2","status":"PENDING","agent":"agent-alpha"}]}`)
	other := newEriTasksServer(t, `{"status":"ok","total":1,"tasks":[{"id":"s1","status":"PROCESSING"}]}`)
	down := downServerURL(t)
	broken := newEriTasksServer(t, `not json`)

	tests := []struct {
		name      string
		targets   []AgentBrainTarget
		wantIDs   []string
		wantAgent []string
	}{
		{
			name:      "healthy rows stamped, down agent dropped",
			targets:   []AgentBrainTarget{{Agent: "agent-alpha", URL: healthy}, {Agent: "agent-beta", URL: down}},
			wantIDs:   []string{"m1", "m2"},
			wantAgent: []string{"agent-alpha", "agent-alpha"},
		},
		{
			name:      "merge preserves target order",
			targets:   []AgentBrainTarget{{Agent: "agent-beta", URL: other}, {Agent: "agent-alpha", URL: healthy}},
			wantIDs:   []string{"s1", "m1", "m2"},
			wantAgent: []string{"agent-beta", "agent-alpha", "agent-alpha"},
		},
		{
			name:      "undecodable body dropped",
			targets:   []AgentBrainTarget{{Agent: "agent-beta", URL: broken}, {Agent: "agent-alpha", URL: healthy}},
			wantIDs:   []string{"m1", "m2"},
			wantAgent: []string{"agent-alpha", "agent-alpha"},
		},
		{name: "all down yields empty list", targets: []AgentBrainTarget{{Agent: "agent-beta", URL: down}}, wantIDs: []string{}, wantAgent: []string{}},
		{name: "no targets", targets: nil, wantIDs: []string{}, wantAgent: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := fetchActiveTasksFromAgents(context.Background(), tc.targets)
			if got == nil {
				t.Fatal("expected non-nil slice")
			}
			ids := make([]string, 0, len(got))
			agents := make([]string, 0, len(got))
			for _, task := range got {
				ids = append(ids, task.ID)
				agents = append(agents, task.Agent)
			}
			if !slices.Equal(ids, tc.wantIDs) || !slices.Equal(agents, tc.wantAgent) {
				t.Errorf("got ids=%v agents=%v, want ids=%v agents=%v", ids, agents, tc.wantIDs, tc.wantAgent)
			}
		})
	}
}
