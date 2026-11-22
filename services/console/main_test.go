package main

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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/httpkittest"
)

func TestStatusHandler(t *testing.T) {
	mockServer := newStatusMockServer(t)
	defer mockServer.Close()
	k8sAPI := newFakeK8sAPI(t)

	tests := append(statusHandlerPrimaryCases(), statusHandlerSecondaryCases()...)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runStatusHandlerCase(t, tc, mockServer.URL, k8sAPI.URL)
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

func statusHandlerPrimaryCases() []statusTestCase {
	return []statusTestCase{
		{name: "Basic_NoClusterStateFallback", wantCode: http.StatusOK, wantMinServices: 1, wantOngoingDeploy: "idle"},
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
			name: "EnrichedFields", yamlContent: "console:\n  quick_launch_links:\n    - name: \"HOME\"\n      url: \"https://home.example.com\"\n",
			wantCode: http.StatusOK, wantQuickLink: "HOME",
		},
		{name: "DegradedClusterStatus", k8sPath: "/degraded", wantCode: http.StatusOK, wantClusterStatus: "degraded"},
	}
}

func statusHandlerSecondaryCases() []statusTestCase {
	return []statusTestCase{
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
}

// statusTestCase routes each upstream to the shared mock server (k8sPath: the fake k8s API)
// by path suffix; an empty suffix leaves that upstream unconfigured.
type statusTestCase struct {
	name                 string
	method               string
	target               string
	useMux               bool
	brainPath            string
	k8sPath              string
	haroPath             string
	vanadisPath          string
	pollerRuns           []GitHubRun
	pollerJobs           map[int64][]GitHubJob
	yamlContent          string
	wantCode             int
	wantClusterStatus    string
	wantOngoingDeploy    string
	wantIsDeploying      bool
	wantTasks            int
	wantDeploys          int
	wantMinServices      int
	wantServiceName      string
	wantQuickLink        string
	wantConfiguredAgents []string
	wantTaskID           string
}

func statusMockURL(mockServerURL, path string) string {
	if path == "" {
		return ""
	}
	return mockServerURL + path
}

func runStatusHandlerCase(t *testing.T, tc statusTestCase, mockServerURL, k8sURL string) {
	t.Helper()
	if tc.pollerRuns != nil {
		oldPoller := globalGHPoller
		defer func() { globalGHPoller = oldPoller }()
		globalGHPoller = &GitHubPoller{
			cachedRuns: tc.pollerRuns,
			cachedJobs: tc.pollerJobs,
		}
	}

	cfgFile := ""
	if tc.yamlContent != "" {
		tempDir := t.TempDir()
		cfgFile = filepath.Join(tempDir, "config.yaml")
		if err := os.WriteFile(cfgFile, []byte(tc.yamlContent), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	brainURL := statusMockURL(mockServerURL, tc.brainPath)
	kubeAPIServer := statusMockURL(k8sURL, tc.k8sPath)
	haroURL := statusMockURL(mockServerURL, tc.haroPath)
	vanadisURL := statusMockURL(mockServerURL, tc.vanadisPath)

	discoveredAgentsCache.reset()

	var h http.Handler
	if tc.useMux {
		cfg := ConsoleConfig{
			Port:          "8080",
			BrainURL:      brainURL,
			ConfigPath:    cfgFile,
			GitCommit:     "testcommit",
			KubeAPIServer: kubeAPIServer,
			HaroURL:       haroURL,
			VanadisURL:    vanadisURL,
		}
		h = SetupConsoleMux(cfg, nil)
	} else {
		h = statusHandler(brainURL, cfgFile, "testcommit", kubeAPIServer, haroURL, vanadisURL)
	}

	method := tc.method
	if method == "" {
		method = http.MethodGet
	}
	target := tc.target
	if target == "" {
		target = "/api/status"
	}
	req := httptest.NewRequest(method, target, http.NoBody)
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

	assertStatusResponse(t, tc, resp)
}

func assertStatusResponse(t *testing.T, tc statusTestCase, resp ClusterResponse) {
	t.Helper()
	assertStatusBasic(t, tc, resp)
	assertStatusServices(t, tc, resp)
}

func assertStatusBasic(t *testing.T, tc statusTestCase, resp ClusterResponse) {
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
	assertStatusActiveTasks(t, tc, resp)
	assertStatusDeployments(t, tc, resp)
}

func assertStatusActiveTasks(t *testing.T, tc statusTestCase, resp ClusterResponse) {
	t.Helper()
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

func assertStatusDeployments(t *testing.T, tc statusTestCase, resp ClusterResponse) {
	t.Helper()
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
}

func assertStatusServices(t *testing.T, tc statusTestCase, resp ClusterResponse) {
	t.Helper()
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
	if tc.wantQuickLink != "" {
		found := false
		for _, l := range resp.QuickLaunchLinks {
			if l.Name == tc.wantQuickLink {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected quick link %q in %+v", tc.wantQuickLink, resp.QuickLaunchLinks)
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
		{name: "legacy_console_next_bare", path: "/console/next", wantCode: http.StatusMovedPermanently, wantLocation: "/console/"},
		{name: "legacy_console_next_slash", path: "/console/next/", wantCode: http.StatusMovedPermanently, wantLocation: "/console/"},
		{name: "legacy_console_next_deep_query", path: "/console/next/memory?x=1", wantCode: http.StatusMovedPermanently, wantLocation: "/console/memory?x=1"},
		{name: "legacy_root_next_bare", path: "/next", wantCode: http.StatusMovedPermanently, wantLocation: "/"},
		{name: "legacy_root_next_slash_query", path: "/next/?tab=memory", wantCode: http.StatusMovedPermanently, wantLocation: "/?tab=memory"},
		{name: "legacy_root_next_deep", path: "/next/builds/abc", wantCode: http.StatusMovedPermanently, wantLocation: "/builds/abc"},
		{name: "legacy_root_next_nested_next", path: "/next/next/x", wantCode: http.StatusMovedPermanently, wantLocation: "/next/x"},
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

func TestMain_Execution(t *testing.T) {
	t.Cleanup(installTestTracing) // main() runs tracing.Init, which replaces the test tracer
	origLoad := loadConfigFn
	origRun := runConsoleFn
	origExit := exitFn
	defer func() {
		loadConfigFn = origLoad
		runConsoleFn = origRun
		exitFn = origExit
	}()

	tests := []struct {
		name         string
		loadErr      error
		runErr       error
		wantRunCall  bool
		wantExitCall bool
	}{
		{name: "clean execution", wantRunCall: true},
		{name: "config load error exits", loadErr: errors.New("mock config error"), wantExitCall: true},
		{name: "run server error exits", runErr: errors.New("mock run error"), wantRunCall: true, wantExitCall: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCalled := false
			exitCalled := false

			loadConfigFn = func(_ string) (ConsoleConfig, error) {
				if tc.loadErr != nil {
					return ConsoleConfig{}, tc.loadErr
				}
				return ConsoleConfig{Port: "8084"}, nil
			}
			runConsoleFn = func(_ context.Context, _ ConsoleConfig) error {
				runCalled = true
				return tc.runErr
			}
			exitFn = func(string, ...any) {
				exitCalled = true
			}

			main()

			if runCalled != tc.wantRunCall {
				t.Errorf("runCalled = %v, want %v", runCalled, tc.wantRunCall)
			}
			if exitCalled != tc.wantExitCall {
				t.Errorf("exitCalled = %v, want %v", exitCalled, tc.wantExitCall)
			}
		})
	}
}
