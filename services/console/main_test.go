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

	"github.com/azylman/aerial/pkg/httpkit"
)

func TestStatusHandler(t *testing.T) {
	mockServer := newStatusMockServer(t)
	defer mockServer.Close()

	tests := append(statusHandlerPrimaryCases(), statusHandlerSecondaryCases()...)

	origDockerDefault := dockerSocketClient
	defer func() { dockerSocketClient = origDockerDefault }()
	dockerSocketClient = &http.Client{Transport: &roundTripperFunc{fn: func(*http.Request) (*http.Response, error) {
		return nil, errors.New("hermetic test: docker socket disabled")
	}}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runStatusHandlerCase(t, tc, mockServer.URL)
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
				"total": 1,
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
						"created_at": "2026-08-30T12:00:00Z",
						"updated_at": "2026-08-30T12:00:10Z"
					}
				]
			}`))
		case strings.HasPrefix(r.URL.Path, "/hangar/synced/status"):
			_ = json.NewEncoder(w).Encode(GitSyncStatusResponse{
				Status:        "synced",
				MaxLagSeconds: 0,
				LastSyncTime:  time.Now().UTC(),
				Repos: map[string]RepoStatus{
					"/share/aerial": {DiskCommit: "abcdef123456"},
				},
			})
		case strings.HasPrefix(r.URL.Path, "/hangar/swapping/status"):
			_ = json.NewEncoder(w).Encode(GitSyncStatusResponse{
				Status: "synced",
				Reconciliation: &ReconciliationStatus{
					Active:         true,
					State:          "swapping",
					CommitSHA:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					TargetServices: []string{"aerial-dashboard"},
					StartedAt:      time.Now().UTC().Add(-10 * time.Second),
				},
			})
		case strings.HasPrefix(r.URL.Path, "/k8s/ok/apis/apps/v1/namespaces/default/deployments"):
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"k8s-brain"},"spec":{"replicas":1},"status":{"replicas":1,"readyReplicas":1}}]}`))
		case strings.HasPrefix(r.URL.Path, "/k8s/ok/api/v1/namespaces/default/pods"):
			_, _ = w.Write([]byte(`{"items":[{"metadata":{"name":"k8s-brain-pod","labels":{"app.kubernetes.io/name":"k8s-brain"}},"status":{"phase":"Running","containerStatuses":[{"name":"k8s-brain","ready":true,"state":{}}]}}]}`))
		case strings.HasPrefix(r.URL.Path, "/k8s/error500"):
			http.Error(w, "k8s server error", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
}

func statusHandlerPrimaryCases() []statusTestCase {
	return []statusTestCase{
		{
			name:              "Basic_DockerFallback",
			wantCode:          http.StatusOK,
			wantMinServices:   1,
			wantOngoingDeploy: "idle",
		},
		{
			name:              "OngoingDeploy",
			yamlContent:       "console: {}\n",
			wantCode:          http.StatusOK,
			wantOngoingDeploy: "idle",
		},
		{
			name:      "ActiveTasks_Success",
			brainMode: "ok",
			wantCode:  http.StatusOK,
			wantTasks: 1,
		},
		{
			name:      "ActiveTasks_Degraded",
			brainMode: "offline",
			wantCode:  http.StatusOK,
			wantTasks: 0,
		},
		{
			name:          "EnrichedFields",
			gitSyncMode:   "synced",
			yamlContent:   "console:\n  quick_launch_links:\n    - name: \"HOME\"\n      url: \"https://home.zylman.com\"\n",
			wantCode:      http.StatusOK,
			wantQuickLink: "HOME",
		},
		{
			name:              "DegradedClusterStatus",
			dockerJSON:        `[{"Names":["/aerial-brain"],"State":"running","Status":"Up 10m (unhealthy)","Created":1775240000}]`,
			wantCode:          http.StatusOK,
			wantClusterStatus: "degraded",
		},
	}
}

func statusHandlerSecondaryCases() []statusTestCase {
	return []statusTestCase{
		{
			name:        "ConcurrentPipelines",
			gitSyncMode: "swapping",
			dockerJSON:  "[]",
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
			wantDeploys:       2,
			wantOngoingDeploy: "swapping",
			wantIsDeploying:   true,
		},
		{
			name:     "MethodNotAllowed",
			method:   http.MethodPost,
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:          "SetupConsoleMux_StatusWiring",
			useMux:        true,
			gitSyncMode:   "synced",
			yamlContent:   "console:\n  quick_launch_links:\n    - name: \"TESTLINK\"\n      url: \"https://test.zylman.com\"\n",
			wantCode:      http.StatusOK,
			wantQuickLink: "TESTLINK",
		},
		{
			name:            "K8s_State_Success",
			k8sMode:         "ok",
			wantCode:        http.StatusOK,
			wantServiceName: "k8s-brain",
		},
		{
			name:            "K8s_State_Success_Via_Mux",
			useMux:          true,
			k8sMode:         "ok",
			wantCode:        http.StatusOK,
			wantServiceName: "k8s-brain",
		},
		{
			name:            "K8s_Error_Fallback_To_Docker",
			k8sMode:         "error500",
			wantCode:        http.StatusOK,
			wantMinServices: 1,
		},
	}
}

type statusTestCase struct {
	name              string
	method            string
	useMux            bool
	brainMode         string
	gitSyncMode       string
	k8sMode           string
	emptyCommit       bool
	dockerJSON        string
	pollerRuns        []GitHubRun
	pollerJobs        map[int64][]GitHubJob
	yamlContent       string
	wantCode          int
	wantClusterStatus string
	wantOngoingDeploy string
	wantIsDeploying   bool
	wantTasks         int
	wantDeploys       int
	wantMinServices   int
	wantServiceName   string
	wantQuickLink     string
}

func runStatusHandlerCase(t *testing.T, tc statusTestCase, mockServerURL string) {
	t.Helper()
	if tc.dockerJSON != "" {
		origDocker := dockerSocketClient
		defer func() { dockerSocketClient = origDocker }()
		dockerSocketClient = &http.Client{
			Transport: &roundTripperFunc{
				fn: func(req *http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(strings.NewReader(tc.dockerJSON)),
						Header:     make(http.Header),
					}, nil
				},
			},
		}
	}

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

	brainURL := ""
	switch tc.brainMode {
	case "ok":
		brainURL = mockServerURL + "/brain/ok"
	case "offline":
		brainURL = "http://127.0.0.1:54321"
	}

	gitSyncURL := ""
	switch tc.gitSyncMode {
	case "synced":
		gitSyncURL = mockServerURL + "/hangar/synced"
	case "swapping":
		gitSyncURL = mockServerURL + "/hangar/swapping"
	}

	kubeAPIServer := ""
	switch tc.k8sMode {
	case "ok":
		kubeAPIServer = mockServerURL + "/k8s/ok"
	case "error500":
		kubeAPIServer = mockServerURL + "/k8s/error500"
	}

	commitArg := "testcommit"
	if tc.emptyCommit {
		commitArg = ""
	}
	var h http.Handler
	if tc.useMux {
		cfg := ConsoleConfig{
			Port:          "8080",
			BrainURL:      brainURL,
			GitSyncURL:    gitSyncURL,
			ConfigPath:    cfgFile,
			GitCommit:     commitArg,
			KubeAPIServer: kubeAPIServer,
		}
		h = SetupConsoleMux(cfg, nil)
	} else {
		h = statusHandler(brainURL, gitSyncURL, cfgFile, commitArg, kubeAPIServer)
	}

	method := tc.method
	if method == "" {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, "/api/status", nil)
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
	if tc.wantOngoingDeploy != "" {
		if resp.OngoingDeploy != "idle" && resp.OngoingDeploy != "swapping" && resp.OngoingDeploy != "building" {
			t.Errorf("unexpected OngoingDeploy: %q", resp.OngoingDeploy)
		}
		if tc.wantOngoingDeploy != "idle" && resp.OngoingDeploy != tc.wantOngoingDeploy {
			t.Errorf("expected OngoingDeploy %q, got %q", tc.wantOngoingDeploy, resp.OngoingDeploy)
		}
	}
	if tc.wantIsDeploying != resp.IsDeploying {
		t.Errorf("expected IsDeploying %v, got %v", tc.wantIsDeploying, resp.IsDeploying)
	}
	assertStatusActiveTasks(t, tc, resp)
	assertStatusDeployments(t, tc, resp)
}

func assertStatusActiveTasks(t *testing.T, tc statusTestCase, resp ClusterResponse) {
	t.Helper()
	if tc.brainMode == "ok" {
		if resp.ActiveTasksCount != tc.wantTasks || len(resp.ActiveTasks) != tc.wantTasks {
			t.Fatalf("expected %d active tasks, got %d: %+v", tc.wantTasks, resp.ActiveTasksCount, resp.ActiveTasks)
		}
		if len(resp.ActiveTasks) > 0 {
			task := resp.ActiveTasks[0]
			if task.ID != "task-abc" || task.SessionID != "session-456" || task.TriggerType != "discord" || task.Summary != "Test execution prompt summary" || task.Status != "PROCESSING" || task.AuthorName != "UserA" {
				t.Errorf("unexpected task contents: %+v", task)
			}
		}
	}
	if tc.brainMode == "offline" {
		if resp.ActiveTasksCount != 0 {
			t.Errorf("expected 0 active tasks on degraded brain, got %d", resp.ActiveTasksCount)
		}
		if resp.ActiveTasks == nil || len(resp.ActiveTasks) != 0 {
			t.Errorf("expected non-nil empty ActiveTasks on degraded brain, got %+v", resp.ActiveTasks)
		}
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
		if stages["aaaaaaa"] != "swapping" {
			t.Errorf("expected commit aaaaaaa swapping, got %q", stages["aaaaaaa"])
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
		if resp.GitSync.Status != "synced" {
			t.Errorf("expected GitSync status 'synced', got %q", resp.GitSync.Status)
		}
	}
}

func TestDeploymentStatus_CommitTimeJSONSerialization(t *testing.T) {
	now := time.Date(2026, 9, 4, 4, 15, 0, 0, time.UTC)
	depWithTime := DeploymentStatus{
		ID:         "dep-1",
		Service:    "aerial-stack",
		Commit:     "abc1234",
		CommitTime: &now,
		Stage:      "live",
		Progress:   100,
	}

	data, err := json.Marshal(depWithTime)
	if err != nil {
		t.Fatalf("failed to marshal DeploymentStatus with time: %v", err)
	}
	if !strings.Contains(string(data), `"commit_time":"2026-09-04T04:15:00Z"`) {
		t.Errorf("expected JSON to contain formatted commit_time, got: %s", string(data))
	}

	depWithoutTime := DeploymentStatus{
		ID:       "dep-2",
		Service:  "aerial-stack",
		Commit:   "abc1234",
		Stage:    "live",
		Progress: 100,
	}

	dataNil, err := json.Marshal(depWithoutTime)
	if err != nil {
		t.Fatalf("failed to marshal DeploymentStatus with nil time: %v", err)
	}
	if strings.Contains(string(dataNil), `"commit_time"`) {
		t.Errorf("expected JSON to omit commit_time when nil, got: %s", string(dataNil))
	}
}

func TestFetchGitSyncStatus(t *testing.T) {
	ctx := context.Background()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(GitSyncStatusResponse{
				Status:        "lagging",
				MaxLagSeconds: 42,
				LastSyncTime:  time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC),
				Repos: map[string]RepoStatus{
					"/share/aerial": {
						Repo:           "/share/aerial",
						DiskCommit:     "commit1",
						RemoteCommit:   "commit2",
						TimeLagSeconds: 42,
						SyncStatus:     "lagging",
					},
				},
			})
		case "/error500/status":
			w.WriteHeader(http.StatusInternalServerError)
		case "/badjson/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{not-json`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockServer.Close()

	tests := []struct {
		name            string
		url             string
		wantStatus      string
		wantMaxLag      int64
		wantRepoCommit  string
		wantErr         string
		wantErrNonEmpty bool
	}{
		{
			name:       "empty_URL_returns_fallback",
			url:        "",
			wantStatus: "synced",
		},
		{
			name:            "invalid_URL_returns_error",
			url:             "http://[::1]:namedport",
			wantErrNonEmpty: true,
		},
		{
			name:           "successful_upstream_response",
			url:            mockServer.URL,
			wantStatus:     "lagging",
			wantMaxLag:     42,
			wantRepoCommit: "commit1",
		},
		{
			name:       "upstream_500_error_returns_fallback",
			url:        mockServer.URL + "/error500",
			wantStatus: "synced",
			wantErr:    "HTTP 500",
		},
		{
			name:            "invalid_JSON_returns_fallback",
			url:             mockServer.URL + "/badjson",
			wantStatus:      "synced",
			wantErrNonEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := fetchGitSyncStatus(ctx, tc.url)
			if tc.wantStatus != "" && resp.Status != tc.wantStatus {
				t.Errorf("expected status %q, got %q", tc.wantStatus, resp.Status)
			}
			if tc.wantMaxLag != 0 && resp.MaxLagSeconds != tc.wantMaxLag {
				t.Errorf("expected max lag %d, got %d", tc.wantMaxLag, resp.MaxLagSeconds)
			}
			if tc.wantRepoCommit != "" {
				if len(resp.Repos) != 1 || resp.Repos["/share/aerial"].DiskCommit != tc.wantRepoCommit {
					t.Errorf("unexpected repo status: %+v", resp.Repos)
				}
			}
			if tc.wantErr != "" && resp.Error != tc.wantErr {
				t.Errorf("expected error %q, got %q", tc.wantErr, resp.Error)
			}
			if tc.wantErrNonEmpty && resp.Error == "" {
				t.Errorf("expected non-empty error")
			}
		})
	}
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	healthHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}
	if rr.Body.String() != "OK" {
		t.Errorf("unexpected body: %s", rr.Body.String())
	}

	// 405 Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/health", nil)
	rrPost := httptest.NewRecorder()
	healthHandler(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /health, got %d", rrPost.Code)
	}
}

func TestSecurityHeadersMiddleware(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	handler := securityHeadersMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("expected X-Content-Type-Options: nosniff")
	}
	if rr.Header().Get("X-Frame-Options") != "DENY" {
		t.Errorf("expected X-Frame-Options: DENY")
	}
	if rr.Header().Get("X-XSS-Protection") != "1; mode=block" {
		t.Errorf("expected X-XSS-Protection header")
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
		{
			name:       "default_candidates_fallback_to_static",
			candidates: nil,
			wantDir:    "static",
		},
		{
			name:       "custom_dir_with_index",
			candidates: []string{tempDirWithIndex},
			wantDir:    tempDirWithIndex,
		},
		{
			name:       "first_missing_second_exists",
			candidates: []string{tempDirWithoutIndex, tempDirWithIndex},
			wantDir:    tempDirWithIndex,
		},
		{
			name:       "none_matching_fallback_to_static",
			candidates: []string{tempDirWithoutIndex, "/nonexistent/static/dir"},
			wantDir:    "static",
		},
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
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()

	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
	}))
	defer ghMock.Close()

	cfg := ConsoleConfig{
		Port:       port,
		BrainURL:   "http://127.0.0.1:54321",
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

func TestFetchDockerClusterState_Extended(t *testing.T) {
	origClient := dockerSocketClient
	defer func() { dockerSocketClient = origClient }()

	sampleContainers := []DockerContainerJSON{
		{
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Status:  "Up 2 hours (healthy)",
			Created: time.Now().UTC().Add(-2 * time.Hour).Unix(),
			Labels: map[string]string{
				"com.docker.compose.project": "aerial",
				"com.docker.compose.service": "brain",
			},
		},
		{
			Names:   []string{"/aerial-scheduler"},
			State:   "running",
			Status:  "Up 10 minutes (unhealthy)",
			Created: time.Now().UTC().Add(-10 * time.Minute).Unix(),
		},
		{
			Names:   []string{"/aerial-proxy"},
			State:   "running",
			Status:  "Up 5 minutes (health: starting)",
			Created: time.Now().UTC().Add(time.Hour).Unix(),
		},
		{
			Names:   []string{"/aerial-db"},
			State:   "exited",
			Status:  "Exited (1)",
			Created: time.Now().UTC().Add(-time.Hour).Unix(),
		},
		{
			Names:   []string{"/haro-6f0e3a1d-8af5-f530-2ca8-eee83dfac17e"},
			State:   "running",
			Status:  "Up 30 minutes (healthy)",
			Created: time.Now().UTC().Add(-30 * time.Minute).Unix(),
			Labels: map[string]string{
				"aerial.service":               "haro",
				"com.hashicorp.nomad.alloc_id": "6f0e3a1d-8af5-f530-2ca8-eee83dfac17e",
			},
		},
		{
			Names:   []string{"/victoriametrics-mcp-6ca68da8-1599-0516-4c28-635658025294"},
			State:   "running",
			Status:  "Up 15 minutes (healthy)",
			Created: time.Now().UTC().Add(-15 * time.Minute).Unix(),
			Labels: map[string]string{
				"com.hashicorp.nomad.alloc_id": "6ca68da8-1599-0516-4c28-635658025294",
			},
		},
		{
			Names:   []string{"/hassio_supervisor"},
			State:   "running",
			Status:  "Up 10 hours",
			Created: time.Now().UTC().Add(-10 * time.Hour).Unix(),
		},
	}
	sampleContainersJSON, _ := json.Marshal(sampleContainers)

	tests := []struct {
		name         string
		statusCode   int
		body         string
		rtErr        error
		wantServices int
		wantErr      bool
	}{
		{
			name:         "Success_MixedContainers",
			statusCode:   http.StatusOK,
			body:         string(sampleContainersJSON),
			wantServices: 5,
		},
		{
			name:       "HTTP500_Error",
			statusCode: http.StatusInternalServerError,
			body:       "server err",
			wantErr:    true,
		},
		{
			name:       "BadJSON_Error",
			statusCode: http.StatusOK,
			body:       "{invalid json",
			wantErr:    true,
		},
		{
			name:    "ClientFailure_Error",
			rtErr:   errors.New("socket closed"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dockerSocketClient = &http.Client{
				Transport: &roundTripperFunc{
					fn: func(req *http.Request) (*http.Response, error) {
						if tc.rtErr != nil {
							return nil, tc.rtErr
						}
						return &http.Response{
							StatusCode: tc.statusCode,
							Body:       io.NopCloser(strings.NewReader(tc.body)),
							Header:     make(http.Header),
						}, nil
					},
				},
			}
			services, _, err := fetchDockerClusterState(context.Background())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("fetchDockerClusterState failed: %v", err)
			}
			if len(services) != tc.wantServices {
				t.Errorf("expected %d services, got %d", tc.wantServices, len(services))
			}
		})
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
		{
			name:     "EmptyURL",
			brainURL: "",
		},
		{
			name:       "HTTP500_Error",
			brainURL:   "http://brain",
			statusCode: http.StatusInternalServerError,
			body:       "error",
			wantErr:    true,
		},
		{
			name:       "BadJSON_Error",
			brainURL:   "http://brain",
			statusCode: http.StatusOK,
			body:       "bad json",
			wantErr:    true,
		},
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

type mockReadCloser struct {
	io.Reader

	closeErr error
}

func (m *mockReadCloser) Close() error {
	return m.closeErr
}

type mockErrReader struct {
	err error
}

func (m *mockErrReader) Read(p []byte) (int, error) {
	return 0, m.err
}

func (m *mockErrReader) Close() error {
	return nil
}

type mockErrWriter struct {
	err         error
	header      http.Header
	statusCode  int
	wroteHeader bool
}

func newMockErrWriter(err error) *mockErrWriter {
	return &mockErrWriter{
		err:    err,
		header: make(http.Header),
	}
}

func (m *mockErrWriter) Header() http.Header {
	return m.header
}

func (m *mockErrWriter) Write(p []byte) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return len(p), nil
}

func (m *mockErrWriter) WriteHeader(statusCode int) {
	m.statusCode = statusCode
	m.wroteHeader = true
}

func TestConsole_CloseWarnAndDisconnect(t *testing.T) {
	// 2. drainAndClose with nil, success, copy error, and close error
	drainAndClose(nil, "nil body")
	drainAndClose(&mockReadCloser{Reader: strings.NewReader("clean body content"), closeErr: nil}, "clean body")
	drainAndClose(&mockErrReader{err: errors.New("simulated copy read error")}, "read err body")
	drainAndClose(&mockReadCloser{Reader: strings.NewReader("hello"), closeErr: errors.New("simulated close error")}, "mock body")

	// 3. isClientDisconnect
	if httpkit.IsClientDisconnect(nil, nil) {
		t.Error("expected false for nil error")
	}
	ctxCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	reqCanceled := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctxCanceled)
	if !httpkit.IsClientDisconnect(reqCanceled, errors.New("some network abort")) {
		t.Error("expected true when request context is canceled")
	}
	reqActive := httptest.NewRequest(http.MethodGet, "/", nil)
	if httpkit.IsClientDisconnect(reqActive, errors.New("unrelated error")) {
		t.Error("expected false for unrelated error with active context")
	}
	if !httpkit.IsClientDisconnect(nil, syscall.EPIPE) {
		t.Error("expected true for EPIPE")
	}
	if !httpkit.IsClientDisconnect(nil, syscall.ECONNRESET) {
		t.Error("expected true for ECONNRESET")
	}
	if !httpkit.IsClientDisconnect(nil, net.ErrClosed) {
		t.Error("expected true for net.ErrClosed")
	}

	// 4. writeResponse
	rr := httptest.NewRecorder()
	httpkit.WriteResponse(rr, reqActive, "console", []byte("success payload"))
	if rr.Body.String() != "success payload" {
		t.Errorf("expected 'success payload', got %q", rr.Body.String())
	}
	mockWDisconnect := newMockErrWriter(syscall.EPIPE)
	httpkit.WriteResponse(mockWDisconnect, reqActive, "console", []byte("data"))
	mockWOtherErr := newMockErrWriter(errors.New("other write failure"))
	httpkit.WriteResponse(mockWOtherErr, reqActive, "console", []byte("data"))

	// 5. writeJSON: success, marshal error, and write error
	rrJSON := httptest.NewRecorder()
	writeJSON(rrJSON, reqActive, http.StatusCreated, map[string]string{"status": "ok"})
	if rrJSON.Code != http.StatusCreated {
		t.Errorf("expected 201 Created, got %d", rrJSON.Code)
	}
	if ct := rrJSON.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}

	rrMarshalErr := httptest.NewRecorder()
	writeJSON(rrMarshalErr, reqActive, http.StatusOK, make(chan int))
	if rrMarshalErr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 InternalServerError on marshal failure, got %d", rrMarshalErr.Code)
	}

	mockWJSONErr := newMockErrWriter(errors.New("write failure"))
	writeJSON(mockWJSONErr, reqActive, http.StatusOK, map[string]string{"status": "ok"})

	reqGzip := httptest.NewRequest(http.MethodGet, "/api/builds", nil)
	reqGzip.Header.Set("Accept-Encoding", "gzip, deflate")
	rrGzip := httptest.NewRecorder()
	writeJSON(rrGzip, reqGzip, http.StatusOK, map[string]string{"status": "ok"})
	if ce := rrGzip.Header().Get("Content-Encoding"); ce != "gzip" {
		t.Fatalf("expected Content-Encoding gzip, got %q", ce)
	}
	gzReader, gzErr := gzip.NewReader(rrGzip.Body)
	if gzErr != nil {
		t.Fatalf("failed to create gzip reader: %v", gzErr)
	}
	defer gzReader.Close()
	unzipped, readErr := io.ReadAll(gzReader)
	if readErr != nil || string(unzipped) != `{"status":"ok"}` {
		t.Errorf("unexpected gzip body %q (err=%v)", string(unzipped), readErr)
	}
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
		path           string
		wantCode       int
		wantLocation   string
		wantCache      string
		wantBodySubstr string
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := client.Get(ts.URL + tc.path)
			if err != nil {
				t.Fatalf("GET %s failed: %v", tc.path, err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body failed: %v", err)
			}
			if resp.StatusCode != tc.wantCode {
				t.Fatalf("GET %s status = %d, want %d", tc.path, resp.StatusCode, tc.wantCode)
			}
			if got := resp.Header.Get("Location"); got != tc.wantLocation {
				t.Errorf("Location = %q, want %q", got, tc.wantLocation)
			}
			if tc.wantCache != "" && resp.Header.Get("Cache-Control") != tc.wantCache {
				t.Errorf("Cache-Control = %q, want %q", resp.Header.Get("Cache-Control"), tc.wantCache)
			}
			if !strings.Contains(string(body), tc.wantBodySubstr) {
				t.Errorf("body = %q, want substring %q", body, tc.wantBodySubstr)
			}
		})
	}
}

func TestCoverageFlushEndpoint(t *testing.T) {
	mux := SetupConsoleMux(ConsoleConfig{}, nil)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	oldFn, oldMetaFn := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
	defer func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldFn, oldMetaFn }()

	called, metaCalled := false, false
	httpkit.WriteCountersDirFn = func(dir string) error {
		called = true
		return nil
	}
	httpkit.WriteMetaDirFn = func(dir string) error {
		metaCalled = true
		return nil
	}

	resp, err := http.Get(ts.URL + "/debug/coverage/flush")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !called || !metaCalled {
		t.Errorf("expected 200 OK and both flush fns called, got status=%d called=%v metaCalled=%v", resp.StatusCode, called, metaCalled)
	}

	resp2, err2 := http.Get(ts.URL + "/console/debug/coverage/flush")
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got: %d", resp2.StatusCode)
	}

	httpkit.WriteMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
	if resp3b, err3b := http.Get(ts.URL + "/debug/coverage/flush"); err3b != nil || resp3b.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK when meta flush errors, got resp=%v err=%v", resp3b, err3b)
	} else {
		resp3b.Body.Close()
	}

	// 3. With mock error -> 500 Internal Server Error
	httpkit.WriteCountersDirFn = func(dir string) error {
		return errors.New("simulated flush error")
	}

	resp4, err4 := http.Get(ts.URL + "/debug/coverage/flush")
	if err4 != nil {
		t.Fatalf("unexpected error: %v", err4)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on error, got: %d", resp4.StatusCode)
	}

	resp5, err5 := http.Get(ts.URL + "/console/debug/coverage/flush")
	if err5 != nil {
		t.Fatalf("unexpected error: %v", err5)
	}
	defer resp5.Body.Close()
	if resp5.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on console subpath error, got: %d", resp5.StatusCode)
	}
}

func TestMain_Execution(t *testing.T) {
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
		{
			name:         "clean execution",
			wantRunCall:  true,
			wantExitCall: false,
		},
		{
			name:         "config load error exits",
			loadErr:      errors.New("mock config error"),
			wantRunCall:  false,
			wantExitCall: true,
		},
		{
			name:         "run server error exits",
			runErr:       errors.New("mock run error"),
			wantRunCall:  true,
			wantExitCall: true,
		},
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
