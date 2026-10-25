package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

func TestStatusHandler(t *testing.T) {
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		case strings.HasPrefix(r.URL.Path, "/nomad/ok/v1/jobs"):
			_, _ = w.Write([]byte(`[{"ID":"nomad-brain","Type":"service","Status":"running"},{"ID":"nomad-proxy","Type":"service","Status":"running"}]`))
		case strings.HasPrefix(r.URL.Path, "/nomad/ok/v1/allocations"):
			_, _ = w.Write([]byte(`[
				{"ID":"a1","JobID":"nomad-brain","ClientStatus":"running","DeploymentStatus":{"Healthy":true}},
				{"ID":"a2","JobID":"nomad-proxy","ClientStatus":"running","DeploymentStatus":{"Healthy":true}}
			]`))
		case strings.HasPrefix(r.URL.Path, "/nomad/error500"):
			http.Error(w, "server error", http.StatusInternalServerError)
		case strings.HasPrefix(r.URL.Path, "/nomad/empty"):
			_, _ = w.Write([]byte(`[]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockServer.Close()

	tests := []struct {
		name              string
		method            string
		useMux            bool
		brainMode         string
		gitSyncMode       string
		nomadMode         string
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
	}{
		{
			name:              "Basic_DockerFallback",
			wantCode:          http.StatusOK,
			wantMinServices:   1,
			wantOngoingDeploy: "idle",
		},
		{
			name:              "OngoingDeploy",
			yamlContent:       "dashboard: {}\n",
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
			yamlContent:   "dashboard:\n  quick_launch_links:\n    - name: \"HOME\"\n      url: \"https://home.zylman.com\"\n",
			wantCode:      http.StatusOK,
			wantQuickLink: "HOME",
		},
		{
			name:              "DegradedClusterStatus",
			dockerJSON:        `[{"Names":["/aerial-brain"],"State":"running","Status":"Up 10m (unhealthy)","Created":1775240000}]`,
			wantCode:          http.StatusOK,
			wantClusterStatus: "degraded",
		},
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
			yamlContent:       "dashboard: {}\n",
			wantCode:          http.StatusOK,
			wantDeploys:       2,
			wantOngoingDeploy: "swapping",
			wantIsDeploying:   true,
		},
		{
			name:            "NomadPrimary",
			nomadMode:       "ok",
			wantCode:        http.StatusOK,
			wantMinServices: 2,
			wantServiceName: "nomad-brain",
		},
		{
			name:            "NomadFallbackOnError",
			nomadMode:       "error500",
			dockerJSON:      "[]",
			wantCode:        http.StatusOK,
			wantMinServices: 1,
			wantServiceName: "brain",
		},
		{
			name:            "NomadFallbackOnEmpty",
			nomadMode:       "empty",
			wantCode:        http.StatusOK,
			wantMinServices: 1,
		},
		{
			name:     "MethodNotAllowed",
			method:   http.MethodPost,
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:          "SetupDashboardMux_StatusWiring",
			useMux:        true,
			gitSyncMode:   "synced",
			yamlContent:   "dashboard:\n  quick_launch_links:\n    - name: \"TESTLINK\"\n      url: \"https://test.zylman.com\"\n",
			wantCode:      http.StatusOK,
			wantQuickLink: "TESTLINK",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
				brainURL = mockServer.URL + "/brain/ok"
			case "offline":
				brainURL = "http://127.0.0.1:54321"
			}

			gitSyncURL := ""
			switch tc.gitSyncMode {
			case "synced":
				gitSyncURL = mockServer.URL + "/hangar/synced"
			case "swapping":
				gitSyncURL = mockServer.URL + "/hangar/swapping"
			}

			nomadAddr := ""
			switch tc.nomadMode {
			case "ok":
				nomadAddr = mockServer.URL + "/nomad/ok"
			case "error500":
				nomadAddr = mockServer.URL + "/nomad/error500"
			case "empty":
				nomadAddr = mockServer.URL + "/nomad/empty"
			}

			var h http.Handler
			if tc.useMux {
				cfg := DashboardConfig{
					Port:       "8080",
					BrainURL:   brainURL,
					GitSyncURL: gitSyncURL,
					ConfigPath: cfgFile,
					GitCommit:  "testcommit",
					NomadAddr:  nomadAddr,
				}
				h = SetupDashboardMux(cfg, nil)
			} else {
				h = statusHandler(brainURL, gitSyncURL, cfgFile, "testcommit", nomadAddr)
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
		})
	}
}

func TestFactsHandler_TableDriven(t *testing.T) {
	var capturedQuery url.Values
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/facts" {
			http.NotFound(w, r)
			return
		}
		if capturedQuery.Get("q") == "error500" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"db failure"}`))
			return
		}
		if capturedQuery.Get("q") == "badjson" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`not-valid-json`))
			return
		}
		if capturedQuery.Get("limit") == "10" && capturedQuery.Get("offset") == "5" {
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
				Facts:  nil,
				Total:  0,
				Limit:  10,
				Offset: 5,
				Status: "ok",
			})
			return
		}
		if capturedQuery.Get("limit") == "25" && capturedQuery.Get("q") == "dark mode" {
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
				Facts: []FactItem{
					{
						ID:               1,
						FactText:         "User prefers dark mode",
						ReinforceCount:   2,
						LastReinforcedAt: time.Now().UTC(),
						CreatedAt:        time.Now().UTC(),
					},
				},
				Total:  1,
				Limit:  25,
				Offset: 0,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(FactsAPIResponse{
			Facts: []FactItem{
				{
					ID:             1,
					FactText:       "Fact 1",
					ReinforceCount: 1,
					CreatedAt:      time.Now().UTC(),
				},
			},
			Total:  1,
			Limit:  0,
			Offset: 0,
		})
	}))
	defer mockBrain.Close()

	offlineURL := "http://127.0.0.1:54321"

	tests := []struct {
		name               string
		brainURL           string
		method             string
		target             string
		wantStatus         int
		wantBodySubstr     []string
		wantBodyNotSubstr  []string
		wantForwardedLimit string
		wantMaxQRunes      int
	}{
		{
			name:           "Success_QueryFilter",
			brainURL:       mockBrain.URL,
			method:         http.MethodGet,
			target:         "/api/facts?limit=25&q=dark%20mode",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"User prefers dark mode", `"last_reinforced_at":`},
		},
		{
			name:               "Success_UnlimitedDefault",
			brainURL:           mockBrain.URL,
			method:             http.MethodGet,
			target:             "/api/facts",
			wantStatus:         http.StatusOK,
			wantBodyNotSubstr:  []string{`"last_reinforced_at"`},
			wantForwardedLimit: "none",
		},
		{
			name:               "Success_HighLimit",
			brainURL:           mockBrain.URL,
			method:             http.MethodGet,
			target:             "/api/facts?limit=500",
			wantStatus:         http.StatusOK,
			wantForwardedLimit: "500",
		},
		{
			name:           "Success_AllParamsAndNilFacts",
			brainURL:       mockBrain.URL,
			method:         http.MethodGet,
			target:         "/api/facts?limit=10&offset=5&category=ignored&q=" + strings.Repeat("searchterm", 10),
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{`"facts":[]`},
			wantMaxQRunes:  64,
		},
		{
			name:           "DegradedFallback",
			brainURL:       offlineURL,
			method:         http.MethodGet,
			target:         "/api/facts",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, `"facts":[]`},
		},
		{
			name:       "Upstream500",
			brainURL:   mockBrain.URL,
			method:     http.MethodGet,
			target:     "/api/facts?q=error500",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "BadGateway_InvalidJSON",
			brainURL:   mockBrain.URL,
			method:     http.MethodGet,
			target:     "/api/facts?q=badjson",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "MethodNotAllowed",
			brainURL:   mockBrain.URL,
			method:     http.MethodPost,
			target:     "/api/facts",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "InvalidUpstreamURL",
			brainURL:   "http://brain:\x7f8080",
			method:     http.MethodGet,
			target:     "/api/facts",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := factsHandler(tt.brainURL)
			req := httptest.NewRequest(tt.method, tt.target, nil)
			rr := httptest.NewRecorder()
			h(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rr.Code)
			}
			bodyStr := rr.Body.String()
			for _, sub := range tt.wantBodySubstr {
				if !strings.Contains(bodyStr, sub) {
					t.Errorf("expected body to contain %q, got: %s", sub, bodyStr)
				}
			}
			for _, sub := range tt.wantBodyNotSubstr {
				if strings.Contains(bodyStr, sub) {
					t.Errorf("expected body NOT to contain %q, got: %s", sub, bodyStr)
				}
			}
			if tt.wantForwardedLimit != "" {
				if tt.wantForwardedLimit == "none" {
					if capturedQuery.Get("limit") != "" {
						t.Errorf("expected empty limit forwarded, got %q", capturedQuery.Get("limit"))
					}
				} else if capturedQuery.Get("limit") != tt.wantForwardedLimit {
					t.Errorf("expected limit=%s forwarded, got %q", tt.wantForwardedLimit, capturedQuery.Get("limit"))
				}
			}
			if tt.wantMaxQRunes > 0 {
				gotQ := capturedQuery.Get("q")
				if len([]rune(gotQ)) > tt.wantMaxQRunes {
					t.Errorf("expected q truncated to %d runes, got len %d: %q", tt.wantMaxQRunes, len([]rune(gotQ)), gotQ)
				}
			}
		})
	}
}

func TestSchedulesAndRunsHandlers_TableDriven(t *testing.T) {
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/error500"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"db failure"}`))
		case strings.Contains(r.URL.Path, "/badjson"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`not-valid-json`))
		case strings.Contains(r.URL.Path, "/nil/schedules"):
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Status:   "ok",
				Crons:    nil,
				OneShots: nil,
			})
		case strings.Contains(r.URL.Path, "/nil/runs"):
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "ok",
				Total:  0,
				Limit:  10,
				Offset: 5,
				Runs:   nil,
			})
		case strings.HasSuffix(r.URL.Path, "/schedules/runs"):
			q := r.URL.Query()
			if q.Get("limit") != "10" || q.Get("offset") != "5" || q.Get("schedule_id") != "cron-1" || q.Get("status") != "success" {
				t.Errorf("unexpected upstream query params: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "ok", Total: 42, Limit: 10, Offset: 5,
				Runs: []ScheduleRun{{ID: "cron-brief-101", ScheduleID: "cron-1", ScheduleType: "cron", Prompt: "Daily summary", Title: "Morning Brief #101", Status: "success", StartedAt: time.Now().UTC().Add(-1 * time.Hour), DurationMs: 1250}},
			})
		case strings.HasSuffix(r.URL.Path, "/schedules"):
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Status:   "ok",
				Summary:  ScheduleSummaryMetrics{TotalActive: 3, CronCount: 2, OneShotCount: 1, TotalRuns24h: 15, SuccessRate24h: 93.3},
				Crons:    []CronSchedule{{ID: "cron-1", ChannelID: "chan-1", TitlePrefix: "Morning Brief", CronExpr: "0 9 * * *", CronDescription: "Every day at 9:00 AM", Prompt: "Generate morning brief", Timezone: "America/Los_Angeles", Enabled: true}},
				OneShots: []OneShotSchedule{{ID: "oneshot-1", ThreadID: "thread-1", Prompt: "Remind about tea", RunAt: time.Now().UTC().Add(10 * time.Minute)}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockBrain.Close()

	mockHaro := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/schedules/runs"):
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "ok", Total: 42, Limit: 10, Offset: 5,
				Runs: []ScheduleRun{{ID: "haro-run-101", ScheduleID: "cron-1", ScheduleType: "cron", Prompt: "Haro summary", Title: "Haro Brief #101", Status: "success", StartedAt: time.Now().UTC().Add(-1 * time.Hour), DurationMs: 1250}},
			})
		case strings.HasSuffix(r.URL.Path, "/schedules"):
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Status:   "ok",
				Summary:  ScheduleSummaryMetrics{TotalActive: 1, CronCount: 1, OneShotCount: 0, TotalRuns24h: 1, SuccessRate24h: 100.0},
				Crons:    []CronSchedule{{ID: "haro-cron-1", ChannelID: "chan-1", TitlePrefix: "Haro Schedule", CronExpr: "0 * * * *", CronDescription: "Every hour", Prompt: "Haro run", Timezone: "America/Los_Angeles", Enabled: true}},
				OneShots: []OneShotSchedule{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockHaro.Close()

	offlineURL := "http://127.0.0.1:54321"
	tests := []struct {
		name           string
		isRuns         bool
		useMux         bool
		haroURL        string
		brainURL       string
		method         string
		target         string
		wantStatus     int
		wantBodySubstr []string
	}{
		{
			name:           "Schedules_Success",
			brainURL:       mockBrain.URL,
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Morning Brief", "Remind about tea", `"total_active":3`},
		},
		{
			name:           "Schedules_SuccessParams_NilSlices",
			brainURL:       mockBrain.URL + "/nil",
			method:         "GET",
			target:         "/api/schedules?limit=5&offset=2&active=true&q=weather",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{`"crons":[]`, `"one_shots":[]`},
		},
		{
			name:       "Schedules_ActiveFalse",
			brainURL:   mockBrain.URL,
			method:     "GET",
			target:     "/api/schedules?active=false",
			wantStatus: http.StatusOK,
		},
		{
			name:           "Schedules_DegradedFallback",
			brainURL:       offlineURL,
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Brain service unreachable. Retrying...", `"crons":[]`, `"one_shots":[]`},
		},
		{
			name:       "Schedules_Upstream500",
			brainURL:   mockBrain.URL + "/error500",
			method:     "GET",
			target:     "/api/schedules",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "Schedules_BadGateway",
			brainURL:   mockBrain.URL + "/badjson",
			method:     "GET",
			target:     "/api/schedules",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "Schedules_MethodNotAllowed",
			brainURL:   offlineURL,
			method:     "POST",
			target:     "/api/schedules",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "Schedules_InvalidUpstreamURL",
			brainURL:   "http://brain:\x7f8080",
			method:     "GET",
			target:     "/api/schedules",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:           "ScheduleRuns_Success",
			isRuns:         true,
			brainURL:       mockBrain.URL,
			method:         "GET",
			target:         "/api/schedules/runs?limit=10&offset=5&schedule_id=cron-1&status=success",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"cron-brief-101", `"total":42`, `"duration_ms":1250`},
		},
		{
			name:           "ScheduleRuns_SuccessParams_NilSlices",
			isRuns:         true,
			brainURL:       mockBrain.URL + "/nil/runs",
			method:         "GET",
			target:         "/api/schedules/runs?limit=10&offset=5&status=SUCCESS&schedule_id=42",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{`"runs":[]`},
		},
		{
			name:           "ScheduleRuns_DegradedFallback",
			isRuns:         true,
			brainURL:       offlineURL,
			method:         "GET",
			target:         "/api/schedules/runs?limit=20&offset=10",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Brain service unreachable. Retrying...", `"runs":[]`},
		},
		{
			name:       "ScheduleRuns_Upstream500",
			isRuns:     true,
			brainURL:   mockBrain.URL + "/error500",
			method:     "GET",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "ScheduleRuns_BadGateway",
			isRuns:     true,
			brainURL:   mockBrain.URL + "/badjson",
			method:     "GET",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "ScheduleRuns_MethodNotAllowed",
			isRuns:     true,
			brainURL:   offlineURL,
			method:     "DELETE",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "ScheduleRuns_InvalidUpstreamURL",
			isRuns:     true,
			brainURL:   "http://brain:\x7f8080",
			method:     "GET",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:           "SetupDashboardMux_Schedules_RoutesToHaro",
			useMux:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Schedule", `"total_active":1`},
		},
		{
			name:           "SetupDashboardMux_DashboardSchedules_RoutesToHaro",
			useMux:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/dashboard/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Schedule", `"total_active":1`},
		},
		{
			name:           "SetupDashboardMux_ScheduleRuns_RoutesToHaro",
			useMux:         true,
			isRuns:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/api/schedules/runs?limit=10&offset=5&schedule_id=cron-1&status=success",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Brief #101", `"total":42`},
		},
		{
			name:           "SetupDashboardMux_DashboardScheduleRuns_RoutesToHaro",
			useMux:         true,
			isRuns:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/dashboard/api/schedules/runs?limit=10&offset=5&schedule_id=cron-1&status=success",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Brief #101", `"total":42`},
		},
		{
			name:           "SetupDashboardMux_Schedules_FallbackToBrainWhenHaroEmpty",
			useMux:         true,
			brainURL:       mockBrain.URL,
			haroURL:        "",
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Morning Brief", `"total_active":3`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h http.Handler
			if tt.useMux {
				h = SetupDashboardMux(DashboardConfig{
					BrainURL: tt.brainURL,
					HaroURL:  tt.haroURL,
				}, nil)
			} else {
				if tt.isRuns {
					h = scheduleRunsHandler(tt.brainURL)
				} else {
					h = schedulesHandler(tt.brainURL)
				}
			}
			req := httptest.NewRequest(tt.method, tt.target, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rr.Code)
			}
			bodyStr := rr.Body.String()
			for _, sub := range tt.wantBodySubstr {
				if !strings.Contains(bodyStr, sub) {
					t.Errorf("expected body to contain %q, got: %s", sub, bodyStr)
				}
			}
		})
	}
}

func TestVanadisBuildsHandlers(t *testing.T) {
	mockVanadis := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/builds" && r.Method == http.MethodGet:
			q := r.URL.Query()
			if q.Get("status") == "error500" {
				http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
				return
			}
			if q.Get("status") == "badjson" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{not valid json`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(BuildsAPIResponse{
				Status:         "ok",
				Total:          1,
				RunningCount:   1,
				QueuedCount:    0,
				CompletedCount: 0,
				Builds: []VanadisBuildDTO{
					{
						ID:         "build-101",
						Tier:       2,
						Title:      "Vanadis Feature",
						Status:     "running",
						QueueState: "running",
					},
				},
			})
		case strings.HasPrefix(r.URL.Path, "/builds/"):
			buildID := strings.TrimPrefix(r.URL.Path, "/builds/")
			switch buildID {
			case "build-101":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"build": VanadisBuildDTO{
						ID:     "build-101",
						Title:  "Vanadis Feature",
						Status: "running",
					},
					"chunks": []VanadisChunkDTO{
						{
							ID:         "chunk-1",
							BuildID:    "build-101",
							ChunkIndex: 1,
							Title:      "Chunk 1",
							Status:     "completed",
						},
					},
					"entry": VanadisBuildDTO{
						ID:         "build-101",
						QueueState: "running",
					},
				})
			case "test-build-1", "test-build-2":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(BuildDetailAPIResponse{Status: "ok", Chunks: []VanadisChunkDTO{}})
			case "build-badjson":
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{invalid json detail`))
			case "build-500":
				http.Error(w, `{"error":"internal upstream error"}`, http.StatusInternalServerError)
			default:
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "build not found"})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockVanadis.Close()

	offlineURL := "http://127.0.0.1:54321"

	tests := []struct {
		name           string
		isDetail       bool
		useMux         bool
		vanadisURL     string
		method         string
		target         string
		wantStatus     int
		wantBodySubstr []string
	}{
		{
			name:           "Builds_Success_WithQuery",
			vanadisURL:     mockVanadis.URL,
			method:         http.MethodGet,
			target:         "/api/builds?limit=25&status=running&queue_state=running",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"build-101", "Vanadis Feature"},
		},
		{
			name:           "Builds_DegradedFallback",
			vanadisURL:     offlineURL,
			method:         http.MethodGet,
			target:         "/api/builds",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Vanadis service unreachable. Retrying...", `"builds":[]`},
		},
		{
			name:       "Builds_MethodNotAllowed",
			vanadisURL: mockVanadis.URL,
			method:     http.MethodPost,
			target:     "/api/builds",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "Builds_Upstream500",
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds?status=error500",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "Builds_BadGateway_InvalidJSON",
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds?status=badjson",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "Builds_InvalidUpstreamURL",
			vanadisURL: "http://vanadis:\x7f8080",
			method:     http.MethodGet,
			target:     "/api/builds",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:           "BuildDetail_Success_APIPrefix",
			isDetail:       true,
			vanadisURL:     mockVanadis.URL,
			method:         http.MethodGet,
			target:         "/api/builds/build-101",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"build-101", "chunk-1"},
		},
		{
			name:           "BuildDetail_Success_DashboardPrefix",
			isDetail:       true,
			vanadisURL:     mockVanadis.URL,
			method:         http.MethodGet,
			target:         "/dashboard/api/builds/build-101",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"build-101"},
		},
		{
			name:           "BuildDetail_NotFound_404",
			isDetail:       true,
			vanadisURL:     mockVanadis.URL,
			method:         http.MethodGet,
			target:         "/api/builds/nonexistent-build",
			wantStatus:     http.StatusNotFound,
			wantBodySubstr: []string{`"status":"error"`, "build not found", `"chunks":[]`},
		},
		{
			name:       "BuildDetail_MethodNotAllowed",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodDelete,
			target:     "/api/builds/build-101",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "BuildDetail_DegradedFallback",
			isDetail:       true,
			vanadisURL:     offlineURL,
			method:         http.MethodGet,
			target:         "/api/builds/build-101",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Vanadis service unreachable. Retrying...", `"chunks":[]`},
		},
		{
			name:       "BuildDetail_BadGateway_InvalidJSON",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/build-badjson",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "BuildDetail_Upstream500",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/build-500",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "BuildDetail_EmptyID_BadRequest",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BuildDetail_PathTraversal_BadRequest",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/../traversal",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BuildDetail_InvalidUpstreamURL",
			isDetail:   true,
			vanadisURL: "http://vanadis:\x7f8080",
			method:     http.MethodGet,
			target:     "/api/builds/build-101",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "MuxWiring_BuildsRoot",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MuxWiring_DashboardBuildsRoot",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/dashboard/api/builds",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MuxWiring_BuildDetail1",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/test-build-1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MuxWiring_DashboardBuildDetail2",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/dashboard/api/builds/test-build-2",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h http.Handler
			if tt.useMux {
				h = SetupDashboardMux(DashboardConfig{VanadisURL: tt.vanadisURL}, nil)
			} else if tt.isDetail {
				h = buildDetailHandler(tt.vanadisURL)
			} else {
				h = buildsHandler(tt.vanadisURL)
			}
			req := httptest.NewRequest(tt.method, tt.target, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tt.wantStatus, rr.Code, rr.Body.String())
			}
			bodyStr := rr.Body.String()
			for _, sub := range tt.wantBodySubstr {
				if !strings.Contains(bodyStr, sub) {
					t.Errorf("expected body to contain %q, got: %s", sub, bodyStr)
				}
			}
		})
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

func TestDashboardServerLifecycle(t *testing.T) {
	// 1. Bad port errors
	errBadPort := RunDashboardServer(context.Background(), DashboardConfig{Port: "bad-port-string"})
	if errBadPort == nil {
		t.Errorf("expected error on bad port in RunDashboardServer")
	}

	errFatal := RunDashboardServer(context.Background(), DashboardConfig{Port: "-1"})
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
	port := fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()

	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GitHubRunsResponse{})
	}))
	defer ghMock.Close()

	cfg := DashboardConfig{
		Port:       port,
		BrainURL:   "http://127.0.0.1:54321",
		GHRepo:     "azylman/aerial",
		GHToken:    "dummy",
		APIBaseURL: ghMock.URL,
		GitCommit:  "testsha",
	}

	t.Cleanup(func() {
		globalGHPoller = nil
	})

	done := make(chan error, 1)
	go func() {
		done <- RunDashboardServer(ctx, cfg)
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

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunDashboardServer returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunDashboardServer did not shut down in time")
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
			wantServices: 6,
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
				t.Errorf("expected %d services (including Nomad), got %d", tc.wantServices, len(services))
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

func TestDashboard_CloseWarnAndDisconnect(t *testing.T) {
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
	httpkit.WriteResponse(rr, reqActive, "dashboard", []byte("success payload"))
	if rr.Body.String() != "success payload" {
		t.Errorf("expected 'success payload', got %q", rr.Body.String())
	}
	mockWDisconnect := newMockErrWriter(syscall.EPIPE)
	httpkit.WriteResponse(mockWDisconnect, reqActive, "dashboard", []byte("data"))
	mockWOtherErr := newMockErrWriter(errors.New("other write failure"))
	httpkit.WriteResponse(mockWOtherErr, reqActive, "dashboard", []byte("data"))

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
}

func TestFetchNomadClusterState(t *testing.T) {
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/jobs500"):
			http.Error(w, "internal server error", http.StatusInternalServerError)
		case strings.HasPrefix(r.URL.Path, "/allocs500"):
			if strings.HasSuffix(r.URL.Path, "/jobs") {
				w.Write([]byte(`[{"ID":"brain","Type":"service"}]`))
				return
			}
			http.Error(w, "allocs failure", http.StatusInternalServerError)
		case strings.HasPrefix(r.URL.Path, "/jobsbadjson"):
			if strings.HasSuffix(r.URL.Path, "/jobs") {
				w.Write([]byte(`not json`))
				return
			}
			w.Write([]byte(`[]`))
		case r.URL.Path == "/v1/jobs":
			if !strings.Contains(r.URL.RawQuery, "Type") {
				http.Error(w, "expected filter query on jobs", http.StatusBadRequest)
				return
			}
			w.Write([]byte(`[{"ID":"brain","Type":"service","Status":"running"}]`))
		case r.URL.Path == "/v1/allocations":
			if !strings.Contains(r.URL.RawQuery, "ClientStatus") {
				http.Error(w, "expected filter query on allocs", http.StatusBadRequest)
				return
			}
			w.Write([]byte(`[{"ID":"alloc-1","JobID":"brain","ClientStatus":"running","CreateTime":1775240000000000000,"DeploymentStatus":{"Healthy":true}}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	tests := []struct {
		name          string
		addr          string
		wantServices  int
		wantName      string
		wantStatus    string
		wantErr       bool
		wantErrSubstr string
	}{
		{
			name:         "success",
			addr:         ts.URL,
			wantServices: 1,
			wantName:     "brain",
			wantStatus:   "healthy",
		},
		{
			name:    "empty_nomad_address",
			addr:    "   ",
			wantErr: true,
		},
		{
			name:    "invalid_url",
			addr:    "http:// ",
			wantErr: true,
		},
		{
			name:          "jobs_500_error",
			addr:          ts.URL + "/jobs500",
			wantErr:       true,
			wantErrSubstr: "nomad jobs returned HTTP 500",
		},
		{
			name:          "allocs_500_error",
			addr:          ts.URL + "/allocs500",
			wantErr:       true,
			wantErrSubstr: "nomad allocs returned HTTP 500",
		},
		{
			name:    "jobs_malformed_json",
			addr:    ts.URL + "/jobsbadjson",
			wantErr: true,
		},
		{
			name:    "network_error",
			addr:    "http://127.0.0.1:59999",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			services, err := fetchNomadClusterState(ctx, tc.addr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("expected error containing %q, got: %v", tc.wantErrSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(services) != tc.wantServices {
				t.Fatalf("expected %d services, got %d", tc.wantServices, len(services))
			}
			if len(services) > 0 && (services[0].Name != tc.wantName || services[0].Status != tc.wantStatus) {
				t.Errorf("unexpected service: %+v", services[0])
			}
		})
	}
}

func TestCoverageFlushEndpoint(t *testing.T) {
	mux := SetupDashboardMux(DashboardConfig{}, nil)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// 1. Without GOCOVERDIR -> 200 OK
	orig := os.Getenv("GOCOVERDIR")
	_ = os.Unsetenv("GOCOVERDIR")
	defer func() {
		if orig != "" {
			_ = os.Setenv("GOCOVERDIR", orig)
		} else {
			_ = os.Unsetenv("GOCOVERDIR")
		}
	}()

	resp, err := http.Get(ts.URL + "/debug/coverage/flush")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK without GOCOVERDIR, got: %d", resp.StatusCode)
	}

	resp2, err2 := http.Get(ts.URL + "/dashboard/debug/coverage/flush")
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got: %d", resp2.StatusCode)
	}

	// 2. With GOCOVERDIR + mock success -> 200 OK
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
	_ = os.Setenv("GOCOVERDIR", t.TempDir())

	resp3, err3 := http.Get(ts.URL + "/debug/coverage/flush")
	if err3 != nil {
		t.Fatalf("unexpected error: %v", err3)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK || !called || !metaCalled {
		t.Errorf("expected 200 OK and both flush fns called, got status=%d called=%v metaCalled=%v", resp3.StatusCode, called, metaCalled)
	}

	httpkit.WriteMetaDirFn = func(dir string) error { return errors.New("simulated meta flush error") }
	if resp3b, err3b := http.Get(ts.URL + "/debug/coverage/flush"); err3b != nil || resp3b.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK when meta flush errors, got resp=%v err=%v", resp3b, err3b)
	} else {
		resp3b.Body.Close()
	}

	// 3. With GOCOVERDIR + mock error -> 500 Internal Server Error
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

	resp5, err5 := http.Get(ts.URL + "/dashboard/debug/coverage/flush")
	if err5 != nil {
		t.Fatalf("unexpected error: %v", err5)
	}
	defer resp5.Body.Close()
	if resp5.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on dashboard subpath error, got: %d", resp5.StatusCode)
	}
}
