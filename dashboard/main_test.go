package main

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSanitizeEnvVars(t *testing.T) {
	input := []string{
		"GEMINI_API_KEY=secret_key_123",
		"DISCORD_TOKEN=bot_token_456",
		"DISCORD_BOT_TOKEN=bot_token_789",
		"GITHUB_PAT=pat_xyz",
		"HA_TOKEN=ha_abc",
		"PORT=8080",
		"AGY_MODEL=Gemini 3.6 Flash",
	}

	sanitized := SanitizeEnvVars(input)

	for _, env := range sanitized {
		if env == "GEMINI_API_KEY=secret_key_123" ||
			env == "DISCORD_TOKEN=bot_token_456" ||
			env == "DISCORD_BOT_TOKEN=bot_token_789" ||
			env == "GITHUB_PAT=pat_xyz" ||
			env == "HA_TOKEN=ha_abc" {
			t.Errorf("found unsanitized secret in output: %s", env)
		}
	}
}

func TestStatusHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/status", nil)
	rr := httptest.NewRecorder()

	handler := statusHandler("")
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}

	if len(resp.Services) == 0 {
		t.Errorf("expected services in response, got 0")
	}

	for _, svc := range resp.Services {
		if svc.UptimeSeconds < 0 {
			t.Errorf("service %s has negative uptime: %d", svc.Name, svc.UptimeSeconds)
		}
	}
}

func TestStatusHandlerActiveTasks(t *testing.T) {
	// 1. Successful active task aggregation
	brainMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tasks" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
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
			return
		}
		http.NotFound(w, r)
	}))
	defer brainMock.Close()

	handler := statusHandler(brainMock.URL)
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}

	if resp.ActiveTasksCount != 1 || len(resp.ActiveTasks) != 1 {
		t.Fatalf("expected 1 active task, got %d: %+v", resp.ActiveTasksCount, resp.ActiveTasks)
	}

	if resp.ActiveTasks[0].ID != "task-abc" || resp.ActiveTasks[0].SessionID != "session-456" || resp.ActiveTasks[0].TriggerType != "discord" || resp.ActiveTasks[0].Summary != "Test execution prompt summary" {
		t.Errorf("unexpected task contents: %+v", resp.ActiveTasks[0])
	}
	if resp.ActiveTasks[0].Status != "PROCESSING" || resp.ActiveTasks[0].AuthorName != "UserA" {
		t.Errorf("unexpected task status/author: %+v", resp.ActiveTasks[0])
	}

	// 2. Graceful degradation on brain error / offline
	degradedHandler := statusHandler("http://127.0.0.1:54321")
	reqDegraded := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rrDegraded := httptest.NewRecorder()
	degradedHandler.ServeHTTP(rrDegraded, reqDegraded)

	if rrDegraded.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on degraded status, got %d", rrDegraded.Code)
	}

	var degradedResp ClusterResponse
	if err := json.NewDecoder(rrDegraded.Body).Decode(&degradedResp); err != nil {
		t.Fatalf("failed to decode degraded status response: %v", err)
	}

	if degradedResp.ActiveTasksCount != 0 {
		t.Errorf("expected 0 active tasks on degraded brain, got %d", degradedResp.ActiveTasksCount)
	}
	if degradedResp.ActiveTasks == nil || len(degradedResp.ActiveTasks) != 0 {
		t.Errorf("expected non-nil empty ActiveTasks slice on degraded brain, got %+v", degradedResp.ActiveTasks)
	}

	// 3. Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/api/status", nil)
	rrPost := httptest.NewRecorder()
	handler.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rrPost.Code)
	}
}

func TestFactsHandler_Success(t *testing.T) {
	// Mock brain upstream server
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/facts" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("limit") != "25" || q.Get("category") != "user_preference" {
			t.Errorf("unexpected upstream query: %s", r.URL.RawQuery)
		}
		resp := FactsAPIResponse{
			Facts: []FactItem{
				{
					ID:         1,
					Category:   "user_preference",
					FactText:   "User prefers dark mode",
					Importance: 0.9,
					ThreadID:   "thread-123",
					CreatedAt:  time.Now().UTC(),
				},
			},
			Total:  1,
			Limit:  25,
			Offset: 0,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer mockBrain.Close()

	handler := factsHandler(mockBrain.URL)
	req := httptest.NewRequest("GET", "/api/facts?limit=25&category=user_preference", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var data FactsAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(data.Facts) != 1 || data.Facts[0].FactText != "User prefers dark mode" {
		t.Errorf("unexpected facts in response: %+v", data.Facts)
	}
}

func TestFactsHandler_DegradedFallback(t *testing.T) {
	// Offline / unreachable brain upstream
	handler := factsHandler("http://127.0.0.1:54321")
	req := httptest.NewRequest("GET", "/api/facts", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on brain offline, got %d", rr.Code)
	}

	var data FactsAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode degraded response: %v", err)
	}

	if data.Status != "degraded" {
		t.Errorf("expected status 'degraded', got %s", data.Status)
	}
	if data.Facts == nil {
		t.Errorf("expected non-nil empty facts array")
	}
}

func TestGetGitCommit(t *testing.T) {
	commit := getGitCommit()
	if commit == "" {
		t.Errorf("expected non-empty git commit")
	}
	if len(commit) > 7 {
		t.Errorf("expected short commit <= 7 chars, got %s", commit)
	}
}

func TestParseMatrixJobChips(t *testing.T) {
	jobs := []GitHubJob{
		{
			ID:          101,
			Name:        "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)",
			Status:      "in_progress",
			StartedAt:   time.Now().Add(-45 * time.Second),
			CompletedAt: time.Time{},
		},
		{
			ID:          102,
			Name:        "Build & Push Images to GHCR (dashboard, ./dashboard, ./dashboard/Dockerfile, aerial-dashboard)",
			Status:      "completed",
			Conclusion:  "success",
			StartedAt:   time.Now().Add(-60 * time.Second),
			CompletedAt: time.Now().Add(-10 * time.Second),
		},
		{
			ID:         103,
			Name:       "Build & Push Images to GHCR (proxy, ./proxy, ./proxy/Dockerfile, aerial-proxy)",
			Status:     "queued",
			Conclusion: "",
		},
	}

	chips := parseMatrixJobChips(jobs)
	if len(chips) != 3 {
		t.Fatalf("expected 3 chips, got %d", len(chips))
	}

	for _, c := range chips {
		if c.Name == "brain" {
			if c.Status != "active" {
				t.Errorf("expected brain status 'active', got %s", c.Status)
			}
			if c.Duration == "" {
				t.Errorf("expected brain duration to be calculated, got empty")
			}
		} else if c.Name == "dashboard" {
			if c.Status != "completed" || c.Conclusion != "success" {
				t.Errorf("expected dashboard completed/success, got %s/%s", c.Status, c.Conclusion)
			}
		} else if c.Name == "proxy" {
			if c.Status != "pending" {
				t.Errorf("expected proxy status 'pending', got %s", c.Status)
			}
		}
	}
}

func TestMergeClusterDeployments_ActiveCIRun(t *testing.T) {
	now := time.Now().UTC()
	runs := []GitHubRun{
		{
			ID:         481,
			Name:       "Continuous Delivery",
			HeadSHA:    "7a9f1b234567",
			Status:     "in_progress",
			Conclusion: "",
			CreatedAt:  now.Add(-2 * time.Minute),
			UpdatedAt:  now.Add(-10 * time.Second),
			HTMLURL:    "https://github.com/azylman/aerial/actions/runs/481",
			HeadCommit: &struct {
				Message string `json:"message"`
			}{Message: "feat(brain): live github actions tracking"},
		},
	}

	jobs := map[int64][]GitHubJob{
		481: {
			{
				ID:         1,
				Name:       "Build & Push Images to GHCR (brain, ., ./brain/Dockerfile, aerial-brain)",
				Status:     "in_progress",
				StartedAt:  now.Add(-40 * time.Second),
				Conclusion: "",
			},
			{
				ID:          2,
				Name:        "Build & Push Images to GHCR (dashboard, ./dashboard, ./dashboard/Dockerfile, aerial-dashboard)",
				Status:      "completed",
				Conclusion:  "success",
				StartedAt:   now.Add(-60 * time.Second),
				CompletedAt: now.Add(-10 * time.Second),
			},
		},
	}

	deploys := mergeClusterDeployments(nil, runs, jobs, "7a9f1b2")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 active deployment, got %d", len(deploys))
	}

	dep := deploys[0]
	if dep.Stage != "building" {
		t.Errorf("expected stage 'building', got %s", dep.Stage)
	}
	if dep.Commit != "7a9f1b2" {
		t.Errorf("expected commit '7a9f1b2', got %s", dep.Commit)
	}
	if len(dep.MatrixJobs) != 2 {
		t.Errorf("expected 2 matrix jobs, got %d", len(dep.MatrixJobs))
	}
	if dep.HTMLURL != "https://github.com/azylman/aerial/actions/runs/481" {
		t.Errorf("expected HTMLURL to match, got %s", dep.HTMLURL)
	}
}

func TestMergeClusterDeployments_FailedCIRun(t *testing.T) {
	now := time.Now().UTC()
	runs := []GitHubRun{
		{
			ID:         482,
			Name:       "Continuous Delivery",
			HeadSHA:    "abc1234567",
			Status:     "completed",
			Conclusion: "failure",
			CreatedAt:  now.Add(-5 * time.Minute),
			UpdatedAt:  now.Add(-4 * time.Minute),
			HTMLURL:    "https://github.com/azylman/aerial/actions/runs/482",
		},
	}

	deploys := mergeClusterDeployments(nil, runs, nil, "abc1234")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 deployment on failed run, got %d", len(deploys))
	}

	dep := deploys[0]
	if dep.Stage != "failed" {
		t.Errorf("expected stage 'failed', got %s", dep.Stage)
	}
	if dep.Steps[1].Status != "failed" {
		t.Errorf("expected CI Build step status 'failed', got %s", dep.Steps[1].Status)
	}
}

func TestMergeClusterDeployments_AwaitingWatchtowerPull(t *testing.T) {
	now := time.Now().UTC()
	runs := []GitHubRun{
		{
			ID:         483,
			Name:       "Continuous Delivery",
			HeadSHA:    "def5678901",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  now.Add(-4 * time.Minute),
			UpdatedAt:  now.Add(-45 * time.Second), // <= 120s
			HTMLURL:    "https://github.com/azylman/aerial/actions/runs/483",
		},
	}

	// Local containers running older version (created 10 hours ago)
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-10 * time.Hour).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
		},
	}

	deploys := mergeClusterDeployments(containers, runs, nil, "old1234")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 deployment in awaiting_pull stage, got %d", len(deploys))
	}

	dep := deploys[0]
	if dep.Stage != "awaiting_pull" {
		t.Errorf("expected stage 'awaiting_pull', got %s", dep.Stage)
	}
	if dep.Steps[2].Status != "active" {
		t.Errorf("expected Watchtower Pull step status 'active', got %s", dep.Steps[2].Status)
	}
}

func TestMergeClusterDeployments_WatchtowerPullTimeout(t *testing.T) {
	now := time.Now().UTC()
	runs := []GitHubRun{
		{
			ID:         484,
			Name:       "Continuous Delivery",
			HeadSHA:    "timeout1234",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  now.Add(-10 * time.Minute),
			UpdatedAt:  now.Add(-150 * time.Second), // > 120s timeout
			HTMLURL:    "https://github.com/azylman/aerial/actions/runs/484",
		},
	}

	// Local containers still not updated
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-10 * time.Hour).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
		},
	}

	deploys := mergeClusterDeployments(containers, runs, nil, "old1234")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 deployment on pull timeout, got %d", len(deploys))
	}

	dep := deploys[0]
	if dep.Stage != "failed" {
		t.Errorf("expected stage 'failed' on Watchtower pull timeout, got %s", dep.Stage)
	}
	if dep.Steps[2].Status != "failed" {
		t.Errorf("expected Watchtower Pull step status 'failed', got %s", dep.Steps[2].Status)
	}
}

func TestMergeClusterDeployments_RollingSwap(t *testing.T) {
	now := time.Now().UTC()
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-30 * time.Second).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain", "org.opencontainers.image.revision": "abc9999999"},
			Health:  &struct {
				Status string `json:"Status"`
			}{Status: "starting"},
		},
		{
			ID:      "c2",
			Names:   []string{"/aerial-dashboard"},
			State:   "running",
			Created: now.Add(-10 * time.Second).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"},
		},
	}

	deploys := mergeClusterDeployments(containers, nil, nil, "abc9999")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 unified deployment on rolling swap, got %d", len(deploys))
	}

	dep := deploys[0]
	if dep.Stage != "swapping" {
		t.Errorf("expected stage 'swapping', got %s", dep.Stage)
	}
	if dep.Commit != "abc9999" {
		t.Errorf("expected commit resolved from image label 'abc9999', got %s", dep.Commit)
	}
	if len(dep.MatrixJobs) != 2 {
		t.Errorf("expected 2 container chips, got %d", len(dep.MatrixJobs))
	}
}

func TestMergeClusterDeployments_SyncedGrace(t *testing.T) {
	now := time.Now().UTC()
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-3 * time.Minute).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
			Health:  &struct {
				Status string `json:"Status"`
			}{Status: "healthy"},
		},
		{
			ID:      "c2",
			Names:   []string{"/aerial-dashboard"},
			State:   "running",
			Created: now.Add(-3 * time.Minute).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"},
			Health:  &struct {
				Status string `json:"Status"`
			}{Status: "healthy"},
		},
	}

	deploys := mergeClusterDeployments(containers, nil, nil, "live123")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 unified deployment in synced grace, got %d", len(deploys))
	}

	dep := deploys[0]
	if dep.Stage != "live" {
		t.Errorf("expected stage 'live', got %s", dep.Stage)
	}
	if dep.Progress != 100 {
		t.Errorf("expected progress 100, got %d", dep.Progress)
	}
	for i, s := range dep.Steps {
		if s.Status != "completed" {
			t.Errorf("expected step %d (%s) to be completed, got %s", i, s.Name, s.Status)
		}
	}
}

func TestMergeClusterDeployments_DegradedState(t *testing.T) {
	now := time.Now().UTC()
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-2 * time.Minute).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
			Health:  &struct {
				Status string `json:"Status"`
			}{Status: "unhealthy"},
		},
		{
			ID:      "c2",
			Names:   []string{"/aerial-dashboard"},
			State:   "running",
			Created: now.Add(-2 * time.Minute).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"},
			Health:  &struct {
				Status string `json:"Status"`
			}{Status: "healthy"},
		},
	}

	deploys := mergeClusterDeployments(containers, nil, nil, "deg1234")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 unified deployment on degraded state, got %d", len(deploys))
	}

	dep := deploys[0]
	if dep.Stage != "degraded" {
		t.Errorf("expected stage 'degraded', got %s", dep.Stage)
	}
	if dep.Steps[4].Status != "failed" {
		t.Errorf("expected Health Check step status 'failed', got %s", dep.Steps[4].Status)
	}
}

func TestMergeClusterDeployments_Idle(t *testing.T) {
	now := time.Now().UTC()
	// Containers up for 2 hours with no active CI
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-2 * time.Hour).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
			Health:  &struct {
				Status string `json:"Status"`
			}{Status: "healthy"},
		},
	}

	deploys := mergeClusterDeployments(containers, nil, nil, "idle123")
	if len(deploys) != 0 {
		t.Fatalf("expected 0 deployments (idle), got %d", len(deploys))
	}
}

func TestMergeClusterDeployments_AdversarialNilLabelsAndNilHealth(t *testing.T) {
	now := time.Now().UTC()
	// Containers with nil Labels map, nil Health pointer, but running state
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-30 * time.Second).Unix(),
			Labels:  nil,
			Health:  nil,
		},
		{
			ID:      "c2",
			Names:   []string{"/aerial-dashboard"},
			State:   "running",
			Created: now.Add(-30 * time.Second).Unix(),
			Labels:  nil,
			Health:  nil,
		},
	}

	deploys := mergeClusterDeployments(containers, nil, nil, "test1234")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 deployment, got %d", len(deploys))
	}
	dep := deploys[0]
	if dep.Stage != "swapping" {
		t.Errorf("expected swapping stage, got %s", dep.Stage)
	}
	if len(dep.MatrixJobs) != 2 {
		t.Errorf("expected 2 container chips, got %d", len(dep.MatrixJobs))
	}
}

func TestMergeClusterDeployments_AdversarialClockSkewFutureTimestamp(t *testing.T) {
	now := time.Now().UTC()
	futureRun := []GitHubRun{
		{
			ID:         999,
			Name:       "Continuous Delivery",
			HeadSHA:    "future1234",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  now.Add(10 * time.Second),
			UpdatedAt:  now.Add(15 * time.Second), // In future due to clock skew
			HTMLURL:    "https://github.com/azylman/aerial/actions/runs/999",
		},
	}

	// Local containers running older version
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-2 * time.Hour).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
		},
	}

	deploys := mergeClusterDeployments(containers, futureRun, nil, "old1234")
	if len(deploys) != 1 {
		t.Fatalf("expected 1 deployment, got %d", len(deploys))
	}
	dep := deploys[0]
	if dep.Stage != "awaiting_pull" {
		t.Errorf("expected stage 'awaiting_pull' despite future timestamp, got %s", dep.Stage)
	}
}

func TestParseMatrixJobChips_LintAndTests(t *testing.T) {
	jobs := []GitHubJob{
		{
			ID:          201,
			Name:        "Run Service Unit Tests",
			Status:      "completed",
			Conclusion:  "success",
			StartedAt:   time.Now().Add(-30 * time.Second),
			CompletedAt: time.Now().Add(-5 * time.Second),
		},
		{
			ID:          202,
			Name:        "Lint Go Microservices",
			Status:      "in_progress",
			StartedAt:   time.Now().Add(-10 * time.Second),
		},
	}

	chips := parseMatrixJobChips(jobs)
	if len(chips) != 2 {
		t.Fatalf("expected 2 chips (unit-tests, lint), got %d", len(chips))
	}
	if chips[0].Name != "unit-tests" || chips[0].Status != "completed" {
		t.Errorf("unexpected chip 0: %+v", chips[0])
	}
	if chips[1].Name != "lint" || chips[1].Status != "active" {
		t.Errorf("unexpected chip 1: %+v", chips[1])
	}
}


func TestSchedulesHandler_Success(t *testing.T) {
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schedules" {
			http.NotFound(w, r)
			return
		}
		resp := SchedulesAPIResponse{
			Status: "ok",
			Summary: ScheduleSummaryMetrics{
				TotalActive:    3,
				CronCount:      2,
				OneShotCount:   1,
				TotalRuns24h:   15,
				SuccessRate24h: 93.3,
			},
			Crons: []CronSchedule{
				{
					ID:              "cron-1",
					ChannelID:       "chan-1",
					TitlePrefix:     "Morning Brief",
					CronExpr:        "0 9 * * *",
					CronDescription: "Every day at 9:00 AM",
					Prompt:          "Generate morning brief",
					Timezone:        "America/Los_Angeles",
					Enabled:         true,
				},
			},
			OneShots: []OneShotSchedule{
				{
					ID:       "oneshot-1",
					ThreadID: "thread-1",
					Prompt:   "Remind about tea",
					RunAt:    time.Now().UTC().Add(10 * time.Minute),
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer mockBrain.Close()

	handler := schedulesHandler(mockBrain.URL)
	req := httptest.NewRequest("GET", "/api/schedules", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var data SchedulesAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if data.Status != "ok" {
		t.Errorf("expected status 'ok', got %s", data.Status)
	}
	if data.Summary.TotalActive != 3 || data.Summary.CronCount != 2 || data.Summary.OneShotCount != 1 {
		t.Errorf("unexpected summary in response: %+v", data.Summary)
	}
	if len(data.Crons) != 1 || data.Crons[0].TitlePrefix != "Morning Brief" {
		t.Errorf("unexpected crons in response: %+v", data.Crons)
	}
	if len(data.OneShots) != 1 || data.OneShots[0].Prompt != "Remind about tea" {
		t.Errorf("unexpected one_shots in response: %+v", data.OneShots)
	}
}

func TestSchedulesHandler_DegradedFallback(t *testing.T) {
	handler := schedulesHandler("http://127.0.0.1:54321")
	req := httptest.NewRequest("GET", "/api/schedules", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on brain offline, got %d", rr.Code)
	}

	var data SchedulesAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode degraded response: %v", err)
	}

	if data.Status != "degraded" {
		t.Errorf("expected status 'degraded', got %s", data.Status)
	}
	if data.Error != "Brain service unreachable. Retrying..." {
		t.Errorf("expected fallback error message, got %s", data.Error)
	}
	if data.Summary.TotalActive != 0 || data.Summary.SuccessRate24h != 100.0 {
		t.Errorf("expected fallback summary with TotalActive=0, SuccessRate24h=100.0, got %+v", data.Summary)
	}
	if data.Crons == nil || len(data.Crons) != 0 {
		t.Errorf("expected non-nil empty crons array")
	}
	if data.OneShots == nil || len(data.OneShots) != 0 {
		t.Errorf("expected non-nil empty one_shots array")
	}
}

func TestSchedulesHandler_MethodNotAllowed(t *testing.T) {
	handler := schedulesHandler("http://127.0.0.1:54321")
	req := httptest.NewRequest("POST", "/api/schedules", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rr.Code)
	}
}

func TestScheduleRunsHandler_Success(t *testing.T) {
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schedules/runs" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("limit") != "10" || q.Get("offset") != "5" || q.Get("schedule_id") != "cron-1" || q.Get("status") != "success" {
			t.Errorf("unexpected upstream query params: %s", r.URL.RawQuery)
		}
		resp := ScheduleRunsAPIResponse{
			Status: "ok",
			Total:  42,
			Limit:  10,
			Offset: 5,
			Runs: []ScheduleRun{
				{
					ID:           101,
					ScheduleID:   "cron-1",
					ScheduleType: "cron",
					Prompt:       "Daily summary",
					Title:        "Morning Brief #101",
					Status:       "success",
					TriggeredAt:  time.Now().UTC().Add(-1 * time.Hour),
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer mockBrain.Close()

	handler := scheduleRunsHandler(mockBrain.URL)
	req := httptest.NewRequest("GET", "/api/schedules/runs?limit=10&offset=5&schedule_id=cron-1&status=success", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var data ScheduleRunsAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if data.Status != "ok" || data.Total != 42 || data.Limit != 10 || data.Offset != 5 {
		t.Errorf("unexpected metadata in response: %+v", data)
	}
	if len(data.Runs) != 1 || data.Runs[0].ID != 101 || data.Runs[0].Status != "success" {
		t.Errorf("unexpected runs in response: %+v", data.Runs)
	}
}

func TestScheduleRunsHandler_DegradedFallback(t *testing.T) {
	handler := scheduleRunsHandler("http://127.0.0.1:54321")
	req := httptest.NewRequest("GET", "/api/schedules/runs?limit=20&offset=10", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable on brain offline, got %d", rr.Code)
	}

	var data ScheduleRunsAPIResponse
	if err := json.NewDecoder(rr.Body).Decode(&data); err != nil {
		t.Fatalf("failed to decode degraded response: %v", err)
	}

	if data.Status != "degraded" {
		t.Errorf("expected status 'degraded', got %s", data.Status)
	}
	if data.Error != "Brain service unreachable. Retrying..." {
		t.Errorf("expected fallback error message, got %s", data.Error)
	}
	if data.Total != 0 || data.Limit != 20 || data.Offset != 10 {
		t.Errorf("expected Total=0, Limit=20, Offset=10, got %+v", data)
	}
	if data.Runs == nil || len(data.Runs) != 0 {
		t.Errorf("expected non-nil empty runs array")
	}
}

func TestScheduleRunsHandler_MethodNotAllowed(t *testing.T) {
	handler := scheduleRunsHandler("http://127.0.0.1:54321")
	req := httptest.NewRequest("DELETE", "/api/schedules/runs", nil)
	rr := httptest.NewRecorder()

	handler(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rr.Code)
	}
}

func TestEmbeddedStaticAssetsIntegrity(t *testing.T) {
	requiredFiles := []string{
		"static/index.html",
		"static/style.css",
		"static/app.js",
	}

	for _, reqFile := range requiredFiles {
		data, err := content.ReadFile(reqFile)
		if err != nil {
			t.Errorf("failed to read required embedded file %s: %v", reqFile, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("embedded file %s is empty", reqFile)
		}
	}
}

func TestAppJSDeclaredFunctions(t *testing.T) {
	data, err := content.ReadFile("static/app.js")
	if err != nil {
		t.Fatalf("failed to read static/app.js: %v", err)
	}
	contentStr := string(data)

	requiredFunctions := []string{
		"function formatAgentsviewSessionUrl",
		"function parseValidTimestampMs",
		"function formatElapsedTicker",
		"function escapeHtml",
		"function formatUptime",
		"function getTriggerBadge",
		"function renderActiveTasks",
		"function renderDeployments",
		"function renderServicesGrid",
		"async function fetchStatus",
	}

	for _, fn := range requiredFunctions {
		if !strings.Contains(contentStr, fn) {
			t.Errorf("critical function definition missing in app.js: %q", fn)
		}
	}
}

func TestIndexHTMLRequiredDOMBindings(t *testing.T) {
	data, err := content.ReadFile("static/index.html")
	if err != nil {
		t.Fatalf("failed to read static/index.html: %v", err)
	}
	htmlStr := string(data)

	requiredIDs := []string{
		`id="overall-status"`,
		`id="cluster-sub"`,
		`id="summary-tasks-val"`,
		`id="summary-tasks-sub"`,
		`id="tasks-count-badge"`,
		`id="active-tasks-container"`,
		`id="deployments-container"`,
		`id="deploy-count-badge"`,
		`id="services-grid"`,
		`id="active-count"`,
		`id="permet-score-val"`,
		`id="permet-bar-fill"`,
		`id="tab-telemetry-btn"`,
		`id="tab-schedules-btn"`,
		`id="tab-memory-btn"`,
	}

	for _, idAttr := range requiredIDs {
		if !strings.Contains(htmlStr, idAttr) {
			t.Errorf("required DOM ID binding missing in index.html: %q", idAttr)
		}
	}
}

func TestZeroPersonalDataAndHardcodedIPs(t *testing.T) {
	files := []string{"static/app.js", "static/index.html", "static/style.css"}

	for _, f := range files {
		data, err := content.ReadFile(f)
		if err != nil {
			t.Fatalf("failed to read %s: %v", f, err)
		}
		str := string(data)

		// Assert zero private LAN IP leaks
		if strings.Contains(str, "192.168.") {
			t.Errorf("found private LAN IP (192.168.x.x) in %s", f)
		}
		if strings.Contains(str, "10.0.") {
			t.Errorf("found private LAN IP (10.0.x.x) in %s", f)
		}
	}
}

func TestMatchesETag(t *testing.T) {
	cases := []struct {
		name        string
		ifNoneMatch string
		targetETag  string
		targetHash  string
		wantMatch   bool
	}{
		{"exact strong match", `"abc1234"`, `"abc1234"`, "abc1234", true},
		{"client weak vs server strong", `W/"abc1234"`, `"abc1234"`, "abc1234", true},
		{"client strong vs server weak", `"abc1234"`, `W/"abc1234"`, "abc1234", true},
		{"client weak vs server weak", `W/"abc1234"`, `W/"abc1234"`, "abc1234", true},
		{"wildcard match", `*`, `"abc1234"`, "abc1234", true},
		{"comma separated list with match", `"other", W/"abc1234", "xyz"`, `"abc1234"`, "abc1234", true},
		{"comma separated list without match", `"other", W/"nomatch", "xyz"`, `"abc1234"`, "abc1234", false},
		{"empty header", "", `"abc1234"`, "abc1234", false},
		{"mismatch", `"diff"`, `"abc1234"`, "abc1234", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MatchesETag(tc.ifNoneMatch, tc.targetETag, tc.targetHash)
			if got != tc.wantMatch {
				t.Errorf("MatchesETag(%q, %q, %q) = %v; want %v", tc.ifNoneMatch, tc.targetETag, tc.targetHash, got, tc.wantMatch)
			}
		})
	}
}

func TestAssetRegistry_ServeHTTP(t *testing.T) {
	staticFS, err := fs.Sub(content, "static")
	if err != nil {
		t.Fatalf("failed to create sub filesystem: %v", err)
	}

	reg, err := NewAssetRegistry(staticFS, "testcommit123")
	if err != nil {
		t.Fatalf("failed to create AssetRegistry: %v", err)
	}

	t.Run("serves index.html on root / with ETag and no-cache", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		reg.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}
		etag := rec.Header().Get("ETag")
		if etag == "" {
			t.Errorf("expected ETag header to be set")
		}
		cc := rec.Header().Get("Cache-Control")
		if !strings.Contains(cc, "no-cache") {
			t.Errorf("expected Cache-Control to contain no-cache, got %q", cc)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "app.js?v=testcommit123") {
			t.Errorf("expected index.html to contain injected asset version, got: %s", body)
		}
	})

	t.Run("serves 304 Not Modified when If-None-Match matches ETag", func(t *testing.T) {
		req1 := httptest.NewRequest("GET", "/app.js", nil)
		rec1 := httptest.NewRecorder()
		reg.ServeHTTP(rec1, req1)
		etag := rec1.Header().Get("ETag")

		req2 := httptest.NewRequest("GET", "/app.js", nil)
		req2.Header.Set("If-None-Match", etag)
		rec2 := httptest.NewRecorder()
		reg.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusNotModified {
			t.Fatalf("expected status 304, got %d", rec2.Code)
		}
		if rec2.Body.Len() != 0 {
			t.Errorf("expected empty body on 304, got %d bytes", rec2.Body.Len())
		}
	})

	t.Run("serves 304 on weak ETag If-None-Match", func(t *testing.T) {
		req1 := httptest.NewRequest("GET", "/style.css", nil)
		rec1 := httptest.NewRecorder()
		reg.ServeHTTP(rec1, req1)
		etag := rec1.Header().Get("ETag")

		req2 := httptest.NewRequest("GET", "/style.css", nil)
		req2.Header.Set("If-None-Match", "W/"+etag)
		rec2 := httptest.NewRecorder()
		reg.ServeHTTP(rec2, req2)

		if rec2.Code != http.StatusNotModified {
			t.Fatalf("expected status 304 for weak ETag, got %d", rec2.Code)
		}
	})

	t.Run("sets immutable cache-control when version query param is present", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/app.js?v=testcommit123", nil)
		rec := httptest.NewRecorder()
		reg.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}
		cc := rec.Header().Get("Cache-Control")
		if !strings.Contains(cc, "immutable") || !strings.Contains(cc, "public") {
			t.Errorf("expected immutable public Cache-Control, got %q", cc)
		}
	})

	t.Run("handles HEAD request with headers and empty body", func(t *testing.T) {
		req := httptest.NewRequest("HEAD", "/app.js", nil)
		rec := httptest.NewRecorder()
		reg.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rec.Code)
		}
		if rec.Header().Get("Content-Type") == "" {
			t.Errorf("expected Content-Type to be set on HEAD")
		}
		if rec.Body.Len() != 0 {
			t.Errorf("expected empty body on HEAD, got %d bytes", rec.Body.Len())
		}
	})

	t.Run("returns 404 for nonexistent asset", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/nonexistent.js", nil)
		rec := httptest.NewRecorder()
		reg.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected status 404, got %d", rec.Code)
		}
	})

	t.Run("returns 405 for POST request", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/app.js", nil)
		rec := httptest.NewRecorder()
		reg.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected status 405, got %d", rec.Code)
		}
	})
}




