package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

	handler := statusHandler("", "", "", "")
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

	handler := statusHandler(brainMock.URL, "", "", "")
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
	degradedHandler := statusHandler("http://127.0.0.1:54321", "", "", "")
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

func TestFactsHandler_UnlimitedAndHighLimit(t *testing.T) {
	var capturedLimit string
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedLimit = r.URL.Query().Get("limit")
		resp := FactsAPIResponse{
			Facts: []FactItem{
				{
					ID:         1,
					Category:   "user_preference",
					FactText:   "Fact 1",
					Importance: 1.0,
					CreatedAt:  time.Now().UTC(),
				},
			},
			Total:  1,
			Limit:  0,
			Offset: 0,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer mockBrain.Close()

	handler := factsHandler(mockBrain.URL)

	// 1. Unlimited request (no limit param)
	req1 := httptest.NewRequest("GET", "/api/facts", nil)
	rr1 := httptest.NewRecorder()
	handler(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr1.Code)
	}
	if capturedLimit != "" {
		t.Errorf("expected empty limit forwarded for unlimited request, got %q", capturedLimit)
	}

	// 2. High limit request (limit=500)
	req2 := httptest.NewRequest("GET", "/api/facts?limit=500", nil)
	rr2 := httptest.NewRecorder()
	handler(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr2.Code)
	}
	if capturedLimit != "500" {
		t.Errorf("expected limit=500 forwarded to brain, got %q", capturedLimit)
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
				Message   string    `json:"message"`
				Timestamp time.Time `json:"timestamp"`
			}{Message: "feat(brain): live github actions tracking", Timestamp: now.Add(-2 * time.Minute)},
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
			Health: &struct {
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
			Health: &struct {
				Status string `json:"Status"`
			}{Status: "healthy"},
		},
		{
			ID:      "c2",
			Names:   []string{"/aerial-dashboard"},
			State:   "running",
			Created: now.Add(-3 * time.Minute).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"},
			Health: &struct {
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
			Health: &struct {
				Status string `json:"Status"`
			}{Status: "unhealthy"},
		},
		{
			ID:      "c2",
			Names:   []string{"/aerial-dashboard"},
			State:   "running",
			Created: now.Add(-2 * time.Minute).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "dashboard"},
			Health: &struct {
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
			Health: &struct {
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
			ID:        202,
			Name:      "Lint Go Microservices",
			Status:    "in_progress",
			StartedAt: time.Now().Add(-10 * time.Second),
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
		started := time.Now().UTC().Add(-1 * time.Hour)
		resp := ScheduleRunsAPIResponse{
			Status: "ok",
			Total:  42,
			Limit:  10,
			Offset: 5,
			Runs: []ScheduleRun{
				{
					ID:           "cron-brief-101",
					ScheduleID:   "cron-1",
					ScheduleType: "cron",
					Prompt:       "Daily summary",
					Title:        "Morning Brief #101",
					Status:       "success",
					StartedAt:    started,
					DurationMs:   1250,
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
	if len(data.Runs) != 1 || data.Runs[0].ID != "cron-brief-101" || data.Runs[0].Status != "success" || data.Runs[0].DurationMs != 1250 {
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
		`id="quick-launch-dock"`,
		`id="tasks-count-badge"`,
		`id="active-tasks-container"`,
		`id="deployments-container"`,
		`id="deploy-count-badge"`,
		`id="gitsync-badge"`,
		`id="services-grid"`,
		`id="active-count"`,
		`id="permet-score-val"`,
		`id="permet-bar-fill"`,
		`id="tab-telemetry-btn"`,
		`id="tab-tasks-btn"`,
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
		if !strings.Contains(body, "style.css?v=testcommit123") {
			t.Errorf("expected index.html to contain injected style version, got: %s", body)
		}
	})

	t.Run("injects independent content hashes for app.js and style.css when versionToken is empty", func(t *testing.T) {
		regFallback, err := NewAssetRegistry(staticFS, "")
		if err != nil {
			t.Fatalf("failed to create AssetRegistry with fallback: %v", err)
		}
		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		regFallback.ServeHTTP(rec, req)

		body := rec.Body.String()
		if !strings.Contains(body, "app.js?v=") || !strings.Contains(body, "style.css?v=") {
			t.Errorf("expected versioned links for both app.js and style.css, got: %s", body)
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

func TestGetContainerCommit_IgnoreAuxiliarySidecars(t *testing.T) {
	containers := []DockerContainerJSON{
		{
			ID:    "c-agentsview",
			Names: []string{"/aerial-agentsview"},
			Image: "ghcr.io/azylman/agentsview:latest",
			Labels: map[string]string{
				"com.docker.compose.project":        "aerial",
				"com.docker.compose.service":        "agentsview",
				"org.opencontainers.image.revision": "080b49fb7a7c1d8f206549e8ee4eaf9cf50a5c20",
			},
		},
		{
			ID:    "c-watchtower",
			Names: []string{"/aerial-watchtower"},
			Image: "containrrr/watchtower:latest",
			Labels: map[string]string{
				"com.docker.compose.project":        "aerial",
				"com.docker.compose.service":        "watchtower",
				"org.opencontainers.image.revision": "ace93994711edfe47036e2d2ee5f5d531df013ed",
			},
		},
		{
			ID:    "c-autoheal",
			Names: []string{"/aerial-autoheal"},
			Image: "willfarrell/autoheal:latest",
			Labels: map[string]string{
				"com.docker.compose.project":        "aerial",
				"com.docker.compose.service":        "autoheal",
				"org.opencontainers.image.revision": "autohealsha1234",
			},
		},
		{
			ID:    "c-brain",
			Names: []string{"/aerial-brain"},
			Image: "ghcr.io/azylman/aerial-brain:latest",
			Labels: map[string]string{
				"com.docker.compose.project": "aerial",
				"com.docker.compose.service": "brain",
				"aerial.commit_sha":          "e056544d32a5f9d8a4cc50915a20db5eaea7db1e",
			},
		},
	}

	commit := getContainerCommit(containers)
	if commit != "e056544" {
		t.Fatalf("expected brain commit 'e056544', got %q", commit)
	}
}

func TestMergeClusterDeployments_CommitTimeAcrossAllStages(t *testing.T) {
	now := time.Now().UTC()
	commitTime := now.Add(-10 * time.Minute)

	// 1. CI Queued
	runsQueued := []GitHubRun{
		{
			ID:         501,
			Name:       "Continuous Delivery",
			HeadSHA:    "1111aaa",
			Status:     "queued",
			Conclusion: "",
			CreatedAt:  now.Add(-2 * time.Minute),
			HeadCommit: &struct {
				Message   string    `json:"message"`
				Timestamp time.Time `json:"timestamp"`
			}{
				Message:   "feat: queued",
				Timestamp: commitTime,
			},
		},
	}
	deploysQueued := mergeClusterDeployments(nil, runsQueued, nil, "1111aaa")
	if len(deploysQueued) != 1 || deploysQueued[0].CommitTime == nil || !deploysQueued[0].CommitTime.Equal(commitTime) {
		t.Errorf("expected queued stage to have valid CommitTime")
	}

	// 2. CI Building
	runsBuilding := []GitHubRun{
		{
			ID:         502,
			Name:       "Continuous Delivery",
			HeadSHA:    "2222bbb",
			Status:     "in_progress",
			Conclusion: "",
			CreatedAt:  now.Add(-1 * time.Minute),
			HeadCommit: &struct {
				Message   string    `json:"message"`
				Timestamp time.Time `json:"timestamp"`
			}{
				Message:   "feat: building",
				Timestamp: commitTime,
			},
		},
	}
	deploysBuilding := mergeClusterDeployments(nil, runsBuilding, nil, "2222bbb")
	if len(deploysBuilding) != 1 || deploysBuilding[0].CommitTime == nil || !deploysBuilding[0].CommitTime.Equal(commitTime) {
		t.Errorf("expected building stage to have valid CommitTime")
	}

	// 3. CI Failed
	runsFailed := []GitHubRun{
		{
			ID:         503,
			Name:       "Continuous Delivery",
			HeadSHA:    "3333ccc",
			Status:     "completed",
			Conclusion: "failure",
			CreatedAt:  now.Add(-5 * time.Minute),
			UpdatedAt:  now.Add(-4 * time.Minute),
			HeadCommit: &struct {
				Message   string    `json:"message"`
				Timestamp time.Time `json:"timestamp"`
			}{
				Message:   "feat: failed",
				Timestamp: commitTime,
			},
		},
	}
	deploysFailed := mergeClusterDeployments(nil, runsFailed, nil, "3333ccc")
	if len(deploysFailed) != 1 || deploysFailed[0].CommitTime == nil || !deploysFailed[0].CommitTime.Equal(commitTime) {
		t.Errorf("expected failed stage to have valid CommitTime")
	}

	// 4. Local Swapping & Live with GH poller cache
	containersSwapping := []DockerContainerJSON{
		{
			ID:      "c-brain",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(-30 * time.Second).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
			Health: &struct {
				Status string `json:"Status"`
			}{Status: "starting"},
		},
	}
	runsSuccess := []GitHubRun{
		{
			ID:         504,
			Name:       "Continuous Delivery",
			HeadSHA:    "4444ddd",
			Status:     "completed",
			Conclusion: "success",
			CreatedAt:  now.Add(-2 * time.Minute),
			UpdatedAt:  now.Add(-1 * time.Minute),
			HeadCommit: &struct {
				Message   string    `json:"message"`
				Timestamp time.Time `json:"timestamp"`
			}{
				Message:   "feat: success",
				Timestamp: commitTime,
			},
		},
	}
	deploysSwapping := mergeClusterDeployments(containersSwapping, runsSuccess, nil, "4444ddd")
	if len(deploysSwapping) != 1 || deploysSwapping[0].Commit != "4444ddd" || deploysSwapping[0].CommitTime == nil || !deploysSwapping[0].CommitTime.Equal(commitTime) {
		t.Errorf("expected swapping stage to resolve commit and CommitTime from runs")
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

func TestLoadQuickLaunchLinks(t *testing.T) {
	// 1. Missing or empty path returns default core links
	defaults := DefaultQuickLaunchLinks()
	if len(defaults) != 3 {
		t.Fatalf("expected 3 default quick launch links, got %d", len(defaults))
	}

	linksEmpty := loadQuickLaunchLinks("/non/existent/path/config.yaml")
	if len(linksEmpty) != 3 {
		t.Errorf("expected 3 links for non-existent file, got %d", len(linksEmpty))
	}
	for _, l := range linksEmpty {
		if !l.IsCore {
			t.Errorf("expected default link to have IsCore=true: %+v", l)
		}
	}

	// 2. Custom links in dashboard.quick_launch_links
	tempDir := t.TempDir()
	cfgFile1 := filepath.Join(tempDir, "config1.yaml")
	yamlContent1 := `
dashboard:
  quick_launch_links:
    - name: "HOME"
      url: "https://home.zylman.com"
      icon: "🏠"
      description: "Home Infrastructure Hub"
    - name: ""
      url: "https://skip.com"
    - name: "NO_URL"
      url: ""
`
	if err := os.WriteFile(cfgFile1, []byte(yamlContent1), 0644); err != nil {
		t.Fatal(err)
	}

	links1 := loadQuickLaunchLinks(cfgFile1)
	if len(links1) != 4 {
		t.Fatalf("expected 4 links (3 core + 1 custom), got %d", len(links1))
	}
	customLink := links1[3]
	if customLink.Name != "HOME" || customLink.URL != "https://home.zylman.com" || customLink.Icon != "🏠" {
		t.Errorf("unexpected custom link: %+v", customLink)
	}
	if customLink.Target != "_blank" || !customLink.IsCustom {
		t.Errorf("expected target '_blank' and IsCustom=true: %+v", customLink)
	}

	// 3. Custom links in root quick_links
	cfgFile2 := filepath.Join(tempDir, "config2.yaml")
	yamlContent2 := `
quick_links:
  - name: "INTERNAL"
    url: "http://internal.lan"
    target: "_self"
`
	if err := os.WriteFile(cfgFile2, []byte(yamlContent2), 0644); err != nil {
		t.Fatal(err)
	}

	links2 := loadQuickLaunchLinks(cfgFile2)
	if len(links2) != 4 {
		t.Fatalf("expected 4 links (3 core + 1 custom), got %d", len(links2))
	}
	if links2[3].Name != "INTERNAL" || links2[3].Target != "_self" {
		t.Errorf("unexpected root custom link: %+v", links2[3])
	}

	// 4. Malformed YAML
	cfgFile3 := filepath.Join(tempDir, "config3.yaml")
	if err := os.WriteFile(cfgFile3, []byte("invalid: [yaml: broken"), 0644); err != nil {
		t.Fatal(err)
	}
	links3 := loadQuickLaunchLinks(cfgFile3)
	if len(links3) != 3 {
		t.Errorf("expected 3 links on malformed YAML, got %d", len(links3))
	}
}

func TestFetchGitSyncStatus(t *testing.T) {
	ctx := context.Background()

	// 1. Empty URL
	fb := fetchGitSyncStatus(ctx, "")
	if fb.Status != "synced" || fb.MaxLagSeconds != 0 {
		t.Errorf("expected default fallback for empty URL, got %+v", fb)
	}

	// 2. Successful upstream response
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
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
	}))
	defer mockServer.Close()

	resp := fetchGitSyncStatus(ctx, mockServer.URL)
	if resp.Status != "lagging" || resp.MaxLagSeconds != 42 {
		t.Errorf("unexpected response from mock server: %+v", resp)
	}
	if len(resp.Repos) != 1 || resp.Repos["/share/aerial"].DiskCommit != "commit1" {
		t.Errorf("unexpected repo status: %+v", resp.Repos)
	}

	// 3. Upstream 500 error returns fallback
	errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer errorServer.Close()

	errResp := fetchGitSyncStatus(ctx, errorServer.URL)
	if errResp.Status != "synced" {
		t.Errorf("expected fallback status 'synced' on error, got %q", errResp.Status)
	}

	// 4. Invalid JSON returns fallback
	badJSONServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not-json`))
	}))
	defer badJSONServer.Close()

	badResp := fetchGitSyncStatus(ctx, badJSONServer.URL)
	if badResp.Status != "synced" {
		t.Errorf("expected fallback status 'synced' on bad JSON, got %q", badResp.Status)
	}
}

func TestStatusHandler_EnrichedFields(t *testing.T) {
	tempDir := t.TempDir()
	cfgFile := filepath.Join(tempDir, "config.yaml")
	cfgContent := `
dashboard:
  quick_launch_links:
    - name: "HOME"
      url: "https://home.zylman.com"
`
	if err := os.WriteFile(cfgFile, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}

	gitsyncMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GitSyncStatusResponse{
			Status:        "synced",
			MaxLagSeconds: 0,
			LastSyncTime:  time.Now().UTC(),
		})
	}))
	defer gitsyncMock.Close()

	handler := statusHandler("", gitsyncMock.URL, cfgFile, "testsha")
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify QuickLaunchLinks
	if len(resp.QuickLaunchLinks) != 4 {
		t.Fatalf("expected 4 quick launch links, got %d", len(resp.QuickLaunchLinks))
	}
	hasHome := false
	for _, l := range resp.QuickLaunchLinks {
		if l.Name == "HOME" && l.URL == "https://home.zylman.com" {
			hasHome = true
		}
	}
	if !hasHome {
		t.Errorf("expected 'HOME' link in QuickLaunchLinks: %+v", resp.QuickLaunchLinks)
	}

	// Verify GitSync
	if resp.GitSync.Status != "synced" {
		t.Errorf("expected GitSync status 'synced', got %q", resp.GitSync.Status)
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

func TestGetMimeType(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"index.html", "text/html; charset=utf-8"},
		{"style.css", "text/css; charset=utf-8"},
		{"data.json", "application/json"},
		{"logo.svg", "image/svg+xml"},
		{"image.png", "image/png"},
		{"custom.woff2", "font/woff2"},
		{"unknown.nonexistent123456", "application/octet-stream"},
	}

	for _, tt := range tests {
		got := getMimeType(tt.path)
		if got != tt.expected {
			t.Errorf("getMimeType(%q) = %q, want %q", tt.path, got, tt.expected)
		}
	}
}

func TestGetContainerCommit_AllLabels(t *testing.T) {
	// 1. Non-aerial container or nil labels
	emptyCommit := getContainerCommit([]DockerContainerJSON{
		{Names: []string{"/other"}, Labels: nil},
	})
	if emptyCommit != "" {
		t.Errorf("expected empty string from getContainerCommit, got %q", emptyCommit)
	}

	// 2. org.opencontainers.image.revision short & long
	revShort := getContainerCommit([]DockerContainerJSON{
		{
			Names:  []string{"/aerial-brain"},
			Labels: map[string]string{"com.docker.compose.project": "aerial", "org.opencontainers.image.revision": "123456"},
		},
	})
	if revShort != "123456" {
		t.Errorf("expected '123456', got %q", revShort)
	}

	revLong := getContainerCommit([]DockerContainerJSON{
		{
			Names:  []string{"/aerial-brain"},
			Labels: map[string]string{"com.docker.compose.project": "aerial", "org.opencontainers.image.revision": "1234567890"},
		},
	})
	if revLong != "1234567" {
		t.Errorf("expected '1234567', got %q", revLong)
	}

	// 3. aerial.commit_sha short & long
	shaShort := getContainerCommit([]DockerContainerJSON{
		{
			Names:  []string{"/aerial-brain"},
			Labels: map[string]string{"com.docker.compose.project": "aerial", "aerial.commit_sha": "abcdef"},
		},
	})
	if shaShort != "abcdef" {
		t.Errorf("expected 'abcdef', got %q", shaShort)
	}

	shaLong := getContainerCommit([]DockerContainerJSON{
		{
			Names:  []string{"/aerial-brain"},
			Labels: map[string]string{"com.docker.compose.project": "aerial", "aerial.commit_sha": "abcdef012345"},
		},
	})
	if shaLong != "abcdef0" {
		t.Errorf("expected 'abcdef0', got %q", shaLong)
	}

	// 4. vcs-ref short & long
	vcsShort := getContainerCommit([]DockerContainerJSON{
		{
			Names:  []string{"/aerial-brain"},
			Labels: map[string]string{"com.docker.compose.project": "aerial", "vcs-ref": "987654"},
		},
	})
	if vcsShort != "987654" {
		t.Errorf("expected '987654', got %q", vcsShort)
	}

	vcsLong := getContainerCommit([]DockerContainerJSON{
		{
			Names:  []string{"/aerial-brain"},
			Labels: map[string]string{"com.docker.compose.project": "aerial", "vcs-ref": "9876543210"},
		},
	})
	if vcsLong != "9876543" {
		t.Errorf("expected '9876543', got %q", vcsLong)
	}
}

func TestGetGitCommitVariations(t *testing.T) {
	// 1. Normalize short commit
	if c := normalizeGitCommit("abcdef"); c != "abcdef" {
		t.Errorf("expected 'abcdef', got %q", c)
	}

	// 2. Normalize long commit
	if c := normalizeGitCommit("abcdef0123456789"); c != "abcdef0" {
		t.Errorf("expected 'abcdef0', got %q", c)
	}

	// 3. From ref file
	tempDir := t.TempDir()
	headFile := filepath.Join(tempDir, "HEAD")
	mainRefFile := filepath.Join(tempDir, "refs", "heads", "main")
	_ = os.MkdirAll(filepath.Dir(mainRefFile), 0755)
	_ = os.WriteFile(headFile, []byte("ref: refs/heads/main\n"), 0644)
	_ = os.WriteFile(mainRefFile, []byte("1234567890abcdef\n"), 0644)

	if c := getGitCommit(headFile); c != "1234567" {
		t.Errorf("expected '1234567', got %q", c)
	}

	// 4. From direct SHA file
	directFile := filepath.Join(tempDir, "direct")
	_ = os.WriteFile(directFile, []byte("fedcba9876543210\n"), 0644)
	if c := getGitCommit(directFile); c != "fedcba9" {
		t.Errorf("expected 'fedcba9', got %q", c)
	}

	// 5. Fallback to latest
	if c := getGitCommit(filepath.Join(tempDir, "nonexistent")); c != "latest" {
		t.Errorf("expected 'latest', got %q", c)
	}
}

func TestNewDashboardConfigFromLookup_Custom(t *testing.T) {
	envMap := map[string]string{
		"PORT":                         "9090",
		"BRAIN_URL":                    "http://brain-custom:9090",
		"GITHUB_REPO":                  "custom/repo",
		"GITHUB_PAT":                   "custom_pat",
		"GITHUB_PERSONAL_ACCESS_TOKEN": "fallback_token",
		"GITSYNC_URL":                  "http://gitsync-custom:9090",
		"AERIAL_CONFIG_PATH":           "/custom/config.yaml",
		"GIT_COMMIT":                   "0123456789",
	}
	lookup := func(k string) string { return envMap[k] }

	cfg := NewDashboardConfigFromLookup(lookup)
	if cfg.Port != "9090" {
		t.Errorf("expected port 9090, got %q", cfg.Port)
	}
	if cfg.BrainURL != "http://brain-custom:9090" {
		t.Errorf("expected brain URL http://brain-custom:9090, got %q", cfg.BrainURL)
	}
	if cfg.GHRepo != "custom/repo" {
		t.Errorf("expected repo custom/repo, got %q", cfg.GHRepo)
	}
	if cfg.GHToken != "custom_pat" {
		t.Errorf("expected pat custom_pat, got %q", cfg.GHToken)
	}
	if cfg.GitSyncURL != "http://gitsync-custom:9090" {
		t.Errorf("expected gitsync URL http://gitsync-custom:9090, got %q", cfg.GitSyncURL)
	}
	if cfg.ConfigPath != "/custom/config.yaml" {
		t.Errorf("expected config path /custom/config.yaml, got %q", cfg.ConfigPath)
	}
	if cfg.GitCommit != "0123456" {
		t.Errorf("expected git commit 0123456, got %q", cfg.GitCommit)
	}
}

func TestNewDashboardConfigFromLookup_Defaults(t *testing.T) {
	// 1. Nil lookup
	cfgNil := NewDashboardConfigFromLookup(nil)
	if cfgNil.Port != "8080" {
		t.Errorf("expected default port 8080, got %q", cfgNil.Port)
	}
	if cfgNil.BrainURL != "http://brain:8080" {
		t.Errorf("expected default brain URL http://brain:8080, got %q", cfgNil.BrainURL)
	}
	if cfgNil.GHRepo != "azylman/aerial" {
		t.Errorf("expected default repo azylman/aerial, got %q", cfgNil.GHRepo)
	}
	if cfgNil.GHToken != "" {
		t.Errorf("expected empty default token, got %q", cfgNil.GHToken)
	}
	if cfgNil.GitSyncURL != "http://gitsync:8080" {
		t.Errorf("expected default gitsync URL http://gitsync:8080, got %q", cfgNil.GitSyncURL)
	}
	if cfgNil.ConfigPath != "/share/aerial-config/config.yaml" {
		t.Errorf("expected default config path /share/aerial-config/config.yaml, got %q", cfgNil.ConfigPath)
	}
	if cfgNil.GitCommit != "latest" {
		t.Errorf("expected default commit latest, got %q", cfgNil.GitCommit)
	}

	// 2. Fallback token lookup
	envMap := map[string]string{
		"GITHUB_PERSONAL_ACCESS_TOKEN": "fallback_token_123",
	}
	cfgFallback := NewDashboardConfigFromLookup(func(k string) string { return envMap[k] })
	if cfgFallback.GHToken != "fallback_token_123" {
		t.Errorf("expected fallback token, got %q", cfgFallback.GHToken)
	}
}

func TestNewDashboardConfigFromEnv_Smoke(t *testing.T) {
	cfg := NewDashboardConfigFromEnv()
	if cfg.Port == "" {
		t.Errorf("expected non-empty Port from NewDashboardConfigFromEnv")
	}
	if cfg.BrainURL == "" {
		t.Errorf("expected non-empty BrainURL from NewDashboardConfigFromEnv")
	}
	if cfg.GHRepo == "" {
		t.Errorf("expected non-empty GHRepo from NewDashboardConfigFromEnv")
	}
	if cfg.GitSyncURL == "" {
		t.Errorf("expected non-empty GitSyncURL from NewDashboardConfigFromEnv")
	}
	if cfg.ConfigPath == "" {
		t.Errorf("expected non-empty ConfigPath from NewDashboardConfigFromEnv")
	}
	if cfg.GitCommit == "" {
		t.Errorf("expected non-empty GitCommit from NewDashboardConfigFromEnv")
	}
}

func TestGitHubPoller_FullLifecycle(t *testing.T) {
	runID := int64(987654)
	mockGH := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/actions/runs/") && strings.HasSuffix(r.URL.Path, "/jobs"):
			if r.Header.Get("If-None-Match") == "job-etag-1" {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", "job-etag-1")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(GitHubJobsResponse{
				TotalCount: 1,
				Jobs: []GitHubJob{
					{
						ID:         111,
						RunID:      runID,
						Name:       "build-backend (brain)",
						Status:     "completed",
						Conclusion: "success",
						StartedAt:  time.Now().Add(-5 * time.Minute),
					},
				},
			})
		case strings.HasSuffix(r.URL.Path, "/actions/runs"):
			if r.Header.Get("If-None-Match") == "runs-etag-1" {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", "runs-etag-1")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(GitHubRunsResponse{
				TotalCount: 1,
				WorkflowRuns: []GitHubRun{
					{
						ID:         runID,
						Name:       "CI",
						HeadBranch: "main",
						HeadSHA:    "abcdef012345",
						Status:     "in_progress",
						Conclusion: "",
						HTMLURL:    "https://github.com/azylman/aerial/actions/runs/987654",
						CreatedAt:  time.Now().Add(-10 * time.Minute),
						UpdatedAt:  time.Now(),
						HeadCommit: &struct {
							Message   string    `json:"message"`
							Timestamp time.Time `json:"timestamp"`
						}{
							Message:   "feat: add feature\n\nDetailed commit message with token ghp_secret",
							Timestamp: time.Now(),
						},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockGH.Close()

	poller := NewGitHubPoller("azylman/aerial", "token123")
	poller.apiBaseURL = mockGH.URL
	poller.pollInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initial poll
	hasActive := poller.pollOnce(ctx)
	if !hasActive {
		t.Errorf("expected hasActive=true for in_progress run")
	}

	runs, jobs := poller.GetSnapshot()
	if len(runs) != 1 || runs[0].ID != runID {
		t.Fatalf("expected 1 run with ID %d, got %+v", runID, runs)
	}
	if len(jobs[runID]) != 1 {
		t.Fatalf("expected 1 job for run %d, got %+v", runID, jobs)
	}

	// 304 Not Modified poll
	hasActive2 := poller.pollOnce(ctx)
	if !hasActive2 {
		t.Errorf("expected hasActive=true on 304 for in_progress run")
	}

	// Nil poller GetSnapshot check
	var nilPoller *GitHubPoller
	nilRuns, nilJobs := nilPoller.GetSnapshot()
	if nilRuns != nil || nilJobs != nil {
		t.Errorf("expected nil from nilPoller.GetSnapshot")
	}

	// Empty repo Start check
	emptyPoller := NewGitHubPoller("", "")
	emptyPoller.Start(ctx)

	// Network error pollOnce check
	badPoller := NewGitHubPoller("azylman/aerial", "")
	badPoller.apiBaseURL = "http://127.0.0.1:54321"
	if badPoller.pollOnce(ctx) {
		t.Errorf("expected false on bad poller network error")
	}
	badPoller.fetchJobsForRun(ctx, 123)

	// Start background loop and test stopCh
	poller.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	close(poller.stopCh)
	time.Sleep(10 * time.Millisecond)
}

func TestFactsSchedulesRuns_ErrorBranches(t *testing.T) {
	// 1. Facts Handler - 405 Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/api/facts", nil)
	rrPost := httptest.NewRecorder()
	handlerFacts := factsHandler("http://127.0.0.1:54321")
	handlerFacts(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /api/facts, got %d", rrPost.Code)
	}

	// Facts Handler - Upstream 500 error pass-through
	mockErrBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"db failure"}`))
	}))
	defer mockErrBrain.Close()

	rrErr := httptest.NewRecorder()
	reqGet := httptest.NewRequest(http.MethodGet, "/api/facts", nil)
	handlerFactsErr := factsHandler(mockErrBrain.URL)
	handlerFactsErr(rrErr, reqGet)
	if rrErr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on upstream 500, got %d", rrErr.Code)
	}

	// Facts Handler - Bad Gateway on invalid json
	mockBadJSONBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not-valid-json`))
	}))
	defer mockBadJSONBrain.Close()

	rrBadJSON := httptest.NewRecorder()
	handlerFactsBadJSON := factsHandler(mockBadJSONBrain.URL)
	handlerFactsBadJSON(rrBadJSON, reqGet)
	if rrBadJSON.Code != http.StatusBadGateway {
		t.Errorf("expected 502 on invalid upstream JSON, got %d", rrBadJSON.Code)
	}

	// 2. Schedules Handler - 405 Method Not Allowed
	handlerSchedules := schedulesHandler(mockErrBrain.URL)
	rrSchedPost := httptest.NewRecorder()
	handlerSchedules(rrSchedPost, reqPost)
	if rrSchedPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /api/schedules, got %d", rrSchedPost.Code)
	}

	// Schedules Handler - Upstream 500 error pass-through
	rrSchedErr := httptest.NewRecorder()
	handlerSchedules(rrSchedErr, reqGet)
	if rrSchedErr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on schedules upstream 500, got %d", rrSchedErr.Code)
	}

	// Schedules Handler - Bad Gateway on invalid json
	rrSchedBadJSON := httptest.NewRecorder()
	handlerSchedBadJSON := schedulesHandler(mockBadJSONBrain.URL)
	handlerSchedBadJSON(rrSchedBadJSON, reqGet)
	if rrSchedBadJSON.Code != http.StatusBadGateway {
		t.Errorf("expected 502 on schedules bad JSON, got %d", rrSchedBadJSON.Code)
	}

	// 3. Schedule Runs Handler - 405 Method Not Allowed
	handlerRuns := scheduleRunsHandler(mockErrBrain.URL)
	rrRunsPost := httptest.NewRecorder()
	handlerRuns(rrRunsPost, reqPost)
	if rrRunsPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /api/schedules/runs, got %d", rrRunsPost.Code)
	}

	// Schedule Runs Handler - Upstream 500 error pass-through
	rrRunsErr := httptest.NewRecorder()
	handlerRuns(rrRunsErr, reqGet)
	if rrRunsErr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on schedule runs upstream 500, got %d", rrRunsErr.Code)
	}

	// Schedule Runs Handler - Bad Gateway on invalid json
	rrRunsBadJSON := httptest.NewRecorder()
	handlerRunsBadJSON := scheduleRunsHandler(mockBadJSONBrain.URL)
	handlerRunsBadJSON(rrRunsBadJSON, reqGet)
	if rrRunsBadJSON.Code != http.StatusBadGateway {
		t.Errorf("expected 502 on schedule runs bad JSON, got %d", rrRunsBadJSON.Code)
	}
}

func TestDashboardServerLifecycle(t *testing.T) {
	// RunDashboardServer with ephemeral port and cancellation
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- RunDashboardServer(ctx, DashboardConfig{
			Port:      "0",
			BrainURL:  "http://127.0.0.1:54321",
			GHRepo:    "",
			GitCommit: "testcommit",
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("RunDashboardServer returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunDashboardServer did not shut down in time")
	}

	// RunDashboardServer bad port error
	errBadPort := RunDashboardServer(context.Background(), DashboardConfig{
		Port: "bad-port-string",
	})
	if errBadPort == nil {
		t.Errorf("expected error on bad port in RunDashboardServer")
	}
}

func TestFactsSchedulesRuns_SuccessParams(t *testing.T) {
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/facts":
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
				Facts:  nil, // tests data.Facts == nil -> []
				Total:  0,
				Limit:  10,
				Offset: 5,
				Status: "ok",
			})
		case "/schedules":
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Crons:    nil, // tests data.Crons == nil -> []
				OneShots: nil, // tests data.OneShots == nil -> []
				Status:   "ok",
			})
		case "/schedules/runs":
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Runs:   nil, // tests data.Runs == nil -> []
				Total:  0,
				Limit:  10,
				Offset: 5,
				Status: "ok",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockBrain.Close()

	// 1. Facts with all query parameters
	longQuery := strings.Repeat("searchterm", 10)
	reqFacts := httptest.NewRequest(http.MethodGet, "/api/facts?limit=10&offset=5&category=user&q="+longQuery, nil)
	rrFacts := httptest.NewRecorder()
	handlerFacts := factsHandler(mockBrain.URL)
	handlerFacts(rrFacts, reqFacts)
	if rrFacts.Code != http.StatusOK {
		t.Errorf("expected 200 OK on facts with query params, got %d", rrFacts.Code)
	}

	// 2. Schedules with active=true, active=false, search, limit, offset
	reqSched := httptest.NewRequest(http.MethodGet, "/api/schedules?limit=5&offset=2&active=true&q=weather", nil)
	rrSched := httptest.NewRecorder()
	handlerSched := schedulesHandler(mockBrain.URL)
	handlerSched(rrSched, reqSched)
	if rrSched.Code != http.StatusOK {
		t.Errorf("expected 200 OK on schedules active=true, got %d", rrSched.Code)
	}

	reqSchedFalse := httptest.NewRequest(http.MethodGet, "/api/schedules?active=false", nil)
	rrSchedFalse := httptest.NewRecorder()
	handlerSched(rrSchedFalse, reqSchedFalse)
	if rrSchedFalse.Code != http.StatusOK {
		t.Errorf("expected 200 OK on schedules active=false, got %d", rrSchedFalse.Code)
	}

	// 3. Schedule runs with status, schedule_id, limit, offset
	reqRuns := httptest.NewRequest(http.MethodGet, "/api/schedules/runs?limit=10&offset=5&status=SUCCESS&schedule_id=42", nil)
	rrRuns := httptest.NewRecorder()
	handlerRuns := scheduleRunsHandler(mockBrain.URL)
	handlerRuns(rrRuns, reqRuns)
	if rrRuns.Code != http.StatusOK {
		t.Errorf("expected 200 OK on runs with params, got %d", rrRuns.Code)
	}

	// 4. Offline degradation for all 3 handlers
	offlineURL := "http://127.0.0.1:54321"
	rrOfflineFacts := httptest.NewRecorder()
	factsHandler(offlineURL)(rrOfflineFacts, reqFacts)
	if rrOfflineFacts.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on offline brain facts, got %d", rrOfflineFacts.Code)
	}

	rrOfflineSched := httptest.NewRecorder()
	schedulesHandler(offlineURL)(rrOfflineSched, reqSched)
	if rrOfflineSched.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on offline brain schedules, got %d", rrOfflineSched.Code)
	}

	rrOfflineRuns := httptest.NewRecorder()
	scheduleRunsHandler(offlineURL)(rrOfflineRuns, reqRuns)
	if rrOfflineRuns.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 on offline brain runs, got %d", rrOfflineRuns.Code)
	}
}

func TestSetupDashboardMux_StatusWiring(t *testing.T) {
	tempDir := t.TempDir()
	cfgFile := filepath.Join(tempDir, "config.yaml")
	cfgContent := `
dashboard:
  quick_launch_links:
    - name: "TESTLINK"
      url: "https://test.zylman.com"
`
	if err := os.WriteFile(cfgFile, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}

	gitsyncMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GitSyncStatusResponse{
			Status:        "synced",
			MaxLagSeconds: 0,
			LastSyncTime:  time.Now().UTC(),
		})
	}))
	defer gitsyncMock.Close()

	brainMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/queue/active" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tasks": []}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer brainMock.Close()

	cfg := DashboardConfig{
		Port:       "8080",
		BrainURL:   brainMock.URL,
		GitSyncURL: gitsyncMock.URL,
		ConfigPath: cfgFile,
		GitCommit:  "wirecommit",
	}

	handler := SetupDashboardMux(cfg, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/status, got %d", rr.Code)
	}

	var resp ClusterResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}

	if resp.GitSync.Status != "synced" {
		t.Errorf("expected GitSync.Status 'synced', got %q", resp.GitSync.Status)
	}

	foundLink := false
	for _, l := range resp.QuickLaunchLinks {
		if l.Name == "TESTLINK" && l.URL == "https://test.zylman.com" {
			foundLink = true
			break
		}
	}
	if !foundLink {
		t.Errorf("expected QuickLaunchLinks to contain TESTLINK, got %+v", resp.QuickLaunchLinks)
	}
}

func TestRunDashboardServer_Extended(t *testing.T) {
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
		GHRepo:     "azylman/aerial",
		GHToken:    "dummy",
		APIBaseURL: ghMock.URL,
		GitCommit:  "testsha",
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunDashboardServer(ctx, cfg)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("RunDashboardServer unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("RunDashboardServer shutdown timed out")
	}

	// Test invalid port failure
	errFatal := RunDashboardServer(context.Background(), DashboardConfig{Port: "-1"})
	if errFatal == nil {
		t.Errorf("expected fatal error on invalid port -1")
	}
}

func TestGitHubPoller_AdaptiveAndStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ghMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := GitHubRunsResponse{
			WorkflowRuns: []GitHubRun{
				{
					ID:        100,
					Status:    "in_progress",
					UpdatedAt: time.Now().UTC(),
					HeadCommit: &struct {
						Message   string    `json:"message"`
						Timestamp time.Time `json:"timestamp"`
					}{
						Message: strings.Repeat("Very long commit message that exceeds seventy-two runes in length so it gets truncated", 2),
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ghMock.Close()

	p := NewGitHubPoller("azylman/aerial", "token")
	p.apiBaseURL = ghMock.URL
	p.pollInterval = 5 * time.Millisecond

	// Seed stale cached jobs to verify pruning
	p.cachedJobs[999] = []GitHubJob{{ID: 1}}
	p.jobsETagMap[999] = "stale-etag"

	p.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	close(p.stopCh)

	p.mu.RLock()
	defer p.mu.RUnlock()
	if _, exists := p.cachedJobs[999]; exists {
		t.Errorf("expected stale cachedJobs to be pruned")
	}
	if _, exists := p.jobsETagMap[999]; exists {
		t.Errorf("expected stale jobsETagMap to be pruned")
	}
}

func TestFetchDockerClusterState_Extended(t *testing.T) {
	origClient := dockerSocketClient
	defer func() { dockerSocketClient = origClient }()

	// 1. Success with various container states
	dockerSocketClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				containers := []DockerContainerJSON{
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
						Created: time.Now().UTC().Add(time.Hour).Unix(), // future time -> uptime 0
					},
					{
						Names:   []string{"/aerial-db"},
						State:   "exited",
						Status:  "Exited (1)",
						Created: time.Now().UTC().Add(-time.Hour).Unix(),
					},
				}
				data, _ := json.Marshal(containers)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(string(data))),
					Header:     make(http.Header),
				}, nil
			},
		},
	}

	services, _, err := fetchDockerClusterState(context.Background())
	if err != nil {
		t.Fatalf("fetchDockerClusterState failed: %v", err)
	}
	if len(services) != 4 {
		t.Errorf("expected 4 services, got %d", len(services))
	}

	// 2. HTTP 500 error
	dockerSocketClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("server err")),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	_, _, err500 := fetchDockerClusterState(context.Background())
	if err500 == nil {
		t.Errorf("expected error on HTTP 500")
	}

	// 3. Bad JSON
	dockerSocketClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("{invalid json")),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	_, _, errJSON := fetchDockerClusterState(context.Background())
	if errJSON == nil {
		t.Errorf("expected error on bad JSON")
	}

	// 4. Client error
	dockerSocketClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return nil, errors.New("socket closed")
			},
		},
	}
	_, _, errNet := fetchDockerClusterState(context.Background())
	if errNet == nil {
		t.Errorf("expected error on client failure")
	}
}

func TestFetchActiveTasksFromBrain_Extended(t *testing.T) {
	origClient := brainHTTPClient
	defer func() { brainHTTPClient = origClient }()

	// 1. Empty brainURL
	tasks, err := fetchActiveTasksFromBrain(context.Background(), "")
	if err != nil || len(tasks) != 0 {
		t.Errorf("expected empty tasks for empty brainURL")
	}

	// 2. HTTP 500 error
	brainHTTPClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("error")),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	_, err500 := fetchActiveTasksFromBrain(context.Background(), "http://brain")
	if err500 == nil {
		t.Errorf("expected error on HTTP 500")
	}

	// 3. Bad JSON
	brainHTTPClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("bad json")),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	_, errJSON := fetchActiveTasksFromBrain(context.Background(), "http://brain")
	if errJSON == nil {
		t.Errorf("expected error on bad JSON")
	}

	// 4. Pending task and empty summary fallback
	brainHTTPClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				resp := `{"status":"ok","total":1,"tasks":[{"id":"t1","status":"PENDING","prompt":"do something","summary":"","created_at":"2026-08-30T12:00:00Z"}]}`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(resp)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	tasks, err = fetchActiveTasksFromBrain(context.Background(), "http://brain")
	if err != nil || len(tasks) != 1 || tasks[0].Summary != "do something" {
		t.Errorf("expected task summary to fallback to prompt, got %+v", tasks)
	}
}

func TestStatusHandler_DegradedAndMethodNotAllowed(t *testing.T) {
	handler := statusHandler("", "", "", "")

	// 1. Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/api/status", nil)
	rrPost := httptest.NewRecorder()
	handler.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 on POST /api/status, got %d", rrPost.Code)
	}

	// 2. Degraded cluster status when container is unhealthy
	origClient := dockerSocketClient
	defer func() { dockerSocketClient = origClient }()

	dockerSocketClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				containers := []DockerContainerJSON{
					{
						Names:   []string{"/aerial-brain"},
						State:   "running",
						Status:  "Up 10m (unhealthy)",
						Created: time.Now().UTC().Unix(),
					},
				}
				data, _ := json.Marshal(containers)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(string(data))),
					Header:     make(http.Header),
				}, nil
			},
		},
	}

	reqGet := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	rrGet := httptest.NewRecorder()
	handler.ServeHTTP(rrGet, reqGet)
	if rrGet.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET /api/status, got %d", rrGet.Code)
	}

	var resp ClusterResponse
	_ = json.NewDecoder(rrGet.Body).Decode(&resp)
	if resp.ClusterStatus != "degraded" {
		t.Errorf("expected clusterStatus 'degraded', got %q", resp.ClusterStatus)
	}
}

func TestFactsHandler_Extended(t *testing.T) {
	// 1. Method not allowed
	h := factsHandler("http://brain:8080")
	reqPost := httptest.NewRequest(http.MethodPost, "/api/facts", nil)
	rrPost := httptest.NewRecorder()
	h.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 from POST /api/facts, got %d", rrPost.Code)
	}

	// 2. Invalid upstream URL (control char)
	hBadURL := factsHandler("http://brain:\x7f8080")
	reqGet := httptest.NewRequest(http.MethodGet, "/api/facts", nil)
	rrBadURL := httptest.NewRecorder()
	hBadURL.ServeHTTP(rrBadURL, reqGet)
	if rrBadURL.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 from invalid upstream URL, got %d", rrBadURL.Code)
	}

	// 3. Search query truncated > 64 runes
	origClient := brainHTTPClient
	defer func() { brainHTTPClient = origClient }()

	var capturedReq *http.Request
	brainHTTPClient = &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				capturedReq = req
				resp := `{"status":"ok","total":0,"limit":10,"offset":5,"facts":[]}`
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(resp)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}

	reqSearch := httptest.NewRequest(http.MethodGet, "/api/facts?q="+strings.Repeat("a", 100)+"&limit=10&offset=5&category=user", nil)
	rrSearch := httptest.NewRecorder()
	h.ServeHTTP(rrSearch, reqSearch)

	if rrSearch.Code != http.StatusOK {
		t.Errorf("expected 200 OK from mocked factsHandler, got %d", rrSearch.Code)
	}
	if capturedReq == nil {
		t.Fatal("expected capturedReq to not be nil")
	}
	gotQ := capturedReq.URL.Query().Get("q")
	if gotQ != strings.Repeat("a", 64) {
		t.Errorf("expected q truncated to 64 runes, got len %d: %q", len(gotQ), gotQ)
	}
	if capturedReq.URL.Query().Get("limit") != "10" {
		t.Errorf("expected limit=10, got %q", capturedReq.URL.Query().Get("limit"))
	}
	if capturedReq.URL.Query().Get("offset") != "5" {
		t.Errorf("expected offset=5, got %q", capturedReq.URL.Query().Get("offset"))
	}
	if capturedReq.URL.Query().Get("category") != "user" {
		t.Errorf("expected category=user, got %q", capturedReq.URL.Query().Get("category"))
	}
}

func TestSchedulesHandlers_Extended(t *testing.T) {
	// 1. schedulesHandler: Method not allowed & invalid URL
	hSched := schedulesHandler("http://brain:8080")
	reqPost := httptest.NewRequest(http.MethodPost, "/api/schedules", nil)
	rrPost := httptest.NewRecorder()
	hSched.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 from POST /api/schedules, got %d", rrPost.Code)
	}

	hSchedBad := schedulesHandler("http://brain:\x7f8080")
	rrBad := httptest.NewRecorder()
	hSchedBad.ServeHTTP(rrBad, httptest.NewRequest(http.MethodGet, "/api/schedules", nil))
	if rrBad.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 from invalid schedules URL")
	}

	// 2. scheduleRunsHandler: Method not allowed & invalid URL
	hRuns := scheduleRunsHandler("http://brain:8080")
	rrRunsPost := httptest.NewRecorder()
	hRuns.ServeHTTP(rrRunsPost, reqPost)
	if rrRunsPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 from POST /api/schedules/runs, got %d", rrRunsPost.Code)
	}

	hRunsBad := scheduleRunsHandler("http://brain:\x7f8080")
	rrRunsBad := httptest.NewRecorder()
	hRunsBad.ServeHTTP(rrRunsBad, httptest.NewRequest(http.MethodGet, "/api/schedules/runs", nil))
	if rrRunsBad.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 from invalid schedule runs URL")
	}
}

func TestNewAssetRegistry_FallbackVersions(t *testing.T) {
	emptyDir := t.TempDir()
	reg, err := NewAssetRegistry(os.DirFS(emptyDir), "dev")
	if err != nil {
		t.Fatalf("NewAssetRegistry failed on empty dir: %v", err)
	}
	if reg == nil {
		t.Errorf("expected non-nil asset registry")
	}
}

type roundTripperFunc struct {
	fn func(req *http.Request) (*http.Response, error)
}

func (r *roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.fn(req)
}

func TestSanitizeEnvVars_NoEquals(t *testing.T) {
	input := []string{"VALID=1", "INVALID_NO_EQUALS", "ANOTHER=2"}
	got := SanitizeEnvVars(input)
	for _, env := range got {
		if strings.HasPrefix(env, "INVALID_NO_EQUALS") {
			t.Errorf("expected INVALID_NO_EQUALS to be skipped")
		}
	}
	if len(got) != 2 {
		t.Errorf("expected 2 sanitized vars, got %d", len(got))
	}
}

func TestExtractServiceNameFromJobName_NoMatch(t *testing.T) {
	if got := extractServiceNameFromJobName("completely-unrelated-job"); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestParseMatrixJobChips_EdgeCases(t *testing.T) {
	jobs := []GitHubJob{
		{
			Name:       "build / brain",
			Status:     "completed",
			Conclusion: "failure",
		},
		{
			Name:       "build / brain",
			Status:     "completed",
			Conclusion: "success",
		},
		{
			Name:       "build / dashboard",
			Status:     "completed",
			Conclusion: "skipped",
		},
	}
	chips := parseMatrixJobChips(jobs)
	if len(chips) != 2 {
		t.Fatalf("expected 2 chips, got %d", len(chips))
	}
	if chips[0].Status != "failed" {
		t.Errorf("expected failure conclusion to result in 'failed', got %s", chips[0].Status)
	}
	if chips[1].Status != "pending" {
		t.Errorf("expected skipped conclusion to result in 'pending', got %s", chips[1].Status)
	}
}

func TestIsCoreAerialContainer_Filters(t *testing.T) {
	cAgentsview := DockerContainerJSON{
		Image: "ghcr.io/azylman/agentsview:latest",
	}
	if isCoreAerialContainer(cAgentsview) {
		t.Errorf("expected agentsview to NOT be recognized as core aerial container")
	}

	cForeign := DockerContainerJSON{
		Image: "other/image",
		Labels: map[string]string{
			"org.opencontainers.image.source": "https://github.com/external/project",
		},
	}
	if isCoreAerialContainer(cForeign) {
		t.Errorf("expected foreign container to not be core aerial container")
	}
}

func TestBuildContainerChips_EdgeCases(t *testing.T) {
	now := time.Now().UTC()
	containers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(1 * time.Hour).Unix(),
			Labels:  map[string]string{"com.docker.compose.service": "brain"},
		},
		{
			ID:      "c2",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Unix(),
			Labels:  map[string]string{"com.docker.compose.service": "brain"},
		},
	}
	chips := buildContainerChips(containers)
	if len(chips) != 1 {
		t.Fatalf("expected 1 deduplicated chip, got %d", len(chips))
	}
	if chips[0].Duration != "0s" {
		t.Errorf("expected Duration '0s' for future created time, got %s", chips[0].Duration)
	}
}

func TestMergeClusterDeployments_EmptyAndFuture(t *testing.T) {
	if deps := mergeClusterDeployments(nil, nil, nil, "commit123"); len(deps) != 0 {
		t.Errorf("expected empty deployments for no aerial containers, got %d", len(deps))
	}

	now := time.Now().UTC()
	futureContainers := []DockerContainerJSON{
		{
			ID:      "c1",
			Names:   []string{"/aerial-brain"},
			State:   "running",
			Created: now.Add(2 * time.Hour).Unix(),
			Labels:  map[string]string{"com.docker.compose.project": "aerial", "com.docker.compose.service": "brain"},
		},
	}
	runs := []GitHubRun{
		{
			ID:         123,
			Status:     "completed",
			Conclusion: "success",
			HeadSHA:    "sha1234567890",
			CreatedAt:  now.Add(-10 * time.Minute),
			HeadCommit: nil,
		},
	}
	deps := mergeClusterDeployments(futureContainers, runs, nil, "")
	if len(deps) == 0 {
		t.Fatalf("expected at least 1 deployment")
	}
}

func TestGetMimeType_Extended(t *testing.T) {
	if got := getMimeType("test.woff2"); got != "font/woff2" {
		t.Errorf("expected font/woff2, got %s", got)
	}
	if got := getMimeType("test.svg"); got != "image/svg+xml" {
		t.Errorf("expected image/svg+xml, got %s", got)
	}
}

func TestGitHubPoller_CoverageBoost(t *testing.T) {
	p := NewGitHubPoller("azylman/aerial", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = p.pollOnce(ctx)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == "test-etag" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", "test-etag")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(GitHubJobsResponse{Jobs: []GitHubJob{}})
	}))
	defer ts.Close()

	p2 := NewGitHubPoller("azylman/aerial", "token")
	p2.apiBaseURL = ts.URL
	p2.jobsETagMap[100] = "test-etag"
	p2.fetchJobsForRun(context.Background(), 100)

	p2.fetchJobsForRun(ctx, 100)
}
