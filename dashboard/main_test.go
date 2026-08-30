package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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


