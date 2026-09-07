package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

func TestHandlePromptValidation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		DB: database,
	})
	pool.Start()
	defer pool.Stop()

	handler := handlePrompt(database, pool)

	// Test GET method not allowed
	req := httptest.NewRequest(http.MethodGet, "/prompt", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405 MethodNotAllowed, got %d", w.Code)
	}

	// Test invalid empty prompt payload
	emptyPayload, _ := json.Marshal(map[string]string{"prompt": ""})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(emptyPayload))
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 BadRequest for empty prompt, got %d", w.Code)
	}

	// Test invalid JSON payload
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader([]byte("{invalid-json")))
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 BadRequest for invalid JSON, got %d", w.Code)
	}

	// Test valid prompt payload accepted
	validPayload, _ := json.Marshal(map[string]string{
		"prompt":          "Test valid prompt",
		"conversation_id": "test-conv-1",
		"message_id":      "msg-123",
	})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(validPayload))
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Expected status 202 StatusAccepted, got %d", w.Code)
	}

	// Verify message persisted to DB
	msg, err := db.GetMessage(database, "msg-123")
	if err != nil || msg == nil {
		t.Fatalf("Failed to retrieve persisted message: %v", err)
	}
	if msg.ThreadID != "test-conv-1" || msg.Content != "Test valid prompt" {
		t.Errorf("Unexpected message fields in DB: %+v", msg)
	}
}

func TestHandleTranscripts(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	handler := handleTranscripts(database)

	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d", w.Code)
	}
}

func TestFormatCronDescription(t *testing.T) {
	tests := []struct {
		expr     string
		expected string
	}{
		{"0 9 * * 1-5", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * *", "Every day at 09:00"},
		{"*/15 * * * *", "Every 15 minutes"},
		{"0 */2 * * *", "Every 2 hours"},
		{"0 0 * * *", "Every day at 00:00"},
		{"30 8 * * 0", "Every Sunday at 08:30"},
		{"0 12 1 * *", "1st of every month at 12:00"},
		{"* * * * *", "Every minute"},
		{"@daily", "Every day at 00:00"},
		{"@hourly", "Every hour"},
	}

	for _, tt := range tests {
		got := FormatCronDescription(tt.expr)
		if got != tt.expected {
			t.Errorf("FormatCronDescription(%q) = %q, want %q", tt.expr, got, tt.expected)
		}
	}
}

func TestSanitizeString(t *testing.T) {
	tests := []struct {
		input       string
		mustNotHave string
		mustHave    string
	}{
		{
			input:       "Execute prompt with token ghp_1234567890abcdefABCDEF123456",
			mustNotHave: "ghp_1234567890abcdefABCDEF123456",
			mustHave:    "[REDACTED]",
		},
		{
			input:       "Error: failed with github_pat_11ABCD1234_abcdef5678",
			mustNotHave: "github_pat_11ABCD1234_abcdef5678",
			mustHave:    "[REDACTED]",
		},
		{
			input:       "Clean prompt with no tokens to redact",
			mustNotHave: "[REDACTED]",
			mustHave:    "Clean prompt with no tokens to redact",
		},
	}

	for _, tt := range tests {
		got := SanitizeString(tt.input)
		if tt.mustNotHave != "" && strings.Contains(got, tt.mustNotHave) {
			t.Errorf("SanitizeString(%q) leaked token %q: %s", tt.input, tt.mustNotHave, got)
		}
		if tt.mustHave != "" && !strings.Contains(got, tt.mustHave) {
			t.Errorf("SanitizeString(%q) = %q, expected to contain %q", tt.input, got, tt.mustHave)
		}
	}
}

func TestSchedulesEndpoints(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()

	// 1. Seed cron schedule
	cron1 := db.CronSchedule{
		ID:          "cron-1",
		TargetID:    "chan-123",
		TitlePrefix: "Morning Brief",
		CronExpr:    "0 9 * * 1-5",
		Prompt:      "Check status with secret token ghp_secretToken1234567890",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   now.Add(1 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := db.CreateCronSchedule(database, cron1); err != nil {
		t.Fatalf("CreateCronSchedule failed: %v", err)
	}

	// 2. Seed one-shot schedule
	once1 := db.OneShotSchedule{
		ID:        "once-1",
		ThreadID:  "thread-456",
		Prompt:    "Reminder with secret key github_pat_1234567890abcdef",
		RunAt:     now.Add(30 * time.Minute),
		CreatedAt: now,
	}
	if err := db.CreateOneShotSchedule(database, once1); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	// 3. Seed schedule runs
	runCompleted := db.ScheduleRun{
		ID:           "run-1",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		MessageID:    "msg-1",
		TargetID:     "chan-123",
		ThreadID:     "th-1",
		Title:        "Morning Brief",
		Prompt:       "Check status with secret token ghp_secretToken1234567890",
		Status:       "completed",
		StartedAt:    now.Add(-2 * time.Hour),
		DurationMs:   4500,
	}
	if err := db.CreateScheduleRun(database, runCompleted); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	runFailed := db.ScheduleRun{
		ID:           "run-2",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		MessageID:    "msg-2",
		TargetID:     "chan-123",
		ThreadID:     "th-2",
		Title:        "Morning Brief",
		Prompt:       "Run routine",
		Status:       "failed",
		StartedAt:    now.Add(-1 * time.Hour),
		DurationMs:   1200,
		Error:        "Failed auth with token ghp_errorToken99999999",
	}
	if err := db.CreateScheduleRun(database, runFailed); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	// Test GET /schedules
	schedulesHandler := handleSchedules(database)
	req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w := httptest.NewRecorder()
	schedulesHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK from /schedules, got %d: %s", w.Code, w.Body.String())
	}

	var schedResp SchedulesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &schedResp); err != nil {
		t.Fatalf("Failed to parse /schedules response JSON: %v", err)
	}

	if schedResp.Status != "ok" {
		t.Errorf("Expected status ok, got %s", schedResp.Status)
	}
	if schedResp.Summary.TotalActive != 2 || schedResp.Summary.CronCount != 1 || schedResp.Summary.OneShotCount != 1 {
		t.Errorf("Unexpected summary metrics: %+v", schedResp.Summary)
	}
	if len(schedResp.Crons) != 1 {
		t.Fatalf("Expected 1 cron schedule, got %d", len(schedResp.Crons))
	}
	cronItem := schedResp.Crons[0]
	if cronItem.ID != "cron-1" || cronItem.ChannelID != "chan-123" {
		t.Errorf("Unexpected cron item fields: %+v", cronItem)
	}
	if cronItem.CronDescription != "Weekdays (Mon–Fri) at 09:00" {
		t.Errorf("Expected cron_description 'Weekdays (Mon–Fri) at 09:00', got %q", cronItem.CronDescription)
	}
	if strings.Contains(cronItem.Prompt, "ghp_secretToken1234567890") {
		t.Errorf("Prompt token not redacted in /schedules: %s", cronItem.Prompt)
	}
	if !strings.Contains(cronItem.Prompt, "[REDACTED]") {
		t.Errorf("Expected [REDACTED] in sanitized prompt, got %s", cronItem.Prompt)
	}

	if len(schedResp.OneShots) != 1 {
		t.Fatalf("Expected 1 one-shot schedule, got %d", len(schedResp.OneShots))
	}
	oneShotItem := schedResp.OneShots[0]
	if oneShotItem.ID != "once-1" || oneShotItem.ThreadID != "thread-456" {
		t.Errorf("Unexpected one-shot item fields: %+v", oneShotItem)
	}
	if strings.Contains(oneShotItem.Prompt, "github_pat_1234567890abcdef") {
		t.Errorf("Prompt token not redacted in one_shots: %s", oneShotItem.Prompt)
	}

	// Test Caching: Adding another schedule directly in DB should not change cached output immediately
	cron2 := db.CronSchedule{
		ID:          "cron-2",
		TargetID:    "chan-999",
		TitlePrefix: "Nightly",
		CronExpr:    "0 0 * * *",
		Prompt:      "Nightly check",
		NextRunAt:   now.Add(12 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	_ = db.CreateCronSchedule(database, cron2)

	wCached := httptest.NewRecorder()
	schedulesHandler(wCached, req)
	var cachedResp SchedulesResponse
	_ = json.Unmarshal(wCached.Body.Bytes(), &cachedResp)
	if len(cachedResp.Crons) != 1 {
		t.Errorf("Expected cached 1 cron schedule within 5s TTL, got %d", len(cachedResp.Crons))
	}

	// Test GET /schedules/runs
	runsHandler := handleScheduleRuns(database)
	reqRuns := httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	wRuns := httptest.NewRecorder()
	runsHandler(wRuns, reqRuns)

	if wRuns.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK from /schedules/runs, got %d: %s", wRuns.Code, wRuns.Body.String())
	}

	var runsResp ScheduleRunsResponse
	if err := json.Unmarshal(wRuns.Body.Bytes(), &runsResp); err != nil {
		t.Fatalf("Failed to parse /schedules/runs response JSON: %v", err)
	}

	if runsResp.Status != "ok" || runsResp.Total != 2 || len(runsResp.Runs) != 2 {
		t.Errorf("Unexpected runs response: total=%d, len=%d", runsResp.Total, len(runsResp.Runs))
	}

	// Check token sanitization in error and prompt
	for _, r := range runsResp.Runs {
		if strings.Contains(r.Prompt, "ghp_secretToken1234567890") {
			t.Errorf("Run prompt token leaked: %s", r.Prompt)
		}
		if strings.Contains(r.Error, "ghp_errorToken99999999") {
			t.Errorf("Run error token leaked: %s", r.Error)
		}
	}

	// Test filtering /schedules/runs?status=failed
	reqFiltered := httptest.NewRequest(http.MethodGet, "/schedules/runs?status=failed", nil)
	wFiltered := httptest.NewRecorder()
	runsHandler(wFiltered, reqFiltered)

	var filteredResp ScheduleRunsResponse
	_ = json.Unmarshal(wFiltered.Body.Bytes(), &filteredResp)
	if filteredResp.Total != 1 || len(filteredResp.Runs) != 1 || filteredResp.Runs[0].Status != "failed" {
		t.Errorf("Unexpected filtered runs: %+v", filteredResp)
	}

	// Test pagination /schedules/runs?limit=1&offset=0
	reqPaginated := httptest.NewRequest(http.MethodGet, "/schedules/runs?limit=1&offset=0", nil)
	wPaginated := httptest.NewRecorder()
	runsHandler(wPaginated, reqPaginated)

	var paginatedResp ScheduleRunsResponse
	_ = json.Unmarshal(wPaginated.Body.Bytes(), &paginatedResp)
	if paginatedResp.Total != 2 || len(paginatedResp.Runs) != 1 || paginatedResp.Limit != 1 {
		t.Errorf("Unexpected paginated runs: %+v", paginatedResp)
	}

	// Test MethodNotAllowed for POST
	reqPost := httptest.NewRequest(http.MethodPost, "/schedules", nil)
	wPost := httptest.NewRecorder()
	schedulesHandler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed for POST /schedules, got %d", wPost.Code)
	}

	reqPostRuns := httptest.NewRequest(http.MethodPost, "/schedules/runs", nil)
	wPostRuns := httptest.NewRecorder()
	runsHandler(wPostRuns, reqPostRuns)
	if wPostRuns.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed for POST /schedules/runs, got %d", wPostRuns.Code)
	}
}

func TestHandleTasks(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	longPrompt := strings.Repeat("A", 600) + " ghp_123456789012345678901234567890123456"

	_ = db.InsertMessage(database, db.Message{
		ID:         "msg-test-task",
		ThreadID:   "thread-1",
		AuthorID:   "user-1",
		AuthorName: "Tester with token ghp_999999999999999999999999999999999999",
		Content:    "Secret token ghp_123456789012345678901234567890123456 in prompt " + longPrompt,
		Status:     db.StatusProcessing,
		CreatedAt:  now,
		UpdatedAt:  now,
	})

	handler := handleTasks(database)

	// 1. Test GET request returns 200 OK
	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode json: %v", err)
	}

	if resp.Status != "ok" || resp.Total != 1 || len(resp.Tasks) != 1 {
		t.Fatalf("unexpected response payload: %+v", resp)
	}

	// 2. Verify token was redacted in prompt, summary, and author name
	if strings.Contains(resp.Tasks[0].Prompt, "ghp_123456") {
		t.Errorf("token was not redacted from prompt: %s", resp.Tasks[0].Prompt)
	}
	if strings.Contains(resp.Tasks[0].Summary, "ghp_123456") {
		t.Errorf("token was not redacted from summary: %s", resp.Tasks[0].Summary)
	}
	if resp.Tasks[0].Summary == "" {
		t.Errorf("expected summary to be populated, got empty")
	}
	if strings.Contains(resp.Tasks[0].AuthorName, "ghp_999999") {
		t.Errorf("token was not redacted from author name: %s", resp.Tasks[0].AuthorName)
	}

	// Verify truncation to <= 503 runes (500 + "...")
	promptRunes := []rune(resp.Tasks[0].Prompt)
	if len(promptRunes) > 503 || !strings.HasSuffix(resp.Tasks[0].Prompt, "...") {
		t.Errorf("prompt was not properly truncated to 500 chars with ellipsis: len=%d, text=%s", len(promptRunes), resp.Tasks[0].Prompt)
	}

	// 3. Test 1s TTL Caching: inserting another active message should not appear immediately
	_ = db.InsertMessage(database, db.Message{
		ID:         "msg-test-task-2",
		ThreadID:   "thread-2",
		AuthorID:   "user-2",
		AuthorName: "Tester 2",
		Content:    "Second prompt",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(time.Second),
		UpdatedAt:  now.Add(time.Second),
	})

	rrCached := httptest.NewRecorder()
	handler.ServeHTTP(rrCached, req)
	if rrCached.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from cached call, got %d", rrCached.Code)
	}

	var cachedResp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	if err := json.NewDecoder(rrCached.Body).Decode(&cachedResp); err != nil {
		t.Fatalf("failed to decode cached json: %v", err)
	}
	if cachedResp.Total != 1 || len(cachedResp.Tasks) != 1 {
		t.Errorf("expected 1 cached task within 1s TTL, got %d", cachedResp.Total)
	}

	// 4. Test Method Not Allowed (e.g. POST)
	reqPost := httptest.NewRequest(http.MethodPost, "/tasks", nil)
	rrPost := httptest.NewRecorder()
	handler.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rrPost.Code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())

	// Record sample metrics to instantiate metric vectors
	metrics.RecordTurnCompleted("success", "discord", "Gemini 3.8 Flash (Low)", 2*time.Second)
	metrics.RecordClassifierRun("success", "Gemini 3.8 Flash (Low)", 500*time.Millisecond, 0.95, "wake")
	metrics.DiscordEventsTotal.WithLabelValues("ready").Inc()
	metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
	metrics.SchedulerExecutionsTotal.WithLabelValues("cron", "enqueued").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK from /metrics, got %d", rec.Code)
	}

	body := rec.Body.String()
	requiredSubstrings := []string{
		"aerial_brain_turns_total",
		"aerial_brain_turn_duration_seconds",
		"aerial_brain_active_workers",
		"aerial_brain_queue_depth",
		"aerial_brain_classifier_duration_seconds",
		"aerial_brain_classifier_decisions_total",
		"aerial_brain_classifier_confidence_score",
		"aerial_brain_discord_events_total",
		"aerial_brain_discord_messages_processed_total",
		"aerial_brain_scheduler_executions_total",
		"aerial_brain_build_info",
	}

	for _, sub := range requiredSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected /metrics output to contain %q, body:\n%s", sub, body)
		}
	}
}



func TestNormalizeRoute(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/prompt", "/prompt"},
		{"/transcripts", "/transcripts"},
		{"/transcripts/abc-123", "/transcripts"},
		{"/tasks", "/tasks"},
		{"/tasks/456", "/tasks"},
		{"/facts", "/facts"},
		{"/facts/789", "/facts"},
		{"/schedules", "/schedules"},
		{"/schedules/runs", "/schedules/runs"},
		{"/internal/reload", "/internal/reload"},
		{"/health", "/health"},
		{"/metrics", "/metrics"},
		{"/unknown/path", "unmatched"},
		{"/random", "unmatched"},
	}

	for _, tt := range tests {
		got := normalizeRoute(tt.input)
		if got != tt.expected {
			t.Errorf("normalizeRoute(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}

func TestMetricsMiddleware(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/unknown", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	handler := metricsMiddleware(mux)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	req404 := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	rec404 := httptest.NewRecorder()
	handler.ServeHTTP(rec404, req404)

	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec404.Code)
	}
}

type mockFlusherRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (m *mockFlusherRecorder) Flush() {
	m.flushed = true
}

func TestStatusRecorder_Flush(t *testing.T) {
	// With flusher
	rec := &mockFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: rec, statusCode: http.StatusOK}
	sr.Flush()
	if !rec.flushed {
		t.Errorf("Expected Flush() to be forwarded to underlying flusher")
	}

	// Without flusher
	plainRec := httptest.NewRecorder()
	srPlain := &statusRecorder{ResponseWriter: struct{ http.ResponseWriter }{plainRec}, statusCode: http.StatusOK}
	srPlain.Flush() // should not panic
}

func TestHandleFacts_Comprehensive(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Seed some facts
	_, _ = db.InsertFact(database, "system", "Fact 1", 1.0, "t1", nil)
	_, _ = db.InsertFact(database, "user_preference", "Fact 2", 1.0, "t2", nil)

	handler := handleFacts(database)

	// 1. Method not allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/facts", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. Valid GET without filters
	req := httptest.NewRequest(http.MethodGet, "/facts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /facts, got %d: %s", w.Code, w.Body.String())
	}

	// 3. Valid GET with query params: category, limit, offset, q (including long string > 64 runes)
	longQuery := strings.Repeat("a", 100)
	reqFiltered := httptest.NewRequest(http.MethodGet, "/facts?category=system&limit=10&offset=0&q="+longQuery, nil)
	wFiltered := httptest.NewRecorder()
	handler(wFiltered, reqFiltered)
	if wFiltered.Code != http.StatusOK {
		t.Errorf("Expected 200 OK with filters, got %d: %s", wFiltered.Code, wFiltered.Body.String())
	}

	// 4. Closed DB error branch
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerErr := handleFacts(closedDB)
	wErr := httptest.NewRecorder()
	handlerErr(wErr, req)
	if wErr.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 InternalServerError on closed DB, got %d", wErr.Code)
	}
}

func TestOrdinal_AllCases(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{1, "1st"},
		{2, "2nd"},
		{3, "3rd"},
		{4, "4th"},
		{11, "11th"},
		{12, "12th"},
		{13, "13th"},
		{14, "14th"},
		{21, "21st"},
		{22, "22nd"},
		{23, "23rd"},
		{24, "24th"},
		{31, "31st"},
	}

	for _, tc := range cases {
		got := ordinal(tc.n)
		if got != tc.want {
			t.Errorf("ordinal(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestFormatCronDescription_AllVariations(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"", ""},
		{"   ", ""},
		{"@yearly", "Every year on Jan 1st at 00:00"},
		{"@annually", "Every year on Jan 1st at 00:00"},
		{"@monthly", "1st of every month at 00:00"},
		{"@weekly", "Every week on Sunday at 00:00"},
		{"@daily", "Every day at 00:00"},
		{"@midnight", "Every day at 00:00"},
		{"@hourly", "Every hour"},
		{"invalid cron string", "invalid cron string"},
		{"* * * * *", "Every minute"},
		{"*/10 * * * *", "Every 10 minutes"},
		{"0 */4 * * *", "Every 4 hours"},
		{"0 9 * * *", "Every day at 09:00"},
		{"0 9 * * 1-5", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * MON-FRI", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * 0,6", "Weekends (Sat–Sun) at 09:00"},
		{"0 9 * * SAT,SUN", "Weekends (Sat–Sun) at 09:00"},
		{"0 9 * * 2", "Every Tuesday at 09:00"},
		{"0 9 * * 1,3,5", "Mon, Wed, Fri at 09:00"},
		{"0 12 1 * *", "1st of every month at 12:00"},
		{"0 12 22 * *", "22nd of every month at 12:00"},
		{"0 0 1 1 *", "Every year on Jan 1st at 00:00"},
		{"0 0 4 7 *", "Every year on Jul 4th at 00:00"},
		{"0 9 * 5 *", "At 09:00 (cron: 0 9 * 5 *)"},
	}

	for _, tc := range cases {
		got := FormatCronDescription(tc.expr)
		if got != tc.want {
			t.Errorf("FormatCronDescription(%q) = %q, want %q", tc.expr, got, tc.want)
		}
	}
}

func TestHandleSchedules_And_Runs_ErrorBranches(t *testing.T) {
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()

	// 1. handleSchedules with closed DB
	schedHandler := handleSchedules(closedDB)
	req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w := httptest.NewRecorder()
	schedHandler(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleSchedules, got %d", w.Code)
	}

	// 2. handleScheduleRuns with closed DB
	runsHandler := handleScheduleRuns(closedDB)
	reqRuns := httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	wRuns := httptest.NewRecorder()
	runsHandler(wRuns, reqRuns)
	if wRuns.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleScheduleRuns, got %d", wRuns.Code)
	}

	// 3. handleTasks with closed DB
	tasksHandler := handleTasks(closedDB)
	reqTasks := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	wTasks := httptest.NewRecorder()
	tasksHandler(wTasks, reqTasks)
	if wTasks.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleTasks, got %d", wTasks.Code)
	}
}

func TestSetupBrainMux_And_Endpoints(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	reloaded := false
	reloadFn := func(source string) {
		reloaded = true
	}

	mux := SetupBrainMux(database, nil, reloadFn)

	// Test /health
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	wHealth := httptest.NewRecorder()
	mux.ServeHTTP(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Errorf("Expected 200 from /health, got %d", wHealth.Code)
	}

	// Test /internal/reload (GET -> 405, POST -> 200)
	reqReloadGet := httptest.NewRequest(http.MethodGet, "/internal/reload", nil)
	wReloadGet := httptest.NewRecorder()
	mux.ServeHTTP(wReloadGet, reqReloadGet)
	if wReloadGet.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 from GET /internal/reload, got %d", wReloadGet.Code)
	}

	reqReloadPost := httptest.NewRequest(http.MethodPost, "/internal/reload", nil)
	wReloadPost := httptest.NewRecorder()
	mux.ServeHTTP(wReloadPost, reqReloadPost)
	if wReloadPost.Code != http.StatusOK || !reloaded {
		t.Errorf("Expected 200 and reloaded=true from POST /internal/reload, got %d, %t", wReloadPost.Code, reloaded)
	}
}

func TestInitializeBrainEnvironment_And_Config(t *testing.T) {
	cfg := config.NewFromData(&config.ConfigData{Model: "gemini-2.5-flash"})
	bCfg := NewBrainConfigFromEnv(cfg)
	if bCfg.Model != "gemini-2.5-flash" {
		t.Errorf("Unexpected model in BrainConfig: %s", bCfg.Model)
	}

	InitializeBrainEnvironment("", "gemini-2.5-flash", "test prompt")
	InitializeBrainEnvironment("test-api-key", "gemini-2.5-flash", "test prompt")

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{})
	reloadFn := CreateReloadConfigFunc(pool, "test-key", "test prompt", nil)
	reloadFn("UnitTest")
}

func TestRunBrainApp_Lifecycle(t *testing.T) {
	t.Setenv("AGY_BIN", "/bin/true")
	t.Setenv("PORT", "0")

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_brain.db")

	bCfg := BrainConfig{
		Port:         "0",
		AgyBin:       "/bin/true",
		Model:        "gemini-2.5-flash",
		APIKey:       "",
		SystemPrompt: "test",
		DBPath:       dbPath,
		DiscordToken: "",
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)

	err := RunBrainApp(ctx, bCfg)
	if err != nil && err != http.ErrServerClosed {
		t.Errorf("Unexpected error running Brain app: %v", err)
	}

	// Test invalid DB path fails cleanly
	badCfg := BrainConfig{
		DBPath: "/nonexistent/invalid/dir/db.sqlite",
	}
	badCtx, badCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer badCancel()
	_ = RunBrainApp(badCtx, badCfg)
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated network read error")
}

func TestHandlePrompt_ErrorBranches(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	// pool is not started so it won't trigger real background worker executions

	handler := handlePrompt(database, pool)

	// 1. Read body error
	reqErr := httptest.NewRequest(http.MethodPost, "/prompt", errReader{})
	wErr := httptest.NewRecorder()
	handler(wErr, reqErr)
	if wErr.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for read error, got %d", wErr.Code)
	}

	// 2. Whitespace only prompt
	reqWs := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"   "}`))
	wWs := httptest.NewRecorder()
	handler(wWs, reqWs)
	if wWs.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for whitespace prompt, got %d", wWs.Code)
	}

	// 3. Auto-generated UUIDs when omitted
	reqAuto := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"hello world"}`))
	wAuto := httptest.NewRecorder()
	handler(wAuto, reqAuto)
	if wAuto.Code != http.StatusAccepted {
		t.Errorf("Expected 202 for auto prompt, got %d", wAuto.Code)
	}

	// 4. Closed DB non-fatal handling
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handlePrompt(closedDB, pool)
	reqClosed := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"hello fallback"}`))
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusAccepted {
		t.Errorf("Expected 202 even with closed DB insert failure, got %d", wClosed.Code)
	}

	// 5. Nil pool handling
	handlerNilPool := handlePrompt(database, nil)
	reqNilPool := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"hello nil pool"}`))
	wNilPool := httptest.NewRecorder()
	handlerNilPool(wNilPool, reqNilPool)
	if wNilPool.Code != http.StatusAccepted {
		t.Errorf("Expected 202 for nil pool, got %d", wNilPool.Code)
	}
}

func TestHandleTranscripts_ErrorAndRawBranches(t *testing.T) {
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	// Seed an external conversation ID mapping
	_ = db.SaveConversationMapping(database, "thread-discord-100", "conv-alpha-100")

	// Create transcript folders
	convDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", "conv-alpha-100", ".system_generated", "logs")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	transcriptContent := `{"step_index": 1, "status": "DONE", "content": "hello"}
{"step_index": 2, "status": "ERROR", "error": "something failed"}
{"step_index": 3, "status": "RUNNING", "error": {"code": 500, "message": "object error"}}
not a valid json line
`
	if err := os.WriteFile(filepath.Join(convDir, "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	handler := handleTranscripts(database)

	// GET transcripts with include_raw=true
	reqGet := httptest.NewRequest(http.MethodGet, "/transcripts?include_raw=true", nil)
	wGet := httptest.NewRecorder()
	handler(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", wGet.Code)
	}
	if !strings.Contains(wGet.Body.String(), "conv-alpha-100") {
		t.Errorf("Expected conv-alpha-100 in response: %s", wGet.Body.String())
	}
}

func TestHandleFacts_TruncationAndErrors(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	handler := handleFacts(database)

	// 1. Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/facts", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. Long query truncation (>64 runes)
	longQuery := strings.Repeat("a", 100)
	reqLong := httptest.NewRequest(http.MethodGet, "/facts?q="+longQuery, nil)
	wLong := httptest.NewRecorder()
	handler(wLong, reqLong)
	if wLong.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", wLong.Code)
	}

	// 3. Closed DB error -> 500
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handleFacts(closedDB)
	reqClosed := httptest.NewRequest(http.MethodGet, "/facts?q=test", nil)
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 InternalServerError for closed DB, got %d", wClosed.Code)
	}
}

func TestHandleSchedules_And_Runs_GranularErrors(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	handlerSched := handleSchedules(database)

	// 1. handleSchedules: Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/schedules", nil)
	wPost := httptest.NewRecorder()
	handlerSched(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. handleSchedules: Closed DB error on summary metrics -> 500
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerSchedClosed := handleSchedules(closedDB)
	reqClosed := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	wClosed := httptest.NewRecorder()
	handlerSchedClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for closed DB, got %d", wClosed.Code)
	}

	// 3. handleScheduleRuns: Method Not Allowed
	handlerRuns := handleScheduleRuns(database)
	reqRunsPost := httptest.NewRequest(http.MethodPost, "/schedules/runs", nil)
	wRunsPost := httptest.NewRecorder()
	handlerRuns(wRunsPost, reqRunsPost)
	if wRunsPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 for runs POST, got %d", wRunsPost.Code)
	}

	// 4. handleScheduleRuns: query parameters & filters
	reqRunsQuery := httptest.NewRequest(http.MethodGet, "/schedules/runs?limit=100&offset=5&schedule_id=sched-1&status=completed", nil)
	wRunsQuery := httptest.NewRecorder()
	handlerRuns(wRunsQuery, reqRunsQuery)
	if wRunsQuery.Code != http.StatusOK {
		t.Errorf("Expected 200 for runs query, got %d", wRunsQuery.Code)
	}

	// 5. handleScheduleRuns: closed DB error -> 500
	handlerRunsClosed := handleScheduleRuns(closedDB)
	reqRunsClosed := httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	wRunsClosed := httptest.NewRecorder()
	handlerRunsClosed(wRunsClosed, reqRunsClosed)
	if wRunsClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for closed DB on runs, got %d", wRunsClosed.Code)
	}
}

func TestHandleTasks_GranularErrors(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	handler := handleTasks(database)

	// 1. Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/tasks", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. Closed DB error -> 500
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handleTasks(closedDB)
	reqClosed := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for closed DB on tasks, got %d", wClosed.Code)
	}
}

func TestInitializeBrainEnvironment_DataSymlink(t *testing.T) {
	tmpHome := t.TempDir()
	t.Setenv("HOME", tmpHome)

	InitializeBrainEnvironment("", "gemini-2.5-flash", "test-prompt")
}

func TestCreateReloadConfigFunc_InvalidYAMLAndAlert(t *testing.T) {
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(yamlPath, []byte("invalid-yaml: [unclosed"), 0644)

	s, _ := discordgo.New("Bot mock-token")
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{})

	reloadFn := CreateReloadConfigFunc(pool, "test-api-key", "test-prompt", s)
	reloadFn("TestReload")
}

func TestRunBrainApp_ServerReadinessAndShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("AGY_BIN", "/bin/true")
	t.Setenv("PORT", "0")

	dbPath := filepath.Join(tmpDir, "app_test.db")
	bCfg := BrainConfig{
		Port:         "0",
		AgyBin:       "/bin/true",
		Model:        "gemini-2.5-flash",
		APIKey:       "",
		SystemPrompt: "test",
		DBPath:       dbPath,
		DiscordToken: "",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- RunBrainApp(ctx, bCfg)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("RunBrainApp returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("RunBrainApp did not shut down within 5 seconds")
	}
}

func TestHandleSchedules_DetailedCoverage(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	// Seed cron and one shot schedules
	_ = db.CreateCronSchedule(database, db.CronSchedule{
		ID:          "cron-1",
		TargetID:    "target-1",
		TitlePrefix: "Daily Backup",
		CronExpr:    "0 0 * * *",
		Prompt:      "Run backup",
		Timezone:    "America/Los_Angeles",
		Enabled:     true,
		CreatedAt:   time.Now().UTC(),
	})
	_ = db.CreateOneShotSchedule(database, db.OneShotSchedule{
		ID:        "oneshot-1",
		ThreadID:  "target-1",
		Prompt:    "Run once",
		RunAt:     time.Now().UTC().Add(1 * time.Hour),
		CreatedAt: time.Now().UTC(),
	})

	handler := handleSchedules(database)

	// 1. Initial request (cache miss)
	req1 := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w1 := httptest.NewRecorder()
	handler(w1, req1)
	if w1.Code != http.StatusOK {
		t.Errorf("Expected 200 on initial fetch, got %d", w1.Code)
	}
	if !strings.Contains(w1.Body.String(), "Daily Backup") {
		t.Errorf("Expected Daily Backup in response: %s", w1.Body.String())
	}

	// 2. Immediate request (cache hit under RLock)
	req2 := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w2 := httptest.NewRecorder()
	handler(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200 on cached fetch, got %d", w2.Code)
	}

	// 3. Rename prompt column on cron_schedules to trigger GetAllCronSchedules error
	dbCronErr, _ := db.InitDB(":memory:")
	defer dbCronErr.Close()
	_, _ = dbCronErr.Exec("ALTER TABLE cron_schedules RENAME COLUMN prompt TO old_prompt;")
	handlerCronErr := handleSchedules(dbCronErr)
	reqCronErr := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	wCronErr := httptest.NewRecorder()
	handlerCronErr(wCronErr, reqCronErr)
	if wCronErr.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for cron query error, got %d", wCronErr.Code)
	}

	// 4. Rename prompt column on one_shot_schedules to trigger GetAllOneShotSchedules error
	dbOneShotErr, _ := db.InitDB(":memory:")
	defer dbOneShotErr.Close()
	_, _ = dbOneShotErr.Exec("ALTER TABLE one_shot_schedules RENAME COLUMN prompt TO old_prompt;")
	handlerOneShotErr := handleSchedules(dbOneShotErr)
	reqOneShotErr := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	wOneShotErr := httptest.NewRecorder()
	handlerOneShotErr(wOneShotErr, reqOneShotErr)
	if wOneShotErr.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for one shot query error, got %d", wOneShotErr.Code)
	}
}

func TestHandleTasks_DetailedCoverage(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	longPrompt := strings.Repeat("a", 600)
	_ = db.InsertMessage(database, db.Message{
		ID:         "task-msg-1",
		ThreadID:   "thread-1",
		AuthorName: "Alice",
		Content:    longPrompt,
		Status:     db.StatusProcessing,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	})

	handler := handleTasks(database)

	// 1. Initial request (cache miss)
	req1 := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w1 := httptest.NewRecorder()
	handler(w1, req1)
	if w1.Code != http.StatusOK {
		t.Errorf("Expected 200 on initial tasks fetch, got %d", w1.Code)
	}
	if !strings.Contains(w1.Body.String(), "...") {
		t.Errorf("Expected prompt truncation in response: %s", w1.Body.String())
	}

	// 2. Immediate request (cache hit under RLock)
	req2 := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w2 := httptest.NewRecorder()
	handler(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200 on cached tasks fetch, got %d", w2.Code)
	}
}

func TestCreateReloadConfigFunc_SuccessCoverage(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	oldPaths := config.ConfigSearchPaths
	defer func() { config.ConfigSearchPaths = oldPaths }()

	// 1. Invalid YAML config path
	invalidYaml := filepath.Join(tmpDir, "bad_config.yaml")
	_ = os.WriteFile(invalidYaml, []byte("invalid_yaml: [unclosed"), 0644)
	config.ConfigSearchPaths = []string{invalidYaml}

	s, _ := discordgo.New("Bot mock-token")
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{})
	reloadFn := CreateReloadConfigFunc(pool, "test-api-key", "test-prompt", s)
	reloadFn("TestReloadFailure")

	// 2. Valid YAML config path
	validYaml := filepath.Join(tmpDir, "good_config.yaml")
	_ = os.WriteFile(validYaml, []byte(`
model: "gemini-2.5-flash"
channels:
  default:
    mode: "channel"
`), 0644)
	config.ConfigSearchPaths = []string{validYaml}
	reloadFn("TestReloadSuccess")
}

func TestRunBrainApp_DetailedOptions(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("PORT", "0")

	// 1. With DiscordToken set
	dbPath := filepath.Join(tmpDir, "app_discord.db")
	bCfg := BrainConfig{
		Port:         "0",
		AgyBin:       "/bin/true",
		Model:        "gemini-2.5-flash",
		APIKey:       "test-key",
		SystemPrompt: "test",
		DBPath:       dbPath,
		DiscordToken: "mock-token",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- RunBrainApp(ctx, bCfg)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errChan:
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("RunBrainApp returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("RunBrainApp did not shut down within 5 seconds")
	}

	// 2. Port listen error
	badPortCfg := BrainConfig{
		Port:         "-1",
		AgyBin:       "/bin/true",
		Model:        "gemini-2.5-flash",
		APIKey:       "",
		SystemPrompt: "test",
		DBPath:       filepath.Join(tmpDir, "app_bad_port.db"),
	}
	ctxBad, cancelBad := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelBad()
	_ = RunBrainApp(ctxBad, badPortCfg)
}

func TestMainFunc(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("PORT", "0")
	dbFile := filepath.Join(tmpDir, "main_test.db")
	t.Setenv("DB_PATH", dbFile)

	validYaml := filepath.Join(tmpDir, "main_config.yaml")
	_ = os.WriteFile(validYaml, []byte(`
model: "gemini-2.5-flash"
channels:
  default:
    mode: "channel"
`), 0644)

	oldPaths := config.ConfigSearchPaths
	config.ConfigSearchPaths = []string{validYaml}
	defer func() { config.ConfigSearchPaths = oldPaths }()

	go func() {
		time.Sleep(100 * time.Millisecond)
		p, err := os.FindProcess(os.Getpid())
		if err == nil {
			_ = p.Signal(os.Interrupt)
		}
	}()

	main()
}

func TestInitializeBrainEnvironment_Complete(t *testing.T) {
	// 1. Success with temporary HOME directory
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	mcpFile := filepath.Join(tmpDir, ".gemini", "antigravity", "mcp_config.json")
	_ = os.MkdirAll(filepath.Dir(mcpFile), 0755)
	_ = os.WriteFile(mcpFile, []byte(`{"test-server":{"command":"echo"}}`), 0644)

	InitializeBrainEnvironment("test-api-key", "gemini-2.5-flash", "test prompt")

	// 2. Call again when directory already exists
	InitializeBrainEnvironment("test-api-key", "gemini-2.5-flash", "test prompt")

	// 3. Error branches with uncreatable path
	t.Setenv("HOME", "/proc/nonexistent")
	InitializeBrainEnvironment("test-api-key", "gemini-2.5-flash", "test prompt")
}

func TestCreateReloadConfigFunc_Complete(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	mcpFile := filepath.Join(tmpDir, ".gemini", "antigravity", "mcp_config.json")
	_ = os.MkdirAll(filepath.Dir(mcpFile), 0755)
	_ = os.WriteFile(mcpFile, []byte(`{"mcp-server":{"command":"echo"}}`), 0644)

	validYaml := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(validYaml, []byte(`
model: "gemini-2.5-flash"
channels:
  default:
    mode: "channel"
`), 0644)

	oldPaths := config.ConfigSearchPaths
	config.ConfigSearchPaths = []string{validYaml}
	defer func() { config.ConfigSearchPaths = oldPaths }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{})
	reloadFn := CreateReloadConfigFunc(pool, "test-api-key", "test system prompt", nil)
	reloadFn("TestSource")

	// Trigger error branches with invalid home
	t.Setenv("HOME", "/proc/read_only_home")
	reloadFn("TestErrorSource")
}

func TestRunBrainApp_ErrorBranches(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	// 1. Invalid DBPath (fails db.InitDB)
	bCfgInvalidDB := BrainConfig{
		Port:   "0",
		DBPath: "/proc/nonexistent/dir/db.sqlite",
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()
	err := RunBrainApp(ctx1, bCfgInvalidDB)
	if err == nil {
		t.Errorf("Expected error from invalid DBPath, got nil")
	}

	// 2. Empty DBPath returns error
	bCfgFallback := BrainConfig{
		Port:   "0",
		DBPath: "",
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	errEmpty := RunBrainApp(ctx2, bCfgFallback)
	if errEmpty == nil {
		t.Errorf("Expected error for empty DBPath, got nil")
	}
}

func TestHandleSchedules_ConcurrentDoubleCheck(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	handler := handleSchedules(database)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
			w := httptest.NewRecorder()
			handler(w, req)
		}()
	}
	wg.Wait()
}

func TestHandleTasks_ConcurrentDoubleCheck(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	handler := handleTasks(database)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
			w := httptest.NewRecorder()
			handler(w, req)
		}()
	}
	wg.Wait()
}

func TestHandleTranscripts_NonDirectoryAndBadHome(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer database.Close()

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	brainDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain")
	_ = os.MkdirAll(brainDir, 0755)
	_ = os.WriteFile(filepath.Join(brainDir, "regular_file.txt"), []byte("not a dir"), 0644)

	handler := handleTranscripts(database)
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w.Code)
	}
}

func TestFormatCronDescription_AdditionalPatterns(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"0 9 * * Mon,Wed,Fri", "Mon, Wed, Fri at 09:00"},
		{"0 9 * * 1,3,5", "Mon, Wed, Fri at 09:00"},
		{"0/15 * * * *", "0/15 * * * *"},
	}
	for _, tc := range cases {
		got := FormatCronDescription(tc.expr)
		if got != tc.want {
			t.Errorf("FormatCronDescription(%q) = %q, want %q", tc.expr, got, tc.want)
		}
	}
}

func TestMainFunc_ConfigError(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("PORT", "0")
	dbFile := filepath.Join(tmpDir, "main_test_err.db")
	t.Setenv("DB_PATH", dbFile)

	invalidYaml := filepath.Join(tmpDir, "bad_config.yaml")
	_ = os.WriteFile(invalidYaml, []byte("bad: [unclosed"), 0644)

	oldPaths := config.ConfigSearchPaths
	config.ConfigSearchPaths = []string{invalidYaml}
	defer func() { config.ConfigSearchPaths = oldPaths }()

	go func() {
		time.Sleep(100 * time.Millisecond)
		p, err := os.FindProcess(os.Getpid())
		if err == nil {
			_ = p.Signal(os.Interrupt)
		}
	}()

	main()
}

func TestMetricsMiddleware_Implicit200(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/implicit", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	handler := metricsMiddleware(mux)
	req := httptest.NewRequest(http.MethodGet, "/implicit", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}
}

func TestHandleTranscripts_DBErrorBranch(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)

	convID := "test-conv-err-1"
	tDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	_ = os.MkdirAll(tDir, 0755)
	_ = os.WriteFile(filepath.Join(tDir, "transcript.jsonl"), []byte(`{"status":"DONE"}`+"\n"), 0644)

	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()

	handler := handleTranscripts(closedDB)
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w.Code)
	}
}

func TestRunBrainApp_PureConfig(t *testing.T) {
	cfg := config.NewFromData(&config.ConfigData{
		DatabaseURL:  filepath.Join(t.TempDir(), "brain_test.db"),
		Port:         "0",
		Model:        "test-model",
		Timezone:     "UTC",
		SystemPrompt: "test prompt",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Immediate cancellation to test lifecycle shutdown

	err := RunBrainApp(ctx, cfg)
	if err != nil && err != http.ErrServerClosed {
		t.Errorf("expected clean shutdown, got %v", err)
	}
}





